//! Native delete-by-filter and patch-by-filter (plan M1.5 Task 9a, D87,
//! overview A46): the filter is evaluated at one pin, and the matches are
//! deleted or patched in atomic batches that follow the pin in primary-key
//! order.
//!
//! What M1 does not promise (rule 5): a key that matched at the pin and
//! changed before its batch is still deleted or patched, and a key that
//! starts matching after the pin is missed. From M2 the filter is re-checked
//! at apply (D89).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use loams_collection::{ConsistencyToken, DocOp, PatchMode, PrimaryKey, SparseVector};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::backlog::Override;
use crate::error::ServiceError;
use crate::ir::{Query, ReadConsistency};
use crate::service::CollectionService;
use crate::types::{OpResult, Projection, SourceFilter, WriteOptions};

/// The most documents one `delete_by_filter` call deletes.
pub const MAX_DELETE_BY_FILTER_ROWS: u64 = 5_000_000;
/// The most documents one `patch_by_filter` call patches.
pub const MAX_PATCH_BY_FILTER_ROWS: u64 = 50_000;
/// Keys per atomic batch (`ServiceConfig::filter_write_batch`'s default).
pub const FILTER_WRITE_BATCH: usize = 1_000;

/// The start of the message of a call refused over its limit (rule 2);
/// [`over_limit`] reads the numbers back.
const OVER_LIMIT: &str = "the filter matches ";

/// A `DocOp::Patch` without its key and without `upsert`: a filter write
/// never creates a document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatchSpec {
    pub mode: PatchMode,
    pub source: Map<String, Value>,
    pub delete_keys: Vec<String>,
    pub vectors: BTreeMap<String, Option<Vec<f32>>>,
    pub sparse_vectors: BTreeMap<String, Option<SparseVector>>,
}

impl PatchSpec {
    /// A `MergeDeep` patch of `source` and nothing else.
    pub fn merge_deep(source: Map<String, Value>) -> Self {
        Self {
            mode: PatchMode::MergeDeep,
            source,
            delete_keys: Vec::new(),
            vectors: BTreeMap::new(),
            sparse_vectors: BTreeMap::new(),
        }
    }

    /// The patch of key `pk`, with no `upsert`.
    pub fn op(&self, pk: PrimaryKey) -> DocOp {
        DocOp::Patch {
            pk,
            mode: self.mode,
            source: self.source.clone(),
            delete_keys: self.delete_keys.clone(),
            vectors: self.vectors.clone(),
            sparse_vectors: self.sparse_vectors.clone(),
            upsert: None,
        }
    }
}

/// Continues a partial filter write at the same snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterWriteCursor {
    pub manifest_version: u64,
    #[serde(with = "crate::json::token")]
    pub token: ConsistencyToken,
    /// The last key written; the next call starts after it.
    #[serde(with = "crate::json::pk")]
    pub after: PrimaryKey,
}

/// The snapshot a filter write evaluated its filter at.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterWritePin {
    pub manifest_version: u64,
    #[serde(with = "crate::json::token")]
    pub token: ConsistencyToken,
}

impl FilterWritePin {
    /// `ReadConsistency::Pinned` at this snapshot.
    pub fn consistency(&self) -> ReadConsistency {
        ReadConsistency::Pinned {
            manifest_version: self.manifest_version,
            token: self.token.clone(),
        }
    }
}

/// The options of one filter write.
#[derive(Clone, Debug, Default)]
pub struct FilterWriteOptions {
    /// `Strong` (default) or `AtLeast(token)`; `Eventual` and `Pinned` are
    /// refused (rule 1).
    pub consistency: ReadConsistency,
    /// Default and ceiling: the kind's limit.
    pub max_rows: Option<u64>,
    /// false: a call matching more than its limit fails before writing.
    pub allow_partial: bool,
    pub cursor: Option<FilterWriteCursor>,
    /// Default `ServiceConfig::filter_write_timeout` (60 s).
    pub deadline: Option<Duration>,
    /// Each batch's backpressure override (M1.3 Task 15).
    pub backpressure: Override,
}

/// The answer to one filter write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterWriteResult {
    /// Documents matching the filter at the pin.
    pub matched: u64,
    /// Documents deleted or patched by this call.
    pub affected: u64,
    /// Keys this call's batches wrote: `affected`, and the keys found
    /// deleted since the pin or (for a patch) already patched. It is what a
    /// caller's own row budget counts (ES `max_docs`).
    #[serde(default)]
    pub written: u64,
    pub batches: u64,
    pub rows_remaining: bool,
    /// `Some` iff `rows_remaining`.
    pub cursor: Option<FilterWriteCursor>,
    /// Covers the pin and every batch of this call.
    #[serde(with = "crate::json::token")]
    pub token: ConsistencyToken,
    /// The snapshot the filter was evaluated at.
    pub pin: FilterWritePin,
    /// Set when the call stopped at its deadline on a batch refused for
    /// backpressure: the refusal's wait (rule 4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// What a filter write does to each matching key.
enum FilterOp<'a> {
    Delete,
    Patch(&'a PatchSpec),
}

impl FilterOp<'_> {
    fn limit(&self) -> u64 {
        match self {
            FilterOp::Delete => MAX_DELETE_BY_FILTER_ROWS,
            FilterOp::Patch(_) => MAX_PATCH_BY_FILTER_ROWS,
        }
    }

    fn op(&self, pk: PrimaryKey) -> DocOp {
        match self {
            FilterOp::Delete => DocOp::Delete(pk),
            FilterOp::Patch(spec) => spec.op(pk),
        }
    }

    fn counts(&self, result: &OpResult) -> bool {
        match self {
            FilterOp::Delete => matches!(result, OpResult::Deleted),
            FilterOp::Patch(_) => matches!(result, OpResult::Updated),
        }
    }
}

/// The error of a call whose filter matches more than its limit (rule 2).
fn over_limit_error(matched: u64, limit: u64) -> ServiceError {
    ServiceError::InvalidArgument(format!(
        "{OVER_LIMIT}{matched} documents, more than the limit of {limit}; \
         set allow_partial and continue with the cursor"
    ))
}

/// `(matched, limit)` of a call refused over its limit, for the native
/// route's `"matched"` and `"limit"` (rule 6); `None` for any other error.
pub fn over_limit(err: &ServiceError) -> Option<(u64, u64)> {
    let ServiceError::InvalidArgument(message) = err else {
        return None;
    };
    let rest = message.strip_prefix(OVER_LIMIT)?;
    let (matched, rest) = rest.split_once(" documents, more than the limit of ")?;
    let (limit, _) = rest.split_once(';')?;
    Some((matched.parse().ok()?, limit.parse().ok()?))
}

impl CollectionService {
    /// Deletes the documents of collection (or single-target alias) `name`
    /// that match `filter` at one pin (plan M1.5 Task 9a rules 1–5).
    pub async fn delete_by_filter(
        &self,
        ns: &str,
        name: &str,
        filter: Query,
        opts: FilterWriteOptions,
    ) -> Result<FilterWriteResult, ServiceError> {
        self.filter_write(ns, name, filter, FilterOp::Delete, opts)
            .await
    }

    /// Patches the documents of collection (or single-target alias) `name`
    /// that match `filter` at one pin with `patch`; a key deleted since the
    /// pin stays deleted (rules 1–5).
    pub async fn patch_by_filter(
        &self,
        ns: &str,
        name: &str,
        filter: Query,
        patch: PatchSpec,
        opts: FilterWriteOptions,
    ) -> Result<FilterWriteResult, ServiceError> {
        self.filter_write(ns, name, filter, FilterOp::Patch(&patch), opts)
            .await
    }

    async fn filter_write(
        &self,
        ns: &str,
        name: &str,
        filter: Query,
        kind: FilterOp<'_>,
        opts: FilterWriteOptions,
    ) -> Result<FilterWriteResult, ServiceError> {
        let started = Instant::now();
        // Rule 1: the consistency.
        if matches!(
            opts.consistency,
            ReadConsistency::Eventual | ReadConsistency::Pinned { .. }
        ) {
            return Err(ServiceError::InvalidArgument(
                "a filter write reads at a fresh pin; continue with its cursor".to_string(),
            ));
        }
        // Rule 2: the limit.
        if opts.max_rows == Some(0) {
            return Err(ServiceError::InvalidArgument(
                "max_rows must be at least 1".to_string(),
            ));
        }
        let limit = opts.max_rows.unwrap_or(u64::MAX).min(kind.limit());
        // Rule 1: the pin, fresh or the cursor's.
        let (pin, mut after, partial) = match opts.cursor {
            Some(cursor) => (
                FilterWritePin {
                    manifest_version: cursor.manifest_version,
                    token: cursor.token,
                },
                Some(cursor.after),
                true,
            ),
            None => {
                let pinned = self.pin(ns, name).await?;
                (
                    FilterWritePin {
                        manifest_version: pinned.manifest_version,
                        token: pinned.token,
                    },
                    None,
                    opts.allow_partial,
                )
            }
        };
        let at = pin.consistency();
        let matched = self
            .count(ns, name, Some(filter.clone()), at.clone())
            .await?;
        if matched > limit && !partial {
            return Err(over_limit_error(matched, limit));
        }
        // Rule 3: the loop.
        let deadline = started + opts.deadline.unwrap_or(self.config.filter_write_timeout);
        let batch = self
            .config
            .filter_write_batch
            .max(1)
            .min(self.config.max_scroll_limit.max(1));
        let select = Projection {
            source: SourceFilter::None,
            vectors: Vec::new(),
            fields: Vec::new(),
        };
        let write_opts = WriteOptions {
            report_existence: true,
            atomic: true,
            backpressure: opts.backpressure,
        };
        let mut token = pin.token.clone();
        let (mut written, mut affected, mut batches) = (0u64, 0u64, 0u64);
        let mut more = true;
        let mut refused = None;
        while written < limit && more {
            // Every call writes at least one batch before its deadline counts.
            if batches > 0 && Instant::now() >= deadline {
                break;
            }
            let page = usize::try_from(limit - written).map_or(batch, |left| left.min(batch));
            let (docs, next) = self
                .scroll(
                    ns,
                    name,
                    Some(filter.clone()),
                    after.clone(),
                    page,
                    &select,
                    at.clone(),
                )
                .await?;
            let Some(last) = docs.last().map(|doc| doc.pk.clone()) else {
                more = false;
                break;
            };
            let ops: Vec<DocOp> = docs.into_iter().map(|doc| kind.op(doc.pk)).collect();
            let n = ops.len() as u64;
            // Rule 4: a refused batch waits and is retried whole.
            let result = loop {
                match self.write(ns, name, ops.clone(), write_opts).await {
                    Ok(result) => break Some(result),
                    Err(ServiceError::ResourceExhausted { retry_after_ms, .. }) => {
                        let pause = Duration::from_millis(retry_after_ms.max(1));
                        if Instant::now() + pause >= deadline {
                            refused = Some(retry_after_ms);
                            break None;
                        }
                        tokio::time::sleep(pause).await;
                    }
                    Err(err) => return Err(err),
                }
            };
            let Some(result) = result else {
                break;
            };
            if let Some(err) = result.results.iter().find_map(|r| match r {
                OpResult::Rejected(err) => Some(err.clone()),
                _ => None,
            }) {
                return Err(err);
            }
            token.merge(&result.token);
            affected += result.results.iter().filter(|r| kind.counts(r)).count() as u64;
            written += n;
            batches += 1;
            after = Some(last);
            more = next.is_some();
        }
        let rows_remaining = more;
        let cursor = match (rows_remaining, after) {
            (false, _) => None,
            (true, Some(after)) => Some(FilterWriteCursor {
                manifest_version: pin.manifest_version,
                token: pin.token.clone(),
                after,
            }),
            // A fresh call refused before its first batch wrote nothing:
            // there is nothing to continue, so it is the refusal.
            (true, None) => {
                let retry_after_ms = refused.unwrap_or_default();
                return Err(ServiceError::ResourceExhausted {
                    message: format!(
                        "a filter write was refused for backpressure before its first batch; retry after {} s",
                        retry_after_ms.div_ceil(1000).max(1)
                    ),
                    retry_after_ms,
                });
            }
        };
        Ok(FilterWriteResult {
            matched,
            affected,
            written,
            batches,
            rows_remaining,
            cursor,
            token,
            pin,
            retry_after_ms: refused,
        })
    }
}
