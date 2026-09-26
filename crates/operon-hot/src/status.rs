//! The hot status of a collection (plan M1.3 Task 8 rule 3): what
//! `GET …/collections/{c}` reports under `"hot"`, from the owning node.
//! Every field name is its JSON key.

use std::collections::BTreeMap;

use operon_collection::{live_manifest, vector_column};
use operon_common::meta::{Consistency, HotConfig};
use operon_common::{CollectionId, NamespaceId};
use operon_query::hot::{HotState, HotStatus};
use serde::Serialize;
use ulid::Ulid;

pub use operon_query::hot::HotStateKind;

use crate::TierError;
use crate::build::{effective_hot, promote_lease_key};
use crate::tier::HotTierImpl;

/// Every structure: what a promotion or `warm` makes hot.
const ALL: HotConfig = HotConfig {
    vectors: true,
    text: true,
    fragments: true,
};

/// The full hot status of one collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DetailedHotStatus {
    /// Whether the reporting node runs a hot tier (`--hot`).
    pub enabled: bool,
    /// The catalog configuration (`PUT …/hot`).
    pub config: HotConfig,
    /// `--hot-pin-all` on the reporting node.
    pub pin_all: bool,
    /// A promotion lease is held.
    pub promoted: bool,
    pub owner: OwnerStatus,
    pub vectors: VectorsStatus,
    pub text: TextStatus,
    pub fragments: FragmentsStatus,
}

/// The node that answered, and whether it owns the collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OwnerStatus {
    pub node_id: u64,
    pub local: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VectorsStatus {
    pub state: HotStateKind,
    /// The minimum over the columns; `null` if any is.
    pub source_version: Option<u64>,
    pub over_budget: bool,
    /// By vector name.
    pub columns: BTreeMap<String, ColumnStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ColumnStatus {
    pub state: HotStateKind,
    /// The view's effective source version (Ruling 1).
    pub source_version: Option<u64>,
    /// The loaded artifact's own source version.
    pub artifact_source_version: Option<u64>,
    /// Points in the delta index.
    pub delta_rows: u64,
    /// The last load error, until the artifact loads.
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TextStatus {
    pub state: HotStateKind,
    pub source_version: Option<u64>,
    pub over_budget: bool,
    /// Splits of the live manifest pinned on this node.
    pub pinned_splits: u64,
    /// Splits of the live manifest.
    pub splits: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FragmentsStatus {
    pub state: HotStateKind,
    pub source_version: Option<u64>,
    pub over_budget: bool,
    /// Bytes of the live Lance version read into the range cache.
    pub prefetched_bytes: u64,
    /// Bytes of the live Lance version's files.
    pub bytes: u64,
}

/// The status a node reports when its hot tier is disabled (`enabled: false`, every state `off`).
pub fn disabled_status(config: HotConfig, pin_all: bool, owner: OwnerStatus) -> DetailedHotStatus {
    DetailedHotStatus {
        enabled: false,
        config,
        pin_all,
        promoted: false,
        owner,
        vectors: VectorsStatus {
            state: HotStateKind::Off,
            source_version: None,
            over_budget: false,
            columns: BTreeMap::new(),
        },
        text: TextStatus {
            state: HotStateKind::Off,
            source_version: None,
            over_budget: false,
            pinned_splits: 0,
            splits: 0,
        },
        fragments: FragmentsStatus {
            state: HotStateKind::Off,
            source_version: None,
            over_budget: false,
            prefetched_bytes: 0,
            bytes: 0,
        },
    }
}

/// What a status is built from.
struct Facts {
    config: HotConfig,
    effective: HotConfig,
    promoted: bool,
    /// `None` before the first commit.
    manifest: Option<ManifestFacts>,
}

struct ManifestFacts {
    version: u64,
    lance_version: u64,
    splits: Vec<Ulid>,
    /// `VectorSpec.name` of each dense vector, in schema order.
    vectors: Vec<String>,
}

impl HotTierImpl {
    /// Rule 3: the status of `(ns, cid)` on this node, from `Local` reads of
    /// its catalog configuration, promotion lease and live manifest.
    pub async fn detailed_status(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
    ) -> Result<DetailedHotStatus, TierError> {
        let ctx = self.ctx();
        let meta = &*ctx.meta;
        let config = meta.collection_hot(Consistency::Local, ns, cid).await?;
        let lease = meta
            .lease(Consistency::Local, &promote_lease_key(ns, cid))
            .await?;
        let now_ms = meta.now_ms();
        let promoted = lease.as_ref().is_some_and(|l| l.is_held_at(now_ms));
        let mut effective = effective_hot(config, lease.as_ref(), self.config().pin_all, now_ms);
        if self.is_warm(ns, cid) || self.holds_promotion(ns, cid) {
            effective = effective.or(ALL);
        }
        let collection = meta.collection(Consistency::Local, cid).await?;
        let vectors = collection
            .filter(|c| c.namespace == ns)
            .map(|c| c.schema.vectors.into_iter().map(|v| v.name).collect())
            .unwrap_or_default();
        let manifest = live_manifest(
            meta,
            &ctx.store,
            &ctx.manifests,
            ns,
            cid,
            Consistency::Local,
        )
        .await?
        .map(|(_, manifest)| ManifestFacts {
            version: manifest.version,
            lance_version: manifest.lance_version,
            splits: manifest.splits.iter().map(|split| split.ulid).collect(),
            vectors,
        });
        Ok(self.compose(
            ns,
            cid,
            Facts {
                config,
                effective,
                promoted,
                manifest,
            },
        ))
    }

    /// `HotTier::status`: rule 3 as of the last reconcile pass, without a
    /// metastore read.
    pub(crate) fn pass_status(&self, ns: NamespaceId, cid: CollectionId) -> HotStatus {
        if !self.config().enabled {
            return HotStatus::default();
        }
        let Some(info) = self.last_info(ns, cid) else {
            return HotStatus::default();
        };
        let manifest = self.last_seen(ns, cid).map(|seen| ManifestFacts {
            version: seen.manifest_version,
            lance_version: seen.lance_version,
            splits: seen.splits,
            vectors: seen.vectors,
        });
        let detailed = self.compose(
            ns,
            cid,
            Facts {
                config: info.catalog,
                effective: info.effective,
                promoted: info.promoted,
                manifest,
            },
        );
        HotStatus {
            vectors: HotState {
                state: detailed.vectors.state,
                source_version: detailed.vectors.source_version,
            },
            text: HotState {
                state: detailed.text.state,
                source_version: detailed.text.source_version,
            },
            fragments: HotState {
                state: detailed.fragments.state,
                source_version: detailed.fragments.source_version,
            },
        }
    }

    fn compose(&self, ns: NamespaceId, cid: CollectionId, facts: Facts) -> DetailedHotStatus {
        let over = self.over_budget_of(ns, cid);
        let mut status = disabled_status(
            facts.config,
            self.config().pin_all,
            OwnerStatus {
                node_id: self.node_id(),
                local: true,
            },
        );
        status.enabled = self.config().enabled;
        status.promoted = facts.promoted;
        status.vectors.over_budget = over.vectors;
        status.text.over_budget = over.text;
        if !status.enabled {
            return status;
        }
        let hot = facts.effective;
        let Some(manifest) = facts.manifest else {
            // Nothing committed yet: what is hot is waiting for data.
            for (on, state) in [
                (hot.vectors, &mut status.vectors.state),
                (hot.text, &mut status.text.state),
                (hot.fragments, &mut status.fragments.state),
            ] {
                if on {
                    *state = HotStateKind::Building;
                }
            }
            return status;
        };

        // Vectors: a column is ready iff a view exists for the live version.
        if hot.vectors && !manifest.vectors.is_empty() {
            let mut all_ready = true;
            let mut minimum = Some(u64::MAX);
            for (index, name) in manifest.vectors.iter().enumerate() {
                let column = vector_column(index);
                let (artifact_source, delta_rows, error) = self.column_facts(ns, cid, &column);
                let column_status = match self.column_view(ns, cid, &column, manifest.version) {
                    Some(view) => ColumnStatus {
                        state: HotStateKind::Ready,
                        source_version: Some(operon_query::hot::HotAnn::source_version(&*view)),
                        artifact_source_version: Some(view.artifact().descriptor.source_version),
                        delta_rows: view.delta().appended(),
                        error: None,
                    },
                    None => ColumnStatus {
                        state: HotStateKind::Building,
                        source_version: None,
                        artifact_source_version: artifact_source,
                        delta_rows,
                        error,
                    },
                };
                all_ready &= column_status.state == HotStateKind::Ready;
                minimum = match (minimum, column_status.source_version) {
                    (Some(m), Some(v)) => Some(m.min(v)),
                    _ => None,
                };
                status.vectors.columns.insert(name.clone(), column_status);
            }
            status.vectors.state = match all_ready {
                true => HotStateKind::Ready,
                false => HotStateKind::Building,
            };
            status.vectors.source_version = minimum;
        }

        // Text: ready iff every split of the live manifest is pinned here.
        if hot.text {
            let pinned = self.pinned_count(ns, cid, &manifest.splits);
            status.text.pinned_splits = pinned;
            status.text.splits = manifest.splits.len() as u64;
            if pinned == status.text.splits {
                status.text.state = HotStateKind::Ready;
                status.text.source_version = Some(manifest.version);
            } else {
                status.text.state = HotStateKind::Building;
            }
        }

        // Fragments: ready iff the live Lance version was read completely.
        if hot.fragments {
            status.fragments.state = HotStateKind::Building;
            if let Some((lance_version, pass)) = self.fragment_facts(ns, cid) {
                status.fragments.prefetched_bytes = pass.prefetched;
                status.fragments.bytes = pass.total;
                if lance_version == manifest.lance_version && pass.complete() {
                    status.fragments.state = HotStateKind::Ready;
                    status.fragments.source_version = Some(manifest.version);
                }
            }
        }
        status
    }
}
