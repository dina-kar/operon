//! The host side of a function call: the [`Host`] that answers `ctx.db`
//! calls, the [`Invocation`] that makes a call deterministic, and the
//! adapter from [`LiveTxn`] (design §20 §6.2; R1 plan Task 13 semantics 1).
//!
//! Every `ctx.db` call becomes one host call, `(op, args)`, answered by the
//! same code as the matching system function (`_system:get`, …), so reads
//! land in the transaction's read set exactly as they do for the built-ins.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use operon_live::catalog::{BY_CREATION_TIME, BY_ID, CREATION_TIME_FIELD, ID_FIELD};
use operon_live::query::object_args;
use operon_live::{LiveError, LiveTxn, LiveValue, system};
use operon_tikv::{Tikv, TimestampExt};
use sha2::{Digest, Sha256};

/// Answers a function's `ctx.db` calls.
pub trait Host: Send {
    /// Runs host call `op` (`get`, `query`, `insert`, `patch`, `replace`,
    /// `delete`) with `args`.
    fn call<'a>(
        &'a mut self,
        op: &'a str,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>>;
}

/// What makes one call deterministic (§20 §6.2): `Date.now()` and the seed
/// of `Math.random`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    /// What `Date.now()` returns: the start timestamp's physical ms.
    pub now_ms: u64,
    /// The `Math.random` seed.
    pub seed: [u8; 32],
}

impl Invocation {
    /// The invocation of a call at start timestamp `ts_version` (whose
    /// physical time is `now_ms`) for request `request_id`: the seed is
    /// SHA-256 of both, so the same timestamp and request give the same
    /// random sequence.
    pub fn new(ts_version: u64, now_ms: u64, request_id: &str) -> Self {
        let mut h = Sha256::new();
        h.update(b"loam.random\0");
        h.update(ts_version.to_be_bytes());
        h.update(request_id.as_bytes());
        Invocation {
            now_ms,
            seed: h.finalize().into(),
        }
    }

    /// The invocation of a call in `txn`.
    pub fn of(txn: &LiveTxn<'_>) -> Self {
        let ts = txn.start_ts();
        Invocation::new(ts.version(), Tikv::physical_ms(&ts), txn.request_id())
    }
}

/// Whether a host call's error ends the call however the function handles
/// it: storage errors (the runner must see them to rerun, R1 plan row
/// T10-14) and exceeded limits (the attempt may already have written past
/// them).
pub fn is_fatal(e: &LiveError) -> bool {
    matches!(e, LiveError::Txn(_) | LiveError::LimitExceeded { .. })
}

/// [`Host`] over a [`LiveTxn`].
pub(crate) struct TxnHost<'t, 'a> {
    pub(crate) txn: &'t mut LiveTxn<'a>,
}

impl Host for TxnHost<'_, '_> {
    fn call<'b>(
        &'b mut self,
        op: &'b str,
        args: LiveValue,
    ) -> BoxFuture<'b, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let name = match op {
                "get" => system::GET,
                "insert" => system::INSERT,
                "patch" => system::PATCH,
                "replace" => system::REPLACE,
                "delete" => system::DELETE,
                "query" => return query(self.txn, args).await,
                other => {
                    return Err(LiveError::InvalidArgument(format!(
                        "no host call '{other}'"
                    )));
                }
            };
            let f = system::lookup(name)
                .ok_or_else(|| LiveError::Internal(format!("no system function {name}")))?;
            f.call(self.txn, args).await
        })
    }
}

/// `ctx.db.query(…)`: checks the range's field names against the index,
/// then reads as `_system:query` does.
async fn query(txn: &mut LiveTxn<'_>, args: LiveValue) -> Result<LiveValue, LiveError> {
    const WHAT: &str = "db.query";
    let mut args = object_args(
        WHAT,
        args,
        &[
            "table",
            "index",
            "eqFields",
            "eq",
            "rangeField",
            "lower",
            "upper",
            "order",
            "limit",
        ],
    )?;
    let eq_fields = strings(WHAT, args.remove("eqFields"))?;
    let range_field = match args.remove("rangeField") {
        None => None,
        Some(LiveValue::Str(s)) => Some(s),
        Some(other) => {
            return Err(LiveError::InvalidArgument(format!(
                "{WHAT}: a field name is a string, not {}",
                other.type_name()
            )));
        }
    };
    let Some(LiveValue::Str(table_name)) = args.get("table").cloned() else {
        return Err(LiveError::InvalidArgument(format!(
            "{WHAT}: the table is a string"
        )));
    };
    let index = match args.get("index") {
        Some(LiveValue::Str(s)) => s.clone(),
        _ => BY_CREATION_TIME.to_string(),
    };
    if let Some(table) = txn.table(&table_name).await? {
        let fields: Vec<String> = match index.as_str() {
            BY_ID => vec![ID_FIELD.to_string()],
            _ => {
                let id = table.index_id(&index).ok_or_else(|| {
                    LiveError::NotFound(format!("index '{index}' of table '{table_name}'"))
                })?;
                let mut fields = table.index_fields(id).unwrap_or_default().to_vec();
                fields.push(CREATION_TIME_FIELD.to_string());
                fields
            }
        };
        let used = eq_fields.iter().chain(range_field.as_ref());
        for (n, field) in used.enumerate() {
            if fields.get(n) != Some(field) {
                return Err(LiveError::InvalidArgument(format!(
                    "{WHAT}: index '{index}' of table '{table_name}' has the fields [{}]; field \
                     {} of the range is '{field}'",
                    fields.join(", "),
                    n + 1
                )));
            }
        }
    }
    let mut out: BTreeMap<String, LiveValue> = args;
    out.retain(|_, v| *v != LiveValue::Null);
    let f = system::lookup(system::QUERY)
        .ok_or_else(|| LiveError::Internal("no system function _system:query".into()))?;
    f.call(txn, LiveValue::Object(out)).await
}

fn strings(what: &str, v: Option<LiveValue>) -> Result<Vec<String>, LiveError> {
    match v {
        None | Some(LiveValue::Null) => Ok(Vec::new()),
        Some(LiveValue::Array(items)) => items
            .into_iter()
            .map(|item| match item {
                LiveValue::Str(s) => Ok(s),
                other => Err(LiveError::InvalidArgument(format!(
                    "{what}: a field name is a string, not {}",
                    other.type_name()
                ))),
            })
            .collect(),
        Some(other) => Err(LiveError::InvalidArgument(format!(
            "{what}: field names are an array, not {}",
            other.type_name()
        ))),
    }
}
