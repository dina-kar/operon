//! The built-in system functions (R1 plan Task 10): `_system:get`,
//! `_system:query`, `_system:insert`, `_system:patch`, `_system:replace` and
//! `_system:delete`. They run through [`LiveTxn`] like deployed functions,
//! so R1 works end to end before any bundle is deployed.
//!
//! | Function | Kind | Arguments | Result |
//! |---|---|---|---|
//! | `_system:get` | query | `{ id }` | the document, or `null` |
//! | `_system:query` | query | see [`QueryArgs`] | an array of documents |
//! | `_system:insert` | mutation | `{ table, fields }` | the new id |
//! | `_system:patch` | mutation | `{ id, fields }` | `null` |
//! | `_system:replace` | mutation | `{ id, fields }` | `null` |
//! | `_system:delete` | mutation | `{ id }` | `null` |
//!
//! Documents are values as [`doc_value`] builds them.

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::query::{QueryArgs, doc_value, fields_arg, id_arg, object_args, str_arg};
use crate::txn::{FnKind, Function, LiveTxn};
use crate::{LiveError, LiveValue};

/// `_system:get`.
pub const GET: &str = "_system:get";
/// `_system:query`.
pub const QUERY: &str = "_system:query";
/// `_system:insert`.
pub const INSERT: &str = "_system:insert";
/// `_system:patch`.
pub const PATCH: &str = "_system:patch";
/// `_system:replace`.
pub const REPLACE: &str = "_system:replace";
/// `_system:delete`.
pub const DELETE: &str = "_system:delete";

/// Every system function's name.
pub const NAMES: [&str; 6] = [GET, QUERY, INSERT, PATCH, REPLACE, DELETE];

/// One system function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct System {
    name: &'static str,
}

/// The system function `name`, or `None`.
pub fn lookup(name: &str) -> Option<Arc<dyn Function>> {
    NAMES
        .iter()
        .find(|n| **n == name)
        .map(|&name| Arc::new(System { name }) as Arc<dyn Function>)
}

impl Function for System {
    fn name(&self) -> &str {
        self.name
    }

    fn kind(&self) -> FnKind {
        match self.name {
            GET | QUERY => FnKind::Query,
            _ => FnKind::Mutation,
        }
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let name = self.name;
            match name {
                GET => {
                    let mut args = object_args(name, args, &["id"])?;
                    let id = id_arg(name, &mut args, "id")?;
                    Ok(txn
                        .get(id)
                        .await?
                        .map_or(LiveValue::Null, |doc| doc_value(&doc)))
                }
                QUERY => {
                    let args = QueryArgs::parse(name, args)?;
                    let Some(table) = txn.table(&args.table).await? else {
                        return Ok(LiveValue::Array(Vec::new()));
                    };
                    let docs = txn.query(args.range(&table)?).await?;
                    Ok(LiveValue::Array(docs.iter().map(doc_value).collect()))
                }
                INSERT => {
                    let mut args = object_args(name, args, &["table", "fields"])?;
                    let table = str_arg(name, &mut args, "table")?;
                    let fields = fields_arg(name, &mut args, "fields")?;
                    let id = txn.insert(&table, fields).await?;
                    Ok(LiveValue::Str(id.to_string()))
                }
                PATCH | REPLACE => {
                    let mut args = object_args(name, args, &["id", "fields"])?;
                    let id = id_arg(name, &mut args, "id")?;
                    let fields = fields_arg(name, &mut args, "fields")?;
                    if name == PATCH {
                        txn.patch(id, fields).await?;
                    } else {
                        txn.replace(id, fields).await?;
                    }
                    Ok(LiveValue::Null)
                }
                DELETE => {
                    let mut args = object_args(name, args, &["id"])?;
                    let id = id_arg(name, &mut args, "id")?;
                    txn.delete(id).await?;
                    Ok(LiveValue::Null)
                }
                other => Err(LiveError::Internal(format!("no system function {other}"))),
            }
        })
    }
}
