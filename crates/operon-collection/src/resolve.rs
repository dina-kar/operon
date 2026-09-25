//! Latest-wins resolution of one key's ops (plan M1.1 Review Focus 3).

use crate::doc::{DocOp, Document, apply_patch};

/// Whether folding `ops` (one key, in partition order) needs the committed
/// document: true iff a Patch comes before every Upsert and Delete.
///
/// Otherwise, for a non-empty `ops`, the first upsert or delete overwrites
/// whatever came before, so the fold's result does not depend on the
/// committed state. (Empty `ops` give false: a key with no ops is not
/// resolved at all.)
pub fn needs_current<'a>(ops: impl IntoIterator<Item = &'a DocOp>) -> bool {
    matches!(ops.into_iter().next(), Some(DocOp::Patch { .. }))
}

/// Latest-wins fold of one key's ops over the committed state `current`: an
/// upsert sets the document, a delete removes it, and a patch applies to the
/// state so far ([`apply_patch`]).
pub fn fold<'a>(
    current: Option<Document>,
    ops: impl IntoIterator<Item = &'a DocOp>,
) -> Option<Document> {
    ops.into_iter().fold(current, |state, op| match op {
        DocOp::Upsert(doc) => Some(doc.clone()),
        DocOp::Delete(_) => None,
        DocOp::Patch { .. } => apply_patch(state.as_ref(), op),
    })
}
