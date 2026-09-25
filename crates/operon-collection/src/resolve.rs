//! Latest-wins resolution of one key's ops (plan M1.1 Review Focus 3).

use crate::doc::{DocOp, Document, apply_patch};

/// Whether the first op for one key is a patch, so folding may need the
/// committed document. A later upsert or delete can make that read unnecessary;
/// this check conservatively returns true anyway. Empty `ops` returns false.
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
