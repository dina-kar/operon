//! What a provider knows about one page, and the parts of the contract both
//! providers share: building the next snapshot, resolving a uid against it,
//! and turning a change into an answer.
//!
//! Keeping this here is what makes the two providers answer identically rather
//! than merely similarly.

use crate::error::BridgeError;
use crate::tool::{
    ActionOutcome, AxNode, ChangeSummary, FindQuery, Snapshot, SnapshotMode, SnapshotNode,
    SnapshotRequest, Uid, diff,
};

/// A page as the provider's driver sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageState {
    /// The current URL.
    pub url: String,
    /// The page title.
    pub title: String,
    /// The accessibility tree's roots.
    pub nodes: Vec<AxNode>,
    /// The page's visible text, for extraction.
    pub text: String,
    /// A screenshot the driver already took, with the extension to write it
    /// under (`png`, `jpeg`). Providers that capture on demand leave it `None`.
    pub screenshot: Option<(Vec<u8>, &'static str)>,
}

/// The snapshot history of a page: the counter and the last two snapshots.
#[derive(Debug, Default, Clone)]
pub struct SnapshotCache {
    next_id: u64,
    latest: Option<Snapshot>,
    previous: Option<Snapshot>,
}

impl SnapshotCache {
    /// A cache whose first snapshot is `s1`.
    pub fn new() -> Self {
        Self {
            next_id: 1,
            latest: None,
            previous: None,
        }
    }

    /// Build the next snapshot of `state`, replacing the last one.
    pub fn build(&mut self, state: &PageState, verbose: bool) -> Snapshot {
        let id = format!("s{}", self.next_id);
        self.next_id += 1;
        let snapshot = Snapshot::build(&id, &state.url, &state.title, &state.nodes, verbose);
        self.previous = self.latest.replace(snapshot.clone());
        self.latest = Some(snapshot.clone());
        snapshot
    }

    /// The page's current snapshot, if one has been taken.
    pub fn latest(&self) -> Option<&Snapshot> {
        self.latest.as_ref()
    }

    /// The snapshot before the current one.
    pub fn previous(&self) -> Option<&Snapshot> {
        self.previous.as_ref()
    }

    /// Forget both snapshots, so the next uid a caller holds is stale.
    pub fn invalidate(&mut self) {
        self.previous = None;
        self.latest = None;
    }

    /// The node a uid names, or a self-healing error naming `take_snapshot`.
    pub fn resolve(&self, uid: &Uid) -> Result<SnapshotNode, BridgeError> {
        let latest = self.latest.as_ref().ok_or_else(|| {
            BridgeError::policy(format!(
                "{uid} names no snapshot yet; call take_snapshot first"
            ))
        })?;
        latest.node(uid).cloned()
    }

    /// Search the current snapshot.
    pub fn find(&self, query: &FindQuery) -> Result<Vec<SnapshotNode>, BridgeError> {
        let latest = self
            .latest
            .as_ref()
            .ok_or_else(|| BridgeError::policy("call take_snapshot before find"))?;
        latest.find(query)
    }

    /// The change from the previous snapshot to the current one.
    pub fn change(&self) -> ChangeSummary {
        match (&self.previous, &self.latest) {
            (Some(before), Some(after)) => diff(before, after),
            _ => ChangeSummary::default(),
        }
    }
}

/// Narrow a snapshot to a scope and a depth, then render it inside the budget.
pub fn render(snapshot: &Snapshot, request: &SnapshotRequest) -> String {
    let selected = select(snapshot, request);
    Snapshot {
        nodes: selected,
        ..snapshot.clone()
    }
    .render(request.budget())
}

/// The lines an action changed, for the `diff` answer mode.
pub fn changed_lines(before: Option<&Snapshot>, after: &Snapshot) -> String {
    let Some(before) = before else {
        return after.render(crate::tool::DEFAULT_BUDGET_CHARS);
    };
    let change = diff(before, after);
    let mut kept: Vec<&SnapshotNode> = Vec::new();
    for node in &after.nodes {
        let signature = format!("{}|{}|{}", node.depth, node.role, node.name);
        let known = before.nodes.iter().any(|old| {
            format!("{}|{}|{}", old.depth, old.role, old.name) == signature
                && old.value == node.value
        });
        if !known && !kept.contains(&node) {
            kept.push(node);
        }
    }
    if kept.is_empty() {
        return change.render();
    }
    kept.iter()
        .map(|node| format!("+ {}", node.render()))
        .collect::<Vec<String>>()
        .join("\n")
}

/// The answer to an action: one line, a change summary and, if asked, the
/// snapshot or the lines that changed.
pub fn outcome(
    message: impl Into<String>,
    cache: &SnapshotCache,
    mode: SnapshotMode,
    request: &SnapshotRequest,
) -> ActionOutcome {
    let change = cache.change();
    let snapshot_text = match (mode, cache.latest()) {
        (SnapshotMode::None, _) => None,
        (SnapshotMode::Full, Some(latest)) => Some(render(latest, request)),
        (SnapshotMode::Diff, Some(latest)) => Some(changed_lines(cache.previous(), latest)),
        (SnapshotMode::Diff, None) | (SnapshotMode::Full, None) => None,
    };
    ActionOutcome {
        message: message.into(),
        change,
        snapshot: mode,
        snapshot_text,
    }
}

fn select(snapshot: &Snapshot, request: &SnapshotRequest) -> Vec<SnapshotNode> {
    let start = match &request.scope {
        None => 0,
        Some(uid) => match snapshot.nodes.iter().position(|node| &node.uid == uid) {
            Some(index) => index,
            None => return Vec::new(),
        },
    };
    let base_depth = snapshot.nodes[start].depth;
    // A scope is a subtree: the node itself, then the following deeper nodes,
    // and nothing after the subtree ends. With no scope the root is the scope,
    // so the whole document comes back.
    let mut end = start + 1;
    while end < snapshot.nodes.len() && snapshot.nodes[end].depth > base_depth {
        end += 1;
    }
    let limit = request.depth.map(|depth| base_depth + depth);
    snapshot.nodes[start..end]
        .iter()
        .filter(|node| limit.is_none_or(|limit| node.depth <= limit))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::AxNode;

    fn state() -> PageState {
        PageState {
            url: "https://example.com/a".to_string(),
            title: "Example".to_string(),
            nodes: vec![
                AxNode::new("document", "Example")
                    .with_backend_id(1)
                    .with_child(AxNode::new("button", "Save").with_backend_id(2))
                    .with_child(AxNode::new("button", "Cancel").with_backend_id(3)),
            ],
            text: "Example".to_string(),
            screenshot: None,
        }
    }

    #[test]
    fn a_uid_from_the_previous_snapshot_is_stale() {
        let mut cache = SnapshotCache::new();
        let first = cache.build(&state(), false);
        let uid = first.nodes[1].uid.clone();
        cache.build(&state(), false);
        let error = cache.resolve(&uid).err().unwrap_or_else(|| panic!("stale"));
        assert!(matches!(error, BridgeError::StaleSnapshot { .. }));
    }

    #[test]
    fn a_scope_narrows_the_render() {
        let mut cache = SnapshotCache::new();
        let snapshot = cache.build(&state(), false);
        let scope = snapshot.nodes[1].uid.clone();
        let rendered = render(
            &snapshot,
            &SnapshotRequest::new().scoped(scope).with_depth(0),
        );
        assert!(rendered.contains("Save"), "{rendered}");
        assert!(!rendered.contains("Cancel"), "{rendered}");
    }
}
