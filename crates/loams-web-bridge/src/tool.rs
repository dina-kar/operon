//! The tool contract every provider implements (§37 §18.14.2, D503, D504):
//! a snapshot with uids, `find`, `click`, `fill`, `wait_for`, a screenshot and
//! a content extraction. The types here are the contract; the providers in
//! [`crate::local`] and [`crate::browser_run`] fill them in.
//!
//! The efficiency rules of D504 are implemented once, here, so the local and
//! the remote provider answer identically:
//!
//! 1. a snapshot is text with uids, never a screenshot;
//! 2. a uid carries its snapshot id (`s7_12`), so a stale uid is a
//!    self-healing error and never a click on the wrong element;
//! 3. an action answers in one line plus a change summary, with a server
//!    default of `diff`;
//! 4. `find` returns matches with context instead of the page;
//! 5. names are truncated, identical siblings are folded, and the whole result
//!    is capped by a token budget with a marker naming the way to continue.

use std::collections::HashSet;

use regex::Regex;

use crate::error::BridgeError;
use crate::redact;

/// The default cap on one snapshot, in characters, estimated as four
/// characters per token (§18.14.2 rule 8).
pub const DEFAULT_BUDGET_CHARS: usize = 6_000 * 4;

/// Names are cut at this many characters (§18.14.2 rule 5).
pub const MAX_NAME_CHARS: usize = 200;

/// A run of this many identical empty siblings is folded into one line.
const FOLD_RUN: usize = 4;

/// An element handle: `s7_12`, snapshot 7, node 12.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Uid(String);

impl Uid {
    /// Parse a uid, checking the `s<n>_<m>` shape.
    pub fn parse(raw: &str) -> Result<Self, BridgeError> {
        let malformed = || {
            BridgeError::policy(format!(
                "{raw:?} is not a uid: expected s<snapshot>_<node>, for example s7_12"
            ))
        };
        let Some((snapshot, node)) = raw.split_once('_') else {
            return Err(malformed());
        };
        let snapshot = snapshot.strip_prefix('s').unwrap_or(snapshot);
        let digits = |part: &str| {
            !part.is_empty() && part.chars().all(|character| character.is_ascii_digit())
        };
        if !digits(snapshot) || !digits(node) {
            return Err(malformed());
        }
        Ok(Uid(raw.to_string()))
    }

    /// The uid as written.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The snapshot the uid came from, `s7` in `s7_12`.
    pub fn snapshot(&self) -> &str {
        self.0.split('_').next().unwrap_or_default()
    }
}

impl std::fmt::Display for Uid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a provider found in a page: one node of an accessibility tree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AxNode {
    /// The ARIA role, for example `button` or `textbox`.
    pub role: String,
    /// The accessible name.
    pub name: String,
    /// The value, for fields that have one.
    pub value: Option<String>,
    /// The states the tree reported: `disabled`, `checked`, `focused`,
    /// `required`, `expanded`, `level`, `readonly`, `protected`.
    pub states: Vec<String>,
    /// The value must never be shown (a password or one-time-code field).
    pub sensitive: bool,
    /// The provider's own handle for the element (a CDP backend node id).
    pub backend_id: i64,
    /// The browser said to ignore this node; its children are still walked.
    pub ignored: bool,
    /// The element's children, in document order.
    pub children: Vec<AxNode>,
}

impl AxNode {
    /// A node with a role and a name.
    pub fn new(role: &str, name: &str) -> Self {
        Self {
            role: role.to_string(),
            name: name.to_string(),
            ..Self::default()
        }
    }

    /// With a value.
    pub fn with_value(mut self, value: &str) -> Self {
        self.value = Some(value.to_string());
        self
    }

    /// With a provider handle.
    pub fn with_backend_id(mut self, backend_id: i64) -> Self {
        self.backend_id = backend_id;
        self
    }

    /// Marked as a credential field, so its value is elided.
    pub fn sensitive(mut self) -> Self {
        self.sensitive = true;
        self
    }

    /// With one state.
    pub fn with_state(mut self, state: &str) -> Self {
        self.states.push(state.to_string());
        self
    }

    /// With a child.
    pub fn with_child(mut self, child: AxNode) -> Self {
        self.children.push(child);
        self
    }
}

/// Every node of a forest, in document order, parents before children.
///
/// A search has to walk the whole tree, not just its roots: the text an agent
/// waits for is usually on a leaf.
pub fn flatten(nodes: &[AxNode]) -> Vec<&AxNode> {
    let mut out = Vec::new();
    for node in nodes {
        out.push(node);
        out.extend(flatten(&node.children));
    }
    out
}

/// One line of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotNode {
    /// The handle an action takes.
    pub uid: Uid,
    /// The depth in the tree, 0 for the root.
    pub depth: u32,
    /// The ARIA role.
    pub role: String,
    /// The accessible name, truncated and escaped.
    pub name: String,
    /// The value, never for a credential field.
    pub value: Option<String>,
    /// The states.
    pub states: Vec<String>,
    /// The provider's handle for the element.
    pub backend_id: i64,
}

impl SnapshotNode {
    /// `uid=s7_12 button "Save" [disabled]`.
    pub fn render(&self) -> String {
        let mut line = format!("uid={} {}", self.uid, self.role);
        if !self.name.is_empty() {
            line.push_str(&format!(" \"{}\"", escape(&self.name)));
        }
        if let Some(value) = &self.value {
            line.push_str(&format!(" value=\"{}\"", escape(value)));
        }
        for state in &self.states {
            line.push_str(&format!(" [{state}]"));
        }
        line
    }
}

/// A page as the agent sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The snapshot id, `s7`.
    pub id: String,
    /// The page's URL with the query redacted.
    pub url: String,
    /// The page title.
    pub title: String,
    /// The nodes in document order.
    pub nodes: Vec<SnapshotNode>,
}

impl Snapshot {
    /// Build a snapshot from the accessibility trees of one or more roots,
    /// assigning `s<N>_<M>` uids in document order.
    ///
    /// Ignored nodes are pruned but their children are kept, identical empty
    /// siblings are folded into one line, and a credential field's value is
    /// elided.
    pub fn build(id: &str, url: &str, title: &str, roots: &[AxNode], verbose: bool) -> Self {
        let mut nodes = Vec::new();
        let mut counter = 0usize;
        for root in roots {
            push_node(&mut nodes, &mut counter, root, 0, id, verbose);
        }
        Snapshot {
            id: id.to_string(),
            url: redact_url(url),
            title: title.to_string(),
            nodes,
        }
    }

    /// The uid of the node with this provider handle.
    pub fn uid_of(&self, backend_id: i64) -> Option<&Uid> {
        self.nodes
            .iter()
            .find(|node| node.backend_id == backend_id && node.backend_id != 0)
            .map(|node| &node.uid)
    }

    /// The node a uid names, or a self-healing stale error.
    pub fn node(&self, uid: &Uid) -> Result<&SnapshotNode, BridgeError> {
        if uid.snapshot() != self.id {
            return Err(BridgeError::StaleSnapshot {
                uid: uid.to_string(),
                seen: uid.snapshot().to_string(),
                latest: self.id.clone(),
            });
        }
        self.nodes
            .iter()
            .find(|node| &node.uid == uid)
            .ok_or_else(|| {
                BridgeError::policy(format!(
                    "{uid} is not in snapshot {}; call take_snapshot again",
                    self.id
                ))
            })
    }

    /// The rendered text, capped at `budget_chars` with a marker naming the
    /// way to continue.
    pub fn render(&self, budget_chars: usize) -> String {
        let mut lines: Vec<String> = Vec::new();
        let mut used = 0usize;
        let mut emitted = 0usize;
        for node in &self.nodes {
            let line = node.render();
            let cost = line.len() + 1;
            if emitted > 0 && used + cost > budget_chars {
                break;
            }
            used += cost;
            lines.push(line);
            emitted += 1;
        }
        let remaining = self.nodes.len().saturating_sub(emitted);
        if remaining > 0 {
            lines.push(format!(
                "… {remaining} more nodes: call take_snapshot with a uid scope, or find, to see \
                 them"
            ));
        }
        lines.join("\n")
    }

    /// Matches of `needle` (case-insensitive) or `pattern` (a regex), each with
    /// `context` lines of its neighbours.
    pub fn find(&self, query: &FindQuery) -> Result<Vec<SnapshotNode>, BridgeError> {
        let matcher = query.build()?;
        let mut found: Vec<SnapshotNode> = Vec::new();
        for (index, node) in self.nodes.iter().enumerate() {
            if !matcher.matches(node) {
                continue;
            }
            let start = index.saturating_sub(query.context);
            let end = (index + query.context + 1).min(self.nodes.len());
            for line in &self.nodes[start..end] {
                if !found.contains(line) {
                    found.push(line.clone());
                }
            }
        }
        Ok(found)
    }
}

/// What to look for in `find`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindQuery {
    /// Case-insensitive substring.
    pub text: Option<String>,
    /// A regular expression. Compiled per call and refused if invalid.
    pub regex: Option<String>,
    /// How many neighbouring lines each match brings.
    pub context: usize,
}

impl FindQuery {
    /// A case-insensitive text search.
    pub fn text(needle: impl Into<String>) -> Self {
        Self {
            text: Some(needle.into()),
            regex: None,
            context: 1,
        }
    }

    /// A regular expression search.
    pub fn regex(pattern: impl Into<String>) -> Self {
        Self {
            text: None,
            regex: Some(pattern.into()),
            context: 1,
        }
    }

    /// With this many context lines.
    pub fn with_context(mut self, context: usize) -> Self {
        self.context = context;
        self
    }

    fn build(&self) -> Result<Matcher, BridgeError> {
        match (&self.text, &self.regex) {
            (Some(text), None) => Ok(Matcher::Text(text.to_lowercase())),
            (None, Some(pattern)) => Regex::new(pattern).map(Matcher::Regex).map_err(|error| {
                BridgeError::policy(format!("{pattern:?} is not a regular expression: {error}"))
            }),
            _ => Err(BridgeError::policy(
                "find takes exactly one of text or regex",
            )),
        }
    }
}

enum Matcher {
    Text(String),
    Regex(Regex),
}

impl Matcher {
    fn matches(&self, node: &SnapshotNode) -> bool {
        let haystack = format!(
            "{} {} {}",
            node.role,
            node.name,
            node.value.as_deref().unwrap_or("")
        );
        match self {
            Matcher::Text(needle) => haystack.to_lowercase().contains(needle.as_str()),
            Matcher::Regex(regex) => regex.is_match(&haystack),
        }
    }
}

/// What changed between two snapshots (D504 rule 3).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeSummary {
    /// Nodes present now and not before.
    pub added: usize,
    /// Nodes present before and not now.
    pub removed: usize,
    /// Nodes whose name or value changed.
    pub changed: usize,
    /// A navigation or a dialog, if the provider saw one.
    pub event: Option<String>,
}

impl ChangeSummary {
    /// The one-line form an action answers with.
    pub fn render(&self) -> String {
        let mut parts = Vec::new();
        if self.added > 0 {
            parts.push(format!("{} added", self.added));
        }
        if self.removed > 0 {
            parts.push(format!("{} removed", self.removed));
        }
        if self.changed > 0 {
            parts.push(format!("{} changed", self.changed));
        }
        if let Some(event) = &self.event {
            parts.push(event.clone());
        }
        if parts.is_empty() {
            "no visible change".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Compare two snapshots by their signatures: `(role, name, value, depth)`.
pub fn diff(before: &Snapshot, after: &Snapshot) -> ChangeSummary {
    let before_signatures: HashSet<String> = before.nodes.iter().map(signature).collect();
    let after_signatures: HashSet<String> = after.nodes.iter().map(signature).collect();
    let mut added = 0usize;
    let mut removed = 0usize;
    let mut changed = 0usize;
    for node in &after.nodes {
        if before_signatures.contains(&signature(node)) {
            continue;
        }
        let same_shape = before
            .nodes
            .iter()
            .any(|old| old.role == node.role && old.depth == node.depth);
        if same_shape {
            changed += 1;
        } else {
            added += 1;
        }
    }
    for node in &before.nodes {
        if !after_signatures.contains(&signature(node)) {
            removed += 1;
        }
    }
    let event = (before.url != after.url).then(|| format!("navigated to {}", after.url));
    ChangeSummary {
        added,
        removed,
        changed,
        event,
    }
}

fn signature(node: &SnapshotNode) -> String {
    format!(
        "{}|{}|{}|{}",
        node.depth,
        node.role,
        node.name,
        node.value.as_deref().unwrap_or("")
    )
}

fn push_node(
    out: &mut Vec<SnapshotNode>,
    counter: &mut usize,
    node: &AxNode,
    depth: u32,
    snapshot: &str,
    verbose: bool,
) {
    let has_name = !node.name.trim().is_empty();
    if node.ignored && !verbose && !has_name && node.children.is_empty() {
        return;
    }
    let children = &node.children;
    let foldable = !verbose
        && node.name.is_empty()
        && children.len() >= FOLD_RUN
        && children
            .iter()
            .all(|child| child.name.is_empty() && child.role == children[0].role);
    if foldable {
        let first = *counter;
        *counter += children.len();
        out.push(SnapshotNode {
            uid: Uid(format!("{snapshot}_{first}-{}", *counter)),
            depth,
            role: children[0].role.clone(),
            name: format!("… {} similar rows", children.len()),
            value: None,
            states: Vec::new(),
            backend_id: children[0].backend_id,
        });
        return;
    }
    *counter += 1;
    out.push(SnapshotNode {
        uid: Uid(format!("{snapshot}_{}", *counter)),
        depth,
        role: if node.role.is_empty() {
            "generic".to_string()
        } else {
            node.role.clone()
        },
        // Truncated here, escaped when rendered: escaping twice would show the
        // reader `&amp;lt;`.
        name: truncate(&node.name, MAX_NAME_CHARS),
        value: node
            .value
            .as_ref()
            .filter(|_| !node.sensitive)
            .map(|value| truncate(value, MAX_NAME_CHARS)),
        states: node
            .states
            .iter()
            .filter(|state| *state != "protected")
            .cloned()
            .collect(),
        backend_id: node.backend_id,
    });
    for child in children {
        push_node(out, counter, child, depth + 1, snapshot, verbose);
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let kept: String = text.chars().take(max).collect();
    format!("{kept}…")
}

/// Escape the characters that would let page text close a field or a marker.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\n' | '\r' | '\t' => out.push(' '),
            other => out.push(other),
        }
    }
    out
}

/// Keep the host and the path of a URL, never its query or fragment (D510:
/// origin with path and query redacted).
pub fn redact_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(url) => {
            let host = url.host_str().unwrap_or_default();
            let path = url.path();
            if path == "/" {
                host.to_string()
            } else {
                format!("{host}{path}")
            }
        }
        Err(_) => redact::scrub(raw),
    }
}

/// What `take_snapshot` should return.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotRequest {
    /// Scope the tree to one node.
    pub scope: Option<Uid>,
    /// How deep to walk.
    pub depth: Option<u32>,
    /// Keep the nodes pruning would drop.
    pub verbose: bool,
    /// Override the character budget.
    pub budget_chars: Option<usize>,
}

impl SnapshotRequest {
    /// A default request: the whole tree, pruned, within the budget.
    pub fn new() -> Self {
        Self::default()
    }

    /// Scoped to a node.
    pub fn scoped(mut self, uid: Uid) -> Self {
        self.scope = Some(uid);
        self
    }

    /// With a depth.
    pub fn with_depth(mut self, depth: u32) -> Self {
        self.depth = Some(depth);
        self
    }

    /// Keeping everything pruning would drop.
    pub fn verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    /// With an explicit budget.
    pub fn with_budget_chars(mut self, budget: usize) -> Self {
        self.budget_chars = Some(budget);
        self
    }

    /// The budget in force.
    pub fn budget(&self) -> usize {
        self.budget_chars.unwrap_or(DEFAULT_BUDGET_CHARS)
    }
}

/// Whether an action's answer carries a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotMode {
    /// No snapshot.
    #[default]
    None,
    /// Only what changed.
    Diff,
    /// The whole tree.
    Full,
}

/// What an action did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionOutcome {
    /// One line: what happened.
    pub message: String,
    /// The change summary.
    pub change: ChangeSummary,
    /// Whether a snapshot came back.
    pub snapshot: SnapshotMode,
    /// The snapshot's text, when one came back.
    pub snapshot_text: Option<String>,
}

impl ActionOutcome {
    /// A one-line answer with no snapshot.
    pub fn line(message: impl Into<String>, change: ChangeSummary) -> Self {
        Self {
            message: message.into(),
            change,
            snapshot: SnapshotMode::None,
            snapshot_text: None,
        }
    }

    /// The whole answer as one string.
    pub fn render(&self) -> String {
        let mut out = format!("{} ({})", self.message, self.change.render());
        if let Some(text) = &self.snapshot_text {
            out.push_str("\n\n");
            out.push_str(text);
        }
        out
    }
}

/// A value to type into a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillValue {
    /// A literal the caller already has.
    Literal(String),
    /// A reference to a stored secret. Never logged, never echoed.
    Secret(crate::secret::SecretRef),
}

/// What to wait for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitCondition {
    /// Text appears in the tree.
    Text(String),
    /// A CSS selector matches.
    Selector(String),
    /// The URL changes to one containing this string.
    Url(String),
    /// The page stops loading.
    NetworkIdle,
}

/// A `wait_for` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitRequest {
    /// The condition.
    pub condition: WaitCondition,
    /// How long to wait, in milliseconds.
    pub timeout_ms: u64,
    /// How often to look, in milliseconds.
    pub poll_interval_ms: u64,
}

impl WaitRequest {
    /// A request with the given condition and a 10 second timeout.
    pub fn new(condition: WaitCondition) -> Self {
        Self {
            condition,
            timeout_ms: 10_000,
            poll_interval_ms: 250,
        }
    }

    /// With a timeout.
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Whether a wait succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitOutcome {
    /// Whether the condition held before the timeout.
    pub matched: bool,
    /// How long the wait took, in milliseconds.
    pub waited_ms: u64,
    /// What to do next when it did not hold.
    pub note: String,
}

/// A screenshot on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Screenshot {
    /// Where it was written.
    pub path: std::path::PathBuf,
    /// Its size in bytes.
    pub bytes: usize,
    /// Its SHA-256, hex, so a caller can tell two runs apart.
    pub sha256: String,
}

/// The text of a page, for extraction (the Kitesurf-shaped use).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extracted {
    /// The page title.
    pub title: String,
    /// The page's URL, query redacted.
    pub url: String,
    /// The visible text, truncated to the budget.
    pub text: String,
    /// Whether the text was cut.
    pub truncated: bool,
}

/// The result of a navigation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Navigation {
    /// Where the page ended up, query redacted.
    pub url: String,
    /// The page title.
    pub title: String,
    /// What changed.
    pub change: ChangeSummary,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> Vec<AxNode> {
        vec![
            AxNode::new("document", "Example")
                .with_backend_id(1)
                .with_child(
                    AxNode::new("button", "Save")
                        .with_backend_id(2)
                        .with_state("disabled"),
                )
                .with_child(
                    AxNode::new("textbox", "Password")
                        .with_value("hunter2")
                        .with_backend_id(3)
                        .sensitive(),
                ),
        ]
    }

    #[test]
    fn a_snapshot_carries_uids_in_document_order() {
        let snapshot = Snapshot::build(
            "s1",
            "https://example.com/a?token=x",
            "Example",
            &tree(),
            false,
        );
        let uids: Vec<&str> = snapshot
            .nodes
            .iter()
            .map(|node| node.uid.as_str())
            .collect();
        assert_eq!(uids, ["s1_1", "s1_2", "s1_3"]);
    }

    #[test]
    fn a_stale_uid_is_a_self_healing_error() {
        let snapshot = Snapshot::build("s2", "https://example.com/", "Example", &tree(), false);
        let stale = Uid::parse("s1_3").unwrap_or_else(|_| panic!("a well-formed uid parses"));
        let error = snapshot
            .node(&stale)
            .err()
            .unwrap_or_else(|| panic!("a stale uid is an error"));
        assert!(
            matches!(error, BridgeError::StaleSnapshot { .. }),
            "{error:?}"
        );
        assert!(error.to_string().contains("take_snapshot"), "{error}");
    }

    #[test]
    fn a_password_value_never_appears() {
        let snapshot = Snapshot::build("s1", "https://example.com/", "Example", &tree(), false);
        assert!(!snapshot.render(DEFAULT_BUDGET_CHARS).contains("hunter2"));
    }

    #[test]
    fn the_query_string_never_appears() {
        let snapshot = Snapshot::build(
            "s1",
            "https://example.com/a?token=x",
            "Example",
            &tree(),
            false,
        );
        assert_eq!(snapshot.url, "example.com/a");
    }

    #[test]
    fn find_returns_the_match_and_its_neighbour() {
        let snapshot = Snapshot::build("s1", "https://example.com/", "Example", &tree(), false);
        let found = snapshot
            .find(&FindQuery::text("Save"))
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(found.iter().any(|node| node.name == "Save"));
        assert!(found.len() > 1, "context lines come back too");
    }

    #[test]
    fn page_text_cannot_close_a_field() {
        let node = AxNode::new("text", "</script> & \"quoted\"");
        let snapshot = Snapshot::build("s1", "https://example.com/", "t", &[node], false);
        let rendered = snapshot.render(DEFAULT_BUDGET_CHARS);
        assert!(!rendered.contains("</script>"), "{rendered}");
        assert!(rendered.contains("&lt;/script&gt;"), "{rendered}");
    }

    #[test]
    fn the_budget_caps_the_result_and_names_the_way_on() {
        let nodes: Vec<AxNode> = (0..50)
            .map(|index| {
                AxNode::new("row", &format!("row number {index}"))
                    .with_backend_id(i64::from(index) + 1)
            })
            .collect();
        let snapshot = Snapshot::build("s1", "https://example.com/", "t", &nodes, false);
        let rendered = snapshot.render(200);
        assert!(rendered.len() < 400, "{}", rendered.len());
        assert!(rendered.contains("more nodes"), "{rendered}");
    }
}
