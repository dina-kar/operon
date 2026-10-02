//! Spec events (§31 §11.3, D311). Every machine reports each transition it
//! takes, named as the TLA+ action it implements, so RT1's trace validation
//! can check recorded runs against `spec/tla/router/` and `loams-specview`
//! can show them.

use serde::Serialize;

/// One transition: the spec, its action and the observed fields, e.g.
/// `ShardMap / Reload {instance: "pgdog-1", gen: 4, ok: true}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SpecEvent {
    pub spec: &'static str,
    pub action: &'static str,
    pub fields: Vec<(&'static str, SpecValue)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum SpecValue {
    Bool(bool),
    Int(i64),
    Str(String),
}

impl From<bool> for SpecValue {
    fn from(v: bool) -> Self {
        SpecValue::Bool(v)
    }
}

impl From<i64> for SpecValue {
    fn from(v: i64) -> Self {
        SpecValue::Int(v)
    }
}

impl From<u32> for SpecValue {
    fn from(v: u32) -> Self {
        SpecValue::Int(i64::from(v))
    }
}

impl From<&str> for SpecValue {
    fn from(v: &str) -> Self {
        SpecValue::Str(v.to_owned())
    }
}

impl From<String> for SpecValue {
    fn from(v: String) -> Self {
        SpecValue::Str(v)
    }
}

pub trait TraceSink {
    fn emit(&mut self, event: SpecEvent);
}

/// Keeps every event in order; for tests and the simulator's run directory.
#[derive(Debug, Default)]
pub struct VecSink(pub Vec<SpecEvent>);

impl TraceSink for VecSink {
    fn emit(&mut self, event: SpecEvent) {
        self.0.push(event);
    }
}

/// Drops every event; for drivers that do not record traces.
#[derive(Debug, Default)]
pub struct NullSink;

impl TraceSink for NullSink {
    fn emit(&mut self, _event: SpecEvent) {}
}
