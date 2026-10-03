//! The event model shared by the server and the browser: one JSON object per
//! line in a recorded run, and one SSE `data:` payload per event on the wire.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One event of a run. On the wire: `{"channel":"run","event":{"type":"State",...}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "channel", content = "event", rename_all = "snake_case")]
pub enum Event {
    /// Model-checker output for one spec variant.
    Run(RunEvent),
    /// Rust test output.
    Test(TestEvent),
    /// A transition reported by a machine in the RT1 simulator.
    Spec(SpecEvent),
    /// The whole run is over.
    Done { pass: bool, secs: f64 },
}

/// TLC output for one variant, in order: `RunStarted`, any number of
/// `Progress`, then (for a violation) `State`s and a `Lasso`, then `Result`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RunEvent {
    RunStarted {
        suite: String,
        spec: String,
        variant: String,
        expect: String,
    },
    Progress {
        states: u64,
        distinct: u64,
        queue: u64,
    },
    /// One state of a counterexample. `vars` is an object: variable name to
    /// the parsed TLA+ value (records and functions are objects, sets and
    /// sequences arrays).
    State {
        step: u32,
        action: String,
        vars: Value,
    },
    /// The trace ends by returning to state `to` (`Back to state N`), or by
    /// stuttering forever when `to` is `None`.
    Lasso { to: Option<u32> },
    Result {
        actual: String,
        expect: String,
        states: u64,
        secs: f64,
        pass: bool,
    },
}

/// Rust test output (nextest `libtest-json`, or libtest's human output).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TestEvent {
    /// A test binary (or the whole nextest run) starts; `count` tests follow.
    SuiteStarted {
        count: u64,
    },
    Started {
        name: String,
    },
    Passed {
        name: String,
        secs: f64,
    },
    Failed {
        name: String,
        secs: f64,
        /// Captured stdout/stderr; empty when the runner prints it later
        /// (the human-output parser sends an `Output` event then).
        output: String,
    },
    Ignored {
        name: String,
    },
    /// Failure output that arrived after the `Failed` event.
    Output {
        name: String,
        output: String,
    },
    Summary {
        passed: u64,
        failed: u64,
        ignored: u64,
        secs: f64,
    },
}

/// What the RT1 simulator emits for every transition a machine takes
/// (design §31 §7.1: `SpecEvent { spec, action, fields }`). A placeholder: the
/// view does not use it until RT1 lands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpecEvent {
    pub spec: String,
    pub action: String,
    pub fields: Vec<(String, SpecValue)>,
}

/// The values a `SpecEvent` field can hold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpecValue {
    Bool(bool),
    Int(i64),
    Str(String),
    List(Vec<SpecValue>),
}

impl Event {
    /// One JSON line, no trailing newline.
    pub fn to_line(&self) -> String {
        serde_json::to_string(self).expect("an Event always serializes")
    }

    pub fn from_line(line: &str) -> Result<Event, serde_json::Error> {
        serde_json::from_str(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(e: Event) {
        let line = e.to_line();
        assert!(!line.contains('\n'));
        assert_eq!(Event::from_line(&line).unwrap(), e, "{line}");
    }

    #[test]
    fn every_variant_round_trips() {
        round_trip(Event::Run(RunEvent::RunStarted {
            suite: "tla".into(),
            spec: "ShardMap".into(),
            variant: "MCShardMap_Small.cfg".into(),
            expect: "ok".into(),
        }));
        round_trip(Event::Run(RunEvent::Progress {
            states: 10,
            distinct: 4,
            queue: 2,
        }));
        round_trip(Event::Run(RunEvent::State {
            step: 2,
            action: "Publish".into(),
            vars: json!({"configmap": 1, "fenced": {"s1": false}, "acked": []}),
        }));
        round_trip(Event::Run(RunEvent::Lasso { to: Some(3) }));
        round_trip(Event::Run(RunEvent::Lasso { to: None }));
        round_trip(Event::Run(RunEvent::Result {
            actual: "ok".into(),
            expect: "ok".into(),
            states: 99,
            secs: 1.5,
            pass: true,
        }));
        round_trip(Event::Test(TestEvent::SuiteStarted { count: 3 }));
        round_trip(Event::Test(TestEvent::Started {
            name: "a::b".into(),
        }));
        round_trip(Event::Test(TestEvent::Passed {
            name: "a::b".into(),
            secs: 0.25,
        }));
        round_trip(Event::Test(TestEvent::Failed {
            name: "a::c".into(),
            secs: 0.5,
            output: "boom\n".into(),
        }));
        round_trip(Event::Test(TestEvent::Ignored {
            name: "a::d".into(),
        }));
        round_trip(Event::Test(TestEvent::Output {
            name: "a::c".into(),
            output: "x".into(),
        }));
        round_trip(Event::Test(TestEvent::Summary {
            passed: 1,
            failed: 1,
            ignored: 1,
            secs: 2.0,
        }));
        round_trip(Event::Spec(SpecEvent {
            spec: "ShardMap".into(),
            action: "Reload".into(),
            fields: vec![
                ("instance".into(), SpecValue::Str("pgdog-1".into())),
                ("gen".into(), SpecValue::Int(4)),
                ("ok".into(), SpecValue::Bool(true)),
            ],
        }));
        round_trip(Event::Done {
            pass: true,
            secs: 3.0,
        });
    }

    #[test]
    fn wire_shape_is_stable() {
        let e = Event::Run(RunEvent::Progress {
            states: 1,
            distinct: 1,
            queue: 0,
        });
        assert_eq!(
            e.to_line(),
            r#"{"channel":"run","event":{"type":"Progress","states":1,"distinct":1,"queue":0}}"#
        );
    }
}
