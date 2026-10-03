//! The state a viewer builds by folding the event stream. Pure and
//! synchronous, so the browser's signals and the native tests share it.

use crate::event::{Event, RunEvent, SpecEvent, TestEvent};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Running,
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceStep {
    pub step: u32,
    pub action: String,
    pub vars: Value,
    /// Variables whose value differs from the previous step (all of them at step 1).
    pub changed: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub actual: String,
    pub states: u64,
    pub secs: f64,
}

/// One spec variant's check.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantRun {
    pub suite: String,
    pub spec: String,
    pub variant: String,
    pub expect: String,
    pub status: Status,
    /// (states generated, distinct, queue) at the last progress line.
    pub progress: Option<(u64, u64, u64)>,
    pub trace: Vec<TraceStep>,
    /// `Some(Some(n))`: the trace returns to state n; `Some(None)`: it stutters.
    pub lasso: Option<Option<u32>>,
    pub verdict: Option<Verdict>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TestRow {
    pub name: String,
    pub status: TestStatus,
    pub secs: f64,
    pub output: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStatus {
    Running,
    Passed,
    Failed,
    Ignored,
}

impl TestRow {
    /// `package::binary$test` as `(binary group, test)`; plain names have no group.
    pub fn split_name(&self) -> (&str, &str) {
        match self.name.split_once('$') {
            Some((group, test)) => (group, test),
            None => ("", &self.name),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Model {
    pub variants: Vec<VariantRun>,
    pub tests: Vec<TestRow>,
    pub spec_events: Vec<SpecEvent>,
    /// `(pass, secs)` once the run is over.
    pub done: Option<(bool, f64)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TestCounts {
    pub running: usize,
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
}

impl Model {
    pub fn apply(&mut self, event: Event) {
        match event {
            Event::Run(e) => self.apply_run(e),
            Event::Test(e) => self.apply_test(e),
            Event::Spec(e) => self.spec_events.push(e),
            Event::Done { pass, secs } => self.done = Some((pass, secs)),
        }
    }

    pub fn test_counts(&self) -> TestCounts {
        let mut c = TestCounts::default();
        for t in &self.tests {
            match t.status {
                TestStatus::Running => c.running += 1,
                TestStatus::Passed => c.passed += 1,
                TestStatus::Failed => c.failed += 1,
                TestStatus::Ignored => c.ignored += 1,
            }
        }
        c
    }

    /// (passed, failed, running) over the spec variants.
    pub fn variant_counts(&self) -> (usize, usize, usize) {
        let n = |s| self.variants.iter().filter(|v| v.status == s).count();
        (n(Status::Pass), n(Status::Fail), n(Status::Running))
    }

    fn apply_run(&mut self, e: RunEvent) {
        if let RunEvent::RunStarted {
            suite,
            spec,
            variant,
            expect,
        } = e
        {
            self.variants.push(VariantRun {
                suite,
                spec,
                variant,
                expect,
                status: Status::Running,
                progress: None,
                trace: Vec::new(),
                lasso: None,
                verdict: None,
            });
            return;
        }
        // Runs are sequential: every other event belongs to the latest variant.
        let Some(v) = self.variants.last_mut() else {
            return;
        };
        match e {
            RunEvent::Progress {
                states,
                distinct,
                queue,
            } => v.progress = Some((states, distinct, queue)),
            RunEvent::State { step, action, vars } => {
                let changed = changed_vars(v.trace.last().map(|s| &s.vars), &vars);
                v.trace.push(TraceStep {
                    step,
                    action,
                    vars,
                    changed,
                });
            }
            RunEvent::Lasso { to } => v.lasso = Some(to),
            RunEvent::Result {
                actual,
                states,
                secs,
                pass,
                ..
            } => {
                v.status = if pass { Status::Pass } else { Status::Fail };
                v.verdict = Some(Verdict {
                    actual,
                    states,
                    secs,
                });
            }
            RunEvent::RunStarted { .. } => unreachable!("handled above"),
        }
    }

    fn row(&mut self, name: &str) -> &mut TestRow {
        if let Some(i) = self.tests.iter().position(|t| t.name == name) {
            return &mut self.tests[i];
        }
        self.tests.push(TestRow {
            name: name.to_owned(),
            status: TestStatus::Running,
            secs: 0.0,
            output: String::new(),
        });
        self.tests.last_mut().expect("just pushed")
    }

    fn apply_test(&mut self, e: TestEvent) {
        match e {
            TestEvent::Started { name } => {
                self.row(&name);
            }
            TestEvent::Passed { name, secs } => {
                let r = self.row(&name);
                (r.status, r.secs) = (TestStatus::Passed, secs);
            }
            TestEvent::Failed { name, secs, output } => {
                let r = self.row(&name);
                (r.status, r.secs) = (TestStatus::Failed, secs);
                if !output.is_empty() {
                    r.output = output;
                }
            }
            TestEvent::Ignored { name } => self.row(&name).status = TestStatus::Ignored,
            TestEvent::Output { name, output } => self.row(&name).output = output,
            TestEvent::SuiteStarted { .. } | TestEvent::Summary { .. } => {}
        }
    }
}

/// `1638861` as `1,638,861`.
pub fn fmt_count(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Names of the variables of `now` that differ from `prev` (all when there is no `prev`).
pub fn changed_vars(prev: Option<&Value>, now: &Value) -> Vec<String> {
    let Some(map) = now.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter(|(k, v)| prev.and_then(|p| p.get(k.as_str())) != Some(*v))
        .map(|(k, _)| k.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn started(m: &mut Model, spec: &str) {
        m.apply(Event::Run(RunEvent::RunStarted {
            suite: "tla".into(),
            spec: spec.into(),
            variant: "v.cfg".into(),
            expect: "violation:X".into(),
        }));
    }

    fn state(step: u32, vars: Value) -> Event {
        Event::Run(RunEvent::State {
            step,
            action: "A".into(),
            vars,
        })
    }

    #[test]
    fn counts_get_thousands_separators() {
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count(999), "999");
        assert_eq!(fmt_count(1_000), "1,000");
        assert_eq!(fmt_count(1_638_861), "1,638,861");
    }

    #[test]
    fn trace_marks_changed_variables() {
        let mut m = Model::default();
        started(&mut m, "S");
        m.apply(state(1, json!({"a": 1, "b": [1], "c": "x"})));
        m.apply(state(2, json!({"a": 2, "b": [1], "c": "x"})));
        m.apply(state(3, json!({"a": 2, "b": [1, 2], "c": "x"})));
        m.apply(Event::Run(RunEvent::Lasso { to: Some(2) }));
        m.apply(Event::Run(RunEvent::Result {
            actual: "violation:X".into(),
            expect: "violation:X".into(),
            states: 5,
            secs: 0.1,
            pass: true,
        }));
        let v = &m.variants[0];
        assert_eq!(v.trace[0].changed, ["a", "b", "c"]);
        assert_eq!(v.trace[1].changed, ["a"]);
        assert_eq!(v.trace[2].changed, ["b"]);
        assert_eq!(v.lasso, Some(Some(2)));
        assert_eq!(v.status, Status::Pass);
        assert_eq!(m.variant_counts(), (1, 0, 0));
    }

    #[test]
    fn events_follow_the_latest_variant() {
        let mut m = Model::default();
        started(&mut m, "One");
        started(&mut m, "Two");
        m.apply(Event::Run(RunEvent::Progress {
            states: 3,
            distinct: 2,
            queue: 1,
        }));
        assert_eq!(m.variants[0].progress, None);
        assert_eq!(m.variants[1].progress, Some((3, 2, 1)));
        assert_eq!(m.variant_counts(), (0, 0, 2));
    }

    #[test]
    fn tests_accumulate_and_late_output_attaches() {
        let mut m = Model::default();
        m.apply(Event::Test(TestEvent::Started {
            name: "p::b$t1".into(),
        }));
        m.apply(Event::Test(TestEvent::Failed {
            name: "t2".into(),
            secs: 0.0,
            output: String::new(),
        }));
        m.apply(Event::Test(TestEvent::Output {
            name: "t2".into(),
            output: "why".into(),
        }));
        m.apply(Event::Test(TestEvent::Passed {
            name: "p::b$t1".into(),
            secs: 0.5,
        }));
        m.apply(Event::Test(TestEvent::Ignored { name: "t3".into() }));
        assert_eq!(m.tests.len(), 3);
        assert_eq!(m.tests[1].output, "why");
        assert_eq!(m.tests[0].split_name(), ("p::b", "t1"));
        assert_eq!(m.tests[1].split_name(), ("", "t2"));
        assert_eq!(
            m.test_counts(),
            TestCounts {
                running: 0,
                passed: 1,
                failed: 1,
                ignored: 1
            }
        );
        m.apply(Event::Done {
            pass: false,
            secs: 1.0,
        });
        assert_eq!(m.done, Some((false, 1.0)));
    }
}
