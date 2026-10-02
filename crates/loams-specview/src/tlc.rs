//! A streaming parser of TLC's stdout: progress lines, counterexample states
//! (`State N: <Action ...>` followed by `/\ var = value` lines) and the
//! `Back to state N` / `Stuttering` trace ends.

use crate::event::RunEvent;
use crate::tla_value::parse_value;
use serde_json::{Map, Value};

/// How the model check ended, from TLC's own words (the same rules as
/// `scripts/spec/check.py`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Violation(String),
    Temporal,
    Deadlock,
    /// TLC ended without a verdict (a parse error, a crash).
    Error,
}

impl Outcome {
    /// The `expect`/`actual` spelling of `specs.toml`.
    pub fn label(&self) -> String {
        match self {
            Outcome::Ok => "ok".into(),
            Outcome::Violation(inv) => format!("violation:{inv}"),
            Outcome::Temporal => "violation:temporal".into(),
            Outcome::Deadlock => "violation:deadlock".into(),
            Outcome::Error => "error".into(),
        }
    }
}

#[derive(Default)]
struct Pending {
    step: u32,
    action: String,
    vars: Map<String, Value>,
    /// The variable whose value may continue on the next line.
    last: Option<(String, String)>,
}

#[derive(Default)]
pub struct TlcParser {
    pending: Option<Pending>,
    outcome: Option<Outcome>,
    /// The `distinct states found` of the last progress or summary line.
    pub distinct: u64,
}

impl TlcParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// The verdict seen so far; `Outcome::Error` when TLC said nothing.
    pub fn outcome(&self) -> Outcome {
        self.outcome.clone().unwrap_or(Outcome::Error)
    }

    /// Feed one line of TLC output (without its newline).
    pub fn push_line(&mut self, line: &str) -> Vec<RunEvent> {
        let mut out = Vec::new();
        if let Some(rest) = line.strip_prefix("/\\ ") {
            if self.pending.is_some() {
                self.var_line(rest);
                return out;
            }
        } else if self.pending.is_some() && !line.trim().is_empty() && !is_boundary(line) {
            // A value that TLC wrapped onto the next line.
            if let Some(p) = self.pending.as_mut()
                && let Some((name, text)) = p.last.as_mut()
            {
                text.push(' ');
                text.push_str(line.trim());
                let v = value_or_raw(text);
                p.vars.insert(name.clone(), v);
            }
            return out;
        }
        self.flush(&mut out);

        if let Some(rest) = line.strip_prefix("State ")
            && let Some((n, action)) = rest.split_once(':')
            && let Ok(step) = n.trim().parse::<u32>()
        {
            self.pending = Some(Pending {
                step,
                action: action_name(action),
                ..Pending::default()
            });
        } else if let Some(rest) = line.strip_prefix("Back to state ") {
            let to = rest.split(':').next().and_then(|n| n.trim().parse().ok());
            out.push(RunEvent::Lasso { to });
        } else if line.starts_with("Stuttering") {
            out.push(RunEvent::Lasso { to: None });
        } else if let Some(p) = parse_counts(line) {
            self.distinct = p.1;
            out.push(RunEvent::Progress {
                states: p.0,
                distinct: p.1,
                queue: p.2,
            });
        } else {
            self.verdict(line);
        }
        out
    }

    /// Call at end of output; emits a state that was still being read.
    pub fn finish(&mut self) -> Vec<RunEvent> {
        let mut out = Vec::new();
        self.flush(&mut out);
        out
    }

    fn var_line(&mut self, rest: &str) {
        let Some((name, text)) = rest.split_once(" = ") else {
            return;
        };
        let p = self.pending.as_mut().expect("checked by the caller");
        p.vars.insert(name.to_owned(), value_or_raw(text));
        p.last = Some((name.to_owned(), text.to_owned()));
    }

    fn flush(&mut self, out: &mut Vec<RunEvent>) {
        if let Some(p) = self.pending.take() {
            out.push(RunEvent::State {
                step: p.step,
                action: p.action,
                vars: Value::Object(p.vars),
            });
        }
    }

    fn verdict(&mut self, line: &str) {
        let line = line.trim();
        if line.contains("Model checking completed. No error has been found.") {
            self.outcome = Some(Outcome::Ok);
        } else if let Some(rest) = line.strip_prefix("Error: Invariant ")
            && let Some(inv) = rest.strip_suffix(" is violated.")
        {
            self.outcome = Some(Outcome::Violation(inv.to_owned()));
        } else if line.contains("Temporal properties were violated") {
            self.outcome.get_or_insert(Outcome::Temporal);
        } else if line.contains("Deadlock reached") {
            self.outcome.get_or_insert(Outcome::Deadlock);
        }
    }
}

fn is_boundary(line: &str) -> bool {
    line.starts_with("State ")
        || line.starts_with("Back to state")
        || line.starts_with("Stuttering")
        || line.starts_with("Error:")
        || line.contains(" states generated")
}

fn value_or_raw(text: &str) -> Value {
    parse_value(text).unwrap_or_else(|_| Value::String(text.to_owned()))
}

/// `<Saga line 108, col 14 to line 113, col 56 of module X>` -> `Saga`;
/// `<Initial predicate>` -> `Init`.
fn action_name(raw: &str) -> String {
    let inner = raw
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim();
    if inner.starts_with("Initial predicate") {
        return "Init".into();
    }
    if let Some(i) = inner.find(" line ") {
        return inner[..i].to_owned();
    }
    inner.split_whitespace().next().unwrap_or("").to_owned()
}

/// Numbers of `... N states generated ..., M distinct states found ..., Q states left on queue.`
fn parse_counts(line: &str) -> Option<(u64, u64, u64)> {
    if !line.contains("states generated") || !line.contains("distinct states found") {
        return None;
    }
    let states = number_before(line, " states generated")?;
    let distinct = number_before(line, " distinct states found")?;
    let queue = number_before(line, " states left on queue").unwrap_or(0);
    Some((states, distinct, queue))
}

fn number_before(line: &str, marker: &str) -> Option<u64> {
    let head = &line[..line.find(marker)?];
    let tok = head
        .rsplit(|c: char| !(c.is_ascii_digit() || c == ','))
        .next()?;
    tok.replace(',', "").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(text: &str) -> (Vec<RunEvent>, Outcome) {
        let mut p = TlcParser::new();
        let mut ev: Vec<RunEvent> = text.lines().flat_map(|l| p.push_line(l)).collect();
        ev.extend(p.finish());
        (ev, p.outcome())
    }

    fn states(ev: &[RunEvent]) -> Vec<(&u32, &String, &Value)> {
        ev.iter()
            .filter_map(|e| match e {
                RunEvent::State { step, action, vars } => Some((step, action, vars)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn unsafe_config_map_trace() {
        let (ev, outcome) = run(include_str!("../tests/fixtures/tlc_unsafe.txt"));
        assert_eq!(outcome, Outcome::Violation("SingleWriter".into()));
        let s = states(&ev);
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].1, "Init");
        assert_eq!(s[1].1, "Publish");
        assert_eq!(s[2].1, "WriteConfigMap");
        assert_eq!(
            s[1].2["record"],
            json!({"gen": 1, "owner": {"k1": "s1", "k2": "s2"}})
        );
        assert_eq!(s[1].2["history"].as_array().unwrap().len(), 2);
        assert_eq!(s[2].2["configmap"], json!(1));
        assert_eq!(
            s[2].2["fenced"],
            json!({"s1": false, "s2": false, "s3": false})
        );
        assert_eq!(s[2].2["acked"], json!([]));
        // Variable order is TLC's, preserved.
        let names: Vec<&String> = s[0].2.as_object().unwrap().keys().collect();
        assert_eq!(names[0], "fenced");
        assert!(ev.iter().any(|e| matches!(
            e,
            RunEvent::Progress {
                states: 121,
                distinct: 103,
                queue: 90
            }
        )));
    }

    #[test]
    fn no_fence_trace_has_eight_states() {
        let (ev, outcome) = run(include_str!("../tests/fixtures/tlc_nofence.txt"));
        assert_eq!(outcome, Outcome::Violation("SingleWriterRange".into()));
        let s = states(&ev);
        let actions: Vec<&str> = s.iter().map(|x| x.1.as_str()).collect();
        assert_eq!(
            actions,
            [
                "Init",
                "Saga",
                "Partition",
                "Saga",
                "Saga",
                "Saga",
                "Saga",
                "Saga"
            ]
        );
        assert_eq!(s[7].2["phase"], json!("Resumed"));
        assert_eq!(s[7].2["routesTo"], json!({"d": "Dst", "i2": "Src"}));
        assert_eq!(s[7].2["reachable"], json!({"d": true, "i2": false}));
        assert_eq!(s[7].2["srcFenced"], json!(false));
    }

    #[test]
    fn clean_run_has_progress_and_ok() {
        let (ev, outcome) = run(include_str!("../tests/fixtures/tlc_small.txt"));
        assert_eq!(outcome, Outcome::Ok);
        assert!(states(&ev).is_empty());
        let progress: Vec<_> = ev
            .iter()
            .filter_map(|e| match e {
                RunEvent::Progress {
                    states,
                    distinct,
                    queue,
                } => Some((*states, *distinct, *queue)),
                _ => None,
            })
            .collect();
        assert_eq!(progress.first(), Some(&(292_276, 124_915, 46_009)));
        assert_eq!(progress.last(), Some(&(1_638_861, 655_107, 0)));
    }

    #[test]
    fn lasso_and_stuttering() {
        let text = "State 1: <Initial predicate>\n/\\ x = 0\n\nState 2: <Next line 3, col 1 to line 3, col 9 of module M>\n/\\ x = 1\n\nBack to state 1: <Next line 3, col 1 to line 3, col 9 of module M>\n";
        let (ev, _) = run(text);
        assert_eq!(ev.len(), 3);
        assert_eq!(ev[2], RunEvent::Lasso { to: Some(1) });
        let (ev, _) = run("State 1: <Initial predicate>\n/\\ x = 0\n\nStuttering\n");
        assert_eq!(ev[1], RunEvent::Lasso { to: None });
    }

    #[test]
    fn wrapped_value_and_unparseable_value() {
        let text = "State 1: <Initial predicate>\n/\\ s = {1,\n  2}\n/\\ weird = @@@\n";
        let (ev, _) = run(text);
        let s = states(&ev);
        assert_eq!(s[0].2["s"], json!([1, 2]));
        assert_eq!(s[0].2["weird"], json!("@@@"));
    }

    #[test]
    fn deadlock_temporal_and_silence() {
        assert_eq!(run("Error: Deadlock reached.\n").1, Outcome::Deadlock);
        assert_eq!(
            run("Temporal properties were violated.\n").1,
            Outcome::Temporal
        );
        assert_eq!(run("Parsing file x.tla\n").1, Outcome::Error);
        assert_eq!(Outcome::Violation("A".into()).label(), "violation:A");
    }
}
