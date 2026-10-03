//! Parsers of Rust test output.
//!
//! * `cargo nextest run --message-format libtest-json` (with
//!   `NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1`) prints libtest's JSON events, one per
//!   line, with test names `package::binary$test`.
//! * `cargo test`'s human output is the fallback when nextest is not installed.
//!
//! [`TestParser`] takes either, line by line, and tells them apart by a leading `{`.

use crate::event::TestEvent;
use serde_json::Value;

#[derive(Default)]
pub struct TestParser {
    /// Inside a `---- name stdout ----` block of the human output.
    block: Option<(String, Vec<String>)>,
}

impl TestParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_line(&mut self, line: &str) -> Vec<TestEvent> {
        if line.starts_with('{') {
            return json_event(line).into_iter().collect();
        }
        self.human_line(line)
    }

    /// Call at end of output.
    pub fn finish(&mut self) -> Vec<TestEvent> {
        self.end_block().into_iter().collect()
    }

    fn end_block(&mut self) -> Option<TestEvent> {
        let (name, mut lines) = self.block.take()?;
        while lines.last().is_some_and(|l| l.trim().is_empty()) {
            lines.pop();
        }
        Some(TestEvent::Output {
            name,
            output: lines.join("\n"),
        })
    }

    fn human_line(&mut self, line: &str) -> Vec<TestEvent> {
        let mut out = Vec::new();
        if let Some(name) = line
            .strip_prefix("---- ")
            .and_then(|r| r.strip_suffix(" stdout ----"))
        {
            out.extend(self.end_block());
            self.block = Some((name.to_owned(), Vec::new()));
            return out;
        }
        if line == "failures:" || line.starts_with("test result:") {
            out.extend(self.end_block());
        } else if let Some((_, lines)) = self.block.as_mut() {
            lines.push(line.to_owned());
            return out;
        }
        if let Some(rest) = line.strip_prefix("running ")
            && let Some(n) = rest.split_whitespace().next().and_then(|n| n.parse().ok())
            && (rest.ends_with(" tests") || rest.ends_with(" test"))
        {
            out.push(TestEvent::SuiteStarted { count: n });
        } else if let Some(rest) = line.strip_prefix("test result:") {
            out.push(summary_of(rest));
        } else if let Some(rest) = line.strip_prefix("test ")
            && let Some((name, status)) = rest.rsplit_once(" ... ")
        {
            let name = name.to_owned();
            if status == "ok" || status.starts_with("ok ") {
                out.push(TestEvent::Passed { name, secs: 0.0 });
            } else if status == "FAILED" {
                out.push(TestEvent::Failed {
                    name,
                    secs: 0.0,
                    output: String::new(),
                });
            } else if status.starts_with("ignored") {
                out.push(TestEvent::Ignored { name });
            }
        }
        out
    }
}

/// `FAILED. 1 passed; 1 failed; 1 ignored; ...; finished in 0.00s`
fn summary_of(rest: &str) -> TestEvent {
    let count = |word: &str| -> u64 {
        rest.split(';')
            .find(|part| part.trim_end().ends_with(word))
            .and_then(|part| {
                part.split_whitespace()
                    .rev()
                    .nth(1)
                    .and_then(|n| n.trim_start_matches('.').parse().ok())
            })
            .unwrap_or(0)
    };
    let secs = rest
        .rsplit_once("finished in ")
        .and_then(|(_, s)| s.trim().trim_end_matches('s').parse().ok())
        .unwrap_or(0.0);
    TestEvent::Summary {
        passed: count("passed"),
        failed: count("failed"),
        ignored: count("ignored"),
        secs,
    }
}

fn json_event(line: &str) -> Option<TestEvent> {
    let v: Value = serde_json::from_str(line).ok()?;
    let name = || v["name"].as_str().map(str::to_owned);
    let secs = v["exec_time"].as_f64().unwrap_or(0.0);
    match (v["type"].as_str()?, v["event"].as_str()?) {
        ("suite", "started") => Some(TestEvent::SuiteStarted {
            count: v["test_count"].as_u64().unwrap_or(0),
        }),
        ("suite", "ok" | "failed") => Some(TestEvent::Summary {
            passed: v["passed"].as_u64().unwrap_or(0),
            failed: v["failed"].as_u64().unwrap_or(0),
            ignored: v["ignored"].as_u64().unwrap_or(0),
            secs,
        }),
        ("test", "started") => Some(TestEvent::Started { name: name()? }),
        ("test", "ok") => Some(TestEvent::Passed {
            name: name()?,
            secs,
        }),
        ("test", "failed") => Some(TestEvent::Failed {
            name: name()?,
            secs,
            output: v["stdout"].as_str().unwrap_or("").to_owned(),
        }),
        ("test", "ignored") => Some(TestEvent::Ignored { name: name()? }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str) -> Vec<TestEvent> {
        let mut p = TestParser::new();
        let mut ev: Vec<_> = text.lines().flat_map(|l| p.push_line(l)).collect();
        ev.extend(p.finish());
        ev
    }

    #[test]
    fn nextest_libtest_json() {
        let ev = run(include_str!("../tests/fixtures/nextest_sample.jsonl"));
        assert_eq!(ev[0], TestEvent::SuiteStarted { count: 3 });
        assert!(ev.contains(&TestEvent::Ignored {
            name: "loams-specview::zz_sample$skipped".into()
        }));
        assert!(
            ev.iter()
                .any(|e| matches!(e, TestEvent::Passed { name, secs }
            if name == "loams-specview::zz_sample$passes" && *secs > 0.0))
        );
        let failed = ev.iter().find_map(|e| match e {
            TestEvent::Failed { name, output, .. } => Some((name, output)),
            _ => None,
        });
        let (name, output) = failed.unwrap();
        assert_eq!(name, "loams-specview::zz_sample$fails");
        assert!(output.contains("boom") && output.contains("before"));
        assert!(matches!(
            ev.last(),
            Some(TestEvent::Summary {
                passed: 0,
                failed: 1,
                ignored: 1,
                ..
            })
        ));
    }

    #[test]
    fn libtest_human_output() {
        let ev = run(include_str!("../tests/fixtures/libtest_sample.txt"));
        assert_eq!(ev[0], TestEvent::SuiteStarted { count: 3 });
        assert!(ev.contains(&TestEvent::Ignored {
            name: "skipped".into()
        }));
        assert!(ev.contains(&TestEvent::Passed {
            name: "passes".into(),
            secs: 0.0
        }));
        assert!(ev.contains(&TestEvent::Failed {
            name: "fails".into(),
            secs: 0.0,
            output: String::new()
        }));
        let out = ev.iter().find_map(|e| match e {
            TestEvent::Output { name, output } => Some((name, output)),
            _ => None,
        });
        let (name, output) = out.unwrap();
        assert_eq!(name, "fails");
        assert!(output.starts_with("before") && output.contains("boom"));
        assert!(!output.contains("failures:"));
        assert_eq!(
            ev.last(),
            Some(&TestEvent::Summary {
                passed: 1,
                failed: 1,
                ignored: 1,
                secs: 0.0
            })
        );
    }

    #[test]
    fn noise_is_ignored() {
        assert!(run("   Compiling x v0.1\n{\"type\":\"other\"}\nnot json").is_empty());
    }
}
