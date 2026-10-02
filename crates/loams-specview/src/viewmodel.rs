//! What the spec-specific views draw, extracted from a trace step's variables.
//! Pure functions over `serde_json::Value`, so they are tested natively; the
//! Leptos components only lay the result out.

use serde_json::Value;
use std::collections::BTreeSet;

/// A client write as the specs model it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub id: i64,
    pub key: String,
    /// The instance that took the write (ShardMap only).
    pub at: Option<String>,
    /// False when the shard lagged behind the key's history (ShardMap only).
    pub fresh: bool,
    pub acked: bool,
}

fn writes(v: Option<&Value>, acked: &BTreeSet<i64>) -> Vec<Write> {
    let mut out: Vec<Write> = v
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|w| {
                    let id = w.get("id")?.as_i64()?;
                    Some(Write {
                        id,
                        key: w.get("key")?.as_str()?.to_owned(),
                        at: w.get("at").and_then(Value::as_str).map(str::to_owned),
                        fresh: w.get("fresh").and_then(Value::as_bool).unwrap_or(true),
                        acked: acked.contains(&id),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_by_key(|w| w.id);
    out
}

fn acked_ids(vars: &Value) -> BTreeSet<i64> {
    vars.get("acked")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|w| w.get("id")?.as_i64()).collect())
        .unwrap_or_default()
}

/// Names (keys of an object variable), sorted.
fn names(v: Option<&Value>) -> Vec<String> {
    let mut n: Vec<String> = v
        .and_then(Value::as_object)
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    n.sort();
    n
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardVm {
    pub name: String,
    pub fenced: bool,
    pub writes: Vec<Write>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardMapVm {
    pub keys: Vec<String>,
    /// `owners[g][k]`: the shard that owns `keys[k]` in generation g.
    pub owners: Vec<Vec<String>>,
    pub record_gen: i64,
    pub configmap: i64,
    /// (instance, applied generation)
    pub instances: Vec<(String, i64)>,
    pub shards: Vec<ShardVm>,
    pub next_write: i64,
}

impl ShardMapVm {
    pub fn from_vars(vars: &Value) -> Option<ShardMapVm> {
        let record = vars.get("record")?;
        let keys = names(record.get("owner"));
        let owners = vars
            .get("history")?
            .as_array()?
            .iter()
            .map(|m| {
                keys.iter()
                    .map(|k| m.get(k).and_then(Value::as_str).unwrap_or("?").to_owned())
                    .collect()
            })
            .collect();
        let acked = acked_ids(vars);
        let fenced = vars.get("fenced");
        let data = vars.get("data");
        let shards = names(fenced)
            .into_iter()
            .map(|name| ShardVm {
                fenced: fenced
                    .and_then(|f| f.get(&name))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                writes: writes(data.and_then(|d| d.get(&name)), &acked),
                name,
            })
            .collect();
        let applied = vars.get("applied");
        let instances = names(applied)
            .into_iter()
            .map(|n| {
                let g = applied
                    .and_then(|a| a.get(&n))
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                (n, g)
            })
            .collect();
        Some(ShardMapVm {
            keys,
            owners,
            record_gen: record.get("gen")?.as_i64()?,
            configmap: vars.get("configmap")?.as_i64()?,
            instances,
            shards,
            next_write: vars.get("nextWrite").and_then(Value::as_i64).unwrap_or(0),
        })
    }

    /// Whether `keys[k]` changed owner going into generation `g`.
    pub fn moved(&self, g: usize, k: usize) -> bool {
        g > 0 && self.owners[g][k] != self.owners[g - 1][k]
    }
}

/// The saga's phases in order, then the rollback branch.
pub const MAIN_PHASES: [&str; 8] = [
    "Copy",
    "CatchUp",
    "Paused",
    "Fenced",
    "CutOver",
    "Published",
    "Resumed",
    "Finalized",
];
pub const ROLLBACK_PHASES: [&str; 2] = ["RollingBack", "RolledBack"];

#[derive(Debug, Clone, PartialEq)]
pub struct NodeVm {
    pub name: String,
    pub reachable: bool,
    pub paused: bool,
    /// "Src" or "Dst"
    pub routes_to: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReshardVm {
    pub phase: String,
    pub nodes: Vec<NodeVm>,
    pub src_fenced: bool,
    pub dst_fenced: bool,
    pub src_data: Vec<Write>,
    pub dst_data: Vec<Write>,
    pub crashed: bool,
    pub crashes: i64,
    /// A store takes client writes when it is unfenced and a running instance routes to it.
    pub src_writable: bool,
    pub dst_writable: bool,
}

impl ReshardVm {
    pub fn from_vars(vars: &Value) -> Option<ReshardVm> {
        let phase = vars.get("phase")?.as_str()?.to_owned();
        let routes = vars.get("routesTo");
        let reachable = vars.get("reachable");
        let paused = vars.get("paused");
        let flag = |v: Option<&Value>, n: &str| {
            v.and_then(|o| o.get(n))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        };
        let nodes: Vec<NodeVm> = names(routes)
            .into_iter()
            .map(|name| NodeVm {
                reachable: flag(reachable, &name),
                paused: flag(paused, &name),
                routes_to: routes
                    .and_then(|r| r.get(&name))
                    .and_then(Value::as_str)
                    .unwrap_or("Src")
                    .to_owned(),
                name,
            })
            .collect();
        let acked = acked_ids(vars);
        let src_fenced = vars
            .get("srcFenced")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let dst_fenced = vars
            .get("dstFenced")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let takes = |store: &str, fenced: bool| {
            !fenced && nodes.iter().any(|n| n.routes_to == store && !n.paused)
        };
        Some(ReshardVm {
            src_writable: takes("Src", src_fenced),
            dst_writable: takes("Dst", dst_fenced),
            phase,
            src_fenced,
            dst_fenced,
            src_data: writes(vars.get("srcData"), &acked),
            dst_data: writes(vars.get("dstData"), &acked),
            crashed: vars
                .get("crashed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            crashes: vars.get("crashes").and_then(Value::as_i64).unwrap_or(0),
            nodes,
        })
    }

    /// Both stores can take writes: the invariant `SingleWriterRange` is broken here.
    pub fn two_writers(&self) -> bool {
        self.src_writable && self.dst_writable
    }

    pub fn rolling_back(&self) -> bool {
        ROLLBACK_PHASES.contains(&self.phase.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use crate::tlc::TlcParser;

    fn last_state(fixture: &str) -> Value {
        let mut p = TlcParser::new();
        let mut m = Model::default();
        m.apply(crate::event::Event::Run(
            crate::event::RunEvent::RunStarted {
                suite: "t".into(),
                spec: "s".into(),
                variant: "v".into(),
                expect: "ok".into(),
            },
        ));
        let mut events: Vec<_> = fixture.lines().flat_map(|l| p.push_line(l)).collect();
        events.extend(p.finish());
        for e in events {
            m.apply(crate::event::Event::Run(e));
        }
        m.variants[0].trace.last().unwrap().vars.clone()
    }

    #[test]
    fn shard_map_view_of_the_unsafe_trace() {
        let vm = ShardMapVm::from_vars(&last_state(include_str!(
            "../tests/fixtures/tlc_unsafe.txt"
        )))
        .unwrap();
        assert_eq!(vm.keys, ["k1", "k2"]);
        assert_eq!(vm.owners, [["s1", "s1"], ["s1", "s2"]]);
        assert!(vm.moved(1, 1) && !vm.moved(1, 0) && !vm.moved(0, 1));
        assert_eq!((vm.record_gen, vm.configmap), (1, 1));
        assert_eq!(vm.instances, [("i1".to_owned(), 0), ("i2".to_owned(), 0)]);
        assert_eq!(vm.shards.len(), 3);
        assert!(vm.shards.iter().all(|s| !s.fenced && s.writes.is_empty()));
    }

    #[test]
    fn writes_are_marked_acked() {
        let vars = serde_json::json!({
            "record": {"gen": 0, "owner": {"k1": "s1"}},
            "history": [{"k1": "s1"}],
            "configmap": 0,
            "applied": {"i1": 0},
            "fenced": {"s1": true},
            "data": {"s1": [{"id": 2, "key": "k1", "at": "i1", "fresh": false}, {"id": 1, "key": "k1", "at": "i1", "fresh": true}]},
            "acked": [{"id": 1, "key": "k1", "at": "i1", "fresh": true}],
            "nextWrite": 3
        });
        let vm = ShardMapVm::from_vars(&vars).unwrap();
        let w = &vm.shards[0].writes;
        assert_eq!((w[0].id, w[0].acked, w[0].fresh), (1, true, true));
        assert_eq!((w[1].id, w[1].acked, w[1].fresh), (2, false, false));
        assert!(vm.shards[0].fenced);
    }

    #[test]
    fn reshard_view_of_the_no_fence_trace_has_two_writers() {
        let vm = ReshardVm::from_vars(&last_state(include_str!(
            "../tests/fixtures/tlc_nofence.txt"
        )))
        .unwrap();
        assert_eq!(vm.phase, "Resumed");
        assert_eq!(vm.nodes.len(), 2);
        let d = &vm.nodes[0];
        assert_eq!(
            (d.name.as_str(), d.routes_to.as_str(), d.reachable, d.paused),
            ("d", "Dst", true, false)
        );
        let i2 = &vm.nodes[1];
        assert_eq!((i2.routes_to.as_str(), i2.reachable), ("Src", false));
        assert!(vm.two_writers());
        assert!(!vm.rolling_back());
    }

    #[test]
    fn missing_variables_give_none() {
        assert!(ShardMapVm::from_vars(&serde_json::json!({"x": 1})).is_none());
        assert!(ReshardVm::from_vars(&serde_json::json!({"x": 1})).is_none());
    }
}
