//! Spec-specific views of one trace step. Unknown specs fall back to a
//! variable table.

use super::trace::VarTable;
use crate::model::TraceStep;
use crate::viewmodel::{MAIN_PHASES, ROLLBACK_PHASES, ReshardVm, ShardMapVm, Write};
use leptos::prelude::*;
use serde_json::Value;

/// A compact one-line rendering of a JSON value (TLA+-like).
pub fn compact(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string().to_uppercase(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{s}\""),
        Value::Array(a) => format!(
            "{{{}}}",
            a.iter().map(compact).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(o) => {
            format!(
                "[{}]",
                o.iter()
                    .map(|(k, v)| format!("{k} ↦ {}", compact(v)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

/// Stable colour slot of a shard or store name.
fn slot(name: &str) -> usize {
    name.bytes()
        .fold(0usize, |a, b| a.wrapping_mul(31).wrapping_add(b as usize))
        % 6
}

#[component]
pub fn SpecView(spec: String, step: TraceStep, prev: Option<TraceStep>) -> impl IntoView {
    let vm_prev = prev.as_ref().map(|p| p.vars.clone());
    let all = step.clone();
    let body = match spec.as_str() {
        "ShardMap" => ShardMapVm::from_vars(&step.vars).map(|vm| {
            let prev = vm_prev.as_ref().and_then(ShardMapVm::from_vars);
            view! { <ShardMapView vm=vm prev=prev /> }.into_any()
        }),
        "ReshardCutover" => ReshardVm::from_vars(&step.vars).map(|vm| {
            let prev = vm_prev.as_ref().and_then(ReshardVm::from_vars);
            view! { <ReshardView vm=vm prev=prev /> }.into_any()
        }),
        _ => None,
    };
    match body {
        Some(b) => view! {
            {b}
            <details class="rawvars">
                <summary>"All variables"</summary>
                <VarTable step=all prev=prev />
            </details>
        }
        .into_any(),
        None => view! { <VarTable step=all prev=prev /> }.into_any(),
    }
}

fn write_chip(w: &Write, show_at: bool) -> impl IntoView + use<> {
    let title = format!(
        "write {} on {}{}{}",
        w.id,
        w.key,
        w.at.as_ref()
            .map(|a| format!(" via {a}"))
            .unwrap_or_default(),
        if w.acked {
            ", acknowledged to the client"
        } else {
            ", not acknowledged"
        }
    );
    let at = if show_at { w.at.clone() } else { None };
    view! {
        <span class="write" class:acked=w.acked class:stale=!w.fresh title=title>
            <b>{format!("w{}", w.id)}</b>
            " "
            {w.key.clone()}
            {at.map(|a| view! { <span class="via">{format!(" via {a}")}</span> })}
            {w.acked.then(|| view! { <span class="ack" aria-label="acknowledged">" ✓"</span> })}
            {(!w.fresh).then(|| view! { <span class="stalemark" title="the shard lagged behind the key's history">" stale"</span> })}
        </span>
    }
}

#[component]
fn ShardMapView(vm: ShardMapVm, prev: Option<ShardMapVm>) -> impl IntoView {
    let has_prev = prev.is_some();
    let cm_changed = prev.as_ref().is_some_and(|p| p.configmap != vm.configmap);
    let rec_changed = prev.as_ref().is_some_and(|p| p.record_gen != vm.record_gen);
    let gens = vm.owners.len();

    let grid_head = (0..gens)
        .map(|g| {
            let g_i = g as i64;
            view! {
                <th class:cur=(g_i == vm.record_gen)>
                    {format!("gen {g}")}
                    {(g_i == vm.record_gen).then(|| view! { <span class="badge">"record"</span> })}
                    {(g_i == vm.configmap).then(|| view! { <span class="badge cm">"ConfigMap"</span> })}
                </th>
            }
        })
        .collect_view();
    let grid_rows = vm
        .keys
        .iter()
        .enumerate()
        .map(|(k, key)| {
            let cells = (0..gens)
                .map(|g| {
                    let owner = vm.owners[g][k].clone();
                    let moved = vm.moved(g, k);
                    let class = format!("chip c{}", slot(&owner));
                    let is_new = has_prev
                        && g + 1 == gens
                        && !prev.as_ref().is_some_and(|p| p.owners.len() == gens);
                    view! {
                        <td class:moved=moved class:chg=is_new>
                            <span class=class>{owner}</span>
                        </td>
                    }
                })
                .collect_view();
            view! { <tr><th class="mono">{key.clone()}</th>{cells}</tr> }
        })
        .collect_view();

    let instances = vm
        .instances
        .iter()
        .map(|(name, g)| {
            let lag = *g < vm.record_gen;
            let changed = prev.as_ref().is_some_and(|p| {
                p.instances.iter().find(|(n, _)| n == name).map(|(_, x)| x) != Some(g)
            });
            view! {
                <div class="node" class:chg=changed class:lag=lag>
                    <div class="mono"><b>{name.clone()}</b></div>
                    <div>"applied gen " <b>{*g}</b></div>
                    {lag.then(|| view! { <div class="sub">"behind the record"</div> })}
                </div>
            }
        })
        .collect_view();

    let shards = vm
        .shards
        .iter()
        .map(|s| {
            let changed_fence = prev.as_ref().is_some_and(|p| p.shards.iter().find(|x| x.name == s.name).map(|x| x.fenced) != Some(s.fenced));
            let before: usize = prev
                .as_ref()
                .and_then(|p| p.shards.iter().find(|x| x.name == s.name))
                .map(|x| x.writes.len())
                .unwrap_or(s.writes.len());
            let grew = has_prev && s.writes.len() != before;
            let chips = s.writes.iter().map(|w| write_chip(w, true)).collect_view();
            view! {
                <div class="node shard" class:fenced=s.fenced class:chg=grew>
                    <div class="shardhead">
                        <span class=format!("chip c{}", slot(&s.name))>{s.name.clone()}</span>
                        <span class="lock" class:chg=changed_fence>{if s.fenced { "fenced" } else { "open" }}</span>
                    </div>
                    <div class="writes">
                        {if s.writes.is_empty() { view! { <span class="sub">"no writes"</span> }.into_any() } else { chips.into_any() }}
                    </div>
                </div>
            }
        })
        .collect_view();

    view! {
        <div class="spec shardmap">
            <div class="facts-row">
                <span class="fact" class:chg=rec_changed>"record generation " <b>{vm.record_gen}</b></span>
                <span class="fact" class:chg=cm_changed>"ConfigMap generation " <b>{vm.configmap}</b></span>
                <span class="fact">"next write " <b>{format!("w{}", vm.next_write)}</b></span>
            </div>
            <h3>"Shard map by generation"</h3>
            <div class="scroll">
                <table class="grid">
                    <thead><tr><th>"key"</th>{grid_head}</tr></thead>
                    <tbody>{grid_rows}</tbody>
                </table>
            </div>
            <h3>"Router instances"</h3>
            <div class="nodes">{instances}</div>
            <h3>"Shards and their writes " <small>"(✓ acknowledged to a client)"</small></h3>
            <div class="nodes">{shards}</div>
        </div>
    }
}

fn phase_index(phase: &str) -> Option<usize> {
    MAIN_PHASES.iter().position(|p| *p == phase)
}

#[component]
fn ReshardView(vm: ReshardVm, prev: Option<ReshardVm>) -> impl IntoView {
    let has_prev = prev.is_some();
    let phase_changed = prev.as_ref().is_some_and(|p| p.phase != vm.phase);
    let cur = phase_index(&vm.phase);
    let timeline = MAIN_PHASES
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let done = cur.is_some_and(|c| i < c) || vm.phase == "Finalized" && i < MAIN_PHASES.len();
            let active = *name == vm.phase;
            view! { <li class:done=done && !active class:active=active class:chg=active && phase_changed>{*name}</li> }
        })
        .collect_view();
    let rollback = ROLLBACK_PHASES
        .iter()
        .map(|name| {
            let active = *name == vm.phase;
            view! { <li class="rb" class:active=active class:chg=active && phase_changed>{*name}</li> }
        })
        .collect_view();

    // Diagram geometry.
    let n = vm.nodes.len().max(1);
    let height = (n as i32 * 76 + 40).max(260);
    let src_y = height / 4;
    let dst_y = height * 3 / 4;
    let store_y = |s: &str| if s == "Src" { src_y } else { dst_y };
    let edges = vm
        .nodes
        .iter()
        .enumerate()
        .map(|(i, nd)| {
            let y = 50 + i as i32 * 76;
            let ty = store_y(&nd.routes_to);
            let was = prev
                .as_ref()
                .and_then(|p| p.nodes.iter().find(|x| x.name == nd.name))
                .map(|x| x.routes_to.clone());
            let rerouted = was.is_some_and(|w| w != nd.routes_to);
            let bad = vm.two_writers() && !nd.paused;
            view! {
                <line
                    class="edge"
                    class:paused=nd.paused
                    class:bad=bad
                    class:chg=rerouted
                    class:dst=(nd.routes_to == "Dst")
                    x1="210"
                    y1=y.to_string()
                    x2="470"
                    y2=ty.to_string()
                />
            }
        })
        .collect_view();
    let nodes = vm
        .nodes
        .iter()
        .enumerate()
        .map(|(i, nd)| {
            let y = 50 + i as i32 * 76;
            let before = prev.as_ref().and_then(|p| p.nodes.iter().find(|x| x.name == nd.name));
            let changed = before.is_some_and(|b| b != nd);
            let status = match (nd.reachable, nd.paused) {
                (false, true) => "unreachable, paused",
                (false, false) => "unreachable",
                (true, true) => "paused",
                (true, false) => "running",
            };
            view! {
                <g class="inst" class:unreach=!nd.reachable class:paused=nd.paused class:chg=changed>
                    <rect x="30" y=(y - 26).to_string() width="180" height="52" rx="8" />
                    <text x="44" y=(y - 5).to_string() class="t1">{nd.name.clone()}</text>
                    <text x="44" y=(y + 14).to_string() class="t2">{format!("{status} → {}", nd.routes_to)}</text>
                </g>
            }
        })
        .collect_view();
    let store = |name: &'static str,
                 y: i32,
                 fenced: bool,
                 writable: bool,
                 count: usize,
                 before: Option<(bool, usize)>| {
        let changed = before.is_some_and(|b| b != (fenced, count));
        view! {
            <g class="store" class:fenced=fenced class:writable=writable class:chg=changed>
                <rect x="470" y=(y - 38).to_string() width="170" height="76" rx="10" />
                <text x="486" y=(y - 12).to_string() class="t1">{format!("{name} store")}</text>
                <text x="486" y=(y + 8).to_string() class="t2">{if fenced { "fenced (NOLOGIN)" } else { "unfenced" }}</text>
                <text x="486" y=(y + 26).to_string() class="t2">{format!("{count} writes · {}", if writable { "writable" } else { "no writer" })}</text>
            </g>
        }
    };
    let src = store(
        "Src",
        src_y,
        vm.src_fenced,
        vm.src_writable,
        vm.src_data.len(),
        prev.as_ref().map(|p| (p.src_fenced, p.src_data.len())),
    );
    let dst = store(
        "Dst",
        dst_y,
        vm.dst_fenced,
        vm.dst_writable,
        vm.dst_data.len(),
        prev.as_ref().map(|p| (p.dst_fenced, p.dst_data.len())),
    );

    let src_chips = vm
        .src_data
        .iter()
        .map(|w| write_chip(w, false))
        .collect_view();
    let dst_chips = vm
        .dst_data
        .iter()
        .map(|w| write_chip(w, false))
        .collect_view();
    let crash_changed = prev.as_ref().is_some_and(|p| p.crashed != vm.crashed);

    view! {
        <div class="spec reshard">
            <ol class="phases">{timeline}</ol>
            <ol class="phases rbrow">{rollback}</ol>
            <div class="facts-row">
                <span class="fact" class:chg=crash_changed>
                    "saga " <b>{if vm.crashed { "crashed" } else { "up" }}</b>
                    " (" {vm.crashes} " crashes)"
                </span>
                {vm.two_writers().then(|| view! {
                    <span class="fact alarm">"both stores can take writes: SingleWriterRange is violated"</span>
                })}
            </div>
            <svg class="diagram" viewBox=format!("0 0 670 {height}") role="img" aria-label="instances and the stores they route to">
                {edges}
                {nodes}
                {src}
                {dst}
            </svg>
            <h3>"Store contents " <small>"(✓ acknowledged to a client)"</small></h3>
            <div class="nodes">
                <div class="node shard" class:fenced=vm.src_fenced>
                    <div class="shardhead"><span class="chip c0">"Src"</span><span class="lock">{if vm.src_fenced { "fenced" } else { "open" }}</span></div>
                    <div class="writes">{if vm.src_data.is_empty() { view! { <span class="sub">"empty"</span> }.into_any() } else { src_chips.into_any() }}</div>
                </div>
                <div class="node shard" class:fenced=vm.dst_fenced>
                    <div class="shardhead"><span class="chip c3">"Dst"</span><span class="lock">{if vm.dst_fenced { "fenced" } else { "open" }}</span></div>
                    <div class="writes">{if vm.dst_data.is_empty() { view! { <span class="sub">"empty"</span> }.into_any() } else { dst_chips.into_any() }}</div>
                </div>
            </div>
            {(!has_prev).then(|| view! { <p class="sub">"Initial state."</p> })}
        </div>
    }
}
