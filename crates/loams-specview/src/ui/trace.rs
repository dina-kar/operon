//! The detail pane: the trace player and the spec-specific view of one step.

use super::views;
use crate::model::{Model, Status, TraceStep, VariantRun};
use leptos::leptos_dom::helpers::{IntervalHandle, set_interval_with_handle};
use leptos::prelude::*;
use serde_json::Value;
use std::time::Duration;

#[component]
pub fn Detail(model: RwSignal<Model>, selected: RwSignal<Option<usize>>) -> impl IntoView {
    let current = Memo::new(move |_| {
        selected
            .get()
            .and_then(|i| model.with(|m| m.variants.get(i).cloned()))
    });
    view! {
        {move || match current.get() {
            None => view! {
                <div class="placeholder">
                    <h2>"No spec selected"</h2>
                    <p>"Counterexample traces open here as soon as a violation is found."</p>
                </div>
            }
            .into_any(),
            Some(v) if v.trace.is_empty() => view! { <Summary v=v /> }.into_any(),
            Some(v) => view! { <Player v=v /> }.into_any(),
        }}
    }
}

#[component]
fn Summary(v: VariantRun) -> impl IntoView {
    let verdict = v.verdict.clone();
    let heading = match v.status {
        Status::Running => "Checking",
        Status::Pass => "Passed",
        Status::Fail => "Failed",
    };
    view! {
        <div class="card">
            <h2>{heading} " " <span class="mono">{v.variant.clone()}</span></h2>
            <p class="sub">{v.spec.clone()} " · expected " <b>{v.expect.clone()}</b></p>
            {match verdict {
                Some(r) => view! {
                    <dl class="facts">
                        <dt>"outcome"</dt><dd>{r.actual}</dd>
                        <dt>"distinct states"</dt><dd>{r.states}</dd>
                        <dt>"time"</dt><dd>{format!("{:.2}s", r.secs)}</dd>
                    </dl>
                    <p class="sub">"No counterexample: every invariant held over the whole state space."</p>
                }
                .into_any(),
                None => view! {
                    <dl class="facts">
                        {v.progress.map(|(g, d, q)| view! {
                            <dt>"generated"</dt><dd>{g}</dd>
                            <dt>"distinct"</dt><dd>{d}</dd>
                            <dt>"queue"</dt><dd>{q}</dd>
                        })}
                    </dl>
                    <p class="sub">"TLC is still running."</p>
                }
                .into_any(),
            }}
        </div>
    }
}

#[component]
fn Player(v: VariantRun) -> impl IntoView {
    let steps = StoredValue::new(v.trace.clone());
    let n = v.trace.len();
    let start = use_context::<super::Link>()
        .and_then(|l| l.step)
        .unwrap_or(0);
    let step = RwSignal::new(start.min(n - 1));
    let playing = RwSignal::new(false);
    let at_end = move || step.get() + 1 >= n;

    // One interval while playing; stops at the last state.
    let handle = StoredValue::new(None::<IntervalHandle>);
    Effect::new(move |_| {
        if let Some(h) = handle.get_value() {
            h.clear();
            handle.set_value(None);
        }
        if playing.get() {
            let h = set_interval_with_handle(
                move || {
                    if step.get_untracked() + 1 < n {
                        step.update(|s| *s += 1);
                    } else {
                        playing.set(false);
                    }
                },
                Duration::from_millis(900),
            )
            .ok();
            handle.set_value(h);
        }
    });
    on_cleanup(move || {
        if let Some(h) = handle.get_value() {
            h.clear();
        }
    });

    let current = move || steps.with_value(|s| s.get(step.get()).cloned());
    let previous =
        move || steps.with_value(|s| step.get().checked_sub(1).and_then(|i| s.get(i).cloned()));
    let lasso = v.lasso;
    let violated = v
        .verdict
        .as_ref()
        .and_then(|r| r.actual.strip_prefix("violation:").map(str::to_owned));
    let spec = v.spec.clone();
    let title = format!("{} · {}", v.spec, v.variant);
    let expect = v.expect.clone();

    view! {
        <div class="card">
            <div class="player-head">
                <div>
                    <h2>{title}</h2>
                    <p class="sub">"counterexample · expected " <b>{expect}</b> " · " {n} " states"</p>
                </div>
            </div>
            <div class="controls">
                <button title="first" on:click=move |_| step.set(0)>"|<"</button>
                <button title="back" on:click=move |_| step.update(|s| *s = s.saturating_sub(1))>"<"</button>
                <button class="primary" on:click=move |_| {
                    if at_end() {
                        step.set(0);
                    }
                    playing.update(|p| *p = !*p);
                }>{move || if playing.get() { "Pause" } else { "Play" }}</button>
                <button title="forward" on:click=move |_| step.update(|s| *s = (*s + 1).min(n - 1))>">"</button>
                <button title="last" on:click=move |_| step.set(n - 1)>">|"</button>
                <input
                    type="range"
                    min="0"
                    max=(n - 1).to_string()
                    prop:value=move || step.get()
                    on:input=move |e| {
                        if let Ok(i) = event_target_value(&e).parse::<usize>() {
                            step.set(i.min(n - 1));
                        }
                    }
                />
                <span class="mono">{move || format!("{} / {}", step.get() + 1, n)}</span>
            </div>
            <ol class="timeline">
                {(0..n)
                    .map(|i| {
                        let label = steps.with_value(|s| s[i].action.clone());
                        let title = label.clone();
                        let past = move || step.get() > i;
                        view! {
                            <li
                                class:cur=move || step.get() == i
                                class:past=past
                                title=title
                                on:click=move |_| step.set(i)
                            >
                                <span class="n">{i + 1}</span>
                                <span class="a">{label}</span>
                            </li>
                        }
                    })
                    .collect_view()}
            </ol>
            {move || current().map(|cur| view! {
                <div class="stephead">
                    <span class="action">{cur.action.clone()}</span>
                    <span class="sub">
                        {if cur.changed.is_empty() || previous().is_none() {
                            "initial state".to_owned()
                        } else {
                            format!("changed: {}", cur.changed.join(", "))
                        }}
                    </span>
                    {(step.get() + 1 == n).then(|| violated.clone().map(|inv| view! {
                        <span class="lasso">{format!("violates {inv}")}</span>
                    }))}
                    {(step.get() + 1 == n).then(|| lasso.map(|l| view! {
                        <span class="lasso">
                            {match l {
                                Some(to) => format!("then back to state {to}"),
                                None => "then stutters forever".to_owned(),
                            }}
                        </span>
                    }))}
                </div>
                <views::SpecView spec=spec.clone() step=cur prev=previous() />
            })}
        </div>
    }
}

/// The generic view: one row per variable, changed ones highlighted.
#[component]
pub fn VarTable(step: TraceStep, prev: Option<TraceStep>) -> impl IntoView {
    let rows: Vec<(String, Value)> = step
        .vars
        .as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let has_prev = prev.is_some();
    let changed = step.changed.clone();
    view! {
        <table class="vars">
            <thead><tr><th>"variable"</th><th>"value"</th></tr></thead>
            <tbody>
                {rows
                    .into_iter()
                    .map(|(k, v)| {
                        let is_changed = has_prev && changed.contains(&k);
                        view! {
                            <tr class:chg=is_changed>
                                <td class="mono">{k}</td>
                                <td class="mono">{views::compact(&v)}</td>
                            </tr>
                        }
                    })
                    .collect_view()}
            </tbody>
        </table>
    }
}
