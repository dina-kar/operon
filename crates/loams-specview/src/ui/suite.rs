//! The suite panel: spec variants and Rust tests with counts and timings.

use crate::model::{Model, Status, TestRow, TestStatus, fmt_count};
use leptos::prelude::*;

#[component]
pub fn SuitePanel(model: RwSignal<Model>, selected: RwSignal<Option<usize>>) -> impl IntoView {
    let variants = move || model.with(|m| m.variants.clone());
    let spec_summary = move || {
        let (p, f, r) = model.with(|m| m.variant_counts());
        format!("{p} passed, {f} failed, {r} running")
    };
    let test_summary = move || {
        let c = model.with(|m| m.test_counts());
        format!(
            "{} passed, {} failed, {} ignored, {} running",
            c.passed, c.failed, c.ignored, c.running
        )
    };
    let tests = move || model.with(|m| m.tests.clone());
    view! {
        <h2>"TLA+ specs" <small>{spec_summary}</small></h2>
        <ul class="rows">
            {move || {
                variants()
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let class = match v.status {
                            Status::Running => "dot run",
                            Status::Pass => "dot pass",
                            Status::Fail => "dot fail",
                        };
                        let has_trace = !v.trace.is_empty();
                        let detail = match (&v.verdict, v.progress) {
                            (Some(r), _) => {
                                format!("{} states, {:.1}s", fmt_count(r.states), r.secs)
                            }
                            (None, Some((_, distinct, queue))) => {
                                format!("{} distinct, {} queued", fmt_count(distinct), fmt_count(queue))
                            }
                            (None, None) => "starting".to_owned(),
                        };
                        let outcome = match &v.verdict {
                            Some(r) if r.actual == v.expect => format!("expected {}", v.expect),
                            Some(r) => format!("expected {}, got {}", v.expect, r.actual),
                            None => format!("expect {}", v.expect),
                        };
                        view! {
                            <li
                                class="row"
                                                                class:sel=move || selected.get() == Some(i)
                                on:click=move |_| selected.set(Some(i))
                            >
                                <span class=class></span>
                                <div class="grow">
                                    <div class="name">
                                        <b>{v.spec.clone()}</b>
                                        " "
                                        <span class="mono">{v.variant.clone()}</span>
                                    </div>
                                    <div class="sub">{outcome} " · " {detail}</div>
                                </div>
                                {has_trace.then(|| view! { <span class="tag">"trace"</span> })}
                            </li>
                        }
                    })
                    .collect_view()
            }}
        </ul>
        <h2>"Rust tests" <small>{test_summary}</small></h2>
        {move || {
            let rows = tests();
            if rows.is_empty() {
                return view! { <p class="empty">"No Rust tests in this run."</p> }.into_any();
            }
            view! { <ul class="rows">{rows.into_iter().map(test_row).collect_view()}</ul> }.into_any()
        }}
    }
}

fn test_row(t: TestRow) -> impl IntoView {
    let class = match t.status {
        TestStatus::Running => "dot run",
        TestStatus::Passed => "dot pass",
        TestStatus::Failed => "dot fail",
        TestStatus::Ignored => "dot skip",
    };
    let (group, name) = t.split_name();
    let (group, name) = (group.to_owned(), name.to_owned());
    let timing = (t.secs > 0.0).then(|| format!("{:.2}s", t.secs));
    let output = (!t.output.is_empty()).then(|| t.output.clone());
    view! {
        <li class="row test">
            <span class=class></span>
            <div class="grow">
                <div class="name">
                    <span class="mono">{name}</span>
                </div>
                {(!group.is_empty()).then(|| view! { <div class="sub">{group}</div> })}
                {output
                    .map(|o| {
                        view! {
                            <details open=t.status == TestStatus::Failed>
                                <summary>"output"</summary>
                                <pre class="out">{o}</pre>
                            </details>
                        }
                    })}
            </div>
            <span class="sub">{timing}</span>
        </li>
    }
}
