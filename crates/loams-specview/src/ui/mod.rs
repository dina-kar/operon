//! The Leptos frontend (client-side rendered, built to wasm by `trunk`).

mod suite;
mod trace;
mod views;

use crate::event::Event;
use crate::model::Model;
use leptos::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::Closure;
use web_sys::{EventSource, MessageEvent};

/// Subscribe to `/api/events`, folding every event into `model`.
fn connect(model: RwSignal<Model>, selected: RwSignal<Option<usize>>, connected: RwSignal<bool>) {
    let Ok(source) = EventSource::new("/api/events") else {
        return;
    };
    let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |m: MessageEvent| {
        let Some(text) = m.data().as_string() else {
            return;
        };
        match Event::from_line(&text) {
            Ok(event) => model.update(|model| model.apply(event)),
            Err(e) => web_sys::console::warn_1(&format!("bad event: {e}: {text}").into()),
        }
    });
    source.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    on_message.forget();

    let on_reset = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
        model.set(Model::default());
        selected.set(None);
    });
    let _ = source.add_event_listener_with_callback("reset", on_reset.as_ref().unchecked_ref());
    on_reset.forget();

    let on_open = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| connected.set(true));
    source.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    on_open.forget();
    let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| connected.set(false));
    source.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    on_error.forget();
}

fn start_run() {
    if let Some(w) = web_sys::window() {
        let init = web_sys::RequestInit::new();
        init.set_method("POST");
        let _ = w.fetch_with_str_and_init("/api/run", &init);
    }
}

fn toggle_theme() {
    let Some(root) = web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.document_element())
    else {
        return;
    };
    let dark_now = match root.get_attribute("data-theme").as_deref() {
        Some("dark") => true,
        Some("light") => false,
        _ => web_sys::window()
            .and_then(|w| w.match_media("(prefers-color-scheme: dark)").ok().flatten())
            .is_some_and(|m| m.matches()),
    };
    let _ = root.set_attribute("data-theme", if dark_now { "light" } else { "dark" });
}

/// Deep link: `?variant=NoFence&step=8` opens the first trace whose spec or variant
/// name contains `variant` (the spec name also matches), at that 1-based state.
#[derive(Clone, Default)]
pub(crate) struct Link {
    pub variant: Option<String>,
    pub step: Option<usize>,
}

fn read_link() -> Link {
    let search = web_sys::window()
        .and_then(|w| w.location().search().ok())
        .unwrap_or_default();
    let mut link = Link::default();
    for pair in search.trim_start_matches('?').split('&') {
        match pair.split_once('=') {
            Some(("variant", v)) if !v.is_empty() => link.variant = Some(v.to_owned()),
            Some(("step", n)) => link.step = n.parse::<usize>().ok().map(|n| n.saturating_sub(1)),
            _ => {}
        }
    }
    link
}

#[component]
pub fn App() -> impl IntoView {
    let model = RwSignal::new(Model::default());
    let selected = RwSignal::new(None::<usize>);
    let connected = RwSignal::new(false);
    connect(model, selected, connected);
    let link = read_link();
    provide_context(link.clone());

    // Show the first counterexample as soon as one arrives.
    Effect::new(move |_| {
        if selected.get_untracked().is_none() {
            let first = model.with(|m| {
                m.variants.iter().position(|v| {
                    !v.trace.is_empty()
                        && link
                            .variant
                            .as_ref()
                            .is_none_or(|q| v.variant.contains(q.as_str()) || v.spec == *q)
                })
            });
            if first.is_some() {
                selected.set(first);
            }
        }
    });

    let running =
        move || model.with(|m| m.done.is_none() && (!m.variants.is_empty() || !m.tests.is_empty()));
    let headline = move || {
        model.with(|m| match m.done {
            Some((true, secs)) => format!("all checks passed in {secs:.1}s"),
            Some((false, secs)) => format!("failures after {secs:.1}s"),
            None if m.variants.is_empty() && m.tests.is_empty() => "waiting for events".to_owned(),
            None => "running".to_owned(),
        })
    };
    let state_class = move || {
        model.with(|m| match m.done {
            Some((true, _)) => "pill pass",
            Some((false, _)) => "pill fail",
            None => "pill run",
        })
    };

    view! {
        <header class="top">
            <h1>"loams-specview"</h1>
            <span class=state_class>{headline}</span>
            <span class="grow"></span>
            <span class="conn" class:off=move || !connected.get()>
                {move || if connected.get() { "connected" } else { "disconnected" }}
            </span>
            <button on:click=move |_| start_run() disabled=running>"Run again"</button>
            <button class="ghost" on:click=move |_| toggle_theme()>"Theme"</button>
        </header>
        <main class="layout">
            <aside class="side">
                <suite::SuitePanel model=model selected=selected />
            </aside>
            <section class="detail">
                <trace::Detail model=model selected=selected />
            </section>
        </main>
    }
}
