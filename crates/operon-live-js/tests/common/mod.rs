//! Helpers shared by the engine's tests: a fake host and value builders.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::BoxFuture;
use operon_live::{LiveError, LiveValue};
use operon_live_js::{Bundle, Host, Invocation, JsConfig};

pub fn s(v: &str) -> LiveValue {
    LiveValue::Str(v.to_string())
}

pub fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

/// The invocation tests use unless they need another.
pub fn invocation() -> Invocation {
    Invocation::new(1 << 40, 1_700_000_000_123, "")
}

type Answer = Box<dyn FnMut(&str, &LiveValue) -> Result<LiveValue, LiveError> + Send>;

/// A host that records its calls and answers them with `answer`, after
/// `delay`.
pub struct FakeHost {
    pub calls: Vec<(String, LiveValue)>,
    pub answer: Answer,
    pub delay: Duration,
}

impl FakeHost {
    pub fn new(
        answer: impl FnMut(&str, &LiveValue) -> Result<LiveValue, LiveError> + Send + 'static,
    ) -> Self {
        FakeHost {
            calls: Vec::new(),
            answer: Box::new(answer),
            delay: Duration::ZERO,
        }
    }

    /// A host that answers every call with `null`.
    pub fn null() -> Self {
        FakeHost::new(|_, _| Ok(LiveValue::Null))
    }
}

impl Host for FakeHost {
    fn call<'a>(
        &'a mut self,
        op: &'a str,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.calls.push((op.to_string(), args.clone()));
            (self.answer)(op, &args)
        })
    }
}

/// Loads `source` with `config`, panicking on an error.
pub fn load(source: &str, config: JsConfig) -> Bundle {
    Bundle::load(source, config).expect("the bundle loads")
}

/// A bundle with one query `t:q` whose handler body is `body` (it sees
/// `ctx` and `args`).
pub fn query_bundle(body: &str) -> String {
    format!(
        "import {{ query, mutation }} from \"loam:server\";\n\
         export const t = {{ q: query({{ handler: async (ctx, args) => {{ {body} }} }}),\n\
         m: mutation({{ handler: async (ctx, args) => {{ {body} }} }}) }};\n"
    )
}

/// Runs `path` of `bundle` with a null host and no arguments.
pub async fn run(bundle: &Bundle, path: &str) -> Result<LiveValue, LiveError> {
    bundle
        .invoke(path, &mut FakeHost::null(), invocation(), LiveValue::Null)
        .await
}
