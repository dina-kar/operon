//! The descriptor model the facade generator renders (design §44 §7.3, D606).
//!
//! `buf` hands the plugin a `CodeGeneratorRequest`: the files it was asked to
//! generate, each with its services, methods and the options set on them.
//! This module turns that into the language-neutral model — modules, calls,
//! retry classes, pagination, and the reason registry — that
//! [`typescript`] (and, in SDK1 Task 3, Python and Go) render.
//!
//! The two options this reads are the extension fields `loams.options.v1`
//! declares in `proto/loams/options/v1/options.proto`:
//!
//! - `ModuleOptions` on `ServiceOptions`, field **50001**: which SDK module a
//!   service belongs to;
//! - `FacadeOptions` on `MethodOptions`, field **50002**, repeated: which
//!   facade calls a method backs, and how each one retries and pages.
//!
//! Neither is behaviour the server reads. They are the contract thirteen SDKs
//! are generated from, so a method that carries no `facade` option is *not*
//! exposed: annotating is how a service admits a method to the SDK surface,
//! which is how an admin-only RPC such as `LiveService/Deploy` stays out of
//! every SDK by default.

pub mod reasons;
pub mod typescript;
pub mod wire;

use std::collections::BTreeMap;

use wire::{
    Reader, Value, WireError, repeated_bytes, repeated_messages, string_field, varint_field,
};

/// `loams.options.v1`'s extension on `ServiceOptions`.
const MODULE_EXTENSION: u32 = 50001;
/// `loams.options.v1`'s extension on `MethodOptions`.
const FACADE_EXTENSION: u32 = 50002;
/// `google.protobuf.MethodOptions.idempotency_level`, field 34.
const IDEMPOTENCY_LEVEL: u32 = 34;

/// `google.protobuf.MethodOptions.IdempotencyLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Idempotency {
    /// `NO_SIDE_EFFECTS`: a read that may be a cacheable `GET`, and that the
    /// SDK always retries.
    NoSideEffects,
    /// `IDEMPOTENT`: repeating it changes nothing beyond the first call.
    Idempotent,
    /// Nothing declared: repeating it may do the work twice.
    None,
}

impl Idempotency {
    /// The `snake_case` name the generated SDKs use.
    pub fn as_str(self) -> &'static str {
        match self {
            Idempotency::NoSideEffects => "no_side_effects",
            Idempotency::Idempotent => "idempotent",
            Idempotency::None => "none",
        }
    }
}

/// How a call may be retried (design §44 §7.4, D610).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// The SDK retries on its own. Only reads and idempotent RPCs get this,
    /// unless `FacadeOptions.retry_safe` says otherwise.
    Safe,
    /// The SDK does not retry: a mutating RPC retries only when the caller
    /// supplied an `idempotency_key`, which the SDK then reuses.
    Manual,
}

impl Retry {
    /// The `snake_case` name the generated SDKs use.
    pub fn as_str(self) -> &'static str {
        match self {
            Retry::Safe => "safe",
            Retry::Manual => "manual",
        }
    }
}

/// AIP-158 pagination, named by `FacadeOptions.pagination` as
/// `"<items field>:<next page token field>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pagination {
    /// The repeated field on the response the iterator yields.
    pub items: String,
    /// The token field: in on the next call, out on this one.
    pub next_page_token: String,
}

/// Whether a method streams, and in which direction. The API has no
/// client-streaming and no bidi (D420).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Streaming {
    Unary,
    Server,
}

impl Streaming {
    /// The `snake_case` name the generated SDKs use.
    pub fn as_str(self) -> &'static str {
        match self {
            Streaming::Unary => "unary",
            Streaming::Server => "server",
        }
    }
}

/// One facade call: a method of a service, exposed under one name.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    /// The module the call is exposed on, which may differ from its
    /// service's module (`FacadeOptions.module`).
    pub module: String,
    /// The call's name in the language's case: `snake_case` here, converted
    /// by the renderer. Empty in the options means the method's own name.
    pub name: String,
    /// The method that backs the call, for example `GetInstance`.
    pub method: String,
    /// The request message, fully qualified.
    pub input: String,
    /// The response message, fully qualified. For a server stream this is the
    /// element type.
    pub output: String,
    /// The service, fully qualified.
    pub service: String,
    /// The proto package the service is in, which is what the generated SDK
    /// imports its message types from.
    pub package: String,
    pub idempotency: Idempotency,
    pub retry: Retry,
    pub streaming: Streaming,
    pub pagination: Option<Pagination>,
}

impl Call {
    /// The call's identity as a caller writes it: `loams.<module>.<name>`.
    pub fn full_name(&self) -> String {
        format!("loams.{}.{}", self.module, self.name)
    }

    /// The RPC that backs the call, `package.Service/Method`, which is the
    /// path a `curl` or a `grpcurl` uses.
    pub fn rpc(&self) -> String {
        format!("{}/{}", self.service, self.method)
    }
}

/// One SDK module: a service, its summary, and the calls on it.
#[derive(Debug, Clone, PartialEq)]
pub struct Module {
    /// `ModuleOptions.name`, for example `instance` or `admin.org`.
    pub name: String,
    /// `ModuleOptions.summary`, one line for the reference docs.
    pub summary: String,
    /// `ModuleOptions.unstable`: the package's wire contract may still change,
    /// so an SDK marks the module experimental (§44 §10.3).
    pub unstable: bool,
    /// The service, fully qualified.
    pub service: String,
    /// The proto package.
    pub package: String,
    /// True when the module is not the service's own `ModuleOptions.name` but
    /// a second facade name for the same RPCs (`loams.tables` on
    /// `loams.live.v1.LiveService`, §44 §7.2). Such a module has no summary
    /// of its own: it inherits the service's, which describes the other half
    /// of the split.
    pub derived: bool,
    /// The module's calls, in proto order.
    pub calls: Vec<Call>,
}

impl Module {
    /// The call with this name, if the module has one.
    pub fn call(&self, name: &str) -> Option<&Call> {
        self.calls.iter().find(|call| call.name == name)
    }
}

/// Everything a renderer needs: the modules, and which reason the server may
/// return (the registry page, which is not in any descriptor).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Model {
    /// The modules, ordered by module name so the generated file does not
    /// churn when a proto is reordered.
    pub modules: Vec<Module>,
    /// Every `reason` in `docs/api/reasons.md`, with the code it is raised
    /// under.
    pub reasons: Vec<reasons::Reason>,
    /// The proto packages the SDK speaks, ordered.
    pub packages: Vec<String>,
}

impl Model {
    /// The module with this name.
    pub fn module(&self, name: &str) -> Option<&Module> {
        self.modules.iter().find(|module| module.name == name)
    }

    /// Every call in every module, for tests and reference output.
    pub fn calls(&self) -> impl Iterator<Item = &Call> {
        self.modules.iter().flat_map(|module| module.calls.iter())
    }
}

/// Where a language's SDK imports a proto package's generated types from:
/// `loams.instance.v1` is `@loams/proto/instance`, `loams.live.v1` is
/// `@loams/live/live`. A `buf.gen.yaml` passes this, so the generator itself
/// carries no per-language knowledge of where a package lives.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PackageMap {
    entries: BTreeMap<String, String>,
}

impl PackageMap {
    /// One `package=<proto package>=<npm module>` parameter. The map is
    /// repeated rather than comma-joined because buf joins a plugin's `opt`
    /// entries with commas, and a comma-separated value would be split.
    pub fn insert(&mut self, parameter: &str) -> Result<(), String> {
        let (package, module) = parameter
            .split_once('=')
            .ok_or_else(|| format!("package parameter {parameter:?} is not pkg=module"))?;
        self.entries
            .insert(package.trim().to_owned(), module.trim().to_owned());
        Ok(())
    }

    /// Every `package=` parameter in a plugin parameter string.
    pub fn parse(parameters: &[&str]) -> Result<Self, String> {
        let mut map = Self::default();
        for parameter in parameters {
            map.insert(parameter)?;
        }
        Ok(map)
    }

    /// The npm module a proto package's generated types are imported from.
    pub fn get(&self, package: &str) -> Option<&str> {
        self.entries.get(package).map(String::as_str)
    }
}

/// What the plugin was asked for.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Options {
    /// The language to render. Only `typescript` is implemented (SDK1 Task 3).
    pub lang: String,
    /// Where `docs/api/reasons.md` is, relative to the working directory
    /// `buf generate` ran in.
    pub reasons: Option<String>,
    /// Where each proto package's generated types are imported from.
    pub packages: PackageMap,
    /// The proto revision the SDK declares (`LOAMS_PROTO_REV`, §44 §10.3).
    pub proto_rev: String,
}

impl Options {
    /// Parses a buf plugin parameter: `key=value` pairs separated by commas.
    pub fn parse(parameter: &str) -> Result<Self, String> {
        let mut options = Self {
            lang: "typescript".to_owned(),
            reasons: Some("docs/api/reasons.md".to_owned()),
            proto_rev: "v1".to_owned(),
            ..Self::default()
        };
        for pair in parameter
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!("plugin parameter {pair:?} is not key=value"));
            };
            match key {
                "lang" => options.lang = value.to_owned(),
                "reasons" => options.reasons = Some(value.to_owned()),
                "proto_rev" => options.proto_rev = value.to_owned(),
                "package" => options.packages.insert(value)?,
                other => return Err(format!("unknown plugin parameter {other:?}")),
            }
        }
        Ok(options)
    }
}

/// Builds the model from a `CodeGeneratorRequest`.
pub fn model_from_request(
    request: &[u8],
    reasons: Vec<reasons::Reason>,
) -> Result<Model, WireError> {
    let fields = Reader::new(request).fields()?;
    let mut modules: BTreeMap<String, Module> = BTreeMap::new();
    let mut packages: Vec<String> = Vec::new();
    for mut file in repeated_messages(&fields, 15)? {
        let file = file.fields()?;
        let Some(package) = string_field(&file, 2)? else {
            continue;
        };
        packages.push(package.clone());
        for mut service in repeated_messages(&file, 6)? {
            let service = service.fields()?;
            let Some(name) = string_field(&service, 1)? else {
                continue;
            };
            let Some(options) = service_options(&service)? else {
                continue;
            };
            for mut method in repeated_messages(&service, 2)? {
                let method = method.fields()?;
                let Some(method_name) = string_field(&method, 1)? else {
                    continue;
                };
                let method_options = method_options(&method)?;
                for facade in facade_options(&method_options)? {
                    // An empty `FacadeOptions.module` means the service's own
                    // module; a named one is how one RPC backs a second
                    // module (`QueryService/Search` is both `loams.search` and
                    // `loams.vector`, design §44 §7.3).
                    let module_name = if facade.module.is_empty() {
                        options.name.clone()
                    } else {
                        facade.module.clone()
                    };
                    let mut call = call_from(
                        &package,
                        &name,
                        &method_name,
                        &method,
                        &method_options,
                        facade,
                    )?;
                    call.module = module_name.clone();
                    let derived = module_name != options.name;
                    let entry = modules
                        .entry(module_name.clone())
                        .or_insert_with(|| Module {
                            name: module_name,
                            summary: if derived {
                                String::new()
                            } else {
                                options.summary.clone()
                            },
                            unstable: options.unstable,
                            service: format!("{package}.{name}"),
                            package: package.clone(),
                            derived,
                            calls: Vec::new(),
                        });
                    entry.calls.push(call);
                }
            }
        }
    }
    let mut modules: Vec<Module> = modules.into_values().collect();
    for module in &mut modules {
        module
            .calls
            .sort_by(|left, right| left.name.cmp(&right.name));
    }
    modules.sort_by(|left, right| left.name.cmp(&right.name));
    packages.sort();
    packages.dedup();
    Ok(Model {
        modules,
        reasons,
        packages,
    })
}

/// `loams.options.v1.ModuleOptions`.
#[derive(Debug, Clone, Default, PartialEq)]
struct ModuleOption {
    name: String,
    summary: String,
    unstable: bool,
}

/// `loams.options.v1.FacadeOptions`.
#[derive(Debug, Clone, Default, PartialEq)]
struct FacadeOption {
    module: String,
    name: String,
    retry_safe: Option<bool>,
    pagination: Option<String>,
}

/// The `ServiceOptions` bytes, or `None` when the service carries no options
/// at all (the common case for a service nobody annotated yet).
fn service_options(fields: &[(u32, Value<'_>)]) -> Result<Option<ModuleOption>, WireError> {
    let mut bytes = None;
    for (number, value) in fields {
        if *number == 3
            && let Value::Bytes(raw) = value
        {
            bytes = Some(*raw);
        }
    }
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let options = Reader::new(bytes).fields()?;
    let Some(raw) = repeated_bytes(&options, MODULE_EXTENSION)
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let fields = Reader::new(raw).fields()?;
    Ok(Some(ModuleOption {
        name: string_field(&fields, 1)?.unwrap_or_default(),
        summary: string_field(&fields, 2)?.unwrap_or_default(),
        unstable: varint_field(&fields, 3).is_some_and(|value| value != 0),
    }))
}

/// The `MethodOptions` bytes, or empty when the method carries none.
fn method_options<'a>(fields: &'a [(u32, Value<'a>)]) -> Result<Vec<(u32, Value<'a>)>, WireError> {
    for (number, value) in fields {
        if *number == 4
            && let Value::Bytes(raw) = value
        {
            return Reader::new(raw).fields();
        }
    }
    Ok(Vec::new())
}

/// Every `FacadeOptions` on a method's options.
fn facade_options(options: &[(u32, Value<'_>)]) -> Result<Vec<FacadeOption>, WireError> {
    let mut out = Vec::new();
    for raw in repeated_bytes(options, FACADE_EXTENSION) {
        let fields = Reader::new(raw).fields()?;
        out.push(FacadeOption {
            module: string_field(&fields, 1)?.unwrap_or_default(),
            name: string_field(&fields, 2)?.unwrap_or_default(),
            retry_safe: varint_field(&fields, 3).map(|value| value != 0),
            pagination: string_field(&fields, 4)?,
        });
    }
    Ok(out)
}

/// One call, from a method and one of its `FacadeOptions`.
fn call_from(
    package: &str,
    service: &str,
    method_name: &str,
    method: &[(u32, Value<'_>)],
    options: &[(u32, Value<'_>)],
    facade: FacadeOption,
) -> Result<Call, WireError> {
    let idempotency = match varint_field(options, IDEMPOTENCY_LEVEL) {
        Some(1) => Idempotency::NoSideEffects,
        Some(2) => Idempotency::Idempotent,
        _ => Idempotency::None,
    };
    // D610: reads and idempotent RPCs retry on their own; a mutation retries
    // only when the caller gave it an idempotency key. `retry_safe` overrides
    // both ways, which is why it is `Option` and not `bool`: an unset option
    // must not read as "false, so never retry".
    let retry = match facade.retry_safe {
        Some(true) => Retry::Safe,
        Some(false) => Retry::Manual,
        None => match idempotency {
            Idempotency::NoSideEffects | Idempotency::Idempotent => Retry::Safe,
            Idempotency::None => Retry::Manual,
        },
    };
    let pagination = match facade.pagination {
        Some(raw) => Some(parse_pagination(package, service, &raw)?),
        None => None,
    };
    Ok(Call {
        module: facade.module,
        name: if facade.name.is_empty() {
            method_name.to_owned()
        } else {
            facade.name
        },
        method: method_name.to_owned(),
        input: string_field(method, 2)?.unwrap_or_default(),
        output: string_field(method, 3)?.unwrap_or_default(),
        service: format!("{package}.{service}"),
        package: package.to_owned(),
        idempotency,
        retry,
        streaming: if varint_field(method, 6).is_some_and(|value| value != 0) {
            Streaming::Server
        } else {
            Streaming::Unary
        },
        pagination,
    })
}

/// `"collections:next_page_token"`, with the fields named relative to the
/// response message. A malformed value fails the generation rather than
/// producing an iterator that silently yields one page.
fn parse_pagination(package: &str, service: &str, raw: &str) -> Result<Pagination, WireError> {
    let invalid = |detail: &str| {
        WireError::Option(
            "loams.options.v1.facade.pagination",
            format!("{package}.{service}: {detail}"),
        )
    };
    let Some((items, next_page_token)) = raw.split_once(':') else {
        return Err(invalid(
            "expected \"<items field>:<next page token field>\"",
        ));
    };
    if items.is_empty() || next_page_token.is_empty() {
        return Err(invalid("one of the two field names is empty"));
    }
    for field in [items, next_page_token] {
        let identifier = field.rsplit('.').next().unwrap_or(field);
        if identifier.is_empty()
            || !identifier
                .chars()
                .next()
                .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
            || !identifier
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(invalid(&format!("{field:?} is not a proto field name")));
        }
    }
    Ok(Pagination {
        items: items.to_owned(),
        next_page_token: next_page_token.to_owned(),
    })
}
