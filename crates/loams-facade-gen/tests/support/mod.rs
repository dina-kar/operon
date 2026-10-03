//! A minimal descriptor *writer*, so the tests can build the
//! `CodeGeneratorRequest` a `buf generate` would send without protoc.
//!
//! `protoc` writes these bytes and this crate reads them; writing them here
//! keeps the unit tests hermetic (no protoc, no fixture protos on disk) while
//! `tests/golden.rs` runs the same assertions over the real `proto/` tree.

/// One service to describe.
pub struct Service {
    pub name: &'static str,
    /// The `ModuleOptions` to set, or `None` for an unannotated service.
    pub module: Option<Module>,
    pub methods: Vec<Method>,
}

/// The `loams.options.v1.ModuleOptions` of a service.
pub struct Module {
    pub name: &'static str,
    pub summary: &'static str,
    pub unstable: bool,
}

/// One method, with the `FacadeOptions` that admit it to the SDK.
pub struct Method {
    pub name: &'static str,
    pub input: &'static str,
    pub output: &'static str,
    pub server_streaming: bool,
    /// `google.protobuf.MethodOptions.idempotency_level`.
    pub idempotency: u64,
    pub facade: Vec<Facade>,
}

/// The `loams.options.v1.FacadeOptions` of one facade call.
pub struct Facade {
    pub module: &'static str,
    pub name: &'static str,
    pub retry_safe: Option<bool>,
    pub pagination: Option<&'static str>,
}

/// `CodeGeneratorRequest` with one `FileDescriptorProto` per entry.
pub fn request(package: &str, services: &[Service]) -> Vec<u8> {
    let mut out = Vec::new();
    for service in services {
        let file = file(package, service);
        put_bytes(&mut out, 15, &file);
    }
    out
}

fn file(package: &str, service: &Service) -> Vec<u8> {
    let mut service_bytes = Vec::new();
    put_bytes(&mut service_bytes, 1, service.name.as_bytes());
    for method in &service.methods {
        // A method is ServiceDescriptorProto field 2.
        put_bytes(&mut service_bytes, 2, &method_bytes(method));
    }
    if let Some(module) = &service.module {
        let mut options = Vec::new();
        put_bytes(&mut options, 1, module.name.as_bytes());
        put_bytes(&mut options, 2, module.summary.as_bytes());
        if module.unstable {
            put_varint_field(&mut options, 3, 1);
        }
        let mut extension = Vec::new();
        put_bytes(&mut extension, 50001, &options);
        put_bytes(&mut service_bytes, 3, &extension);
    }
    let mut file = Vec::new();
    put_bytes(&mut file, 1, format!("{package}.proto").as_bytes());
    put_bytes(&mut file, 2, package.as_bytes());
    put_bytes(&mut file, 6, &service_bytes);
    file
}

fn method_bytes(method: &Method) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, 1, method.name.as_bytes());
    put_bytes(&mut out, 2, method.input.as_bytes());
    put_bytes(&mut out, 3, method.output.as_bytes());
    let mut options = Vec::new();
    put_varint_field(&mut options, 34, method.idempotency);
    for facade in &method.facade {
        let mut facade_bytes = Vec::new();
        put_bytes(&mut facade_bytes, 1, facade.module.as_bytes());
        put_bytes(&mut facade_bytes, 2, facade.name.as_bytes());
        if let Some(retry_safe) = facade.retry_safe {
            put_varint_field(&mut facade_bytes, 3, u64::from(retry_safe));
        }
        if let Some(pagination) = facade.pagination {
            put_bytes(&mut facade_bytes, 4, pagination.as_bytes());
        }
        put_bytes(&mut options, 50002, &facade_bytes);
    }
    put_bytes(&mut out, 4, &options);
    if method.server_streaming {
        put_varint_field(&mut out, 6, 1);
    }
    out
}

fn put_bytes(out: &mut Vec<u8>, number: u32, bytes: &[u8]) {
    put_varint(out, (u64::from(number) << 3) | 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_varint_field(out: &mut Vec<u8>, number: u32, value: u64) {
    put_varint(out, u64::from(number) << 3);
    put_varint(out, value);
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// A `read`-only facade call: no module, no retry override, no paging.
pub fn read_call(name: &'static str) -> Facade {
    Facade {
        module: "",
        name,
        retry_safe: None,
        pagination: None,
    }
}
