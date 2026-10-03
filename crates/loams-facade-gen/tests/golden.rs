//! The generated TypeScript facade, over the real `proto/` tree (SDK1 Task 3's
//! `golden_typescript`).
//!
//! `model.rs` pins the reading of the annotations with hand-written
//! descriptors. This pins the *output*: `protoc` compiles the repository's own
//! protos, the model is built from that descriptor set exactly as buf hands it
//! to the plugin, and the rendered text must equal the committed
//! `facade.ts` byte for byte. A proto edit that changes the SDK surface
//! therefore fails here until the generated file is regenerated and committed,
//! which is the same rule CI's drift check enforces through `buf generate`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use loams_facade_gen::{Options, model_from_request, reasons, typescript};

/// The proto packages the SDK's message types are imported from. It mirrors
/// the `package=` parameters in `sdks/typescript/buf.gen.yaml`, and
/// `the_package_map_matches_the_buf_template` below fails if the two drift.
const PACKAGE_PARAMETERS: &[&str] = &[
    "loams.instance.v1=@loams/proto/instance",
    "loams.devices.v1=@loams/proto/devices",
    "loams.approvals.v1=@loams/proto/approvals",
    "loams.operations.v1=@loams/proto/operations",
    "loams.notifications.v1=@loams/proto/notifications",
    "loams.errors.v1=@loams/proto/errors",
    "loams.live.v1=@loams/live/live",
];

const PROTO_REV: &str = "v1";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The request, built once: `protoc` runs for every test in this file and the
/// descriptor set does not change.
fn code_generator_request() -> &'static [u8] {
    static REQUEST: OnceLock<Vec<u8>> = OnceLock::new();
    REQUEST.get_or_init(build_request)
}

/// Every proto file, as protoc's descriptor set, wrapped the way a
/// `CodeGeneratorRequest` wraps them: field 15, repeated, once per file, in
/// dependency order. The whole `loams` tree is compiled, because that is what
/// the buf template's single input directory hands the plugin.
fn build_request() -> Vec<u8> {
    let root = root();
    let proto = root.join("proto");
    let out = std::env::temp_dir().join(format!("loams-facade-golden-{}.bin", std::process::id()));
    let status = Command::new("protoc")
        .current_dir(&root)
        .arg(format!("--proto_path={}", proto.display()))
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", out.display()))
        .args(proto_files(&proto.join("loams")))
        .status()
        .expect("protoc is on PATH (the crate's own build.rs needs it too)");
    assert!(status.success(), "protoc failed with {status}");
    let bytes = std::fs::read(&out).expect("the descriptor set");
    let _ = std::fs::remove_file(&out);
    wrap_in_request(&bytes)
}

/// Every `.proto` under a directory, as paths relative to `proto/`, sorted so
/// protoc's input order does not depend on the filesystem.
fn proto_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("read the proto tree") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "proto")
            {
                out.push(
                    path.strip_prefix(dir.parent().expect("proto/"))
                        .expect("under proto/")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    out.sort();
    assert!(!out.is_empty(), "no protos under {}", dir.display());
    out
}

/// `FileDescriptorSet` (field 1 per file) to `CodeGeneratorRequest` (field 15
/// per file). The two differ only in the field number, which is why this is a
/// re-tag rather than a re-parse.
fn wrap_in_request(descriptor_set: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < descriptor_set.len() {
        let (tag, next) = read_varint(descriptor_set, at);
        at = next;
        let number = tag >> 3;
        let wire = tag & 0x07;
        assert_eq!(wire, 2, "a descriptor set is all length-delimited");
        let (len, next) = read_varint(descriptor_set, at);
        at = next;
        let payload = &descriptor_set[at..at + len as usize];
        at += len as usize;
        if number == 1 {
            push_bytes(&mut out, 15, payload);
        }
    }
    out
}

fn read_varint(bytes: &[u8], mut at: usize) -> (u64, usize) {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = bytes[at];
        at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return (value, at);
        }
        shift += 7;
    }
}

fn push_bytes(out: &mut Vec<u8>, number: u32, payload: &[u8]) {
    push_varint(out, (u64::from(number) << 3) | 2);
    push_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// The rendered facade, from the repository's own protos and reason registry.
fn render() -> String {
    let reasons = reasons::read(&root().join("docs/api/reasons.md")).expect("the reason registry");
    let model = model_from_request(code_generator_request(), reasons).expect("the descriptors");
    let mut packages = Options::default().packages;
    for parameter in PACKAGE_PARAMETERS {
        packages.insert(parameter).expect("a package parameter");
    }
    typescript::render(&model, &packages, PROTO_REV).expect("the facade renders")
}

/// SDK1 Task 3's `golden_typescript`: the committed generated file is what the
/// generator writes.
#[test]
fn golden_typescript() {
    let path = root().join("sdks/typescript/packages/client/src/gen/facade.ts");
    let committed =
        std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
    let rendered = render();
    assert_eq!(
        rendered,
        committed,
        "{} is stale: run `scripts/sdk/gen.sh typescript` and commit the result",
        path.display()
    );
}

/// The package map is the one thing the golden file cannot check about itself,
/// so the committed template and this test's constants are compared directly:
/// a package added to one and not the other fails generation or fails the
/// typecheck, and this says which.
#[test]
fn the_package_map_matches_the_buf_template() {
    let template = std::fs::read_to_string(root().join("sdks/typescript/buf.gen.yaml"))
        .expect("sdks/typescript/buf.gen.yaml");
    for parameter in PACKAGE_PARAMETERS {
        assert!(
            template.contains(&format!("- package={parameter}")),
            "{parameter} is not in buf.gen.yaml"
        );
    }
}

/// Every module the design's catalogue names for the live package is generated
/// from the annotations: `live` for the session half and `tables` for the
/// table half (§44 §7.2), both facade names for `loams.live.v1.LiveService`.
#[test]
fn the_live_package_splits_into_live_and_tables() {
    let rendered = render();
    for expected in [
        "export interface LiveModule {",
        "export interface TablesModule {",
        "'loams.live.v1.LiveService': LiveService,",
        "derived: true,",
    ] {
        assert!(rendered.contains(expected), "{expected} is missing");
    }
    assert!(
        !rendered.contains("export interface ErrorsModule {"),
        "loams.errors.v1 declares no service"
    );
}

/// The reason registry reaches the SDK as a type, so a caller switching on it
/// is exhaustive and a reason the registry has lost stops compiling.
#[test]
fn the_reason_union_is_generated_from_the_registry() {
    let rendered = render();
    assert!(
        rendered.contains("export type Reason = (typeof REASONS)[number];"),
        "{rendered}"
    );
    assert!(
        rendered.contains("export const FEATURE_NOT_IN_VARIANT = 'feature_not_in_variant' as const satisfies Reason;"),
        "the unavailable-service reason must be in the union"
    );
    let registry =
        std::fs::read_to_string(root().join("docs/api/reasons.md")).expect("the reason registry");
    for reason in loams_facade_gen::reasons::parse(&registry, Path::new("docs/api/reasons.md"))
        .expect("parse")
    {
        assert!(
            rendered.contains(&format!("  '{}',", reason.reason)),
            "{} is in the registry but not in the generated union",
            reason.reason
        );
    }
}
