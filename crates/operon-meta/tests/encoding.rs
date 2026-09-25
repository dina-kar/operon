//! Golden encodings of every `Command`, `Reply`, `ApplyError` and a v5
//! snapshot (M1.2a plan, Task 1, ruling 6). postcard is not self-describing:
//! it writes struct fields in declaration order and enum variants by index,
//! never type names or module paths, so a type moved with identical fields,
//! field order, variant order, serde attributes and derives encodes to
//! identical bytes. These tests pin that encoding before anything moves.
//!
//! Run with `OPERON_BLESS_GOLDEN=1` to (re)write the files under
//! `tests/golden/`; otherwise they compare against the committed bytes.
//! The files are never re-blessed after Task 1 (Global Constraints).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use operon_common::schema::{
    CollectionSchema, Distance, DynamicMapping, FieldKind, FieldSpec, HnswParams, Quantization,
    SparseModifier, SparseVectorSpec, VectorElement, VectorIndexSpec, VectorSpec,
};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_meta::{
    AliasAction, ApplyError, Command, Fence, Freshness, LeaseGrant, LinkId, MetaState, Pointer,
    Reply, Retention, TargetRef, WalChunk, WalClass,
};

const GOLDEN_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");

fn golden_path(name: &str) -> PathBuf {
    Path::new(GOLDEN_DIR).join(name)
}

/// In bless mode, writes `fresh` to the golden file and returns it; otherwise
/// reads and returns the committed golden bytes.
fn golden_bytes(name: &str, fresh: &[u8]) -> Vec<u8> {
    let path = golden_path(name);
    if std::env::var_os("OPERON_BLESS_GOLDEN").is_some() {
        let dir = path.parent().expect("golden path has a parent directory");
        std::fs::create_dir_all(dir).expect("create tests/golden");
        std::fs::write(&path, fresh).unwrap_or_else(|e| panic!("writing {path:?}: {e}"));
        fresh.to_vec()
    } else {
        std::fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "reading golden file {path:?}: {e}; rerun with OPERON_BLESS_GOLDEN=1 to create it"
            )
        })
    }
}

/// The `CollectionSchema` used by `golden_commands`'s `CreateCollection`: uses
/// every [`FieldKind`], every [`VectorIndexSpec`], every [`Quantization`], a
/// sparse vector with each [`SparseModifier`], and a non-empty `annotations`
/// map.
fn golden_schema() -> CollectionSchema {
    let fields = vec![
        FieldSpec {
            name: "title".to_string(),
            source_path: "title".to_string(),
            kind: FieldKind::Text {
                analyzer: "standard".to_string(),
                positions: true,
            },
            indexed: true,
            fast: false,
            ignore_malformed: false,
        },
        FieldSpec {
            name: "tag".to_string(),
            source_path: "tag".to_string(),
            kind: FieldKind::Keyword,
            indexed: true,
            fast: true,
            ignore_malformed: false,
        },
        FieldSpec {
            name: "count".to_string(),
            source_path: "count".to_string(),
            kind: FieldKind::I64,
            indexed: false,
            fast: true,
            ignore_malformed: true,
        },
        FieldSpec {
            name: "score".to_string(),
            source_path: "score".to_string(),
            kind: FieldKind::F64,
            indexed: true,
            fast: false,
            ignore_malformed: false,
        },
        FieldSpec {
            name: "active".to_string(),
            source_path: "active".to_string(),
            kind: FieldKind::Bool,
            indexed: true,
            fast: false,
            ignore_malformed: false,
        },
        FieldSpec {
            name: "created".to_string(),
            source_path: "created".to_string(),
            kind: FieldKind::Date,
            indexed: true,
            fast: true,
            ignore_malformed: false,
        },
        FieldSpec {
            name: "uid".to_string(),
            source_path: "uid".to_string(),
            kind: FieldKind::Uuid,
            indexed: true,
            fast: false,
            ignore_malformed: false,
        },
        FieldSpec {
            name: "raw".to_string(),
            source_path: String::new(),
            kind: FieldKind::Json,
            indexed: false,
            fast: true,
            ignore_malformed: false,
        },
    ];
    let vectors = vec![
        VectorSpec {
            name: "v1".to_string(),
            dim: 8,
            distance: Distance::Cosine,
            element: VectorElement::F32,
            index: VectorIndexSpec::Auto,
            hnsw: HnswParams::default(),
            quantization: None,
        },
        VectorSpec {
            name: "v2".to_string(),
            dim: 4,
            distance: Distance::Manhattan,
            element: VectorElement::F32,
            index: VectorIndexSpec::None,
            hnsw: HnswParams::default(),
            quantization: None,
        },
        VectorSpec {
            name: "v3".to_string(),
            dim: 8,
            distance: Distance::Dot,
            element: VectorElement::F32,
            index: VectorIndexSpec::IvfPq {
                num_partitions: Some(16),
                num_sub_vectors: Some(4),
                num_bits: 8,
            },
            hnsw: HnswParams::default(),
            quantization: Some(Quantization::Scalar {
                quantile_ppm: Some(700_000),
                always_ram: true,
            }),
        },
        VectorSpec {
            name: "v4".to_string(),
            dim: 8,
            distance: Distance::Euclid,
            element: VectorElement::F32,
            index: VectorIndexSpec::IvfRq {
                num_partitions: Some(32),
                num_bits: 4,
            },
            hnsw: HnswParams::default(),
            quantization: Some(Quantization::Product {
                compression_ratio: 8,
                always_ram: false,
            }),
        },
        VectorSpec {
            name: "v5".to_string(),
            dim: 16,
            distance: Distance::Cosine,
            element: VectorElement::F32,
            index: VectorIndexSpec::IvfHnswSq {
                num_partitions: Some(64),
            },
            hnsw: HnswParams {
                m: 16,
                ef_construct: 100,
                full_scan_threshold_kb: 10_000,
                payload_m: Some(8),
                on_disk: true,
            },
            quantization: Some(Quantization::Binary { always_ram: true }),
        },
    ];
    let sparse_vectors = vec![
        SparseVectorSpec {
            name: "sparse1".to_string(),
            modifier: SparseModifier::None,
        },
        SparseVectorSpec {
            name: "sparse2".to_string(),
            modifier: SparseModifier::Idf,
        },
    ];
    let mut schema = CollectionSchema::new(fields, vectors, DynamicMapping::Ignore)
        .with_sparse_vectors(sparse_vectors);
    schema
        .annotations
        .insert("operon.owner".to_string(), "search-team".to_string());
    schema
        .annotations
        .insert("es.note".to_string(), "golden fixture".to_string());
    schema
}

/// At least one value of every [`Command`] variant, with every `Option`
/// field present in one value and absent in another where the variant has
/// one (`fence`, `fresh`, `expected`): every [`WalClass`], both `EntryKind`s
/// (reached through the two [`Command::SwapSegment`] values below, which
/// each replace a `Wal` entry with a `Segment` entry), both `AliasAction`s,
/// and [`golden_schema`]'s full coverage of `FieldKind`, `VectorIndexSpec`,
/// `Quantization` and `SparseModifier`.
///
/// Applying this list in order to `MetaState::default()` succeeds for every
/// command; it ends with a `DropCollection` so `retired` holds a prefix.
fn golden_commands() -> Vec<Command> {
    let ns1 = NamespaceId(1);
    let stream1 = StreamId(1);
    let fence = Fence {
        lease: "fence/segment".to_string(),
        epoch: 1,
    };
    vec![
        // -- CreateNamespace --
        Command::CreateNamespace {
            name: "acme".to_string(),
        },
        // -- CreateStream (every WalClass) --
        Command::CreateStream {
            namespace: ns1,
            name: "events".to_string(),
            partitions: 2,
            class: WalClass::Standard,
            retention: Retention::default(),
        },
        Command::CreateStream {
            namespace: ns1,
            name: "fast".to_string(),
            partitions: 1,
            class: WalClass::Express,
            retention: Retention::default(),
        },
        Command::CreateStream {
            namespace: ns1,
            name: "quorum".to_string(),
            partitions: 1,
            class: WalClass::Quorum,
            retention: Retention::default(),
        },
        // -- CreateLink --
        Command::CreateLink {
            namespace: ns1,
            name: "counts".to_string(),
            source: stream1,
            target: TargetRef {
                kind: "counter".to_string(),
                name: "counts".to_string(),
            },
            options: BTreeMap::from([("batch_interval".to_string(), "2s".to_string())]),
        },
        // -- CommitWal --
        Command::CommitWal {
            object: "wal-1".to_string(),
            created_at_ms: 100,
            chunks: vec![WalChunk {
                stream: stream1,
                partition: 0,
                records: 2,
                byte_range: 0..10,
                max_timestamp_ms: 5,
            }],
        },
        Command::CommitWal {
            object: "wal-2".to_string(),
            created_at_ms: 101,
            chunks: vec![WalChunk {
                stream: stream1,
                partition: 0,
                records: 2,
                byte_range: 10..20,
                max_timestamp_ms: 6,
            }],
        },
        Command::CommitWal {
            object: "wal-3".to_string(),
            created_at_ms: 102,
            chunks: vec![WalChunk {
                stream: stream1,
                partition: 0,
                records: 2,
                byte_range: 20..30,
                max_timestamp_ms: 7,
            }],
        },
        // -- SetRetention --
        Command::SetRetention {
            stream: StreamId(2),
            retention: Retention {
                max_age_ms: Some(5_000),
                max_bytes: Some(1_000_000),
            },
        },
        // -- SwapSegment (fence absent, then present; each replaces a Wal
        // entry with a Segment entry) --
        Command::SwapSegment {
            stream: stream1,
            partition: 0,
            replaces: vec![(2, "wal-2".to_string())],
            segment: "seg-a".to_string(),
            byte_range: 40..50,
            max_timestamp_ms: 6,
            fence: None,
            now_ms: 1_000,
            fresh: Freshness {
                created_at_ms: 1_000,
                max_age_ms: 60_000,
            },
        },
        // -- TrimPartition (fence absent) --
        Command::TrimPartition {
            stream: stream1,
            partition: 0,
            before_offset: 2,
            fence: None,
            now_ms: 1_000,
        },
        // -- PruneWalCommits (fence absent) --
        Command::PruneWalCommits {
            fence: None,
            now_ms: 1_000,
        },
        // -- ForgetObjects (fence absent) --
        Command::ForgetObjects {
            objects: vec!["wal-1".to_string()],
            fence: None,
        },
        // -- AcquireLease --
        Command::AcquireLease {
            key: "fence/segment".to_string(),
            owner: "worker-a".to_string(),
            ttl_ms: 600_000,
            now_ms: 1_000,
        },
        Command::AcquireLease {
            key: "worker/task".to_string(),
            owner: "worker-b".to_string(),
            ttl_ms: 5_000,
            now_ms: 1_000,
        },
        // -- RenewLease --
        Command::RenewLease {
            key: "worker/task".to_string(),
            owner: "worker-b".to_string(),
            epoch: 1,
            ttl_ms: 5_000,
            now_ms: 1_500,
        },
        // -- ReacquireLease --
        Command::ReacquireLease {
            key: "worker/task".to_string(),
            owner: "worker-b".to_string(),
            epoch: 1,
            ttl_ms: 5_000,
            now_ms: 1_500,
        },
        // -- ReleaseLease --
        Command::ReleaseLease {
            key: "worker/task".to_string(),
            owner: "worker-b".to_string(),
            epoch: 1,
        },
        // -- CasPointer (expected/fence/fresh absent, then all present) --
        Command::CasPointer {
            namespace: ns1,
            key: "ptr/a".to_string(),
            expected: None,
            value: "v1".to_string(),
            fence: None,
            fresh: None,
        },
        Command::CasPointer {
            namespace: ns1,
            key: "ptr/a".to_string(),
            expected: Some(1),
            value: "v2".to_string(),
            fence: Some(fence.clone()),
            fresh: Some(Freshness {
                created_at_ms: 1_500,
                max_age_ms: 100_000,
            }),
        },
        // -- SwapSegment (fence present) --
        Command::SwapSegment {
            stream: stream1,
            partition: 0,
            replaces: vec![(4, "wal-3".to_string())],
            segment: "seg-b".to_string(),
            byte_range: 60..70,
            max_timestamp_ms: 7,
            fence: Some(fence.clone()),
            now_ms: 1_500,
            fresh: Freshness {
                created_at_ms: 1_500,
                max_age_ms: 100_000,
            },
        },
        // -- TrimPartition (fence present) --
        Command::TrimPartition {
            stream: stream1,
            partition: 0,
            before_offset: 4,
            fence: Some(fence.clone()),
            now_ms: 1_500,
        },
        // -- PruneWalCommits (fence present) --
        Command::PruneWalCommits {
            fence: Some(fence.clone()),
            now_ms: 1_500,
        },
        // -- ForgetObjects (fence present) --
        Command::ForgetObjects {
            objects: vec![
                "wal-2".to_string(),
                "wal-3".to_string(),
                "seg-a".to_string(),
            ],
            fence: Some(fence),
        },
        // -- CreateCollection --
        Command::CreateCollection {
            namespace: ns1,
            name: "docs".to_string(),
            schema: golden_schema(),
            partitions: 2,
        },
        // -- UpdateCollectionSchema --
        Command::UpdateCollectionSchema {
            collection: CollectionId(1),
            expected_version: 1,
            schema: {
                let mut schema = golden_schema();
                schema.version = 2;
                schema.fields.push(FieldSpec {
                    name: "extra".to_string(),
                    source_path: "extra".to_string(),
                    kind: FieldKind::Keyword,
                    indexed: true,
                    fast: false,
                    ignore_malformed: false,
                });
                schema.dynamic = DynamicMapping::Map;
                schema
                    .annotations
                    .insert("operon.updated".to_string(), "true".to_string());
                schema
            },
        },
        // -- UpdateAliases (both AliasActions) --
        Command::UpdateAliases {
            namespace: ns1,
            actions: vec![AliasAction::Create {
                alias: "latest".to_string(),
                collection: "docs".to_string(),
            }],
        },
        Command::UpdateAliases {
            namespace: ns1,
            actions: vec![AliasAction::Delete {
                alias: "latest".to_string(),
            }],
        },
        // -- DropCollection (last, so `retired` holds a prefix) --
        Command::DropCollection {
            namespace: ns1,
            name: "docs".to_string(),
            now_ms: 2_000,
        },
    ]
}

/// Every [`Reply`] variant (`Ok`) and every [`ApplyError`] variant (`Err`).
fn golden_replies() -> Vec<Result<Reply, ApplyError>> {
    vec![
        Ok(Reply::NamespaceCreated(NamespaceId(1))),
        Ok(Reply::StreamCreated(StreamId(1))),
        Ok(Reply::LinkCreated(LinkId(1))),
        Ok(Reply::WalCommitted {
            base_offsets: vec![0, 2],
        }),
        Ok(Reply::Lease(LeaseGrant {
            epoch: 1,
            deadline_ms: 1_000,
        })),
        Ok(Reply::LeaseReleased),
        Ok(Reply::PointerSet { version: 1 }),
        Ok(Reply::RetentionSet),
        Ok(Reply::SegmentSwapped),
        Ok(Reply::Trimmed {
            log_start_offset: 4,
        }),
        Ok(Reply::Pruned { removed: 0 }),
        Ok(Reply::Forgotten { removed: 1 }),
        Ok(Reply::CollectionCreated {
            id: CollectionId(1),
            stream: StreamId(4),
            link: LinkId(2),
        }),
        Ok(Reply::CollectionDropped(Some(CollectionId(1)))),
        Ok(Reply::SchemaUpdated { version: 2 }),
        Ok(Reply::AliasesUpdated),
        Err(ApplyError::InvalidArgument("bad argument".to_string())),
        Err(ApplyError::NamespaceExists(NamespaceId(1))),
        Err(ApplyError::NamespaceNotFound(NamespaceId(2))),
        Err(ApplyError::StreamExists(StreamId(1))),
        Err(ApplyError::StreamNotFound(StreamId(2))),
        Err(ApplyError::LinkExists(LinkId(1))),
        Err(ApplyError::PartitionNotFound {
            stream: StreamId(1),
            partition: 0,
        }),
        Err(ApplyError::LeaseHeld {
            owner: "worker-a".to_string(),
            deadline_ms: 600_000,
        }),
        Err(ApplyError::LeaseLost {
            key: "worker/task".to_string(),
        }),
        Err(ApplyError::VersionMismatch {
            current: Some(Pointer {
                version: 1,
                value: "v1".to_string(),
            }),
        }),
        Err(ApplyError::Fenced {
            lease: "fence/segment".to_string(),
        }),
        Err(ApplyError::IndexMismatch {
            stream: StreamId(1),
            partition: 0,
        }),
        Err(ApplyError::StaleCommit {
            object: "wal-9".to_string(),
        }),
        Err(ApplyError::StaleObject {
            object: "seg-c".to_string(),
            created_at_ms: 1_000,
            max_age_ms: 60_000,
            clock_ms: 2_000,
        }),
        Err(ApplyError::CollectionExists(CollectionId(1))),
        Err(ApplyError::CollectionNotFound(CollectionId(2))),
        Err(ApplyError::NameTaken("docs".to_string())),
        Err(ApplyError::IncompatibleSchema(
            "bad schema change".to_string(),
        )),
        Err(ApplyError::SchemaVersionMismatch {
            collection: CollectionId(1),
            current: 2,
        }),
        Err(ApplyError::UnknownCollection("ghost".to_string())),
    ]
}

/// A state built by applying [`golden_commands`] to `MetaState::default()`.
fn golden_state() -> MetaState {
    let mut state = MetaState::default();
    for command in golden_commands() {
        state
            .apply(command.clone())
            .unwrap_or_else(|e| panic!("{command:?} failed to apply: {e}"));
    }
    state
}

/// A `match` with no wildcard arm: a new `Command` variant fails to compile
/// here until it is added to [`golden_commands`].
#[test]
fn every_command_variant_is_in_the_golden_list() {
    let mut seen = std::collections::BTreeSet::new();
    for command in golden_commands() {
        let name = match command {
            Command::CreateNamespace { .. } => "CreateNamespace",
            Command::CreateStream { .. } => "CreateStream",
            Command::CreateLink { .. } => "CreateLink",
            Command::CommitWal { .. } => "CommitWal",
            Command::SetRetention { .. } => "SetRetention",
            Command::SwapSegment { .. } => "SwapSegment",
            Command::TrimPartition { .. } => "TrimPartition",
            Command::PruneWalCommits { .. } => "PruneWalCommits",
            Command::ForgetObjects { .. } => "ForgetObjects",
            Command::AcquireLease { .. } => "AcquireLease",
            Command::RenewLease { .. } => "RenewLease",
            Command::ReacquireLease { .. } => "ReacquireLease",
            Command::ReleaseLease { .. } => "ReleaseLease",
            Command::CasPointer { .. } => "CasPointer",
            Command::CreateCollection { .. } => "CreateCollection",
            Command::DropCollection { .. } => "DropCollection",
            Command::UpdateCollectionSchema { .. } => "UpdateCollectionSchema",
            Command::UpdateAliases { .. } => "UpdateAliases",
        };
        seen.insert(name);
    }
    assert_eq!(
        seen.len(),
        18,
        "every Command variant must have a value in golden_commands(): {seen:?}"
    );
}

#[test]
fn commands_encode_to_the_golden_bytes() {
    let fresh = postcard::to_stdvec(&golden_commands()).expect("encode golden commands");
    let golden = golden_bytes("commands.bin", &fresh);
    assert_eq!(fresh, golden);
}

#[test]
fn the_golden_commands_decode_to_the_same_commands() {
    let fresh = postcard::to_stdvec(&golden_commands()).expect("encode golden commands");
    let golden = golden_bytes("commands.bin", &fresh);
    let decoded: Vec<Command> = postcard::from_bytes(&golden).expect("decode golden commands");
    assert_eq!(decoded, golden_commands());
}

#[test]
fn the_golden_commands_apply_cleanly_to_an_empty_state() {
    let mut state = MetaState::default();
    for command in golden_commands() {
        let result = state.apply(command.clone());
        assert!(result.is_ok(), "{command:?} => {result:?}");
    }
}

#[test]
fn replies_and_rejections_encode_to_the_golden_bytes() {
    let fresh = postcard::to_stdvec(&golden_replies()).expect("encode golden replies");
    let golden = golden_bytes("replies.bin", &fresh);
    assert_eq!(fresh, golden);
}

#[test]
fn the_golden_replies_decode() {
    let fresh = postcard::to_stdvec(&golden_replies()).expect("encode golden replies");
    let golden = golden_bytes("replies.bin", &fresh);
    let decoded: Vec<Result<Reply, ApplyError>> =
        postcard::from_bytes(&golden).expect("decode golden replies");
    assert_eq!(decoded, golden_replies());
}

#[test]
fn a_snapshot_encodes_to_the_golden_bytes() {
    let state = golden_state();
    let fresh = operon_meta::snapshot_bytes(&state).expect("encode golden snapshot");
    let golden = golden_bytes("snapshot-v5.bin", &fresh);
    assert_eq!(fresh, golden);
}

#[test]
fn the_golden_snapshot_decodes_to_the_same_state() {
    let state = golden_state();
    let fresh = operon_meta::snapshot_bytes(&state).expect("encode golden snapshot");
    let golden = golden_bytes("snapshot-v5.bin", &fresh);
    let decoded = operon_meta::state_from_snapshot_bytes(&golden).expect("decode golden snapshot");
    assert_eq!(decoded, state);
}
