//! Scan pinning (plan M1.2 Task 14, D53): scan plans for `current`, a
//! manifest version and a consistency token; the Lance version they name,
//! read with the `lance` crate alone; the tail and its pin; expiry; and the
//! columns of each Lance version.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::common::{Fixture, WAIT, fold_history, mixed_history, upsert, vector};
use datafusion::arrow::array::{Array, BinaryArray};
use lance::dataset::builder::DatasetBuilder;
use lance_table::io::deletion::relative_deletion_file_path;
use operon_collection::{
    CollectionConfig, CollectionSchema, ConsistencyToken, DocOp, Document, DynamicMapping,
    PrimaryKey, SparseModifier, SparseVector, SparseVectorSpec,
};
use operon_common::meta::{Consistency, MetaStore};
use operon_query::flight::{StatementTicket, decode_ticket, encode_ticket, ticket_consistency};
use operon_query::sql::{rows_to_json, run_read_only};
use operon_query::{
    CollectionInfo, CollectionService, ColumnRole, ReadConsistency, ScanAt, ScanPlan, ScanRequest,
    ServiceConfig, ServiceError, WriteOptions,
};
use serde_json::{Map, Value, json};
use tempfile::TempDir;

const NS: &str = "default";
const KB: &str = "kb";

/// No typed fields and no vectors; unmapped paths are ignored.
fn plain_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore);
    schema.validate().expect("valid schema");
    schema
}

async fn create(service: &CollectionService, schema: CollectionSchema) -> CollectionInfo {
    service
        .create_collection(NS, KB, schema, Some(2))
        .await
        .expect("create kb")
}

async fn write(service: &CollectionService, ops: Vec<DocOp>) -> ConsistencyToken {
    service
        .write(NS, KB, ops, WriteOptions::default())
        .await
        .expect("write")
        .token
}

/// Upserts of keys `keys`.
fn upserts(keys: std::ops::Range<u64>) -> Vec<DocOp> {
    keys.map(|k| upsert(k, json!({"n": k}))).collect()
}

async fn plan(service: &CollectionService, at: ScanAt) -> ScanPlan {
    service.scan_plan(NS, KB, at).await.expect("scan plan")
}

/// The token of every partition's high watermark of kb.
async fn high_watermarks(f: &Fixture, info: &CollectionInfo) -> ConsistencyToken {
    let head = f
        .meta
        .client
        .collection_head(Consistency::Linearizable, info.id)
        .await
        .expect("read")
        .expect("kb exists");
    ConsistencyToken(
        (0..info.partitions)
            .map(|p| (info.stream, p, head.high_watermarks[p as usize]))
            .collect(),
    )
}

async fn sql_count(service: &CollectionService, consistency: ReadConsistency) -> Value {
    let ctx = service.sql_context_with(NS, consistency);
    let result = run_read_only(&ctx, "SELECT count(*) AS n FROM kb", &service.config().sql)
        .await
        .expect("count");
    rows_to_json(&result)["rows"][0][0].clone()
}

#[tokio::test]
async fn a_fresh_collection_plans_nothing() {
    let f = Fixture::start().await;
    let service = f.service();
    let info = create(&service, plain_schema()).await;
    let current = plan(&service, ScanAt::Current).await;
    let s = info.stream.0;
    let token = format!("v1:s{s}/p0@0,s{s}/p1@0");
    let expected = json!({
        "namespace": "default", "collection": "kb", "collection_id": info.id.0,
        "manifest_version": 0, "schema_version": 1, "lance": null, "fragments": [],
        "live_rows": 0, "columns": [], "pk_encoding": "operon_canonical_v1",
        "tail": false, "tail_records": 0,
        "offsets": [{"partition": 0, "applied": 0, "target": 0}, {"partition": 1, "applied": 0, "target": 0}],
        "durable_token": token, "pin": {"manifest_version": 0, "token": token},
        "planned_at_ms": current.planned_at_ms, "expires_at_ms": null
    });
    let value = serde_json::to_value(&current).expect("JSON");
    assert_eq!(value, expected);
    assert_eq!(
        serde_json::from_value::<ScanPlan>(value).expect("parses"),
        current
    );

    let version_zero = plan(&service, ScanAt::ManifestVersion(0)).await;
    assert_eq!(
        ScanPlan {
            planned_at_ms: current.planned_at_ms,
            ..version_zero
        },
        current
    );
    f.shutdown().await;
}

#[tokio::test]
async fn the_lance_version_holds_exactly_the_live_documents() {
    let dir = TempDir::new().expect("temp dir");
    let f = Fixture::with_local_bucket(dir.path()).await;
    let service = f.service();
    create(&service, plain_schema()).await;
    let history = mixed_history(14, 120, 24);
    for chunk in history.chunks(40) {
        write(&service, chunk.to_vec()).await;
        f.settle().await;
    }
    let plan = plan(&service, ScanAt::Current).await;
    assert!(!plan.tail, "{plan:?}");
    let lance = plan.lance.clone().expect("a lance version");
    assert_eq!(lance.storage_format, "2.1");
    assert!(lance.stable_row_ids);
    assert!(
        lance.manifest_path.starts_with("_versions/d")
            && lance.manifest_path.ends_with(".manifest"),
        "{}",
        lance.manifest_path
    );
    assert_ne!(lance.version & (1 << 63), 0, "a detached version id");
    let uri = lance.uri.clone().expect("a uri");
    let base = crate::common::file_url(dir.path());
    assert!(
        uri.starts_with(&base)
            && uri.ends_with(&format!("/collections/{}/lance", plan.collection_id)),
        "{uri}"
    );

    // Lance alone: the default session and object store registry.
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_version(lance.version)
        .load()
        .await
        .expect("open the planned version");
    let batch = dataset
        .scan()
        .project(&["_pk", "_source"])
        .expect("project")
        .try_into_batch()
        .await
        .expect("scan");
    let pks = batch
        .column_by_name("_pk")
        .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
        .expect("_pk");
    let sources = batch
        .column_by_name("_source")
        .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
        .expect("_source");
    let mut rows: BTreeMap<PrimaryKey, Map<String, Value>> = BTreeMap::new();
    for i in 0..batch.num_rows() {
        let pk = PrimaryKey::from_canonical(pks.value(i)).expect("canonical key");
        let source = serde_json::from_slice(sources.value(i)).expect("UTF-8 JSON source");
        assert!(rows.insert(pk.clone(), source).is_none(), "{pk:?} twice");
    }
    let expected: BTreeMap<PrimaryKey, Map<String, Value>> = fold_history(&history)
        .into_iter()
        .map(|(pk, doc)| (pk, doc.source))
        .collect();
    assert!(
        !expected.is_empty() && expected.len() < 24,
        "{}",
        expected.len()
    );
    assert_eq!(rows, expected);

    let info = service.get_collection(NS, KB).await.expect("describe");
    let counted = dataset.count_rows(None).await.expect("count") as u64;
    assert_eq!(counted, plan.live_rows);
    assert_eq!(plan.live_rows, info.live_doc_count);
    assert_eq!(
        plan.live_rows,
        plan.fragments.iter().map(|f| f.live_rows).sum::<u64>()
    );

    // The plan's fragments are the dataset's.
    let fragments = dataset.fragments();
    assert_eq!(fragments.len(), plan.fragments.len());
    for (lance, planned) in fragments.iter().zip(&plan.fragments) {
        assert_eq!(planned.id, lance.id);
        let files: Vec<String> = lance
            .files
            .iter()
            .map(|file| format!("data/{}", file.path))
            .collect();
        let planned_files: Vec<String> = planned.files.iter().map(|f| f.path.clone()).collect();
        assert_eq!(planned_files, files);
        assert_eq!(
            planned.deletion_file.as_ref().map(|d| d.path.clone()),
            lance
                .deletion_file
                .as_ref()
                .map(|d| relative_deletion_file_path(lance.id, d))
        );
        assert_eq!(planned.lance, serde_json::to_value(lance).expect("JSON"));
        assert_eq!(
            planned.live_rows,
            planned.physical_rows - planned.deleted_rows
        );
        for file in &planned.files {
            assert!(
                dir.path().join(uri_path(&uri, &file.path)).exists(),
                "{}",
                file.path
            );
        }
    }
    assert!(
        plan.fragments.iter().any(|f| f.deletion_file.is_some()),
        "{:?}",
        plan.fragments
    );

    // The mainline holds nothing: readers need the plan's version (D34).
    let mainline = DatasetBuilder::from_uri(&uri)
        .with_version(1)
        .load()
        .await
        .expect("open the mainline");
    assert_eq!(mainline.count_rows(None).await.expect("count"), 0);
    f.shutdown().await;
}

/// The path of `relative` under the dataset at `uri`, relative to the bucket.
fn uri_path(uri: &str, relative: &str) -> String {
    let dataset = uri.split("/ns/").nth(1).expect("a dataset under ns/");
    format!("ns/{dataset}/{relative}")
}

#[tokio::test]
async fn unapplied_writes_set_the_tail_flag() {
    let f = Fixture::start().await;
    let service = f.service();
    let info = create(&service, plain_schema()).await;
    write(&service, upserts(0..5)).await;
    f.settle().await;
    let before = plan(&service, ScanAt::Current).await;
    assert!(!before.tail);
    assert_eq!(before.durable_token, before.pin.token);

    write(&service, upserts(5..12)).await;
    let plan_ = plan(&service, ScanAt::Current).await;
    assert_eq!(plan_.manifest_version, before.manifest_version);
    assert!(plan_.tail);
    assert_eq!(plan_.tail_records, 7);
    assert_eq!(plan_.pin.token, high_watermarks(&f, &info).await);
    assert_eq!(plan_.durable_token, before.durable_token);
    assert_eq!(
        plan_
            .offsets
            .iter()
            .map(|o| o.target - o.applied)
            .sum::<u64>(),
        7
    );

    f.settle().await;
    let after = plan(&service, ScanAt::Current).await;
    assert!(!after.tail);
    assert_eq!(after.tail_records, 0);
    assert_eq!(after.durable_token, after.pin.token);
    assert!(after.manifest_version > before.manifest_version);
    f.shutdown().await;
}

#[tokio::test]
async fn the_pin_reads_the_tail_through_flight_sql() {
    let f = Fixture::start().await;
    let service = f.service();
    create(&service, plain_schema()).await;
    write(&service, upserts(0..5)).await;
    f.settle().await;
    write(&service, upserts(5..12)).await;
    let plan = plan(&service, ScanAt::Current).await;
    assert!(plan.tail);

    assert_eq!(sql_count(&service, plan.pinned()).await, json!(12));
    // Later writes do not change the pinned state.
    write(&service, upserts(12..17)).await;
    assert_eq!(sql_count(&service, plan.pinned()).await, json!(12));
    assert_eq!(
        sql_count(&service, ReadConsistency::Strong).await,
        json!(17)
    );

    // A Flight SQL ticket carries the pin, so DoGet reads the same state.
    let ticket = StatementTicket {
        namespace: NS.to_string(),
        query: "SELECT count(*) AS n FROM kb".to_string(),
        token: Some(plan.pin.token.to_string()),
        pinned_manifest: Some(plan.pin.manifest_version),
    };
    let decoded = decode_ticket(&encode_ticket(&ticket)).expect("decodes");
    assert_eq!(decoded, ticket);
    let consistency = ticket_consistency(&decoded).expect("pinned");
    assert_eq!(consistency, plan.pinned());
    assert_eq!(sql_count(&service, consistency).await, json!(12));
    f.shutdown().await;
}

#[tokio::test]
async fn a_manifest_version_plan_is_that_manifest() {
    let f = Fixture::start().await;
    let service = f.service();
    create(&service, plain_schema()).await;
    write(&service, upserts(0..6)).await;
    f.settle().await;
    let at_v = plan(&service, ScanAt::Current).await;
    let v = at_v.manifest_version;
    write(&service, upserts(3..9)).await;
    f.settle().await;
    write(&service, vec![DocOp::Delete(PrimaryKey::U64(0))]).await;
    f.settle().await;

    let old = plan(&service, ScanAt::ManifestVersion(v)).await;
    assert_eq!(old.manifest_version, v);
    assert_eq!(old.lance, at_v.lance);
    assert_eq!(old.fragments, at_v.fragments);
    assert_eq!(old.live_rows, at_v.live_rows);
    assert!(!old.tail);
    assert_eq!(old.durable_token, old.pin.token);
    let versions = service.versions(NS, KB).await.expect("versions");
    let child = versions
        .iter()
        .find(|m| m.version == v + 1)
        .expect("version v + 1");
    assert_eq!(
        old.expires_at_ms,
        Some(child.created_at_ms + 24 * 60 * 60 * 1000)
    );
    // The pin reads that manifest.
    assert_eq!(sql_count(&service, old.pinned()).await, json!(6));
    f.shutdown().await;
}

#[tokio::test]
async fn a_gone_manifest_is_not_found() {
    let collection = CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let f = Fixture::start_configured(ServiceConfig::default(), collection).await;
    let service = f.service();
    create(&service, plain_schema()).await;
    write(&service, upserts(0..2)).await;
    f.settle().await;
    let v = plan(&service, ScanAt::Current).await.manifest_version;
    for k in 2..5 {
        write(&service, upserts(k..k + 1)).await;
        f.settle().await;
    }
    match service.scan_plan(NS, KB, ScanAt::ManifestVersion(v)).await {
        Err(ServiceError::NotFound { kind, name }) => {
            assert_eq!(kind, "pin");
            assert_eq!(name, format!("kb@{v}"));
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
    // A version that never existed is gone too; an unknown collection is
    // not found as a collection.
    assert!(matches!(
        service
            .scan_plan(NS, KB, ScanAt::ManifestVersion(1_000))
            .await,
        Err(ServiceError::NotFound { kind: "pin", .. })
    ));
    assert!(matches!(
        service.scan_plan(NS, "nope", ScanAt::Current).await,
        Err(ServiceError::NotFound {
            kind: "collection",
            ..
        })
    ));
    f.shutdown().await;
}

#[tokio::test]
async fn expiry_follows_retention() {
    let collection = CollectionConfig {
        time_travel_retention: Duration::from_secs(3_600),
        ..CollectionConfig::default()
    };
    let f = Fixture::start_configured(ServiceConfig::default(), collection).await;
    let service = f.service();
    create(&service, plain_schema()).await;
    assert_eq!(plan(&service, ScanAt::Current).await.expires_at_ms, None);
    write(&service, upserts(0..3)).await;
    f.settle().await;
    let current = plan(&service, ScanAt::Current).await;
    assert!(current.manifest_version > 0);
    assert_eq!(
        current.expires_at_ms,
        Some(current.planned_at_ms + 3_600_000)
    );
    f.shutdown().await;
}

#[tokio::test]
async fn a_token_plan_waits_for_the_link() {
    let f = Fixture::start().await;
    let service = f.service();
    let info = create(&service, plain_schema()).await;
    let token = write(&service, upserts(0..8)).await;
    let waiting = {
        let service = service.clone();
        let token = token.clone();
        tokio::spawn(async move { service.scan_plan(NS, KB, ScanAt::Token(token)).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "the plan did not wait for the link");
    f.apply_link().await;
    let plan = tokio::time::timeout(WAIT, waiting)
        .await
        .expect("the plan completes")
        .expect("the task")
        .expect("the plan");
    for offsets in &plan.offsets {
        let wanted = token.offset(info.stream, offsets.partition).unwrap_or(0);
        assert!(offsets.applied >= wanted, "{offsets:?} against {token}");
        assert_eq!(offsets.target, offsets.applied);
    }
    assert!(!plan.tail);
    assert_eq!(plan.live_rows, 8);
    assert_eq!(plan.durable_token, plan.pin.token);
    f.shutdown().await;
}

#[tokio::test]
async fn a_token_plan_times_out_like_a_strong_read() {
    let mut config = ServiceConfig::default();
    config.read.consistency_wait = Duration::from_millis(300);
    let f = Fixture::start_with(config).await;
    let service = f.service();
    create(&service, plain_schema()).await;
    let token = write(&service, upserts(0..3)).await;
    let started = Instant::now();
    let result = service.scan_plan(NS, KB, ScanAt::Token(token)).await;
    assert!(
        matches!(result, Err(ServiceError::Timeout)),
        "{:?}",
        result.map(|p| p.manifest_version)
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    f.shutdown().await;
}

#[tokio::test]
async fn columns_follow_the_lance_version() {
    let f = Fixture::start().await;
    let service = f.service();
    let schema = CollectionSchema::new(
        vec![crate::common::field(
            "tag",
            operon_collection::FieldKind::Keyword,
        )],
        vec![vector("e", 3)],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::Idf,
    }]);
    schema.validate().expect("valid schema");
    create(&service, schema).await;
    let doc = |pk: u64, vectors: &[(&str, Vec<f32>)]| {
        DocOp::Upsert(Document {
            pk: PrimaryKey::U64(pk),
            source: crate::common::obj(json!({"tag": "a"})),
            vectors: vectors
                .iter()
                .map(|(name, v)| (name.to_string(), v.clone()))
                .collect(),
            sparse_vectors: BTreeMap::from([(
                "s".to_string(),
                SparseVector::new(vec![1, 4], vec![0.5, 1.0]).expect("sparse"),
            )]),
        })
    };
    write(&service, vec![doc(1, &[("e", vec![1.0, 0.0, 0.0])])]).await;
    f.settle().await;

    let names =
        |plan: &ScanPlan| -> Vec<String> { plan.columns.iter().map(|c| c.name.clone()).collect() };
    let first = plan(&service, ScanAt::Current).await;
    let columns: Vec<Value> = first
        .columns
        .iter()
        .map(|c| serde_json::to_value(c).expect("JSON"))
        .collect();
    assert_eq!(
        columns[..4],
        [
            json!({"name": "_pk", "data_type": "Binary", "role": "pk"}),
            json!({"name": "_source", "data_type": "Binary", "role": "source"}),
            json!({"name": "_ingest_partition", "data_type": "UInt32", "role": "ingest_partition"}),
            json!({"name": "_ingest_offset", "data_type": "UInt64", "role": "ingest_offset"}),
        ]
    );
    assert_eq!(
        names(&first),
        [
            "_pk",
            "_source",
            "_ingest_partition",
            "_ingest_offset",
            "_vector_0",
            "_sparse_0"
        ]
    );
    let e = &columns[4];
    assert_eq!(
        (
            &e["role"],
            &e["vector"],
            &e["dim"],
            &e["distance"],
            e.get("modifier")
        ),
        (
            &json!("vector"),
            &json!("e"),
            &json!(3),
            &json!("cosine"),
            None
        )
    );
    let s = &columns[5];
    assert_eq!(
        (
            &s["role"],
            &s["vector"],
            &s["modifier"],
            s.get("dim"),
            s.get("distance")
        ),
        (
            &json!("sparse_vector"),
            &json!("s"),
            &json!("idf"),
            None,
            None
        )
    );
    assert!(first.columns.iter().all(|c| c.name != "tag"));

    // A vector added after this Lance version is absent from it.
    service
        .add_fields(NS, KB, Vec::new(), vec![vector("f", 2)], BTreeMap::new())
        .await
        .expect("add f");
    let unchanged = plan(&service, ScanAt::Current).await;
    assert_eq!(names(&unchanged), names(&first));

    // A commit carrying f adds its column.
    write(&service, vec![doc(2, &[("f", vec![0.0, 1.0])])]).await;
    f.settle().await;
    let later = plan(&service, ScanAt::Current).await;
    let f_column = later
        .columns
        .iter()
        .find(|c| c.name == "_vector_1")
        .expect("_vector_1");
    assert_eq!(f_column.role, ColumnRole::Vector);
    assert_eq!(f_column.vector.as_deref(), Some("f"));
    assert_eq!(f_column.dim, Some(2));
    f.shutdown().await;
}

#[test]
fn scan_points_parse_and_tags_are_refused() {
    let token: ConsistencyToken = "v1:s9/p0@4,s9/p1@2".parse().expect("token");
    for (at, value) in [
        (ScanAt::Current, json!("current")),
        (ScanAt::ManifestVersion(7), json!({"manifest_version": 7})),
        (
            ScanAt::ManifestVersion(u64::MAX),
            json!({"manifest_version": u64::MAX}),
        ),
        (
            ScanAt::Token(token.clone()),
            json!({"token": "v1:s9/p0@4,s9/p1@2"}),
        ),
    ] {
        assert_eq!(serde_json::to_value(&at).expect("JSON"), value);
        assert_eq!(ScanAt::from_json(&value).expect("parses"), at);
        assert_eq!(serde_json::from_value::<ScanAt>(value).expect("parses"), at);
    }
    let refused = |value: Value| match ScanAt::from_json(&value) {
        Err(ServiceError::InvalidArgument(message)) => message,
        other => panic!("{value}: expected InvalidArgument, got {other:?}"),
    };
    assert_eq!(refused(json!({"tag": "x"})), "tags arrive in M2 (D52)");
    assert_eq!(
        refused(json!({"manifest": 1})),
        r#"unknown scan point {"manifest":1}"#
    );
    assert_eq!(refused(json!("latest")), r#"unknown scan point "latest""#);
    assert_eq!(
        refused(json!({"manifest_version": -1})),
        r#"unknown scan point {"manifest_version":-1}"#
    );

    // The request body: `at` defaults to current; unknown keys are refused.
    let request: ScanRequest = serde_json::from_value(json!({})).expect("empty body");
    assert_eq!(request.at, ScanAt::Current);
    let request: ScanRequest =
        serde_json::from_value(json!({"at": {"manifest_version": 3}})).expect("body");
    assert_eq!(request.at, ScanAt::ManifestVersion(3));
    let err =
        serde_json::from_value::<ScanRequest>(json!({"at": {"tag": "x"}})).expect_err("a tag");
    assert!(err.to_string().contains("tags arrive in M2 (D52)"), "{err}");
    assert!(serde_json::from_value::<ScanRequest>(json!({"when": "current"})).is_err());
}
