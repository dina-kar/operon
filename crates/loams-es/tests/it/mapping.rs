//! Task 2: ES mappings and settings ⇄ collection schemas.

use loams_collection::{
    CollectionSchema, Distance, DynamicMapping, FieldKind, FieldSpec, HnswParams, Quantization,
    VectorIndexSpec,
};
use loams_common::{CollectionId, StreamId};
use loams_es::EsError;
use loams_es::mapping::{
    ANN_SETTINGS, ANN_SIMILARITY, EsSimilarity, IndexView, VectorView, dynamic_plan, index_uuid,
    normalize_settings, plan_create, plan_put_mapping, render_mappings, render_settings,
};
use loams_query::CollectionInfo;
use loams_query::backlog::BackpressureStatus;
use loams_query::hot::HotStatus;
use serde_json::{Map, Value, json};

fn info(schema: CollectionSchema) -> CollectionInfo {
    CollectionInfo {
        id: CollectionId(7),
        name: "i".to_string(),
        namespace: "default".to_string(),
        schema,
        partitions: 3,
        aliases: Vec::new(),
        stream: StreamId(1),
        manifest_version: 0,
        live_doc_count: 0,
        size_bytes: 0,
        created_at_ms: 1_700_000_000_000,
        link_lag_records: 0,
        hot: HotStatus::default(),
        unapplied_bytes: 0,
        backpressure: BackpressureStatus::default(),
    }
}

fn create(mappings: Value) -> CollectionSchema {
    plan_create(Some(&mappings), None)
        .expect("plan_create")
        .schema()
}

fn create_err(mappings: Value, settings: Option<Value>) -> EsError {
    plan_create(Some(&mappings), settings.as_ref()).expect_err("refused")
}

fn obj(value: Value) -> Map<String, Value> {
    value.as_object().cloned().expect("object")
}

/// `schema` with the fields dynamic mapping adds for `source`.
fn with_dynamic(schema: &CollectionSchema, source: Value) -> CollectionSchema {
    let view = IndexView::new(info(schema.clone()));
    let added = dynamic_plan(&view, &obj(source)).expect("dynamic_plan");
    let mut next = schema.clone();
    next.fields.extend(added);
    next.validate().expect("valid");
    next
}

fn field<'a>(schema: &'a CollectionSchema, name: &str) -> &'a FieldSpec {
    schema
        .field(name)
        .unwrap_or_else(|| panic!("no field {name}"))
}

fn text(analyzer: &str) -> FieldKind {
    FieldKind::Text {
        analyzer: analyzer.to_string(),
        positions: true,
    }
}

#[test]
fn declared_types_round_trip_through_the_schema() {
    // LlamaIndex's keyword ids and LangChain's metadata mapping (C8, C9)
    // with a 16-dim cosine vector (C5).
    let properties = json!({
        "document_id": {"type": "keyword"},
        "doc_id": {"type": "keyword"},
        "ref_doc_id": {"type": "keyword"},
        "content": {"type": "text"},
        "embedding": {"type": "dense_vector", "dims": 16, "index": true, "similarity": "cosine"},
        "metadata": {"properties": {
            "category": {"type": "keyword"},
            "score": {"type": "float"},
            "tags": {"type": "text"},
            "at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "n": {"type": "integer", "index": false},
        }},
    });
    let schema = create(json!({"properties": properties.clone()}));
    assert_eq!(field(&schema, "doc_id").kind, FieldKind::Keyword);
    assert_eq!(field(&schema, "metadata.score").kind, FieldKind::F64);
    assert_eq!(field(&schema, "metadata.n").kind, FieldKind::I64);
    assert!(!field(&schema, "metadata.n").indexed);
    assert_eq!(field(&schema, "content").kind, text("standard"));
    let (_, vector) = schema.vector("embedding").expect("vector");
    assert_eq!(
        (vector.dim, vector.distance, vector.index),
        (16, Distance::Cosine, VectorIndexSpec::Auto)
    );
    let rendered = render_mappings(&schema);
    assert_eq!(rendered, json!({"properties": properties}));
    assert_eq!(
        rendered["properties"]["metadata"]["properties"]["score"],
        json!({"type": "float"})
    );

    let view = IndexView::new(info(schema));
    assert_eq!(view.es.es_types["metadata.score"], "float");
    assert_eq!(view.es.es_types["metadata.n"], "integer");
    assert_eq!(view.es.es_types["embedding"], "dense_vector");
    assert_eq!(
        view.es.vectors["embedding"],
        VectorView {
            dim: 16,
            similarity: EsSimilarity::Cosine,
            indexed: true,
        }
    );
    assert_eq!(view.es.max_result_window, 10_000);
    assert_eq!(view.field_kind("doc_id"), Some(&FieldKind::Keyword));
    assert_eq!(view.field_kind("nope"), None);
}

#[test]
fn dense_vector_similarities_map_to_distances() {
    for (similarity, distance) in [
        ("cosine", Distance::Cosine),
        ("dot_product", Distance::Dot),
        ("l2_norm", Distance::Euclid),
        ("max_inner_product", Distance::Dot),
    ] {
        let schema = create(json!({"properties": {
            "v": {"type": "dense_vector", "dims": 3, "similarity": similarity}
        }}));
        let (_, v) = schema.vector("v").expect("vector");
        assert_eq!(v.distance, distance, "{similarity}");
        assert_eq!(v.index, VectorIndexSpec::Auto);
        assert_eq!(v.hnsw, HnswParams::default());
        assert_eq!(
            schema.annotations[&format!("{ANN_SIMILARITY}v")],
            similarity
        );
        let view = IndexView::new(info(schema));
        assert_eq!(view.es.vectors["v"].similarity.as_str(), similarity);
    }
    // index: false is exact search only, cosine unless a similarity is given.
    let schema = create(json!({"properties": {
        "v": {"type": "dense_vector", "dims": 3, "index": false}
    }}));
    let (_, v) = schema.vector("v").expect("vector");
    assert_eq!(
        (v.index, v.distance),
        (VectorIndexSpec::None, Distance::Cosine)
    );
    assert!(!IndexView::new(info(schema)).es.vectors["v"].indexed);

    let schema = create(json!({"properties": {
        "v": {"type": "dense_vector", "dims": 8, "index_options": {"type": "int8_hnsw", "m": 32}}
    }}));
    let (_, v) = schema.vector("v").expect("vector");
    assert_eq!(v.index, VectorIndexSpec::Auto);
    assert_eq!(
        v.hnsw,
        HnswParams {
            m: 32,
            ef_construct: 100,
            ..HnswParams::default()
        }
    );
    assert_eq!(
        v.quantization,
        Some(Quantization::Scalar {
            quantile_ppm: None,
            always_ram: false,
        })
    );
    let schema = create(json!({"properties": {
        "v": {"type": "dense_vector", "dims": 8, "index_options": {"type": "flat"}}
    }}));
    assert_eq!(
        schema.vector("v").expect("vector").1.index,
        VectorIndexSpec::None
    );

    let e = create_err(
        json!({"properties": {"v": {"type": "dense_vector", "dims": 8, "index_options": {"type": "bbq_hnsw"}}}}),
        None,
    );
    assert_eq!(
        (e.status, e.kind.as_str()),
        (400, "illegal_argument_exception")
    );
    let e = create_err(
        json!({"properties": {"v": {"type": "dense_vector", "dims": 5000}}}),
        None,
    );
    assert_eq!(e.kind, "mapper_parsing_exception");
    assert_eq!(
        e.reason,
        "Failed to parse mapping: The number of dimensions should be in the range [1, 4096] but \
         was [5000]"
    );
    // ES's wrapping: the inner error is the root cause and `caused_by`.
    let body = e.to_body();
    assert_eq!(
        body["error"]["root_cause"][0]["reason"],
        "The number of dimensions should be in the range [1, 4096] but was [5000]"
    );
    assert_eq!(
        body["error"]["caused_by"],
        json!({"type": "mapper_parsing_exception", "reason": "The number of dimensions should be in the range [1, 4096] but was [5000]"})
    );
    let e = create_err(
        json!({"properties": {"v": {"type": "dense_vector", "dims": 3, "similarity": "hamming"}}}),
        None,
    );
    assert_eq!(
        e.reason,
        "Failed to parse mapping: Unknown value [hamming] for field [similarity] - accepted \
         values are [l2_norm, cosine, dot_product, max_inner_product]"
    );

    // Without dims the vector waits for its first document.
    let schema = create(json!({"properties": {"v": {"type": "dense_vector"}}}));
    assert!(schema.vectors.is_empty());
    let view = IndexView::new(info(schema.clone()));
    assert_eq!(
        view.es.pending_vectors["v"],
        json!({"type": "dense_vector"})
    );
    assert_eq!(
        render_mappings(&schema),
        json!({"properties": {"v": {"type": "dense_vector"}}})
    );
}

#[test]
fn bm25_similarity_settings_are_accepted_at_defaults() {
    let settings = json!({"similarity": {"custom_bm25": {"type": "BM25"}}});
    let flat = normalize_settings(&settings).expect("normalise");
    assert_eq!(flat["index.similarity.custom_bm25.type"], "BM25");
    let mappings = json!({"properties": {"text": {"type": "text", "similarity": "custom_bm25"}}});
    let plan = plan_create(Some(&mappings), Some(&settings)).expect("plan");
    assert_eq!(plan.fields[0].kind, text("standard"));
    assert_eq!(
        plan.annotations[ANN_SETTINGS],
        r#"{"index.similarity.custom_bm25.type":"BM25"}"#
    );
    let defaults = json!({"index": {"similarity": {"s": {"type": "BM25", "k1": 1.2, "b": 0.75}}}});
    plan_create(None, Some(&defaults)).expect("defaults spelled out");

    let e = create_err(
        mappings.clone(),
        Some(json!({"similarity": {"custom_bm25": {"type": "BM25", "k1": 2.0}}})),
    );
    assert_eq!(e.kind, "illegal_argument_exception");
    assert_eq!(
        e.reason,
        "Loams supports BM25 with k1=1.2 and b=0.75 only (Phase A)"
    );
    // A text similarity that no setting defines.
    let e = create_err(mappings, None);
    assert_eq!(e.kind, "mapper_parsing_exception");
    assert!(
        e.reason.contains("Unknown Similarity type [custom_bm25]"),
        "{}",
        e.reason
    );
}

#[test]
fn nested_and_unknown_types_are_refused() {
    let e = create_err(json!({"properties": {"x": {"type": "nested"}}}), None);
    assert_eq!(
        (e.status, e.kind.as_str()),
        (400, "mapper_parsing_exception")
    );
    assert_eq!(
        e.reason,
        "Loams does not support field type [nested] (declared on field [x]) in Elasticsearch \
         API Phase A"
    );
    let e = create_err(json!({"properties": {"x": {"type": "foo"}}}), None);
    assert!(
        e.reason
            .ends_with("The mapper type [foo] declared on field [x] does not exist. It might have been created within a future version or requires a plugin to be installed. Check the documentation."),
        "{}",
        e.reason
    );
    let e = create_err(
        json!({"properties": {"x": {"type": "text", "copy_to": "all"}}}),
        None,
    );
    assert_eq!(
        e.reason,
        "Loams does not support [copy_to] (Elasticsearch API Phase A)"
    );
    let e = create_err(
        json!({"properties": {"x": {"type": "keyword", "bogus": 1}}}),
        None,
    );
    assert_eq!(
        e.reason,
        "Failed to parse mapping: unknown parameter [bogus] on mapper [x] of type [keyword]"
    );
    let e = create_err(
        json!({"properties": {"x": {"type": "text", "analyzer": "french"}}}),
        None,
    );
    assert!(
        e.reason
            .contains("analyzer [french] has not been configured in mappings"),
        "{}",
        e.reason
    );
    let e = create_err(json!({"properties": {}, "_meta": {"a": 1}}), None);
    assert_eq!(e.kind, "mapper_parsing_exception");
    assert!(
        e.reason.starts_with(
            "Failed to parse mapping: Root mapping definition has unsupported parameters:"
        ),
        "{}",
        e.reason
    );
    assert!(e.reason.ends_with("[_meta : {\"a\":1}]"), "{}", e.reason);
    // Accepted and ignored.
    create(
        json!({"properties": {"x": {"type": "keyword", "store": true, "boost": 2}},
                  "_source": {"enabled": true}}),
    );
}

#[test]
fn a_string_gets_a_keyword_subfield() {
    let empty = create(json!({}));
    assert_eq!(empty.dynamic, DynamicMapping::Map);
    let schema = with_dynamic(&empty, json!({"title": "x"}));
    assert_eq!(field(&schema, "title").kind, text("standard"));
    let keyword = field(&schema, "title.keyword");
    assert_eq!(
        (&keyword.kind, keyword.source_path.as_str()),
        (&FieldKind::Keyword, "title")
    );
    assert_eq!(
        render_mappings(&schema),
        json!({"properties": {"title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        }}})
    );
    // A declared multi-field renders as declared.
    let declared = json!({"properties": {"t": {
        "type": "text", "analyzer": "english",
        "fields": {"raw": {"type": "keyword"}},
    }}});
    let schema = create(declared.clone());
    assert_eq!(field(&schema, "t.raw").source_path, "t");
    assert_eq!(render_mappings(&schema), declared);
}

#[test]
fn dynamic_fields_follow_es_rules() {
    let empty = create(json!({}));
    let schema = with_dynamic(
        &empty,
        json!({"page": 1, "score": 0.5, "ok": true, "at": "2026-09-24T10:00:00Z",
               "meta": {"a": "b"}, "n": null, "e": []}),
    );
    assert_eq!(field(&schema, "page").kind, FieldKind::I64);
    assert_eq!(field(&schema, "score").kind, FieldKind::F64);
    assert_eq!(field(&schema, "ok").kind, FieldKind::Bool);
    assert_eq!(field(&schema, "at").kind, FieldKind::Date);
    assert_eq!(field(&schema, "meta.a").kind, text("standard"));
    assert_eq!(field(&schema, "meta.a.keyword").kind, FieldKind::Keyword);
    assert!(schema.field("n").is_none() && schema.field("e").is_none());
    assert_eq!(
        render_mappings(&schema)["properties"]["page"],
        json!({"type": "long"})
    );

    // An integer above 2^63−1 is a float, as in ES (O-M15-3); i64::MAX
    // stays a long.
    let schema = with_dynamic(
        &empty,
        json!({"big": 18_446_744_073_709_551_615_u64, "top": i64::MAX}),
    );
    assert_eq!(field(&schema, "big").kind, FieldKind::F64);
    assert_eq!(field(&schema, "top").kind, FieldKind::I64);
    let rendered = render_mappings(&schema);
    assert_eq!(rendered["properties"]["big"], json!({"type": "float"}));
    assert_eq!(rendered["properties"]["top"], json!({"type": "long"}));

    // Vectors and disabled objects are not mapped dynamically.
    let schema = create(json!({"properties": {
        "v": {"type": "dense_vector", "dims": 2},
        "p": {"type": "dense_vector"},
        "blob": {"type": "object", "enabled": false},
    }}));
    let view = IndexView::new(info(schema));
    assert!(view.es.disabled_objects.contains("blob"));
    let added = dynamic_plan(
        &view,
        &obj(json!({"v": [1.0, 2.0], "p": [1.0], "blob": {"x": "y"}, "k": "w"})),
    )
    .expect("plan");
    let names: Vec<&str> = added.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["k", "k.keyword"]);

    // The field limit.
    let limited = plan_create(
        Some(&json!({"properties": {"a": {"type": "long"}}})),
        Some(&json!({"index.mapping.total_fields.limit": 2})),
    )
    .expect("plan")
    .schema();
    let view = IndexView::new(info(limited));
    let e = dynamic_plan(&view, &obj(json!({"s": "text"}))).expect_err("over the limit");
    assert_eq!(e.kind, "document_parsing_exception");
    assert_eq!(
        e.reason,
        "[1:1] failed to parse: Limit of total fields [2] has been exceeded while adding new \
         fields [2]"
    );
    assert_eq!(e.extra["caused_by"]["type"], "illegal_argument_exception");
}

#[test]
fn settings_normalise_and_refuse_unknown_keys() {
    for settings in [
        json!({"number_of_shards": 3}),
        json!({"index": {"number_of_shards": "3"}}),
        json!({"index.number_of_shards": 3}),
    ] {
        let plan = plan_create(None, Some(&settings)).expect("plan");
        assert_eq!(plan.partitions, Some(3), "{settings}");
        assert!(!plan.annotations.contains_key(ANN_SETTINGS), "{settings}");
    }
    let e = plan_create(None, Some(&json!({"index": {"knn": true}}))).expect_err("knn");
    assert_eq!(e.kind, "illegal_argument_exception");
    assert_eq!(
        e.reason,
        "unknown setting [index.knn] please check that any required plugins are installed, or \
         check the breaking changes documentation for removed settings"
    );
    let e = plan_create(None, Some(&json!({"number_of_shards": 0}))).expect_err("zero");
    assert_eq!(
        e.reason,
        "Failed to parse value [0] for setting [index.number_of_shards] must be >= 1"
    );
    let analysis = json!({"analysis": {"analyzer": {"mine": {"type": "custom"}}}});
    let e = plan_create(None, Some(&analysis)).expect_err("analysis");
    assert_eq!(
        e.reason,
        "Loams does not support [index.analysis] (Elasticsearch API Phase A)"
    );

    // Stored settings render under `settings.index`, nested by their dots.
    let settings = json!({"index": {"number_of_replicas": 0, "default_pipeline": "p",
                                    "max_result_window": 50,
                                    "mapping": {"total_fields": {"limit": 20}}}});
    let plan = plan_create(None, Some(&settings)).expect("plan");
    assert_eq!(plan.max_fields, 20);
    let info = info(plan.schema());
    let rendered = render_settings(&info);
    let index = &rendered["index"];
    assert_eq!(index["number_of_shards"], "3");
    assert_eq!(index["number_of_replicas"], "0");
    assert_eq!(index["default_pipeline"], "p");
    assert_eq!(index["mapping"]["total_fields"]["limit"], "20");
    assert_eq!(index["uuid"], index_uuid(CollectionId(7)));
    assert_eq!(index["creation_date"], "1700000000000");
    assert_eq!(index["provided_name"], "i");
    assert_eq!(index["version"]["created"], "8190099");
    let view = IndexView::new(info);
    assert_eq!(view.es.max_result_window, 50);
    assert_eq!(view.es.default_pipeline.as_deref(), Some("p"));
}

#[test]
fn strict_dynamic_is_reported_with_es_wording() {
    let schema = create(json!({"dynamic": "strict", "properties": {
        "a": {"properties": {"c": {"type": "long"}}},
    }}));
    assert_eq!(schema.dynamic, DynamicMapping::Strict);
    assert_eq!(render_mappings(&schema)["dynamic"], json!("strict"));
    let view = IndexView::new(info(schema));
    let e = dynamic_plan(&view, &obj(json!({"a": {"b": 1}}))).expect_err("strict");
    assert_eq!(
        (e.status, e.kind.as_str()),
        (400, "strict_dynamic_mapping_exception")
    );
    assert_eq!(
        e.reason,
        "[1:1] mapping set to strict, dynamic introduction of [b] within [a] is not allowed"
    );
    let e = dynamic_plan(&view, &obj(json!({"z": {"y": 1}}))).expect_err("top level");
    assert_eq!(
        e.reason,
        "[1:1] mapping set to strict, dynamic introduction of [z] within [_doc] is not allowed"
    );
    assert!(
        dynamic_plan(&view, &obj(json!({"a": {"c": 5}})))
            .expect("mapped")
            .is_empty()
    );
    // dynamic: false maps nothing and renders as "false".
    let schema = create(json!({"dynamic": false}));
    assert_eq!(render_mappings(&schema)["dynamic"], json!("false"));
    let view = IndexView::new(info(schema));
    assert!(
        dynamic_plan(&view, &obj(json!({"x": 1})))
            .expect("ignored")
            .is_empty()
    );
}

#[test]
fn put_mapping_refuses_a_type_change() {
    let empty = create(json!({}));
    let schema = with_dynamic(&empty, json!({"page": 1}));
    let e = plan_put_mapping(
        &schema,
        &json!({"properties": {"page": {"type": "keyword"}}}),
    )
    .expect_err("type change");
    assert_eq!(e.kind, "illegal_argument_exception");
    assert_eq!(
        e.reason,
        "mapper [page] cannot be changed from type [long] to [keyword]"
    );
    // The same type again adds nothing; a new field is an addition.
    let plan = plan_put_mapping(
        &schema,
        &json!({"properties": {"page": {"type": "long"}, "tag": {"type": "keyword"}}}),
    )
    .expect("additions");
    let names: Vec<&str> = plan.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["tag"]);
    let mut next = schema.clone();
    next.fields.extend(plan.fields);
    next.annotations.extend(plan.annotations);
    let rendered = render_mappings(&next);
    assert_eq!(rendered["properties"]["tag"], json!({"type": "keyword"}));
    assert_eq!(rendered["properties"]["page"], json!({"type": "long"}));
    let e = plan_put_mapping(&next, &json!({"properties": {"tag": {"type": "long"}}}))
        .expect_err("keyword to long");
    assert_eq!(
        e.reason,
        "mapper [tag] cannot be changed from type [keyword] to [long]"
    );
    // A declared path redeclared verbatim is a no-op.
    let again = plan_put_mapping(&next, &json!({"properties": {"tag": {"type": "keyword"}}}))
        .expect("same");
    assert!(again.fields.is_empty() && again.annotations.is_empty());
}

#[test]
fn the_cache_mappings_are_accepted_and_round_trip() {
    // The LangChain LLM cache and embeddings cache mappings (C50).
    let llm = json!({"properties": {
        "llm_output": {"type": "text", "index": false},
        "llm_params": {"type": "text", "index": false},
        "llm_input": {"type": "text", "index": false},
        "metadata": {"type": "object"},
        "timestamp": {"type": "date"},
    }});
    let embeddings = json!({"properties": {
        "text_input": {"type": "text", "index": false},
        "vector_dump": {"type": "binary", "doc_values": false},
        "metadata": {"type": "object"},
        "timestamp": {"type": "date"},
        "namespace": {"type": "keyword"},
    }});
    for mapping in [&llm, &embeddings] {
        let schema = create(mapping.clone());
        assert_eq!(&render_mappings(&schema), mapping);
        let put = plan_put_mapping(&create(json!({})), mapping).expect("put");
        let plan = plan_create(Some(mapping), None).expect("create");
        assert_eq!(put.fields, plan.fields);
        assert_eq!(put.annotations, plan.annotations);
    }
    let schema = create(embeddings);
    // M1.1 keeps a field only if it is indexed or fast (row T2-2): the
    // binary and the unindexed text are unindexed keywords.
    let dump = field(&schema, "vector_dump");
    assert_eq!((&dump.kind, dump.indexed), (&FieldKind::Keyword, false));
    let text_input = field(&schema, "text_input");
    assert_eq!(
        (&text_input.kind, text_input.indexed),
        (&FieldKind::Keyword, false)
    );
    let view = IndexView::new(info(schema));
    assert_eq!(view.es.es_types["vector_dump"], "binary");
    assert_eq!(view.es.es_types["text_input"], "text");
    assert!(!view.es.es_types.contains_key("metadata"));
}

#[test]
fn the_standard_analyzer_keeps_stop_words() {
    let schema = create(json!({"properties": {
        "a": {"type": "text"},
        "b": {"type": "text", "analyzer": "english"},
        "c": {"type": "text", "analyzer": "default"},
    }}));
    assert_eq!(field(&schema, "a").kind, text("standard"));
    assert_eq!(field(&schema, "b").kind, text("english"));
    assert_eq!(field(&schema, "c").kind, text("standard"));
}
