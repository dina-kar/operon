//! Elasticsearch query DSL → `QueryAst` → Tantivy query.

use operon_quickwit::query::ElasticQueryDsl;
use operon_quickwit::query::query_ast::{BuildTantivyAstContext, QueryAst};
use tantivy::collector::DocSetCollector;
use tantivy::schema::{FAST, STRING, Schema};
use tantivy::{Index, TantivyDocument};

#[test]
fn es_bool_term_range_convert_to_a_tantivy_query() {
    let mut schema_builder = Schema::builder();
    let tag = schema_builder.add_text_field("tag", STRING);
    let n = schema_builder.add_i64_field("n", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
    // Matches: tag "a" and n >= 2, i.e. documents 2 and 4.
    for (doc_tag, doc_n) in [("a", 1), ("a", 2), ("b", 3), ("b", 1), ("a", 5)] {
        let mut doc = TantivyDocument::default();
        doc.add_text(tag, doc_tag);
        doc.add_i64(n, doc_n);
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();

    let dsl: ElasticQueryDsl = serde_json::from_str(
        r#"{"bool":{"must":[{"term":{"tag":"a"}}],"filter":[{"range":{"n":{"gte":2}}}]}}"#,
    )
    .unwrap();
    let query_ast = QueryAst::try_from(dsl).unwrap();
    let query = query_ast
        .build_tantivy_query(&BuildTantivyAstContext::for_test(&schema))
        .unwrap();

    let searcher = index.reader().unwrap().searcher();
    let mut hits: Vec<i64> = searcher
        .search(&query, &DocSetCollector)
        .unwrap()
        .into_iter()
        .map(|address| {
            let segment_reader = searcher.segment_reader(address.segment_ord);
            let column = segment_reader.fast_fields().i64("n").unwrap();
            column.first(address.doc_id).unwrap()
        })
        .collect();
    hits.sort_unstable();
    assert_eq!(hits, vec![2, 5]);
}

#[test]
fn unsupported_queries_are_rejected() {
    for query in [
        r#"{"fuzzy":{"tag":{"value":"a"}}}"#,
        r#"{"ids":{"values":["1","2"]}}"#,
    ] {
        assert!(
            serde_json::from_str::<ElasticQueryDsl>(query).is_err(),
            "{query} should not deserialize"
        );
    }
}
