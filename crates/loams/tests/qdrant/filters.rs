//! Filters and payload indexes end to end (plan M1.4 Task 4): the gateway
//! against the reference evaluator over a seeded corpus, the LangChain and
//! LlamaIndex filter shapes, and payload-index creation.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use loams_collection::{DocOp, Document, PrimaryKey};
use loams_qdrant::filter::{parse_datetime, reference_eval};
use loams_qdrant::model::filter::Filter;
use loams_qdrant::proto::qdrant as pb;
use loams_query::{OpResult, WriteOptions};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use reqwest::StatusCode;
use serde_json::{Map, Value, json};

use crate::harness::Qd;

const NS: &str = "default";

fn error(body: &Value) -> &str {
    body["status"]["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no status.error: {body}"))
}

/// Creates `name` through the gateway (so it has the `payload` field).
async fn create(qd: &Qd, name: &str) {
    let (status, reply) = qd
        .put(
            &format!("/collections/{name}"),
            Some(json!({"vectors": {"size": 2, "distance": "Dot"}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
}

/// Upserts `(id, payload)` points through the collection service (the
/// upsert route is Task 5's).
async fn write(qd: &Qd, name: &str, points: &[(u64, Value)]) {
    let ops = points
        .iter()
        .map(|(id, payload)| {
            DocOp::Upsert(Document {
                pk: PrimaryKey::U64(*id),
                source: payload.as_object().cloned().expect("an object"),
                vectors: BTreeMap::from([(String::new(), vec![1.0, 0.0])]),
                sparse_vectors: BTreeMap::new(),
            })
        })
        .collect();
    let result = qd
        .server
        .collections()
        .write(NS, name, ops, WriteOptions::default())
        .await
        .expect("write");
    for op in &result.results {
        assert!(!matches!(op, OpResult::Rejected(_)), "{op:?}");
    }
}

/// Waits until the link has applied every record of `name`.
async fn applied(qd: &Qd, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = qd
            .server
            .collections()
            .get_collection(NS, name)
            .await
            .expect("info");
        if info.link_lag_records == 0 && info.manifest_version > 0 {
            return;
        }
        assert!(Instant::now() < deadline, "{name} did not settle");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The ids the filter selects, read through the scroll route (row T4-1:
/// Task 6 serves it), every page followed.
async fn scroll_ids(qd: &Qd, name: &str, filter: &Value) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    let mut offset = Value::Null;
    loop {
        let (status, reply) = qd
            .post(
                &format!("/collections/{name}/points/scroll"),
                Some(json!({"filter": filter, "limit": 1000, "offset": offset, "with_payload": false})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{filter}: {reply}");
        for point in reply["result"]["points"].as_array().expect("points") {
            out.insert(point["id"].as_u64().expect("u64 id"));
        }
        offset = reply["result"]["next_page_offset"].clone();
        if offset.is_null() {
            return out;
        }
    }
}

/// `POST …/points/count` with `filter`.
async fn rest_count(qd: &Qd, name: &str, filter: &Value) -> u64 {
    let (status, reply) = qd
        .post(
            &format!("/collections/{name}/points/count"),
            Some(json!({"filter": filter, "exact": true})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{filter}: {reply}");
    reply["result"]["count"].as_u64().expect("count")
}

/// gRPC `Points/Count` with `filter`.
async fn grpc_count(qd: &Qd, name: &str, filter: pb::Filter) -> u64 {
    let mut points = qd.points().await;
    points
        .count(pb::CountPoints {
            collection_name: name.into(),
            filter: Some(filter),
            exact: Some(true),
            ..Default::default()
        })
        .await
        .expect("count")
        .into_inner()
        .result
        .expect("result")
        .count
}

/// `PUT …/index?wait=true`.
async fn create_index(qd: &Qd, name: &str, body: Value) -> (StatusCode, Value) {
    qd.put(&format!("/collections/{name}/index?wait=true"), Some(body))
        .await
}

// ----- the differential test -----

/// A generated filter, rendered as REST JSON and as a gRPC message.
#[derive(Clone, Debug)]
struct F {
    must: Vec<C>,
    should: Vec<C>,
    must_not: Vec<C>,
    min_should: Option<(Vec<C>, usize)>,
}

#[derive(Clone, Debug)]
enum C {
    Field(Field),
    IsEmpty(&'static str),
    IsNull(&'static str),
    HasId(Vec<u64>),
    Filter(Box<F>),
}

#[derive(Clone, Debug, Default)]
struct Field {
    key: &'static str,
    m: Option<M>,
    range: Option<R>,
    count: Option<[Option<u64>; 4]>,
    is_empty: Option<bool>,
    is_null: Option<bool>,
}

#[derive(Clone, Debug)]
enum M {
    Int(i64),
    Str(&'static str),
    Bool(bool),
    AnyInts(Vec<i64>),
    AnyStrs(Vec<&'static str>),
    ExceptInts(Vec<i64>),
    ExceptStrs(Vec<&'static str>),
    Prefix(&'static str),
    Text(&'static str),
    TextAny(&'static str),
    Phrase(&'static str),
}

/// `[gt, gte, lt, lte]`.
#[derive(Clone, Debug)]
enum R {
    Num([Option<f64>; 4]),
    Date([Option<&'static str>; 4]),
}

const BOUNDS: [&str; 4] = ["gt", "gte", "lt", "lte"];

fn bounds_json<T: Into<Value> + Clone>(b: &[Option<T>; 4]) -> Value {
    let mut out = Map::new();
    for (name, v) in BOUNDS.iter().zip(b) {
        if let Some(v) = v {
            out.insert((*name).into(), v.clone().into());
        }
    }
    Value::Object(out)
}

impl F {
    fn json(&self) -> Value {
        let list = |l: &[C]| Value::Array(l.iter().map(C::json).collect());
        let mut out = Map::new();
        out.insert("must".into(), list(&self.must));
        out.insert("should".into(), list(&self.should));
        out.insert("must_not".into(), list(&self.must_not));
        if let Some((conditions, min_count)) = &self.min_should {
            out.insert(
                "min_should".into(),
                json!({"conditions": list(conditions), "min_count": min_count}),
            );
        }
        Value::Object(out)
    }

    fn pb(&self) -> pb::Filter {
        let list = |l: &[C]| l.iter().map(C::pb).collect();
        pb::Filter {
            must: list(&self.must),
            should: list(&self.should),
            must_not: list(&self.must_not),
            min_should: self.min_should.as_ref().map(|(c, n)| pb::MinShould {
                conditions: list(c),
                min_count: *n as u64,
            }),
        }
    }
}

impl C {
    fn json(&self) -> Value {
        match self {
            C::Field(f) => f.json(),
            C::IsEmpty(k) => json!({"is_empty": {"key": k}}),
            C::IsNull(k) => json!({"is_null": {"key": k}}),
            C::HasId(ids) => json!({"has_id": ids}),
            C::Filter(f) => f.json(),
        }
    }

    fn pb(&self) -> pb::Condition {
        use pb::condition::ConditionOneOf as O;
        let one = match self {
            C::Field(f) => O::Field(f.pb()),
            C::IsEmpty(k) => O::IsEmpty(pb::IsEmptyCondition { key: (*k).into() }),
            C::IsNull(k) => O::IsNull(pb::IsNullCondition { key: (*k).into() }),
            C::HasId(ids) => O::HasId(pb::HasIdCondition {
                has_id: ids
                    .iter()
                    .map(|id| pb::PointId {
                        point_id_options: Some(pb::point_id::PointIdOptions::Num(*id)),
                    })
                    .collect(),
            }),
            C::Filter(f) => O::Filter(f.pb()),
        };
        pb::Condition {
            condition_one_of: Some(one),
        }
    }
}

impl Field {
    fn json(&self) -> Value {
        let mut out = Map::new();
        out.insert("key".into(), json!(self.key));
        if let Some(m) = &self.m {
            let m = match m {
                M::Int(n) => json!({"value": n}),
                M::Str(s) => json!({"value": s}),
                M::Bool(b) => json!({"value": b}),
                M::AnyInts(v) => json!({"any": v}),
                M::AnyStrs(v) => json!({"any": v}),
                M::ExceptInts(v) => json!({"except": v}),
                M::ExceptStrs(v) => json!({"except": v}),
                M::Prefix(s) => json!({"prefix": s}),
                M::Text(s) => json!({"text": s}),
                M::TextAny(s) => json!({"text_any": s}),
                M::Phrase(s) => json!({"phrase": s}),
            };
            out.insert("match".into(), m);
        }
        match &self.range {
            Some(R::Num(b)) => {
                out.insert("range".into(), bounds_json(b));
            }
            Some(R::Date(b)) => {
                out.insert("range".into(), bounds_json(b));
            }
            None => {}
        }
        if let Some(b) = &self.count {
            out.insert("values_count".into(), bounds_json(b));
        }
        if let Some(e) = self.is_empty {
            out.insert("is_empty".into(), json!(e));
        }
        if let Some(n) = self.is_null {
            out.insert("is_null".into(), json!(n));
        }
        Value::Object(out)
    }

    fn pb(&self) -> pb::FieldCondition {
        use pb::r#match::MatchValue as V;
        let strs = |v: &[&str]| pb::RepeatedStrings {
            strings: v.iter().map(|s| (*s).to_string()).collect(),
        };
        let ints = |v: &[i64]| pb::RepeatedIntegers {
            integers: v.to_vec(),
        };
        let m = self.m.as_ref().map(|m| pb::Match {
            match_value: Some(match m {
                M::Int(n) => V::Integer(*n),
                M::Str(s) => V::Keyword((*s).into()),
                M::Bool(b) => V::Boolean(*b),
                M::AnyInts(v) => V::Integers(ints(v)),
                M::AnyStrs(v) => V::Keywords(strs(v)),
                M::ExceptInts(v) => V::ExceptIntegers(ints(v)),
                M::ExceptStrs(v) => V::ExceptKeywords(strs(v)),
                M::Prefix(s) => V::Prefix((*s).into()),
                M::Text(s) => V::Text((*s).into()),
                M::TextAny(s) => V::TextAny((*s).into()),
                M::Phrase(s) => V::Phrase((*s).into()),
            }),
        });
        let (range, datetime_range) = match &self.range {
            Some(R::Num([gt, gte, lt, lte])) => (
                Some(pb::Range {
                    gt: *gt,
                    gte: *gte,
                    lt: *lt,
                    lte: *lte,
                }),
                None,
            ),
            Some(R::Date(b)) => {
                let ts = |s: &Option<&str>| {
                    s.map(|s| {
                        let us = parse_datetime(s).expect("date");
                        prost_types::Timestamp {
                            seconds: us.div_euclid(1_000_000),
                            nanos: (us.rem_euclid(1_000_000) * 1000) as i32,
                        }
                    })
                };
                (
                    None,
                    Some(pb::DatetimeRange {
                        gt: ts(&b[0]),
                        gte: ts(&b[1]),
                        lt: ts(&b[2]),
                        lte: ts(&b[3]),
                    }),
                )
            }
            None => (None, None),
        };
        pb::FieldCondition {
            key: self.key.into(),
            r#match: m,
            range,
            datetime_range,
            values_count: self
                .count
                .map(|[gt, gte, lt, lte]| pb::ValuesCount { gt, gte, lt, lte }),
            is_empty: self.is_empty,
            is_null: self.is_null,
            ..Default::default()
        }
    }
}

const KEYS: [&str; 8] = ["a", "b", "c.d", "c", "e[].f", "e.f", "doc_id", "nope"];
const STRS: [&str; 12] = [
    "x",
    "y",
    "1",
    "quick fox",
    "The Quick brown Fox",
    "lazy dog",
    "fox",
    "doc-1",
    "2023-02-08T10:49:00Z",
    "2023-03-01",
    "2023-02-08 10:49:00",
    "20230208",
];
const TEXTS: [&str; 6] = ["quick fox", "fox", "brown", "lazy dog", "QUICK", "dog fox"];
const PHRASES: [&str; 5] = ["quick fox", "quick brown", "brown fox", "lazy dog", "fox"];
const PREFIXES: [&str; 6] = ["q", "x", "doc-", "doc-1", "Th", "2023"];
const DATES: [&str; 5] = [
    "2023-01-01",
    "2023-02-08T10:49:00Z",
    "2023-02-08T10:49:00.001Z",
    "2023-02-15",
    "2023-03-01T00:00:00+01:00",
];
const CORPUS: u64 = 500;

fn key() -> impl Strategy<Value = &'static str> {
    prop::sample::select(&KEYS[..])
}

fn match_strategy() -> impl Strategy<Value = M> {
    let int = -2i64..=3;
    let s = prop::sample::select(&STRS[..]);
    prop_oneof![
        int.clone().prop_map(M::Int),
        s.clone().prop_map(M::Str),
        any::<bool>().prop_map(M::Bool),
        prop::collection::vec(int.clone(), 0..3).prop_map(M::AnyInts),
        prop::collection::vec(s.clone(), 1..3).prop_map(M::AnyStrs),
        prop::collection::vec(int, 0..3).prop_map(M::ExceptInts),
        prop::collection::vec(s, 1..3).prop_map(M::ExceptStrs),
        prop::sample::select(&PREFIXES[..]).prop_map(M::Prefix),
        prop::sample::select(&TEXTS[..]).prop_map(M::Text),
        prop::sample::select(&TEXTS[..]).prop_map(M::TextAny),
        prop::sample::select(&PHRASES[..]).prop_map(M::Phrase),
    ]
}

/// Bounds with at least one set.
fn bounds<T: Clone + std::fmt::Debug>(
    v: impl Strategy<Value = T> + Clone,
) -> impl Strategy<Value = [Option<T>; 4]> {
    prop::array::uniform4(prop::option::of(v))
        .prop_filter("a bound", |b| b.iter().any(Option::is_some))
}

fn range_strategy() -> impl Strategy<Value = R> {
    let num = prop::sample::select(&[-2.5, -1.0, 0.0, 1.0, 1.5, 2.0, 3.0][..]);
    prop_oneof![
        bounds(num).prop_map(R::Num),
        bounds(prop::sample::select(&DATES[..])).prop_map(R::Date),
    ]
}

fn field_strategy() -> impl Strategy<Value = Field> {
    (
        key(),
        prop::option::of(match_strategy()),
        prop::option::weighted(0.3, range_strategy()),
        prop::option::weighted(0.15, bounds(0u64..=4)),
        prop::option::weighted(0.1, any::<bool>()),
        prop::option::weighted(0.1, any::<bool>()),
    )
        .prop_map(|(key, m, range, count, is_empty, is_null)| Field {
            key,
            m,
            range,
            count,
            is_empty,
            is_null,
        })
        .prop_filter("a sub-condition", |f| {
            f.m.is_some()
                || f.range.is_some()
                || f.count.is_some()
                || f.is_empty.is_some()
                || f.is_null.is_some()
        })
}

fn leaf() -> impl Strategy<Value = C> {
    prop_oneof![
        6 => field_strategy().prop_map(C::Field),
        1 => key().prop_map(C::IsEmpty),
        1 => key().prop_map(C::IsNull),
        1 => prop::collection::vec(0..CORPUS + 3, 0..6).prop_map(C::HasId),
    ]
}

fn filter_strategy() -> impl Strategy<Value = F> {
    let cond = leaf().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            3 => inner.clone(),
            1 => filter_of(inner).prop_map(|f| C::Filter(Box::new(f))),
        ]
    });
    filter_of(cond.boxed())
}

fn filter_of(c: BoxedStrategy<C>) -> impl Strategy<Value = F> {
    let list = |max| prop::collection::vec(c.clone(), 0..max);
    (
        list(2),
        list(3),
        list(2),
        prop::option::weighted(0.15, (prop::collection::vec(c.clone(), 0..4), 1usize..3)),
    )
        .prop_map(|(must, should, must_not, min_should)| F {
            must,
            should,
            must_not,
            min_should,
        })
}

/// A small deterministic generator for the corpus.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }

    fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.next(items.len() as u64) as usize].clone()
    }

    fn scalar(&mut self) -> Value {
        match self.next(3) {
            0 => json!(self.next(6) as i64 - 2),
            1 => json!(self.pick(&STRS)),
            _ => json!(self.pick(&[1.0, 2.5, -1.0, 3.0])),
        }
    }
}

/// Point `i`: `a` (int, float, string, bool, null or missing), `b` (an
/// array of ints and strings), `c.d` (nested), `e` (objects `{f}`) and
/// `doc_id`.
fn corpus_point(rng: &mut Lcg, i: u64) -> Value {
    let mut p = Map::new();
    match rng.next(7) {
        0 => {
            p.insert("a".into(), json!(rng.next(6) as i64 - 2));
        }
        1 => {
            p.insert("a".into(), json!(rng.pick(&[1.0, 2.5, -1.0, 3.0])));
        }
        2 | 3 => {
            p.insert("a".into(), json!(rng.pick(&STRS)));
        }
        4 => {
            p.insert("a".into(), json!(rng.next(2) == 0));
        }
        5 => {
            p.insert("a".into(), Value::Null);
        }
        _ => {}
    }
    if rng.next(5) != 0 {
        let n = rng.next(4);
        let b: Vec<Value> = (0..n)
            .map(|_| {
                if rng.next(2) == 0 {
                    json!(rng.next(6) as i64 - 2)
                } else {
                    json!(rng.pick(&STRS))
                }
            })
            .collect();
        p.insert("b".into(), Value::Array(b));
    }
    match rng.next(5) {
        0 => {}
        1 => {
            p.insert("c".into(), json!({"d": null}));
        }
        2 => {
            let (x, y) = (rng.scalar(), rng.scalar());
            p.insert("c".into(), json!({"d": [x, y]}));
        }
        _ => {
            p.insert("c".into(), json!({"d": rng.scalar()}));
        }
    }
    if rng.next(3) != 0 {
        let n = rng.next(3);
        let e: Vec<Value> = (0..n).map(|_| json!({"f": rng.scalar()})).collect();
        p.insert("e".into(), Value::Array(e));
    }
    p.insert("doc_id".into(), json!(format!("doc-{}", i % 7)));
    Value::Object(p)
}

#[tokio::test]
async fn gateway_filters_equal_the_reference_evaluator() {
    let qd = Qd::start().await;
    create(&qd, "corpus").await;
    let mut rng = Lcg(0x5eed);
    let corpus: Vec<(u64, Value)> = (0..CORPUS)
        .map(|i| (i, corpus_point(&mut rng, i)))
        .collect();
    write(&qd, "corpus", &corpus).await;

    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let strategy = filter_strategy();
    let mut matched_any = 0;
    for case in 0..128 {
        if case == 64 {
            // The first half runs over the live tail, the second over
            // splits.
            applied(&qd, "corpus").await;
        }
        let f = strategy.new_tree(&mut runner).expect("a filter").current();
        let body = f.json();
        let filter: Filter = serde_json::from_value(body.clone()).expect("parses");
        let expected: BTreeSet<u64> = corpus
            .iter()
            .filter(|(id, payload)| {
                reference_eval(
                    &filter,
                    &PrimaryKey::U64(*id),
                    payload.as_object().expect("object"),
                )
                .unwrap_or_else(|e| panic!("{body}: {e}"))
            })
            .map(|(id, _)| *id)
            .collect();
        if !expected.is_empty() && expected.len() < corpus.len() {
            matched_any += 1;
        }
        let ids = scroll_ids(&qd, "corpus", &body).await;
        if ids != expected {
            let diff: Vec<_> = ids.symmetric_difference(&expected).take(5).collect();
            let shown: Vec<_> = diff
                .iter()
                .map(|id| (**id, corpus[**id as usize].1.clone()))
                .collect();
            panic!(
                "case {case}: {body}\ngateway {} vs reference {}; differ on {shown:?}",
                ids.len(),
                expected.len()
            );
        }
        assert_eq!(
            rest_count(&qd, "corpus", &body).await,
            expected.len() as u64,
            "REST {body}"
        );
        assert_eq!(
            grpc_count(&qd, "corpus", f.pb()).await,
            expected.len() as u64,
            "gRPC {body}"
        );
    }
    // The generator must produce selective filters, not only all or none.
    eprintln!("{matched_any} of 128 filters were selective");
    assert!(matched_any >= 40, "only {matched_any} selective filters");
}

// ----- client shapes -----

#[tokio::test]
async fn langchain_filter_shapes_match() {
    let qd = Qd::start().await;
    create(&qd, "lc").await;
    let points: Vec<(u64, Value)> = (0..3)
        .map(|i| {
            let text = ["foo", "bar", "baz"][i as usize];
            (
                i,
                json!({"page_content": text,
                       "metadata": {"page": i, "details": {"page": i + 1, "pages": [i + 2, 10 * i + 20]}}}),
            )
        })
        .collect();
    write(&qd, "lc", &points).await;
    let cases = [
        (
            json!({"must": [{"key": "metadata.page", "match": {"value": 1}}]}),
            vec![1],
        ),
        (
            json!({"must": [{"key": "metadata.details.page", "match": {"value": 2}}]}),
            vec![1],
        ),
        (
            json!({"must": [{"key": "metadata.details.pages", "match": {"any": [3]}}]}),
            vec![1],
        ),
        (
            json!({"must": [{"key": "metadata.page", "match": {"value": "1"}}]}),
            vec![],
        ),
        (
            json!({"should": [{"key": "metadata.page", "range": {"gte": 1}}], "must_not": [{"has_id": [2]}]}),
            vec![1],
        ),
    ];
    for (filter, want) in cases {
        let want: BTreeSet<u64> = want.into_iter().collect();
        assert_eq!(scroll_ids(&qd, "lc", &filter).await, want, "{filter}");
        assert_eq!(
            rest_count(&qd, "lc", &filter).await,
            want.len() as u64,
            "{filter}"
        );
    }
}

#[tokio::test]
async fn llamaindex_type_strict_filters_match() {
    let qd = Qd::start().await;
    create(&qd, "li").await;
    let (status, reply) = create_index(
        &qd,
        "li",
        json!({"field_name": "some_key", "field_schema": "integer"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    write(
        &qd,
        "li",
        &[
            (1, json!({"some_key": 1})),
            (2, json!({"some_key": 2})),
            (3, json!({"some_key": "3"})),
        ],
    )
    .await;
    let any12 = json!({"must": [{"key": "some_key", "match": {"any": [1, 2]}}]});
    let except12 = json!({"must": [{"key": "some_key", "match": {"except": [1, 2]}}]});
    let any3 = json!({"must": [{"key": "some_key", "match": {"any": ["3"]}}]});
    assert_eq!(scroll_ids(&qd, "li", &any12).await, BTreeSet::from([1, 2]));
    assert_eq!(scroll_ids(&qd, "li", &except12).await, BTreeSet::from([3]));
    assert_eq!(scroll_ids(&qd, "li", &any3).await, BTreeSet::from([3]));
    assert_eq!(rest_count(&qd, "li", &any12).await, 2);
    assert_eq!(rest_count(&qd, "li", &except12).await, 1);
    assert_eq!(rest_count(&qd, "li", &any3).await, 1);
    // `should: []` matches everything.
    assert_eq!(rest_count(&qd, "li", &json!({"should": []})).await, 3);
}

/// A numeric `range` compares by value whatever the key's other values are:
/// a key holding only integers is an integer column, and a fractional bound
/// must not be truncated there (the Python client run of Task 10 found
/// `gte: 30.5` matching `30`), nor may an integer-rounded bound widen a key
/// that holds floats (`gt: 30.5` matched `30.25`). In the tail and after
/// the link applied.
#[tokio::test]
async fn numeric_ranges_with_fractional_bounds_compare_by_value() {
    let qd = Qd::start().await;
    create(&qd, "fr").await;
    // `i`: 28..=33; `m`: 30, 30.25, 30.5, 31, -30.25, -31.
    let m = [30.0, 30.25, 30.5, 31.0, -30.25, -31.0];
    let points: Vec<(u64, Value)> = (0..6_u64)
        .map(|k| (k + 1, json!({"i": 28 + k, "m": m[k as usize]})))
        .collect();
    write(&qd, "fr", &points).await;
    let value = |key: &str, id: u64| match key {
        "i" => 27.0 + id as f64,
        _ => m[id as usize - 1],
    };
    for phase in ["tail", "applied"] {
        if phase == "applied" {
            applied(&qd, "fr").await;
        }
        for key in ["i", "m"] {
            for (op, b) in [
                ("gte", 30.5),
                ("gt", 30.5),
                ("lt", 30.1),
                ("lte", 30.5),
                ("gt", -30.5),
                ("lte", -30.1),
                ("gte", 31.0),
            ] {
                let filter = json!({"must": [{"key": key, "range": {op: b}}]});
                let want: BTreeSet<u64> = (1..=6)
                    .filter(|&id| {
                        let v = value(key, id);
                        match op {
                            "gt" => v > b,
                            "gte" => v >= b,
                            "lt" => v < b,
                            _ => v <= b,
                        }
                    })
                    .collect();
                assert_eq!(
                    scroll_ids(&qd, "fr", &filter).await,
                    want,
                    "{phase}: {key} {op} {b}"
                );
                assert_eq!(rest_count(&qd, "fr", &filter).await, want.len() as u64);
            }
        }
    }
}

// ----- payload indexes -----

#[tokio::test]
async fn payload_indexes_are_idempotent_and_lenient() {
    let qd = Qd::start().await;
    create(&qd, "pi").await;
    write(
        &qd,
        "pi",
        &[(1, json!({"some_key": "not a number", "tenant_id": "t1"}))],
    )
    .await;
    for _ in 0..2 {
        let (status, reply) = create_index(
            &qd,
            "pi",
            json!({"field_name": "tenant_id", "field_schema": "keyword"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        assert_eq!(reply["result"]["status"], "completed", "{reply}");
    }
    let (status, reply) = create_index(
        &qd,
        "pi",
        json!({"field_name": "some_key", "field_schema": {"type": "integer", "lookup": true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    // Without `wait` the answer is `acknowledged`.
    let (status, reply) = qd
        .put(
            "/collections/pi/index",
            Some(json!({"field_name": "f", "field_schema": "float"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["result"]["status"], "acknowledged", "{reply}");
    // A later write with a malformed value still succeeds (lenient).
    write(&qd, "pi", &[(2, json!({"some_key": "x", "f": "y"}))]).await;
    // gRPC creates the same field, idempotently.
    let mut points = qd.points().await;
    let reply = points
        .create_field_index(pb::CreateFieldIndexCollection {
            collection_name: "pi".into(),
            wait: Some(true),
            field_name: "tenant_id".into(),
            field_type: Some(pb::FieldType::Keyword as i32),
            ..Default::default()
        })
        .await
        .expect("create_field_index")
        .into_inner();
    assert_eq!(
        reply.result.expect("result").status,
        pb::UpdateStatus::Completed as i32
    );
    // Missing `field_schema` is 400; a missing collection 404.
    let (status, reply) = create_index(&qd, "pi", json!({"field_name": "g"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(error(&reply), "Wrong input: field_schema is required");
    let (status, _) = create_index(
        &qd,
        "nope",
        json!({"field_name": "g", "field_schema": "keyword"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let err = points
        .create_field_index(pb::CreateFieldIndexCollection {
            collection_name: "pi".into(),
            field_name: "g".into(),
            ..Default::default()
        })
        .await
        .expect_err("no type");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn payload_schema_lists_exactly_the_created_indexes() {
    let qd = Qd::start().await;
    create(&qd, "ps").await;
    write(
        &qd,
        "ps",
        &[
            (1, json!({"tenant_id": "t1", "some_key": 1})),
            (2, json!({"tenant_id": "t2"})),
            (3, json!({"other": true})),
        ],
    )
    .await;
    for (field, schema) in [("tenant_id", "keyword"), ("some_key", "integer")] {
        let (status, reply) = create_index(
            &qd,
            "ps",
            json!({"field_name": field, "field_schema": schema}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
    }
    let (status, reply) = qd.get("/collections/ps", None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let schema = reply["result"]["payload_schema"]
        .as_object()
        .expect("payload_schema");
    let keys: BTreeSet<&str> = schema.keys().map(String::as_str).collect();
    assert_eq!(keys, BTreeSet::from(["some_key", "tenant_id"]));
    assert_eq!(schema["tenant_id"]["data_type"], "keyword");
    assert_eq!(schema["tenant_id"]["points"], 2);
    assert_eq!(schema["some_key"]["data_type"], "integer");
    assert_eq!(schema["some_key"]["points"], 1);
}

#[tokio::test]
async fn text_match_needs_no_index_and_no_backfill() {
    let qd = Qd::start().await;
    create(&qd, "tx").await;
    write(
        &qd,
        "tx",
        &[
            (1, json!({"body": "The QUICK brown fox"})),
            (2, json!({"body": "a fox that is quick"})),
            (3, json!({"body": "lazy dog"})),
        ],
    )
    .await;
    let (status, reply) = create_index(
        &qd,
        "tx",
        json!({"field_name": "body", "field_schema": {"type": "text", "phrase_matching": true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    write(
        &qd,
        "tx",
        &[
            (4, json!({"body": "Quick fox!"})),
            (5, json!({"body": "fox"})),
            (6, json!({"body": "quick brown dog"})),
        ],
    )
    .await;
    let m = |form: &str, text: &str| json!({"must": [{"key": "body", "match": {form: text}}]});
    for pass in 0..2 {
        if pass == 1 {
            applied(&qd, "tx").await;
        }
        assert_eq!(
            scroll_ids(&qd, "tx", &m("text", "quick fox")).await,
            BTreeSet::from([1, 2, 4])
        );
        assert_eq!(
            scroll_ids(&qd, "tx", &m("text_any", "quick fox")).await,
            BTreeSet::from([1, 2, 4, 5, 6])
        );
        assert_eq!(
            scroll_ids(&qd, "tx", &m("phrase", "quick fox")).await,
            BTreeSet::from([4])
        );
        assert_eq!(rest_count(&qd, "tx", &m("text", "quick fox")).await, 3);
    }
}

#[tokio::test]
async fn datetime_ranges_need_no_index() {
    let qd = Qd::start().await;
    create(&qd, "dt").await;
    write(
        &qd,
        "dt",
        &[
            (1, json!({"ts": "2023-02-08T10:49:00Z"})),
            (2, json!({"ts": "2023-02-10"})),
            (3, json!({"ts": "not a date"})),
        ],
    )
    .await;
    let (status, reply) = create_index(
        &qd,
        "dt",
        json!({"field_name": "ts", "field_schema": "datetime"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    write(
        &qd,
        "dt",
        &[
            (4, json!({"ts": "2023-02-09T00:00:00+01:00"})),
            (5, json!({"ts": "2023-03-01T00:00:00Z"})),
        ],
    )
    .await;
    let range = json!({"must": [{"key": "ts", "range": {"gte": "2023-02-08T10:49:00Z", "lt": "2023-02-11"}}]});
    assert_eq!(
        scroll_ids(&qd, "dt", &range).await,
        BTreeSet::from([1, 2, 4])
    );
    assert_eq!(rest_count(&qd, "dt", &range).await, 3);
    let after = json!({"must": [{"key": "ts", "range": {"gt": "2023-02-08 10:49:00"}}]});
    assert_eq!(
        scroll_ids(&qd, "dt", &after).await,
        BTreeSet::from([2, 4, 5])
    );
    let (status, reply) = qd
        .post(
            "/collections/dt/points/count",
            Some(json!({"filter": {"must": [{"key": "ts", "range": {"gt": "soon"}}]}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert_eq!(error(&reply), "Wrong input: Unable to parse datetime soon");
}

#[tokio::test]
async fn a_non_default_text_index_is_501() {
    let qd = Qd::start().await;
    create(&qd, "nt").await;
    for schema in [
        json!({"type": "text", "tokenizer": "whitespace"}),
        json!({"type": "text", "lowercase": false}),
    ] {
        let (status, reply) = create_index(
            &qd,
            "nt",
            json!({"field_name": "body", "field_schema": schema}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{reply}");
        assert!(
            error(&reply).starts_with("Unsupported in Loams: text index option"),
            "{reply}"
        );
    }
    let mut points = qd.points().await;
    let err = points
        .create_field_index(pb::CreateFieldIndexCollection {
            collection_name: "nt".into(),
            field_name: "body".into(),
            field_type: Some(pb::FieldType::Text as i32),
            field_index_params: Some(pb::PayloadIndexParams {
                index_params: Some(pb::payload_index_params::IndexParams::TextIndexParams(
                    pb::TextIndexParams {
                        tokenizer: pb::TokenizerType::Whitespace as i32,
                        ..Default::default()
                    },
                )),
            }),
            ..Default::default()
        })
        .await
        .expect_err("whitespace");
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    // Nothing was added.
    let info = qd
        .server
        .collections()
        .get_collection(NS, "nt")
        .await
        .expect("info");
    assert_eq!(info.schema.fields.len(), 1);
}

#[tokio::test]
async fn payload_index_type_change_is_501() {
    let qd = Qd::start().await;
    create(&qd, "tc").await;
    let (status, _) = create_index(
        &qd,
        "tc",
        json!({"field_name": "k", "field_schema": "keyword"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, reply) = create_index(
        &qd,
        "tc",
        json!({"field_name": "k", "field_schema": "integer"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{reply}");
    assert_eq!(
        error(&reply),
        "Unsupported in Loams: changing payload index type"
    );
    // Deleting an index stays unsupported.
    let (status, _) = qd.delete("/collections/tc/index/k", None).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn filter_errors_keep_qdrant_statuses() {
    let qd = Qd::start().await;
    create(&qd, "fe").await;
    let count = |filter: Value| {
        let qd = &qd;
        async move {
            qd.post(
                "/collections/fe/points/count",
                Some(json!({"filter": filter})),
            )
            .await
        }
    };
    let (status, reply) = count(json!({"must": [{"key": "a", "mtch": {"value": 1}}]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{reply}");
    assert!(
        error(&reply).starts_with("Format error in JSON body:"),
        "{reply}"
    );
    let (status, reply) = count(json!({"must": [{"key": "g", "geo_radius": {}}]})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{reply}");
    assert_eq!(error(&reply), "Unsupported in Loams: geo_radius condition");
    let (status, reply) = count(json!({"must": [{"key": "a[0]", "match": {"value": 1}}]})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{reply}");
    // gRPC answers the same codes.
    let mut points = qd.points().await;
    let err = points
        .count(pb::CountPoints {
            collection_name: "fe".into(),
            filter: Some(pb::Filter {
                must: vec![pb::Condition {
                    condition_one_of: Some(pb::condition::ConditionOneOf::HasVector(
                        pb::HasVectorCondition {
                            has_vector: "v".into(),
                        },
                    )),
                }],
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect_err("has_vector");
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert_eq!(err.message(), "Unsupported in Loams: has_vector condition");
}
