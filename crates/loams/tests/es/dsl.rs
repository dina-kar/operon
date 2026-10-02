//! Task 7: the parsed Query DSL end to end, differentially. A seeded
//! corpus of ES filter queries over the non-text field types is parsed by
//! `dsl::parse_leaf`, run through the collection service, and checked
//! against `dsl::reference_eval` (ES semantics computed over `_source`),
//! as M1.4's Qdrant filter tests check theirs.

use std::collections::BTreeSet;

use loams_es::dsl::{QueryContext, parse_leaf, reference_eval};
use loams_es::mapping::IndexView;
use loams_query::SearchRequest;
use reqwest::StatusCode;
use serde_json::{Map, Value, json};

use crate::harness::Es;

const NS: &str = "default";

/// 2026-09-24T10:11:12.345Z.
const NOW_MS: i64 = 1_790_244_672_345;

/// SplitMix64: a small deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize].clone()
    }
}

const KEYWORDS: &[&str] = &["a", "ab", "abc", "b", "ba", "Stephen King", "b?c"];
const TEAMS: &[&str] = &["core", "infra", "ml"];

/// An ISO timestamp `hours` after 2026-09-20T00:00:00Z, with milliseconds.
fn iso(hours: i64, ms: i64) -> String {
    let base = time::macros::datetime!(2026-09-20 0:00 UTC);
    let at = base + time::Duration::hours(hours) + time::Duration::milliseconds(ms);
    at.format(&time::format_description::well_known::Rfc3339)
        .expect("format")
}

fn epoch_ms(hours: i64, ms: i64) -> i64 {
    1_789_862_400_000 + hours * 3_600_000 + ms
}

fn document(rng: &mut Rng) -> Map<String, Value> {
    let mut doc = Map::new();
    if rng.chance(85) {
        doc.insert("session_id".into(), json!(rng.pick(KEYWORDS)));
    }
    if rng.chance(80) {
        let n = if rng.chance(20) {
            json!([rng.int(-5, 20), rng.int(-5, 20)])
        } else {
            json!(rng.int(-5, 20))
        };
        doc.insert("n".into(), n);
    }
    if rng.chance(80) {
        doc.insert("x".into(), json!(rng.int(-8, 20) as f64 / 4.0));
    }
    if rng.chance(75) {
        doc.insert("flag".into(), json!(rng.chance(50)));
    }
    if rng.chance(85) {
        let (hours, ms) = (rng.int(0, 200), rng.int(0, 999));
        let ts = if rng.chance(50) {
            json!(iso(hours, ms))
        } else {
            json!(epoch_ms(hours, ms))
        };
        doc.insert("ts".into(), ts);
    }
    if rng.chance(80) {
        let mut labels = Map::new();
        if rng.chance(80) {
            let priority = if rng.chance(70) {
                json!(rng.int(0, 5))
            } else {
                json!(format!("p{}", rng.int(0, 3)))
            };
            labels.insert("priority".into(), priority);
        }
        if rng.chance(70) {
            labels.insert("team".into(), json!(rng.pick(TEAMS)));
        }
        doc.insert("labels".into(), Value::Object(labels));
    }
    if rng.chance(60) {
        let tags: Vec<&str> = (0..rng.int(1, 3)).map(|_| rng.pick(KEYWORDS)).collect();
        doc.insert("tags".into(), json!(tags));
    }
    doc
}

/// A leaf query of the corpus.
fn leaf(rng: &mut Rng, ids: usize) -> Value {
    let number = |rng: &mut Rng| -> Value {
        match rng.below(4) {
            0 => json!(rng.int(-5, 20)),
            1 => json!(rng.int(-5, 20).to_string()),
            2 => json!(rng.int(-10, 40) as f64 / 2.0),
            _ => json!(format!("{}", rng.int(-10, 40) as f64 / 4.0)),
        }
    };
    let date = |rng: &mut Rng| -> Value {
        match rng.below(6) {
            0 => json!(iso(rng.int(0, 200), rng.int(0, 999))),
            1 => json!(epoch_ms(rng.int(0, 200), 0)),
            2 => json!(format!("2026-09-{:02}", rng.int(19, 28))),
            3 => json!(format!("now-{}d/d", rng.int(0, 5))),
            4 => json!(format!("now-{}h", rng.int(0, 100))),
            _ => json!(format!(
                "2026-09-{:02}T{:02}",
                rng.int(20, 27),
                rng.int(0, 23)
            )),
        }
    };
    let bounds = |rng: &mut Rng, value: &dyn Fn(&mut Rng) -> Value| -> Value {
        let mut map = Map::new();
        if rng.chance(70) {
            map.insert(rng.pick(&["gt", "gte"]).to_string(), value(rng));
        }
        if rng.chance(70) {
            map.insert(rng.pick(&["lt", "lte"]).to_string(), value(rng));
        }
        Value::Object(map)
    };
    match rng.below(22) {
        0 => json!({"term": {"session_id": rng.pick(KEYWORDS)}}),
        1 => json!({"term": {"n": number(rng)}}),
        2 => json!({"term": {"x": number(rng)}}),
        3 => json!({"term": {"flag": rng.pick(&[json!(true), json!("false"), json!(false)])}}),
        4 => json!({"term": {"ts": date(rng)}}),
        5 => {
            let priority = if rng.chance(70) {
                json!(rng.int(0, 5))
            } else {
                json!("p1")
            };
            json!({"term": {"labels.priority": priority}})
        }
        6 => json!({"term": {"_id": format!("d{}", rng.below(ids as u64))}}),
        7 => json!({"terms": {"tags": [rng.pick(KEYWORDS), rng.pick(KEYWORDS)]}}),
        8 => json!({"terms": {"n": [number(rng), number(rng), number(rng)]}}),
        9 => json!({"range": {"n": bounds(rng, &number)}}),
        10 => json!({"range": {"x": bounds(rng, &number)}}),
        11 => json!({"range": {"ts": bounds(rng, &date)}}),
        12 => json!({"range": {"labels.priority": bounds(rng, &|rng: &mut Rng| {
            json!(rng.int(-1, 12) as f64 / 2.0)
        })}}),
        13 => {
            json!({"range": {"session_id": bounds(rng, &|rng: &mut Rng| json!(rng.pick(KEYWORDS)))}})
        }
        14 => {
            json!({"exists": {"field": rng.pick(&["n", "ts", "labels.team", "labels", "tags", "nope"])}})
        }
        15 => json!({"prefix": {"session_id": rng.pick(&["a", "ab", "b", "S"])}}),
        16 => json!({"wildcard": {"session_id": rng.pick(&["a*c", "?b", "*a", "b?c", "*"])}}),
        17 => {
            json!({"ids": {"values": [format!("d{}", rng.below(ids as u64)), format!("d{}", rng.below(ids as u64))]}})
        }
        18 => json!({"term": {"nope": "x"}}),
        19 => json!({"term": {"labels.team": rng.pick(TEAMS)}}),
        20 => json!({"range": {"labels.team": {"gte": rng.pick(TEAMS)}}}),
        _ => json!({"match_all": {}}),
    }
}

/// A query tree of the corpus, `depth` compound levels at most.
fn query(rng: &mut Rng, ids: usize, depth: u32) -> Value {
    if depth == 0 || rng.chance(45) {
        return leaf(rng, ids);
    }
    if rng.chance(15) {
        return json!({"constant_score": {"filter": query(rng, ids, depth - 1)}});
    }
    let mut body = Map::new();
    for key in ["must", "should", "must_not", "filter"] {
        if rng.chance(45) {
            let n = rng.int(1, 3);
            let clauses: Vec<Value> = (0..n).map(|_| query(rng, ids, depth - 1)).collect();
            body.insert(key.into(), Value::Array(clauses));
        }
    }
    if body.contains_key("should") && rng.chance(30) {
        body.insert("minimum_should_match".into(), json!(rng.int(1, 2)));
    }
    json!({"bool": body})
}

async fn ids_of(es: &Es, filter: loams_query::Query) -> BTreeSet<String> {
    let mut request = SearchRequest::new("d");
    request.filter = Some(filter);
    request.limit = 1000;
    let response = es
        .server
        .collections()
        .search(NS, request)
        .await
        .expect("search");
    response
        .hits
        .iter()
        .map(|hit| loams_es::doc::id_of(&hit.pk))
        .collect()
}

#[tokio::test]
async fn parsed_filters_agree_with_the_reference_evaluator() {
    let es = Es::start().await;
    let a = es
        .put(
            "/d",
            Some(json!({"mappings": {"properties": {
                "session_id": {"type": "keyword"},
                "n": {"type": "long"},
                "x": {"type": "double"},
                "flag": {"type": "boolean"},
                "ts": {"type": "date"},
                "labels": {"type": "flattened"},
                "tags": {"type": "keyword"}
            }}})),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    let mut rng = Rng(0x005E_ED0E_5D51);
    let docs: Vec<(String, Map<String, Value>)> = (0..80)
        .map(|i| (format!("d{i}"), document(&mut rng)))
        .collect();
    let mut body = String::new();
    for (id, doc) in &docs {
        body.push_str(&json!({"index": {"_index": "d", "_id": id}}).to_string());
        body.push('\n');
        body.push_str(&Value::Object(doc.clone()).to_string());
        body.push('\n');
    }
    let a = es
        .send_raw(
            reqwest::Method::POST,
            "/_bulk?refresh=true",
            Some(("application/x-ndjson", body.into_bytes())),
            &[],
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    assert_eq!(a.body["errors"], false, "{}", a.text);
    let info = es
        .server
        .collections()
        .get_collection(NS, "d")
        .await
        .expect("info");
    let view = IndexView::new(info);
    let ctx = QueryContext::new(&view, NOW_MS);
    let mut checked = 0;
    let mut matched_some = 0;
    for _ in 0..400 {
        let q = query(&mut rng, docs.len(), 3);
        let parsed = parse_leaf(&q, &ctx);
        let reference: Result<BTreeSet<String>, _> = docs
            .iter()
            .filter_map(
                |(id, doc)| match reference_eval(&q, &view, id, doc, NOW_MS) {
                    Ok(true) => Some(Ok(id.clone())),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            )
            .collect();
        match (parsed, reference) {
            (Ok(ir), Ok(expected)) => {
                let got = ids_of(&es, ir.clone()).await;
                assert_eq!(got, expected, "query {q}\nIR {ir:?}");
                checked += 1;
                matched_some += usize::from(!expected.is_empty() && expected.len() < docs.len());
            }
            (Err(p), Err(r)) => assert_eq!(p.kind, r.kind, "{q}: {p} vs {r}"),
            (p, r) => panic!("{q}: parser {p:?}, reference {r:?}"),
        }
    }
    assert!(checked > 300, "{checked} checked");
    assert!(matched_some > 100, "{matched_some} selective queries");
    es.server.shutdown().await.expect("shutdown");
}
