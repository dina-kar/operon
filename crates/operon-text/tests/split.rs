//! Splits round-trip through object storage and open in one GET (plan M1.1
//! Task 8 rules 5–7, Ruling 9).

use std::path::Path;
use std::sync::Arc;

use object_store::memory::InMemory;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_quickwit::storage::{Storage, StorageErrorKind};
use operon_store::{Fault, FaultyStore, Op, Store};
use operon_text::{
    BuiltSplit, OperonStorage, STANDARD, TextError, build_split, open_split, warm_up_all,
};
use tantivy::collector::DocSetCollector;
use tantivy::query::TermQuery;
use tantivy::schema::{
    FAST, Field, IndexRecordOption, STRING, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::{DocAddress, Searcher, TantivyDocument, Term};

const NUM_DOCS: u32 = 500;
const ROOT: &str = "ns/n/collections/1/";
const SPLIT: &str = "text/splits/01J00000000000000000000000.split";

struct Fields {
    schema: Schema,
    body: Field,
    tag: Field,
    rowid: Field,
}

fn fields() -> Fields {
    let mut builder = Schema::builder();
    let body = builder.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(STANDARD)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        ),
    );
    let tag = builder.add_text_field("tag", STRING | FAST);
    let rowid = builder.add_u64_field("_rowid", FAST);
    Fields {
        schema: builder.build(),
        body,
        tag,
        rowid,
    }
}

fn tag_of(i: u32) -> String {
    format!("t{}", i % 7)
}

fn split() -> (Fields, BuiltSplit) {
    let fields = fields();
    let docs = (0..NUM_DOCS)
        .map(|i| {
            let mut doc = TantivyDocument::default();
            doc.add_text(fields.body, format!("Document NUMBER {i}"));
            doc.add_text(fields.tag, tag_of(i));
            doc.add_u64(fields.rowid, 1000 + u64::from(i));
            doc
        })
        .collect();
    let split = build_split(fields.schema.clone(), docs).expect("split builds");
    (fields, split)
}

async fn cache(store: Store, block_size: u64) -> RangeCache {
    RangeCache::new(
        store,
        RangeCacheConfig {
            block_size,
            memory_bytes: 64 << 20,
            disk: None,
        },
    )
    .await
    .expect("cache builds")
}

fn faulty() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    (faulty, store)
}

fn matching(searcher: &Searcher, term: Term) -> Vec<u32> {
    let query = TermQuery::new(term, IndexRecordOption::Basic);
    let mut docs: Vec<u32> = searcher
        .search(&query, &DocSetCollector)
        .expect("search")
        .into_iter()
        .map(|DocAddress { doc_id, .. }| doc_id)
        .collect();
    docs.sort_unstable();
    docs
}

async fn open(storage: Arc<dyn Storage>, split: &BuiltSplit) -> Result<tantivy::Index, TextError> {
    open_split(
        storage,
        SPLIT,
        split.bytes.len() as u64,
        split.footer_range.clone(),
    )
    .await
}

#[tokio::test]
async fn a_split_round_trips_through_object_storage() {
    let (fields, split) = split();
    assert_eq!(split.doc_count, u64::from(NUM_DOCS));
    assert_eq!(split.footer_range.end, split.bytes.len() as u64);
    let store = Store::in_memory();
    store
        .put_if_absent(&format!("{ROOT}{SPLIT}"), split.bytes.clone())
        .await
        .unwrap();
    let storage = Arc::new(OperonStorage::new(
        store.clone(),
        cache(store, 4096).await,
        ROOT,
    ));

    let index = open(storage, &split).await.unwrap();
    let searcher = warm_up_all(&index).await.unwrap();
    assert_eq!(searcher.num_docs(), u64::from(NUM_DOCS));
    assert_eq!(searcher.segment_readers().len(), 1);

    assert_eq!(
        matching(&searcher, Term::from_field_text(fields.tag, "t3")),
        (0..NUM_DOCS).filter(|i| i % 7 == 3).collect::<Vec<_>>()
    );
    // The body went through the `standard` analyzer (lowercased).
    assert_eq!(
        matching(&searcher, Term::from_field_text(fields.body, "number")),
        (0..NUM_DOCS).collect::<Vec<_>>()
    );
    assert_eq!(
        matching(&searcher, Term::from_field_text(fields.body, "42")),
        [42]
    );
    let rowids = searcher
        .segment_reader(0)
        .fast_fields()
        .u64("_rowid")
        .unwrap();
    for doc in 0..NUM_DOCS {
        assert_eq!(rowids.first(doc), Some(1000 + u64::from(doc)), "doc {doc}");
    }
    let tags = searcher
        .segment_reader(0)
        .fast_fields()
        .str("tag")
        .unwrap()
        .unwrap();
    let mut tag = String::new();
    let ord = tags.term_ords(7).next().unwrap();
    tags.ord_to_str(ord, &mut tag).unwrap();
    assert_eq!(tag, tag_of(7));
}

#[tokio::test]
async fn opening_a_split_is_one_get() {
    let (fields, split) = split();
    let (faulty, store) = faulty();
    store
        .put_if_absent(&format!("{ROOT}{SPLIT}"), split.bytes.clone())
        .await
        .unwrap();
    // A cold cache whose blocks are at least as long as the footer, and
    // where the footer straddles a block boundary.
    let footer = split.footer_range.clone();
    let footer_len = footer.end - footer.start;
    let block_size = (footer_len..)
        .find(|bs| footer.start / bs != (footer.end - 1) / bs)
        .expect("a straddling block size");
    assert!(footer.start % block_size != 0);
    assert_eq!((footer.end - 1) / block_size, footer.start / block_size + 1);
    let storage = Arc::new(OperonStorage::new(
        store.clone(),
        cache(store, block_size).await,
        ROOT,
    ));

    let gets = faulty.calls(Op::Get);
    let index = open(storage, &split).await.unwrap();
    assert_eq!(faulty.calls(Op::Get), gets + 1, "one ranged GET, no HEAD");

    // The index is complete.
    let searcher = warm_up_all(&index).await.unwrap();
    assert_eq!(
        matching(&searcher, Term::from_field_text(fields.tag, "t0")).len(),
        (0..NUM_DOCS).filter(|i| i % 7 == 0).count()
    );
}

#[tokio::test]
async fn a_split_survives_a_lost_put_ack() {
    let (fields, split) = split();
    let (faulty, store) = faulty();
    let storage = OperonStorage::new(store.clone(), cache(store, 4096).await, ROOT);
    let path = Path::new(SPLIT);

    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    let lost = storage.put(path, Box::new(split.bytes.to_vec())).await;
    assert!(lost.is_err(), "the acknowledgement is lost");
    storage
        .put(path, Box::new(split.bytes.to_vec()))
        .await
        .expect("the retry finds identical bytes and succeeds");

    let mut other = split.bytes.to_vec();
    other[0] ^= 1;
    let err = storage.put(path, Box::new(other)).await.unwrap_err();
    assert_eq!(err.kind(), StorageErrorKind::Internal);

    let index = open(Arc::new(storage), &split).await.unwrap();
    let searcher = warm_up_all(&index).await.unwrap();
    assert_eq!(
        matching(&searcher, Term::from_field_text(fields.tag, "t6")).len(),
        71
    );
}

#[tokio::test]
async fn a_bad_footer_range_or_a_truncated_split_is_an_error() {
    let (_, split) = split();
    let store = Store::in_memory();
    let size = split.bytes.len() as u64;
    let footer = split.footer_range.clone();
    store
        .put_if_absent(&format!("{ROOT}{SPLIT}"), split.bytes.clone())
        .await
        .unwrap();
    let truncated = "text/splits/truncated.split";
    store
        .put_if_absent(
            &format!("{ROOT}{truncated}"),
            split.bytes.slice(..split.bytes.len() - 100),
        )
        .await
        .unwrap();

    let expect_error = |result: Result<tantivy::Index, TextError>, what: &str| match result {
        Err(TextError::Corrupt(_) | TextError::Storage(_)) => {}
        Err(other) => panic!("{what}: unexpected error {other:?}"),
        Ok(_) => panic!("{what}: opened"),
    };
    // One cache for every attempt: a failed open must not poison it.
    let shared = cache(store.clone(), 4096).await;
    for (what, size, range) in [
        // First, while the cache knows no size for the split.
        ("a smaller size", size - 1, footer.start..footer.end - 1),
        (
            "a footer that does not end the split",
            size,
            footer.start..footer.end - 1,
        ),
        (
            "a footer that starts late",
            size,
            footer.start + 1..footer.end,
        ),
        (
            "a footer that starts early",
            size,
            footer.start - 1..footer.end,
        ),
        ("an empty footer", size, size..size),
        ("an inverted footer", size, size..footer.start),
        ("a wrong size", size + 5, footer.start + 5..footer.end + 5),
        (
            "a footer past the end",
            size + 1,
            footer.start..footer.end + 1,
        ),
    ] {
        let storage = Arc::new(OperonStorage::new(store.clone(), shared.clone(), ROOT));
        expect_error(open_split(storage, SPLIT, size, range).await, what);
    }
    let storage = Arc::new(OperonStorage::new(store.clone(), shared, ROOT));
    let index = open_split(storage, SPLIT, size, footer.clone())
        .await
        .expect("the right reference still opens the split");
    assert_eq!(
        warm_up_all(&index).await.unwrap().num_docs(),
        u64::from(NUM_DOCS)
    );
    let storage = Arc::new(OperonStorage::new(
        store.clone(),
        cache(store.clone(), 4096).await,
        ROOT,
    ));
    expect_error(
        open_split(storage, truncated, size, footer.clone()).await,
        "a truncated split",
    );
    let storage = Arc::new(OperonStorage::new(
        store.clone(),
        cache(store.clone(), 4096).await,
        ROOT,
    ));
    expect_error(
        open_split(storage, "text/splits/missing.split", size, footer).await,
        "a missing split",
    );
}
