//! A 100-document split, shared by the bundle and hotcache tests.

use std::path::PathBuf;

use operon_quickwit::directories::write_hotcache;
use operon_quickwit::storage::{PutPayload, SplitPayload, SplitPayloadBuilder};
use tantivy::directory::{Directory, RamDirectory};
use tantivy::schema::{FAST, Schema, TEXT};
use tantivy::{Index, IndexSettings, TantivyDocument};

pub const NUM_DOCS: u64 = 100;

/// The value of the `n` fast field of document `i`.
pub fn fast_value(i: u64) -> u64 {
    i * 7 + 3
}

/// A split of a 100-document index with a text field `body` and a u64 fast field `n`,
/// bundling every index file plus its hotcache.
pub fn build_split() -> SplitPayload {
    let mut schema_builder = Schema::builder();
    let body = schema_builder.add_text_field("body", TEXT);
    let n = schema_builder.add_u64_field("n", FAST);
    let schema = schema_builder.build();
    let directory = RamDirectory::create();
    let index =
        Index::create(directory.clone(), schema, IndexSettings::default()).expect("create index");
    let mut writer = index
        .writer_with_num_threads(1, 15_000_000)
        .expect("index writer");
    for i in 0..NUM_DOCS {
        let mut doc = TantivyDocument::default();
        doc.add_text(body, format!("document number {i}"));
        doc.add_u64(n, fast_value(i));
        writer.add_document(doc).expect("add document");
    }
    writer.commit().expect("commit");
    writer.wait_merging_threads().expect("wait for merges");

    let mut hotcache = Vec::new();
    write_hotcache(directory.clone(), &mut hotcache).expect("write hotcache");

    let mut files: Vec<PathBuf> = index
        .load_metas()
        .expect("load metas")
        .segments
        .iter()
        .flat_map(|segment| segment.list_files())
        .collect();
    files.push(PathBuf::from("meta.json"));
    files.push(PathBuf::from(".managed.json"));
    files.sort();

    let mut builder = SplitPayloadBuilder::default();
    for file in files {
        let Ok(bytes) = directory.atomic_read(&file) else {
            continue;
        };
        let payload: Box<dyn PutPayload> = Box::new(bytes);
        builder.add_payload(file.to_string_lossy().into_owned(), payload);
    }
    builder.finalize(&hotcache).expect("finalize split")
}
