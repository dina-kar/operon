//! A split bundle round-trips: files, hotcache and the 16-byte footer trailer.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use operon_quickwit::directories::{BundleDirectory, HotDirectory};
use operon_quickwit::storage::{BundleStorage, PutPayload, RamStorage};
use tantivy::directory::FileSlice;
use tantivy::{Index, ReloadPolicy};

#[tokio::test]
async fn a_split_bundle_round_trips() {
    let split = common::build_split();
    let footer_range = split.footer_range.clone();
    let split_bytes = split.read_all().await.unwrap();
    assert_eq!(split_bytes.len() as u64, split.len());

    let footer_bytes = split_bytes.slice(footer_range.start as usize..split_bytes.len());
    let (bundle_storage, hotcache) = BundleStorage::open_from_split_bytes(
        Arc::new(RamStorage::default()),
        PathBuf::from("split"),
        footer_bytes,
    )
    .unwrap();
    assert!(
        bundle_storage
            .iter_files()
            .any(|file| file.ends_with("meta.json"))
    );

    let bundle_directory =
        BundleDirectory::open_split(FileSlice::new(Arc::new(split_bytes))).unwrap();
    let hot_directory = HotDirectory::open(bundle_directory, hotcache).unwrap();
    let index = Index::open(hot_directory).unwrap();
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();
    assert_eq!(searcher.num_docs(), common::NUM_DOCS);

    let mut values = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let column = segment_reader.fast_fields().u64("n").unwrap();
        for doc in 0..segment_reader.max_doc() {
            values.push(column.first(doc).unwrap());
        }
    }
    values.sort_unstable();
    let expected: Vec<u64> = (0..common::NUM_DOCS).map(common::fast_value).collect();
    assert_eq!(values, expected);
}

#[tokio::test]
async fn the_footer_trailer_is_present() {
    let split = common::build_split();
    let split_bytes = split.read_all().await.unwrap();
    let trailer = &split_bytes.as_slice()[split_bytes.len() - 16..];

    let footer_start = u64::from_le_bytes(trailer[0..8].try_into().unwrap());
    assert_eq!(footer_start, split.footer_range.start);
    assert_eq!(u32::from_le_bytes(trailer[8..12].try_into().unwrap()), 1);
    assert_eq!(&trailer[12..16], b"QWFT");
    assert_eq!(split.footer_range.end, split_bytes.len() as u64);
}
