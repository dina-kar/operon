//! Operon's Tantivy integration (design §06 §1–§2, plan M1.1 Task 8):
//!
//! - the Lucene-compatible analyzers ([`tokenizer_manager`]) and the Porter
//!   stemmer ([`porter_stem`]);
//! - [`OperonStorage`], Quickwit's `Storage` over `operon-store` and
//!   `operon-cache`;
//! - splits: [`build_split`], [`open_split`] (one ranged GET) and
//!   [`warm_up_all`];
//! - the delete-bitmap format ([`encode_delete_bitmap`],
//!   [`decode_delete_bitmap`]).
//!
//! The Tantivy schema of a collection is mapped in `operon-collection`
//! (`tantivy_schema`).

mod analyzers;
mod bitmap;
mod error;
mod porter;
mod split;
mod storage;

pub use analyzers::{
    ENGLISH, EnglishPossessiveFilter, KEYWORD, LUCENE_ENGLISH_STOP_WORDS, LetterTokenizer,
    MAX_TOKEN_CHARS, PorterStemFilter, SIMPLE, STANDARD, StandardTokenizer,
    UnicodeWhitespaceTokenizer, WHITESPACE, tokenizer_manager,
};
pub use bitmap::{
    DELETE_BITMAP_MAGIC, DELETE_BITMAP_VERSION, decode_delete_bitmap, encode_delete_bitmap,
};
pub use error::TextError;
pub use porter::porter_stem;
pub use split::{BuiltSplit, SPLIT_WRITER_MEMORY, build_split, open_split, warm_up_all};
pub use storage::OperonStorage;
