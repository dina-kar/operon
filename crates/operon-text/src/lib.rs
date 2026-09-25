//! Operon's Tantivy integration (design §06 §1–§2, plan M1.1 Task 8): the
//! Lucene-compatible analyzers and the Porter stemmer.

mod analyzers;
mod porter;

pub use analyzers::{
    ENGLISH, EnglishPossessiveFilter, KEYWORD, LUCENE_ENGLISH_STOP_WORDS, LetterTokenizer,
    MAX_TOKEN_CHARS, PorterStemFilter, SIMPLE, STANDARD, StandardTokenizer,
    UnicodeWhitespaceTokenizer, WHITESPACE, tokenizer_manager,
};
pub use porter::porter_stem;
