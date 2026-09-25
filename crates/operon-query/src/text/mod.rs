//! Text queries over splits and the tail (plan M1.2 Tasks 2, 5 and 8).
//!
//! Task 2: field resolution ([`fields`]), value coercion by field kind
//! ([`coerce`]) and the compiler from the IR [`Query`](crate::Query) to a
//! Tantivy query for one split's schema ([`compile`]).
//!
//! Task 5: opening a view's splits for a request ([`splits`]), Tantivy's
//! fieldnorm buckets ([`norms`]) and global live-only BM25 statistics
//! ([`stats`]).
//!
//! Task 7: the per-split primary-key dictionaries that scroll walks
//! ([`pkdict`]). Task 8: highlighting ([`highlight`]).
//!
//! Query analysis uses Quickwit's [`TokenizerManager`] ([`query_tokenizers`],
//! P28, row 0.44); splits and the tail index keep Tantivy's
//! `operon_text::tokenizer_manager()`.

pub mod coerce;
pub mod compile;
pub mod fields;
pub mod highlight;
pub mod norms;
pub mod pkdict;
pub mod splits;
pub mod stats;

use operon_quickwit::query::tokenizers::TokenizerManager;
use operon_text::{ENGLISH, KEYWORD, SIMPLE, STANDARD, WHITESPACE};

pub use coerce::{Coerced, RangeOp, coerce_bound, coerce_term, date_bound_ms, format_date_us};
pub use compile::{
    CompileMode, CompiledQuery, QueryCompiler, fuzziness_edits, highlight_terms,
    parse_minimum_should_match,
};
pub use fields::{ResolvedField, resolve_field};
pub use highlight::{check_highlight, highlight, highlight_stats};
pub use norms::{FIELD_NORMS_TABLE, norm_mid2};
pub use pkdict::{PkCursor, PkDictCache};
pub use splits::{LocalSplitStorage, OpenSplit, open_splits};
pub use stats::{GlobalStats, StatsCache};

/// Quickwit's `TokenizerManager::new()` plus operon-text's five analyzers
/// (from `operon_text::tokenizer_manager().get(name)`), registered with
/// `does_lowercasing` = true for "standard", "english", "simple" and false
/// for "whitespace", "keyword".
pub fn query_tokenizers() -> TokenizerManager {
    let manager = TokenizerManager::new();
    let analyzers = operon_text::tokenizer_manager();
    for (name, does_lowercasing) in [
        (STANDARD, true),
        (ENGLISH, true),
        (SIMPLE, true),
        (WHITESPACE, false),
        (KEYWORD, false),
    ] {
        let analyzer = analyzers
            .get(name)
            .expect("operon-text registers its five analyzers");
        manager.register(name, analyzer, does_lowercasing);
    }
    manager
}
