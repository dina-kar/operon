//! The five analyzers match Lucene's (overview A5, plan M1.1 Ruling 25).

use operon_common::schema::KNOWN_ANALYZERS;
use operon_text::{ENGLISH, KEYWORD, SIMPLE, STANDARD, WHITESPACE, tokenizer_manager};
use tantivy::tokenizer::Token;

fn tokens(analyzer: &str, text: &str) -> Vec<Token> {
    let mut analyzer = tokenizer_manager()
        .get(analyzer)
        .unwrap_or_else(|| panic!("{analyzer} is registered"));
    let mut stream = analyzer.token_stream(text);
    let mut out = Vec::new();
    while let Some(token) = stream.next() {
        out.push(token.clone());
    }
    out
}

fn texts(analyzer: &str, text: &str) -> Vec<String> {
    tokens(analyzer, text).into_iter().map(|t| t.text).collect()
}

const SENTENCE: &str = "The QUICK brown-fox's 3.5 jumps!";

#[test]
fn standard_follows_uax29_and_lowercases() {
    assert_eq!(
        texts(STANDARD, SENTENCE),
        ["the", "quick", "brown", "fox's", "3.5", "jumps"]
    );
    assert_eq!(texts(STANDARD, "中文字"), ["中", "文", "字"]);

    let word: String = (0..600)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    let cut = tokens(STANDARD, &word);
    let lengths: Vec<usize> = cut.iter().map(|t| t.text.chars().count()).collect();
    assert_eq!(lengths, [255, 255, 90]);
    let positions: Vec<usize> = cut.iter().map(|t| t.position).collect();
    assert_eq!(positions, [0, 1, 2]);
    let offsets: Vec<(usize, usize)> = cut.iter().map(|t| (t.offset_from, t.offset_to)).collect();
    assert_eq!(offsets, [(0, 255), (255, 510), (510, 600)]);
    assert_eq!(
        cut.iter().map(|t| t.text.as_str()).collect::<String>(),
        word
    );

    // Offsets are byte offsets into the original text.
    let fox = &tokens(STANDARD, SENTENCE)[3];
    assert_eq!(&SENTENCE[fox.offset_from..fox.offset_to], "fox's");
    let han = tokens(STANDARD, "中文字");
    assert_eq!((han[1].offset_from, han[1].offset_to), (3, 6));
}

#[test]
fn english_matches_lucenes_chain() {
    let english = tokens(ENGLISH, SENTENCE);
    let words: Vec<&str> = english.iter().map(|t| t.text.as_str()).collect();
    assert_eq!(words, ["quick", "brown", "fox", "3.5", "jump"]);
    let positions: Vec<usize> = english.iter().map(|t| t.position).collect();
    assert_eq!(
        positions,
        [1, 2, 3, 4, 5],
        "the stop word leaves a gap at 0"
    );

    assert_eq!(texts(ENGLISH, "Operon’s"), ["operon"]);
    assert_eq!(texts(ENGLISH, "OPERON'S"), ["operon"]);
    assert_eq!(texts(ENGLISH, "Operon＇s"), ["operon"]);
    // Only a trailing possessive is removed, before stemming.
    assert_eq!(texts(ENGLISH, "cats"), ["cat"]);
    assert_eq!(
        texts(ENGLISH, "generalizations of the oscillators"),
        ["gener", "oscil"]
    );
}

#[test]
fn simple_splits_on_non_letters() {
    assert_eq!(texts(SIMPLE, "don't stop-2x"), ["don", "t", "stop", "x"]);
    assert_eq!(texts(SIMPLE, "ÜBER straße"), ["über", "straße"]);
}

#[test]
fn whitespace_keeps_case() {
    assert_eq!(
        texts(WHITESPACE, "Hello  World\tX"),
        ["Hello", "World", "X"]
    );
    // Unicode whitespace too (U+3000 IDEOGRAPHIC SPACE, U+00A0 NO-BREAK SPACE).
    assert_eq!(texts(WHITESPACE, "a\u{3000}B\u{a0}c"), ["a", "B", "c"]);
    let offsets: Vec<(usize, usize)> = tokens(WHITESPACE, " ab  c")
        .iter()
        .map(|t| (t.offset_from, t.offset_to))
        .collect();
    assert_eq!(offsets, [(1, 3), (5, 6)]);
}

#[test]
fn keyword_is_one_token() {
    assert_eq!(
        texts(KEYWORD, "The QUICK brown-fox's"),
        ["The QUICK brown-fox's"]
    );
}

#[test]
fn every_known_analyzer_is_registered() {
    let manager = tokenizer_manager();
    for name in KNOWN_ANALYZERS {
        assert!(manager.get(name).is_some(), "{name} is not registered");
    }
    // Tantivy's defaults stay available.
    for name in ["raw", "default", "en_stem"] {
        assert!(manager.get(name).is_some(), "{name} is not registered");
    }
}
