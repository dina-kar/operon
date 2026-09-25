//! The five M1 analyzers (overview A5, plan M1.1 Ruling 25), built to match
//! Lucene's, because the BEIR gate compares Operon's ranking with ES's:
//!
//! - `standard`: [`StandardTokenizer`] → lowercase (ES `standard`, no stop
//!   words);
//! - `english`: [`StandardTokenizer`] → [`EnglishPossessiveFilter`] →
//!   lowercase → [`LUCENE_ENGLISH_STOP_WORDS`] → [`PorterStemFilter`]
//!   (Lucene 9's `EnglishAnalyzer`, without keyword marking);
//! - `simple`: [`LetterTokenizer`] → lowercase;
//! - `whitespace`: [`UnicodeWhitespaceTokenizer`], case kept;
//! - `keyword`: the whole value as one token (tantivy's `RawTokenizer`).

use std::str::CharIndices;

use tantivy::tokenizer::{
    LowerCaser, RawTokenizer, StopWordFilter, TextAnalyzer, Token, TokenFilter, TokenStream,
    Tokenizer, TokenizerManager,
};
use unicode_segmentation::{UnicodeSegmentation, UnicodeWordIndices};

use crate::porter::porter_stem;

pub const STANDARD: &str = "standard";
pub const ENGLISH: &str = "english";
pub const SIMPLE: &str = "simple";
pub const WHITESPACE: &str = "whitespace";
pub const KEYWORD: &str = "keyword";

/// The longest `standard` token, in `char`s; a longer word is cut into
/// consecutive tokens of this length (Lucene's `StandardTokenizer`).
pub const MAX_TOKEN_CHARS: usize = 255;

/// Lucene's `EnglishAnalyzer.ENGLISH_STOP_WORDS_SET`.
pub const LUCENE_ENGLISH_STOP_WORDS: [&str; 33] = [
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

/// Tantivy's defaults ("raw", "default", …) plus the five M1 analyzers.
pub fn tokenizer_manager() -> TokenizerManager {
    let manager = TokenizerManager::default();
    manager.register(
        STANDARD,
        TextAnalyzer::builder(StandardTokenizer)
            .filter(LowerCaser)
            .build(),
    );
    manager.register(
        ENGLISH,
        TextAnalyzer::builder(StandardTokenizer)
            .filter(EnglishPossessiveFilter)
            .filter(LowerCaser)
            .filter(StopWordFilter::remove(
                LUCENE_ENGLISH_STOP_WORDS.map(str::to_string),
            ))
            .filter(PorterStemFilter)
            .build(),
    );
    manager.register(
        SIMPLE,
        TextAnalyzer::builder(LetterTokenizer)
            .filter(LowerCaser)
            .build(),
    );
    // Replaces tantivy's own "whitespace", which splits on ASCII whitespace
    // only.
    manager.register(WHITESPACE, UnicodeWhitespaceTokenizer);
    manager.register(KEYWORD, RawTokenizer::default());
    manager
}

/// UAX #29 word segmentation (`unicode_word_indices`: the words that contain
/// a letter, digit or ideograph), each word cut into tokens of at most
/// [`MAX_TOKEN_CHARS`] `char`s. Positions increase by one per token; offsets
/// are byte offsets.
#[derive(Clone, Debug, Default)]
pub struct StandardTokenizer;

/// The token stream of [`StandardTokenizer`].
#[derive(Debug)]
pub struct StandardTokenStream<'a> {
    words: UnicodeWordIndices<'a>,
    /// The rest of a word cut at [`MAX_TOKEN_CHARS`]: its byte offset and
    /// text.
    rest: Option<(usize, &'a str)>,
    token: Token,
}

impl Tokenizer for StandardTokenizer {
    type TokenStream<'a> = StandardTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> StandardTokenStream<'a> {
        StandardTokenStream {
            words: text.unicode_word_indices(),
            rest: None,
            token: Token::default(),
        }
    }
}

impl TokenStream for StandardTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let Some((offset, word)) = self.rest.take().or_else(|| self.words.next()) else {
            return false;
        };
        let text = match word.char_indices().nth(MAX_TOKEN_CHARS) {
            Some((cut, _)) => {
                self.rest = Some((offset + cut, &word[cut..]));
                &word[..cut]
            }
            None => word,
        };
        set_token(&mut self.token, offset, text);
        true
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

/// Makes `token` the next token, `text` at byte `offset`.
fn set_token(token: &mut Token, offset: usize, text: &str) {
    token.text.clear();
    token.text.push_str(text);
    token.offset_from = offset;
    token.offset_to = offset + text.len();
    token.position = token.position.wrapping_add(1);
}

/// Tokens are the maximal runs of `char::is_alphabetic` (Lucene's
/// `LetterTokenizer`).
#[derive(Clone, Debug, Default)]
pub struct LetterTokenizer;

/// Tokens are the maximal runs of `char`s that are not
/// `char::is_whitespace` (Lucene's `WhitespaceTokenizer`, whose whitespace
/// is Unicode's).
#[derive(Clone, Debug, Default)]
pub struct UnicodeWhitespaceTokenizer;

/// The token stream of [`LetterTokenizer`] and
/// [`UnicodeWhitespaceTokenizer`]: the maximal runs of `char`s that satisfy
/// `in_token`.
#[derive(Debug)]
pub struct CharRunTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    in_token: fn(char) -> bool,
    token: Token,
}

impl<'a> CharRunTokenStream<'a> {
    fn new(text: &'a str, in_token: fn(char) -> bool) -> Self {
        Self {
            text,
            chars: text.char_indices(),
            in_token,
            token: Token::default(),
        }
    }
}

impl Tokenizer for LetterTokenizer {
    type TokenStream<'a> = CharRunTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> CharRunTokenStream<'a> {
        CharRunTokenStream::new(text, char::is_alphabetic)
    }
}

impl Tokenizer for UnicodeWhitespaceTokenizer {
    type TokenStream<'a> = CharRunTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> CharRunTokenStream<'a> {
        CharRunTokenStream::new(text, |c| !c.is_whitespace())
    }
}

impl TokenStream for CharRunTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let in_token = self.in_token;
        let Some((start, _)) = self.chars.by_ref().find(|&(_, c)| in_token(c)) else {
            return false;
        };
        let end = self
            .chars
            .by_ref()
            .find(|&(_, c)| !in_token(c))
            .map_or(self.text.len(), |(end, _)| end);
        set_token(&mut self.token, start, &self.text[start..end]);
        true
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

/// Removes a trailing `'s` or `'S`, where the apostrophe is U+0027, U+2019
/// or U+FF07 (Lucene's `EnglishPossessiveFilter`).
#[derive(Clone, Debug, Default)]
pub struct EnglishPossessiveFilter;

/// Replaces each token with its [`porter_stem`] (Lucene's
/// `PorterStemFilter`).
#[derive(Clone, Debug, Default)]
pub struct PorterStemFilter;

/// A tokenizer whose every token's text is rewritten by `F`.
#[derive(Clone, Debug)]
pub struct MapText<T, F> {
    inner: T,
    map: F,
}

/// The token stream of [`MapText`].
#[derive(Debug)]
pub struct MapTextStream<S, F> {
    tail: S,
    map: F,
}

/// Rewrites a token's text in place.
pub trait TextMap: Clone + Send + Sync + 'static {
    fn apply(&self, text: &mut String);
}

impl TextMap for EnglishPossessiveFilter {
    fn apply(&self, text: &mut String) {
        let mut rev = text.char_indices().rev();
        if let (Some((_, 's' | 'S')), Some((at, '\'' | '\u{2019}' | '\u{FF07}'))) =
            (rev.next(), rev.next())
        {
            text.truncate(at);
        }
    }
}

impl TextMap for PorterStemFilter {
    fn apply(&self, text: &mut String) {
        *text = porter_stem(text);
    }
}

macro_rules! text_map_filter {
    ($filter:ty) => {
        impl TokenFilter for $filter {
            type Tokenizer<T: Tokenizer> = MapText<T, $filter>;

            fn transform<T: Tokenizer>(self, inner: T) -> MapText<T, $filter> {
                MapText { inner, map: self }
            }
        }
    };
}
text_map_filter!(EnglishPossessiveFilter);
text_map_filter!(PorterStemFilter);

impl<T: Tokenizer, F: TextMap> Tokenizer for MapText<T, F> {
    type TokenStream<'a> = MapTextStream<T::TokenStream<'a>, F>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        MapTextStream {
            tail: self.inner.token_stream(text),
            map: self.map.clone(),
        }
    }
}

impl<S: TokenStream, F: TextMap> TokenStream for MapTextStream<S, F> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        self.map.apply(&mut self.tail.token_mut().text);
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}
