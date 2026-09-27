//! The order-preserving tuple codec (design §20 §4.3; R1 plan Task 2).
//!
//! [`tuple::encode`] writes a value so that the byte order of two encodings is
//! the order of the values: a TiKV range scan over index entries returns them
//! in value order. The order is null < int64 < float64 < bool < string <
//! bytes < array; inside a type it is the natural one, strings and bytes
//! compare bytewise, and arrays compare element by element with a shorter
//! prefix first.

/// Errors of [`tuple::decode`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// The buffer ended inside an element.
    #[error("tuple: truncated at byte {0}")]
    Truncated(usize),
    /// An unknown type tag.
    #[error("tuple: unknown tag 0x{tag:02x} at byte {at}")]
    BadTag { tag: u8, at: usize },
    /// `0x00` followed by something other than `0xFF` or `0x00`.
    #[error("tuple: bad escape at byte {0}")]
    BadEscape(usize),
    /// A string element is not UTF-8.
    #[error("tuple: string at byte {0} is not UTF-8")]
    BadUtf8(usize),
}

/// The tuple codec.
pub mod tuple {
    use std::borrow::Cow;

    use super::CodecError;

    /// The tag of null.
    pub const NULL: u8 = 0x05;
    /// The tag of int64.
    pub const I64: u8 = 0x10;
    /// The tag of float64.
    pub const F64: u8 = 0x20;
    /// The tag of `false`.
    pub const FALSE: u8 = 0x30;
    /// The tag of `true`.
    pub const TRUE: u8 = 0x31;
    /// The tag of a string.
    pub const STR: u8 = 0x40;
    /// The tag of a byte string.
    pub const BYTES: u8 = 0x50;
    /// The tag of an array.
    pub const ARRAY: u8 = 0x60;

    const SIGN: u64 = 1 << 63;
    /// The canonical NaN every NaN is encoded as, so all NaNs sort last and
    /// equal.
    const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

    /// One indexable value. Strings and byte strings borrow from the caller
    /// when encoding and own their bytes after [`decode`] (which unescapes).
    #[derive(Debug, Clone)]
    pub enum Elem<'a> {
        Null,
        I64(i64),
        F64(f64),
        Bool(bool),
        Str(Cow<'a, str>),
        Bytes(Cow<'a, [u8]>),
        Array(Vec<Elem<'a>>),
    }

    impl Elem<'_> {
        /// A copy that owns all of its data.
        pub fn into_owned(self) -> Elem<'static> {
            match self {
                Elem::Null => Elem::Null,
                Elem::I64(v) => Elem::I64(v),
                Elem::F64(v) => Elem::F64(v),
                Elem::Bool(v) => Elem::Bool(v),
                Elem::Str(s) => Elem::Str(Cow::Owned(s.into_owned())),
                Elem::Bytes(b) => Elem::Bytes(Cow::Owned(b.into_owned())),
                Elem::Array(a) => Elem::Array(a.into_iter().map(Elem::into_owned).collect()),
            }
        }
    }

    /// Equality as the encoding sees it: `-0.0 == 0.0` and `NaN == NaN`.
    impl PartialEq for Elem<'_> {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Elem::Null, Elem::Null) => true,
                (Elem::I64(a), Elem::I64(b)) => a == b,
                (Elem::F64(a), Elem::F64(b)) => float_bits(*a) == float_bits(*b),
                (Elem::Bool(a), Elem::Bool(b)) => a == b,
                (Elem::Str(a), Elem::Str(b)) => a == b,
                (Elem::Bytes(a), Elem::Bytes(b)) => a == b,
                (Elem::Array(a), Elem::Array(b)) => a == b,
                _ => false,
            }
        }
    }

    /// The float's order-preserving 8 bytes (as a `u64`): NaN canonical and
    /// last, `-0.0` as `0.0`, positive values with the sign bit set, negative
    /// values with every bit inverted.
    fn float_bits(v: f64) -> u64 {
        let bits = if v.is_nan() {
            CANONICAL_NAN
        } else if v == 0.0 {
            0
        } else {
            v.to_bits()
        };
        if bits & SIGN == 0 { bits | SIGN } else { !bits }
    }

    fn float_from_bits(bits: u64) -> f64 {
        let raw = if bits & SIGN != 0 { bits ^ SIGN } else { !bits };
        f64::from_bits(raw)
    }

    /// Appends the encoding of `e` to `out`.
    pub fn encode(out: &mut Vec<u8>, e: &Elem) {
        match e {
            Elem::Null => out.push(NULL),
            Elem::I64(v) => {
                out.push(I64);
                out.extend_from_slice(&((*v as u64) ^ SIGN).to_be_bytes());
            }
            Elem::F64(v) => {
                out.push(F64);
                out.extend_from_slice(&float_bits(*v).to_be_bytes());
            }
            Elem::Bool(false) => out.push(FALSE),
            Elem::Bool(true) => out.push(TRUE),
            Elem::Str(s) => {
                out.push(STR);
                escape(out, s.as_bytes());
            }
            Elem::Bytes(b) => {
                out.push(BYTES);
                escape(out, b);
            }
            Elem::Array(items) => {
                out.push(ARRAY);
                for item in items {
                    encode(out, item);
                }
                out.extend_from_slice(&[0x00, 0x00]);
            }
        }
    }

    /// `0x00` → `0x00 0xFF`, then the terminator `0x00 0x00`.
    fn escape(out: &mut Vec<u8>, bytes: &[u8]) {
        for &b in bytes {
            out.push(b);
            if b == 0x00 {
                out.push(0xFF);
            }
        }
        out.extend_from_slice(&[0x00, 0x00]);
    }

    /// Decodes one element from the start of `buf` and returns it with the
    /// number of bytes it took.
    pub fn decode(buf: &[u8]) -> Result<(Elem<'static>, usize), CodecError> {
        decode_at(buf, 0)
    }

    fn decode_at(buf: &[u8], at: usize) -> Result<(Elem<'static>, usize), CodecError> {
        let tag = *buf.get(at).ok_or(CodecError::Truncated(at))?;
        let body = at + 1;
        match tag {
            NULL => Ok((Elem::Null, body)),
            I64 => {
                let raw = fixed8(buf, body)?;
                Ok((Elem::I64((raw ^ SIGN) as i64), body + 8))
            }
            F64 => {
                let raw = fixed8(buf, body)?;
                Ok((Elem::F64(float_from_bits(raw)), body + 8))
            }
            FALSE => Ok((Elem::Bool(false), body)),
            TRUE => Ok((Elem::Bool(true), body)),
            STR => {
                let (bytes, end) = unescape(buf, body)?;
                let s = String::from_utf8(bytes).map_err(|_| CodecError::BadUtf8(at))?;
                Ok((Elem::Str(Cow::Owned(s)), end))
            }
            BYTES => {
                let (bytes, end) = unescape(buf, body)?;
                Ok((Elem::Bytes(Cow::Owned(bytes)), end))
            }
            ARRAY => {
                let mut items = Vec::new();
                let mut pos = body;
                loop {
                    match buf.get(pos) {
                        None => return Err(CodecError::Truncated(pos)),
                        Some(0x00) => match buf.get(pos + 1) {
                            Some(0x00) => return Ok((Elem::Array(items), pos + 2)),
                            Some(_) => return Err(CodecError::BadEscape(pos)),
                            None => return Err(CodecError::Truncated(pos + 1)),
                        },
                        Some(_) => {
                            let (item, next) = decode_at(buf, pos)?;
                            items.push(item);
                            pos = next;
                        }
                    }
                }
            }
            tag => Err(CodecError::BadTag { tag, at }),
        }
    }

    fn fixed8(buf: &[u8], at: usize) -> Result<u64, CodecError> {
        let bytes: [u8; 8] = buf
            .get(at..at + 8)
            .and_then(|s| s.try_into().ok())
            .ok_or(CodecError::Truncated(buf.len()))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn unescape(buf: &[u8], mut pos: usize) -> Result<(Vec<u8>, usize), CodecError> {
        let mut out = Vec::new();
        loop {
            let b = *buf.get(pos).ok_or(CodecError::Truncated(pos))?;
            if b != 0x00 {
                out.push(b);
                pos += 1;
                continue;
            }
            match buf.get(pos + 1) {
                Some(0xFF) => {
                    out.push(0x00);
                    pos += 2;
                }
                Some(0x00) => return Ok((out, pos + 2)),
                Some(_) => return Err(CodecError::BadEscape(pos)),
                None => return Err(CodecError::Truncated(pos + 1)),
            }
        }
    }

    /// The exclusive end of the range of keys that start with `prefix`: the
    /// smallest key greater than every extension of `prefix`. Empty when there
    /// is none (`prefix` is empty or all `0xFF`), which callers read as "no
    /// upper bound".
    pub fn successor(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        while let Some(last) = end.pop() {
            if last != 0xFF {
                end.push(last + 1);
                return end;
            }
        }
        end
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn enc(e: &Elem) -> Vec<u8> {
            let mut out = Vec::new();
            encode(&mut out, e);
            out
        }

        #[test]
        fn tags_match_the_design_table() {
            assert_eq!(enc(&Elem::Null), [0x05]);
            assert_eq!(enc(&Elem::Bool(false)), [0x30]);
            assert_eq!(enc(&Elem::Bool(true)), [0x31]);
            assert_eq!(enc(&Elem::I64(0)), [0x10, 0x80, 0, 0, 0, 0, 0, 0, 0]);
            assert_eq!(
                enc(&Elem::I64(-1)),
                [0x10, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
            );
            assert_eq!(
                enc(&Elem::Str(Cow::Borrowed("a\0b"))),
                [0x40, b'a', 0x00, 0xFF, b'b', 0x00, 0x00]
            );
            assert_eq!(enc(&Elem::Bytes(Cow::Borrowed(&[]))), [0x50, 0x00, 0x00]);
            assert_eq!(
                enc(&Elem::Array(vec![Elem::Null])),
                [0x60, 0x05, 0x00, 0x00]
            );
        }

        #[test]
        fn zero_and_nan_normalise() {
            assert_eq!(enc(&Elem::F64(-0.0)), enc(&Elem::F64(0.0)));
            assert_eq!(enc(&Elem::F64(f64::NAN)), enc(&Elem::F64(-f64::NAN)));
            assert!(enc(&Elem::F64(f64::INFINITY)) < enc(&Elem::F64(f64::NAN)));
            assert!(enc(&Elem::F64(f64::NEG_INFINITY)) < enc(&Elem::F64(-1e308)));
        }

        #[test]
        fn decode_refuses_malformed_input() {
            assert_eq!(decode(&[]).unwrap_err(), CodecError::Truncated(0));
            assert!(matches!(
                decode(&[0x07]),
                Err(CodecError::BadTag { tag: 0x07, .. })
            ));
            assert!(matches!(
                decode(&[0x40, b'a', 0x00, 0x01]),
                Err(CodecError::BadEscape(2))
            ));
            assert!(matches!(
                decode(&[0x40, 0xC3, 0x00, 0x00]),
                Err(CodecError::BadUtf8(0))
            ));
            assert!(matches!(
                decode(&[0x10, 1, 2]),
                Err(CodecError::Truncated(_))
            ));
            assert!(matches!(
                decode(&[0x60, 0x05]),
                Err(CodecError::Truncated(_))
            ));
        }

        #[test]
        fn successor_edge_cases() {
            assert_eq!(successor(b""), Vec::<u8>::new());
            assert_eq!(successor(&[0xFF, 0xFF]), Vec::<u8>::new());
            assert_eq!(successor(&[0x01, 0xFF]), vec![0x02]);
            assert_eq!(successor(b"ab"), b"ac".to_vec());
        }
    }
}
