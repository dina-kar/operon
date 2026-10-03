//! A protobuf wire-format reader, just enough of it.
//!
//! `prost` (and every generated Rust type) throws away the fields it does not
//! know, and the two fields this plugin exists to read *are* fields it does not
//! know: `loams.options.v1.ModuleOptions` on `ServiceOptions` (50001) and
//! `loams.options.v1.FacadeOptions` on `MethodOptions` (50002). So the
//! descriptor is walked by hand instead: skip over the fields that are known
//! and keep the payloads of the two that matter.
//!
//! Only what a `CodeGeneratorRequest` carries is implemented: varints,
//! length-delimited fields and nested messages. Groups (wire types 3 and 4)
//! are rejected rather than skipped, because no proto in this repository uses
//! them and silently mis-reading one would be worse than failing.

/// A field number paired with the payload the wire format kept.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Varint(u64),
    Fixed64(u64),
    Bytes(&'a [u8]),
    Fixed32(u32),
}

/// One field of a message: its number and its payload. A length-delimited
/// field that is really a nested message is [`Value::Bytes`]; the caller
/// decides whether to walk it again.
pub type Field<'a> = (u32, Value<'a>);

/// A cursor over one message's bytes.
#[derive(Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// A reader over a whole message.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    /// The next field, or `None` at the end of the message. Fails on a
    /// truncated field, a wire type this reader does not implement, or bytes
    /// left over after the last field.
    ///
    /// Named `next_field` rather than `next`: this is not an iterator, it is a
    /// cursor, and clippy is right that the two should not look alike.
    #[allow(clippy::should_implement_trait)]
    pub fn next_field(&mut self) -> Result<Option<Field<'a>>, WireError> {
        if self.at == self.bytes.len() {
            return Ok(None);
        }
        let tag = self.varint()?;
        let number = u32::try_from(tag >> 3).map_err(|_| WireError::FieldNumber(tag >> 3))?;
        let value = match tag & 0x07 {
            0 => Value::Varint(self.varint()?),
            1 => {
                let raw = self.take(8)?;
                Value::Fixed64(u64::from_le_bytes(raw.try_into().expect("8 bytes")))
            }
            2 => {
                let len = usize::try_from(self.varint()?).map_err(|_| WireError::Length(0))?;
                Value::Bytes(self.take(len)?)
            }
            5 => {
                let raw = self.take(4)?;
                Value::Fixed32(u32::from_le_bytes(raw.try_into().expect("4 bytes")))
            }
            other => return Err(WireError::WireType(other as u8)),
        };
        Ok(Some((number, value)))
    }

    /// Every field of the message, in order.
    pub fn fields(&mut self) -> Result<Vec<Field<'a>>, WireError> {
        let mut out = Vec::new();
        while let Some(field) = self.next_field()? {
            out.push(field);
        }
        Ok(out)
    }

    fn varint(&mut self) -> Result<u64, WireError> {
        let mut out = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = *self.bytes.get(self.at).ok_or(WireError::TruncatedVarint)?;
            self.at += 1;
            out |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err(WireError::TruncatedVarint)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], WireError> {
        let end = self.at.checked_add(len).ok_or(WireError::Truncated)?;
        let out = self.bytes.get(self.at..end).ok_or(WireError::Truncated)?;
        self.at = end;
        Ok(out)
    }
}

/// Why a message did not parse.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum WireError {
    #[error("the message ends inside a field")]
    Truncated,
    #[error("the message ends inside a varint")]
    TruncatedVarint,
    #[error("a length does not fit in memory: {0}")]
    Length(u64),
    #[error("field number {0} is out of range")]
    FieldNumber(u64),
    #[error("wire type {0} is not implemented (groups are rejected)")]
    WireType(u8),
    #[error("the option {0} is malformed: {1}")]
    Option(&'static str, String),
}

/// The first length-delimited field with this number, as UTF-8. An absent
/// field is `None`; a present one that is not valid UTF-8 is an error, because
/// a name that does not decode is a descriptor this plugin cannot trust.
pub fn string_field(fields: &[(u32, Value<'_>)], number: u32) -> Result<Option<String>, WireError> {
    let mut out = None;
    for (field, value) in fields {
        if *field != number {
            continue;
        }
        let Value::Bytes(bytes) = value else {
            return Err(WireError::WireType(2));
        };
        if out.is_none() {
            out = Some(
                std::str::from_utf8(bytes)
                    .map_err(|_| WireError::Truncated)?
                    .to_owned(),
            );
        }
    }
    Ok(out)
}

/// The first varint field with this number, or `None`.
pub fn varint_field(fields: &[(u32, Value<'_>)], number: u32) -> Option<u64> {
    fields.iter().find_map(|(field, value)| match value {
        Value::Varint(raw) if *field == number => Some(*raw),
        _ => None,
    })
}

/// Every length-delimited field with this number, in order. `repeated` fields
/// need all of them, not the first.
pub fn repeated_bytes<'a>(fields: &'a [(u32, Value<'a>)], number: u32) -> Vec<&'a [u8]> {
    fields
        .iter()
        .filter_map(|(field, value)| match value {
            Value::Bytes(bytes) if *field == number => Some(*bytes),
            _ => None,
        })
        .collect()
}

/// Every nested message with this number, walked in order.
pub fn repeated_messages<'a>(
    fields: &'a [(u32, Value<'a>)],
    number: u32,
) -> Result<Vec<Reader<'a>>, WireError> {
    Ok(repeated_bytes(fields, number)
        .into_iter()
        .map(Reader::new)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tag and a length-delimited payload, the way protoc writes one.
    fn tagged(number: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        push_varint(&mut out, (u64::from(number) << 3) | 2);
        push_varint(&mut out, payload.len() as u64);
        out.extend_from_slice(payload);
        out
    }

    /// A tag and a varint payload.
    fn varint(number: u32, value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        push_varint(&mut out, u64::from(number) << 3);
        push_varint(&mut out, value);
        out
    }

    fn push_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    #[test]
    fn reads_a_length_delimited_field() {
        let bytes = tagged(1, b"abc");
        let fields = Reader::new(&bytes).fields().expect("parse");
        assert_eq!(
            string_field(&fields, 1).expect("string"),
            Some("abc".to_owned())
        );
        assert_eq!(string_field(&fields, 2).expect("string"), None);
    }

    #[test]
    fn reads_a_varint_and_a_nested_message() {
        // field 34 (idempotency_level) = 1, then field 2 = a nested message.
        let mut bytes = varint(34, 1);
        bytes.extend_from_slice(&tagged(2, &tagged(1, b"abc")));
        let fields = Reader::new(&bytes).fields().expect("parse");
        assert_eq!(varint_field(&fields, 34), Some(1));
        let mut nested = repeated_messages(&fields, 2).expect("nested");
        assert_eq!(nested.len(), 1);
        assert_eq!(
            string_field(&nested[0].fields().expect("inner"), 1).expect("string"),
            Some("abc".to_owned())
        );
    }

    #[test]
    fn rejects_a_truncated_field() {
        let bytes = tagged(1, b"aaaaa")[..4].to_vec();
        assert_eq!(Reader::new(&bytes).fields(), Err(WireError::Truncated));
    }

    #[test]
    fn rejects_a_group() {
        let bytes = vec![(1 << 3) | 3];
        assert_eq!(Reader::new(&bytes).fields(), Err(WireError::WireType(3)));
    }

    #[test]
    fn reads_a_multi_byte_varint() {
        let bytes = varint(34, 300);
        let fields = Reader::new(&bytes).fields().expect("parse");
        assert_eq!(varint_field(&fields, 34), Some(300));
    }

    #[test]
    fn collects_every_repeated_field() {
        // Two FacadeOptions on field 50002, whose tag needs three varint
        // bytes: the case a hand-rolled tag must not get wrong.
        let mut bytes = tagged(50002, b"one");
        bytes.extend_from_slice(&tagged(50002, b"two"));
        let fields = Reader::new(&bytes).fields().expect("parse");
        let values = repeated_bytes(&fields, 50002);
        assert_eq!(values, vec![b"one".as_slice(), b"two".as_slice()]);
    }

    #[test]
    fn rejects_a_field_that_is_not_utf8() {
        let bytes = tagged(1, &[0xff, 0xfe]);
        let fields = Reader::new(&bytes).fields().expect("parse");
        assert!(string_field(&fields, 1).is_err());
    }
}
