//! The hand-written JSON forms of [`FieldValue`], [`SortValue`],
//! [`Fuzziness`] and [`SourceFilter`] (plan M1.2 Task 1 rule 2, Ruling 9),
//! and RFC 3339 dates in µs.

use std::fmt;

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::ir::{FieldValue, Fuzziness, SortValue};
use crate::json::pk::{format_uuid, parse_uuid};
use crate::types::SourceFilter;

/// `YYYY-MM-DDTHH:MM:SS.ffffffZ` of µs since the epoch; `None` outside the
/// years 0000–9999.
pub fn format_date(micros: i64) -> Option<String> {
    let at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1000).ok()?;
    at.format(format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]Z"
    ))
    .ok()
}

/// µs since the epoch of an RFC 3339 date-time (any offset; finer digits are
/// truncated toward the past).
pub fn parse_date(text: &str) -> Option<i64> {
    let at = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    i64::try_from(at.unix_timestamp_nanos().div_euclid(1000)).ok()
}

/// An integer as `I64` when it fits, else `U64`.
fn int_u64<T>(n: u64, i: fn(i64) -> T, u: fn(u64) -> T) -> T {
    match i64::try_from(n) {
        Ok(n) => i(n),
        Err(_) => u(n),
    }
}

/// Reads a one-key object `{key: "<text>"}` and returns the text.
fn single_string<'de, A: MapAccess<'de>>(mut map: A, key: &str) -> Result<String, A::Error> {
    let Some(found) = map.next_key::<String>()? else {
        return Err(de::Error::custom(format!("expected {{\"{key}\": …}}")));
    };
    if found != key {
        return Err(de::Error::custom(format!(
            "unexpected key {found:?}, expected {key:?}"
        )));
    }
    let text = map.next_value::<String>()?;
    if let Some(extra) = map.next_key::<String>()? {
        return Err(de::Error::custom(format!("unexpected key {extra:?}")));
    }
    Ok(text)
}

impl Serialize for FieldValue {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            FieldValue::Str(v) => s.serialize_str(v),
            FieldValue::I64(v) => s.serialize_i64(*v),
            FieldValue::U64(v) => s.serialize_u64(*v),
            FieldValue::F64(v) => s.serialize_f64(*v),
            FieldValue::Bool(v) => s.serialize_bool(*v),
            FieldValue::Date(micros) => {
                let text = format_date(*micros).ok_or_else(|| {
                    serde::ser::Error::custom(format!("date {micros} µs is out of range"))
                })?;
                let mut map = s.serialize_map(Some(1))?;
                map.serialize_entry("date", &text)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = FieldValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string, bool, number or {\"date\": \"<RFC 3339>\"}")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<FieldValue, E> {
                Ok(FieldValue::Bool(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<FieldValue, E> {
                Ok(FieldValue::I64(v))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<FieldValue, E> {
                Ok(int_u64(v, FieldValue::I64, FieldValue::U64))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<FieldValue, E> {
                Ok(FieldValue::F64(v))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<FieldValue, E> {
                Ok(FieldValue::Str(v.to_string()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<FieldValue, E> {
                Ok(FieldValue::Str(v))
            }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<FieldValue, A::Error> {
                let text = single_string(map, "date")?;
                parse_date(&text)
                    .map(FieldValue::Date)
                    .ok_or_else(|| de::Error::custom(format!("invalid RFC 3339 date {text:?}")))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for SortValue {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            SortValue::Null => s.serialize_unit(),
            SortValue::Bool(v) => s.serialize_bool(*v),
            SortValue::I64(v) => s.serialize_i64(*v),
            SortValue::U64(v) => s.serialize_u64(*v),
            SortValue::F64(v) => s.serialize_f64(*v),
            SortValue::Str(v) => s.serialize_str(v),
            SortValue::Uuid(bytes) => {
                let mut map = s.serialize_map(Some(1))?;
                map.serialize_entry("uuid", &format_uuid(bytes))?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SortValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = SortValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("null, a bool, number, string or {\"uuid\": \"…\"}")
            }
            fn visit_unit<E: de::Error>(self) -> Result<SortValue, E> {
                Ok(SortValue::Null)
            }
            fn visit_none<E: de::Error>(self) -> Result<SortValue, E> {
                Ok(SortValue::Null)
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<SortValue, E> {
                Ok(SortValue::Bool(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<SortValue, E> {
                Ok(SortValue::I64(v))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<SortValue, E> {
                Ok(int_u64(v, SortValue::I64, SortValue::U64))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<SortValue, E> {
                Ok(SortValue::F64(v))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<SortValue, E> {
                Ok(SortValue::Str(v.to_string()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<SortValue, E> {
                Ok(SortValue::Str(v))
            }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<SortValue, A::Error> {
                let text = single_string(map, "uuid")?;
                parse_uuid(&text)
                    .map(SortValue::Uuid)
                    .ok_or_else(|| de::Error::custom(format!("invalid uuid {text:?}")))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for Fuzziness {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Fuzziness::Auto => s.serialize_str("auto"),
            Fuzziness::Edits(n) => s.serialize_u8(*n),
        }
    }
}

impl<'de> Deserialize<'de> for Fuzziness {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Fuzziness;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("\"auto\" or an integer 0-2")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Fuzziness, E> {
                match v {
                    "auto" => Ok(Fuzziness::Auto),
                    other => Err(E::custom(format!(
                        "invalid fuzziness {other:?}: expected \"auto\" or 0-2"
                    ))),
                }
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Fuzziness, E> {
                match u8::try_from(v) {
                    Ok(n @ 0..=2) => Ok(Fuzziness::Edits(n)),
                    _ => Err(E::custom(format!(
                        "invalid fuzziness {v}: expected \"auto\" or 0-2"
                    ))),
                }
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Fuzziness, E> {
                match u64::try_from(v) {
                    Ok(v) => self.visit_u64(v),
                    Err(_) => Err(E::custom(format!(
                        "invalid fuzziness {v}: expected \"auto\" or 0-2"
                    ))),
                }
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for SourceFilter {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            SourceFilter::All => s.serialize_str("all"),
            SourceFilter::None => s.serialize_str("none"),
            SourceFilter::Paths { include, exclude } => {
                let mut map = s.serialize_map(Some(2))?;
                map.serialize_entry("include", include)?;
                map.serialize_entry("exclude", exclude)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SourceFilter {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = SourceFilter;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("\"all\", \"none\" or {\"include\": [..], \"exclude\": [..]}")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<SourceFilter, E> {
                match v {
                    "all" => Ok(SourceFilter::All),
                    "none" => Ok(SourceFilter::None),
                    other => Err(E::custom(format!("invalid source filter {other:?}"))),
                }
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<SourceFilter, A::Error> {
                let (mut include, mut exclude) = (None, None);
                while let Some(key) = map.next_key::<String>()? {
                    let slot = match key.as_str() {
                        "include" => &mut include,
                        "exclude" => &mut exclude,
                        other => {
                            return Err(de::Error::custom(format!(
                                "unknown key {other} in the source filter"
                            )));
                        }
                    };
                    if slot.is_some() {
                        return Err(de::Error::custom(format!("repeated key {key}")));
                    }
                    *slot = Some(map.next_value::<Vec<String>>()?);
                }
                Ok(SourceFilter::Paths {
                    include: include.unwrap_or_default(),
                    exclude: exclude.unwrap_or_default(),
                })
            }
        }
        d.deserialize_any(V)
    }
}
