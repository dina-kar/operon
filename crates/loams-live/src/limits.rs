//! [`Limits`]: the R1 defaults of design §20 §4.1 (documents) and §5.1
//! (mutations), and the document checks. A limit that is exceeded is a
//! [`LiveError::LimitExceeded`] naming the field of [`Limits`].

use std::collections::BTreeMap;
use std::time::Duration;

use crate::{LiveError, LiveValue};

/// The limits of one app. `Default` is R1's defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// The encoded `DocumentRecord` of one document (1 MiB).
    pub max_document_bytes: usize,
    /// Fields of one document, and of one object value (1 024).
    pub max_fields: usize,
    /// Nesting of arrays and objects in a value (16; a scalar field is at
    /// depth 1).
    pub max_depth: usize,
    /// Elements of one array (8 192).
    pub max_array_len: usize,
    /// Bytes of one field name (1 024).
    pub max_field_name_bytes: usize,
    /// Fields of one user index (16).
    pub max_index_fields: usize,
    /// User indexes of one table (32).
    pub max_indexes: usize,
    /// The encoded values of one index entry (4 KiB), so every index key
    /// stays well below TiKV's 8 KiB key size limit.
    pub max_index_key_bytes: usize,
    /// Bytes one mutation writes (8 MiB).
    pub max_written_bytes: usize,
    /// Documents one mutation writes (16 000).
    pub max_written_docs: usize,
    /// Documents one function scans (32 000); also the most one unlimited
    /// index scan reads.
    pub max_scanned_docs: usize,
    /// Index ranges one function reads (4 096).
    pub max_index_ranges: usize,
    /// JavaScript CPU time of one function call (1 s).
    pub max_js_cpu: Duration,
    /// The wall-clock deadline of one mutation (10 s).
    pub mutation_deadline: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_document_bytes: 1024 * 1024,
            max_fields: 1024,
            max_depth: 16,
            max_array_len: 8192,
            max_field_name_bytes: 1024,
            max_index_fields: 16,
            max_indexes: 32,
            max_index_key_bytes: 4096,
            max_written_bytes: 8 * 1024 * 1024,
            max_written_docs: 16_000,
            max_scanned_docs: 32_000,
            max_index_ranges: 4096,
            max_js_cpu: Duration::from_secs(1),
            mutation_deadline: Duration::from_secs(10),
        }
    }
}

impl Limits {
    /// Checks a document's user fields: names (non-empty, not starting with
    /// `_`, which system fields use, at most `max_field_name_bytes`), the
    /// field count, and each value's depth, array lengths and object sizes.
    /// The encoded size is checked when the record is built
    /// ([`Limits::check_document_bytes`]).
    pub fn check_fields(&self, fields: &BTreeMap<String, LiveValue>) -> Result<(), LiveError> {
        if fields.len() > self.max_fields {
            return Err(LiveError::limit(
                "max_fields",
                format!(
                    "a document has {} fields, more than {}",
                    fields.len(),
                    self.max_fields
                ),
            ));
        }
        for (name, value) in fields {
            self.check_name(name)?;
            if name.starts_with('_') {
                return Err(LiveError::invalid(format!(
                    "field '{name}': names starting with '_' are reserved for system fields"
                )));
            }
            self.check_value(name, value, 1)?;
        }
        Ok(())
    }

    /// Checks the encoded size of a document record.
    pub fn check_document_bytes(&self, len: usize) -> Result<(), LiveError> {
        if len > self.max_document_bytes {
            return Err(LiveError::limit(
                "max_document_bytes",
                format!(
                    "a document encodes to {len} bytes, more than {}",
                    self.max_document_bytes
                ),
            ));
        }
        Ok(())
    }

    fn check_name(&self, name: &str) -> Result<(), LiveError> {
        if name.is_empty() {
            return Err(LiveError::invalid("a field name is empty"));
        }
        if name.len() > self.max_field_name_bytes {
            return Err(LiveError::limit(
                "max_field_name_bytes",
                format!(
                    "a field name has {} bytes, more than {}",
                    name.len(),
                    self.max_field_name_bytes
                ),
            ));
        }
        Ok(())
    }

    fn check_value(&self, field: &str, value: &LiveValue, depth: usize) -> Result<(), LiveError> {
        if depth > self.max_depth {
            return Err(LiveError::limit(
                "max_depth",
                format!("field '{field}' nests deeper than {}", self.max_depth),
            ));
        }
        match value {
            LiveValue::Array(items) => {
                if items.len() > self.max_array_len {
                    return Err(LiveError::limit(
                        "max_array_len",
                        format!(
                            "field '{field}' has an array of {} elements, more than {}",
                            items.len(),
                            self.max_array_len
                        ),
                    ));
                }
                for item in items {
                    self.check_value(field, item, depth + 1)?;
                }
            }
            LiveValue::Object(inner) => {
                if inner.len() > self.max_fields {
                    return Err(LiveError::limit(
                        "max_fields",
                        format!(
                            "field '{field}' has an object of {} fields, more than {}",
                            inner.len(),
                            self.max_fields
                        ),
                    ));
                }
                for (name, item) in inner {
                    self.check_name(name)?;
                    self.check_value(field, item, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}
