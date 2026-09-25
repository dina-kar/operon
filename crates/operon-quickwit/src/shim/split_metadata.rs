// Copyright 2026 The Operon Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The split metadata the vendored merge policy plans over (a subset of
//! `quickwit-metastore`'s `SplitMetadata` and `quickwit-proto`'s `SplitId`).

use std::fmt;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use time::OffsetDateTime;

/// Identifies a split. Cheap to clone. The default is the empty id.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SplitId(pub Arc<str>);

impl Serialize for SplitId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl SplitId {
    /// Generates a new split id (a ULID string).
    pub fn new() -> Self {
        SplitId(Arc::from(ulid::Ulid::generate().to_string()))
    }

    /// Returns the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SplitId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, formatter)
    }
}

impl fmt::Display for SplitId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for SplitId {
    fn from(split_id: String) -> Self {
        SplitId(split_id.into())
    }
}

impl From<&str> for SplitId {
    fn from(split_id: &str) -> Self {
        SplitId(Arc::from(split_id))
    }
}

/// Whether a split can still be merged.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub enum SplitMaturity {
    /// The split is mature and no longer a candidate for merges.
    #[default]
    Mature,
    /// The split is immature and can undergo merges until `maturation_period` passes,
    /// measured from the split's creation timestamp.
    Immature {
        /// Maturation period.
        maturation_period: Duration,
    },
}

/// The metadata of a split that the merge policy needs.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct SplitMetadata {
    /// The split's id.
    pub split_id: SplitId,
    /// Number of documents in the split.
    pub num_docs: usize,
    /// Timestamp range of the split's documents, if it has a timestamp field.
    pub time_range: Option<RangeInclusive<i64>>,
    /// Maturity of the split.
    pub maturity: SplitMaturity,
    /// Creation time of the split, in unix seconds.
    pub create_timestamp: i64,
    /// Number of merge operations the split went through.
    pub num_merge_ops: usize,
    /// Byte range of the split footer (hotcache and bundle metadata).
    pub footer_offsets: Range<u64>,
}

impl SplitMetadata {
    /// Returns the split id.
    pub fn split_id(&self) -> &str {
        self.split_id.as_str()
    }

    /// Returns true if the split is mature at `datetime`.
    pub fn is_mature(&self, datetime: OffsetDateTime) -> bool {
        match self.maturity {
            SplitMaturity::Mature => true,
            SplitMaturity::Immature {
                maturation_period: time_to_maturity,
            } => {
                self.create_timestamp + time_to_maturity.as_secs() as i64
                    <= datetime.unix_timestamp()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_split(maturity: SplitMaturity) -> SplitMetadata {
        SplitMetadata {
            split_id: SplitId::new(),
            num_docs: 1,
            time_range: None,
            maturity,
            create_timestamp: 1_000,
            num_merge_ops: 0,
            footer_offsets: 0..0,
        }
    }

    #[test]
    fn an_immature_split_matures_after_its_maturation_period() {
        let split = new_split(SplitMaturity::Immature {
            maturation_period: Duration::from_secs(10),
        });
        assert!(!split.is_mature(OffsetDateTime::from_unix_timestamp(1_009).unwrap()));
        assert!(split.is_mature(OffsetDateTime::from_unix_timestamp(1_010).unwrap()));
        assert!(new_split(SplitMaturity::Mature).is_mature(OffsetDateTime::UNIX_EPOCH));
    }

    #[test]
    fn split_ids_are_ulids() {
        let split_id = SplitId::new();
        assert!(ulid::Ulid::from_string(split_id.as_str()).is_ok());
        assert_ne!(split_id, SplitId::new());
    }
}
