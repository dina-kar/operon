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

//! Constants and small helpers from `quickwit-common` and `quickwit-config`.
//!
//! `chunk_range`, `ignore_error_kind!` and `true_fn` are copied from
//! quickwit-oss/quickwit af0591a3 (`quickwit-common/src/lib.rs`, Copyright 2021-Present
//! Datadog, Inc., Apache-2.0).

use std::ops::Range;

/// Name of the field that records which fields a document has (`quickwit-common`
/// `shared_consts`).
pub const FIELD_PRESENCE_FIELD_NAME: &str = "_field_presence";

/// Name of the split-fields file inside a split bundle (`quickwit-common` `shared_consts`).
pub const SPLIT_FIELDS_FILE_NAME: &str = "split_fields";

/// Name of the recovery-metadata file inside a split bundle (`quickwit-common`
/// `shared_consts`).
pub const SPLIT_RECOVERY_METADATA_FILE_NAME: &str = "split_recovery_metadata";

/// `IndexingSettings::default_split_num_docs_target()` (`quickwit-config`).
pub const DEFAULT_SPLIT_NUM_DOCS_TARGET: usize = 10_000_000;

/// Splits `range` into consecutive chunks of at most `chunk_size` elements.
pub fn chunk_range(range: Range<usize>, chunk_size: usize) -> impl Iterator<Item = Range<usize>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + chunk_size).min(range.end);
        block_start..block_end
    })
}

/// Evaluates a `Result` whose error has a `kind()`, turning an error of kind `$kind` into
/// `Ok(())`.
macro_rules! ignore_error_kind {
    ($kind:path, $expr:expr) => {
        match $expr {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == $kind => Ok(()),
            Err(error) => Err(error),
        }
    };
}
pub(crate) use ignore_error_kind;

/// Returns true at compile time. This function is mostly used with serde to initialize boolean
/// fields to true.
pub const fn true_fn() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_range_covers_the_range_in_order() {
        assert_eq!(
            chunk_range(0..10, 4).collect::<Vec<_>>(),
            vec![0..4, 4..8, 8..10]
        );
        assert_eq!(chunk_range(3..3, 4).count(), 0);
    }

    #[test]
    fn ignore_error_kind_ignores_only_that_kind() {
        use std::io::{Error, ErrorKind};
        let not_found: Result<(), Error> = Err(Error::from(ErrorKind::NotFound));
        assert!(ignore_error_kind!(ErrorKind::NotFound, not_found).is_ok());
        let other: Result<(), Error> = Err(Error::from(ErrorKind::Other));
        assert!(ignore_error_kind!(ErrorKind::NotFound, other).is_err());
    }
}
