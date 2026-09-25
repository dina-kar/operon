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

//! Quickwit's storage abstraction (`quickwit-storage`): the async [`Storage`] trait, RAM,
//! local-file and prefix storages, the split bundle and its footer, and the byte-range cache.

// As in `quickwit-storage/src/lib.rs`.
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::len_without_is_empty)]

mod bundle_storage;
mod byte_range_cache;
mod error;
mod local_file_storage;
mod payload;
mod prefix_storage;
mod ram_storage;
mod split;
#[allow(clippy::module_inception)]
mod storage;
mod versioned_component;

pub use tantivy::directory::OwnedBytes;

pub use self::bundle_storage::{
    BundleFileRanges, BundleStorage, locate_split_footer_range, strip_split_footer_trailer,
};
pub use self::byte_range_cache::{ByteRangeCache, FileByteRangeCache};
pub use self::error::{
    BulkDeleteError, DeleteFailure, StorageError, StorageErrorKind, StorageResolverError,
    StorageResult,
};
pub use self::local_file_storage::LocalFileStorage;
pub use self::payload::{PutPayload, PutPayloadClone};
pub use self::prefix_storage::add_prefix_to_storage;
pub use self::ram_storage::{RamStorage, RamStorageBuilder};
pub use self::split::{SplitPayload, SplitPayloadBuilder};
pub use self::storage::{ListObjectsStream, ObjectMetadata, SendableAsync, Storage};
pub use self::versioned_component::VersionedComponent;
