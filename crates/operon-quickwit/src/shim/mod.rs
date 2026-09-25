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

//! Stand-ins for the parts of `quickwit-common`, `quickwit-proto`, `quickwit-config` and
//! `quickwit-metastore` that the vendored files use.

pub mod consts;
pub mod path_hasher;
mod payload;
pub mod split_metadata;
pub mod uri;

pub use path_hasher::PathHasher;
pub use split_metadata::{SplitId, SplitMaturity, SplitMetadata};
pub use uri::{Protocol, Uri};
