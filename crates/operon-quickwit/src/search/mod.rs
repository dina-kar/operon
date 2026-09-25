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

//! The parts of `quickwit-search` Operon uses: the async warmup that must run before a
//! synchronous Tantivy search over a `StorageDirectory`, and the merge of per-split
//! intermediate aggregation results.

mod aggregation_merge;
mod warmup;

pub use self::aggregation_merge::merge_intermediate_aggregation_result;
pub use self::warmup::warmup;
