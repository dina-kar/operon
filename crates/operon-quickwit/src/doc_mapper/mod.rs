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

//! The parts of `quickwit-doc-mapper` Operon uses: the query builder, which turns a
//! [`QueryAst`](crate::query::query_ast::QueryAst) into a Tantivy query plus the
//! [`WarmupInfo`] it needs, and its error types.

mod error;
mod query_builder;
mod warmup;

pub use self::error::{DocParsingError, QueryParserError};
pub use self::query_builder::build_query;
pub use self::warmup::{Automaton, FastFieldWarmupInfo, TermRange, WarmupInfo};
