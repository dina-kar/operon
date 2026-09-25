// Copyright 2021-Present Datadog, Inc.
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
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-search/src/collector.rs lines 862-914); modified for Operon: tantivy Aggregations in place of QuickwitAggregations, FindTraceIdsAggregation arm and pruning removed; made pub; imports added.

use tantivy::TantivyError;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;

fn map_error(error: postcard::Error) -> TantivyError {
    TantivyError::InternalError(format!(
        "failed to merge intermediate aggregation results: Postcard error: {error}"
    ))
}

/// Merges a set of Leaf Results.
pub fn merge_intermediate_aggregation_result<'a>(
    aggregations_opt: &Option<Aggregations>,
    intermediate_aggregation_results: impl Iterator<Item = &'a [u8]>,
) -> tantivy::Result<Option<Vec<u8>>> {
    let merged_intermediate_aggregation_result = match aggregations_opt {
        Some(_aggregations) => {
            let merged_opt = intermediate_aggregation_results
                .map(|bytes| postcard::from_bytes(bytes).map_err(map_error))
                .try_fold::<_, _, Result<_, TantivyError>>(
                    None,
                    |acc: Option<IntermediateAggregationResults>, fruits_res| {
                        let fruits = fruits_res?;
                        match acc {
                            Some(mut merged_fruits) => {
                                merged_fruits.merge_fruits(fruits)?;
                                Ok(Some(merged_fruits))
                            }
                            None => Ok(Some(fruits)),
                        }
                    },
                )?;
            let merged = merged_opt.unwrap_or_default();
            let serialized = postcard::to_allocvec(&merged).map_err(map_error)?;
            Some(serialized)
        }
        None => None,
    };

    Ok(merged_intermediate_aggregation_result)
}
