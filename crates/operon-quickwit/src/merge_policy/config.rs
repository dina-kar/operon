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
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-config/src/merge_policy_config.rs); modified for Operon: only StableLogMergePolicyConfig kept; utoipa derives removed.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(deny_unknown_fields)]
pub struct StableLogMergePolicyConfig {
    /// Number of docs below which all splits are considered as belonging to the same level.
    #[serde(default = "default_min_level_num_docs")]
    pub min_level_num_docs: usize,
    /// Number of splits to merge together in a single merge operation.
    #[serde(default = "default_merge_factor")]
    pub merge_factor: usize,
    /// Maximum number of splits that can be merged together in a single merge operation.
    #[serde(default = "default_max_merge_factor")]
    pub max_merge_factor: usize,
    /// Duration relative to `split.created_timestamp` after which a split
    /// becomes mature.
    /// If `now() >= split.created_timestamp + maturation_period` then
    /// the split is mature.
    #[serde(default = "default_maturation_period")]
    #[serde(deserialize_with = "parse_human_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub maturation_period: Duration,
}

fn default_merge_factor() -> usize {
    10
}

fn default_max_merge_factor() -> usize {
    12
}

fn default_min_level_num_docs() -> usize {
    100_000
}

fn default_maturation_period() -> Duration {
    Duration::from_hours(48)
}

impl Default for StableLogMergePolicyConfig {
    fn default() -> Self {
        StableLogMergePolicyConfig {
            min_level_num_docs: default_min_level_num_docs(),
            merge_factor: default_merge_factor(),
            max_merge_factor: default_max_merge_factor(),
            maturation_period: default_maturation_period(),
        }
    }
}

fn parse_human_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value: String = Deserialize::deserialize(deserializer)?;
    let duration = humantime::parse_duration(&value).map_err(|error| {
        de::Error::custom(format!(
            "failed to parse human-readable duration `{value}`: {error:?}",
        ))
    })?;
    Ok(duration)
}

fn serialize_duration<S>(value: &Duration, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let value_str = humantime::format_duration(*value).to_string();
    s.serialize_str(&value_str)
}
