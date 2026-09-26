//! `ConsistencyToken` as its text form (`v1:s7/p3@918274`).

use operon_collection::ConsistencyToken;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S: Serializer>(token: &ConsistencyToken, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(token)
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ConsistencyToken, D::Error> {
    let text = String::deserialize(d)?;
    text.parse().map_err(D::Error::custom)
}

/// `Option<ConsistencyToken>`; `None` is `null`.
pub mod opt {
    use super::*;

    pub fn serialize<S: Serializer>(
        token: &Option<ConsistencyToken>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match token {
            Some(token) => s.collect_str(token),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<ConsistencyToken>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|text| text.parse().map_err(D::Error::custom))
            .transpose()
    }
}
