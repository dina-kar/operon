//! Per-collection hot configuration (M1.3 Task 4, Rulings 7 and 20) and the
//! lease prefix query (Ruling 12).

use std::collections::BTreeMap;
use std::ops::Bound;

use operon_common::CollectionId;
use operon_common::meta::{ApplyError, HotConfig, Lease};

use super::MetaState;
use crate::command::Reply;

impl MetaState {
    pub(super) fn set_collection_hot(
        &mut self,
        collection: CollectionId,
        hot: HotConfig,
    ) -> Result<Reply, ApplyError> {
        if !self.collections.contains_key(&collection) {
            return Err(ApplyError::CollectionNotFound(collection));
        }
        match hot.any() {
            true => self.collection_hot.insert(collection, hot),
            false => self.collection_hot.remove(&collection),
        };
        Ok(Reply::CollectionHotSet)
    }

    /// The hot configuration of collection `id`: the default (all false)
    /// when none is set.
    pub fn collection_hot(&self, id: CollectionId) -> HotConfig {
        self.collection_hot.get(&id).copied().unwrap_or_default()
    }

    /// Every collection with a hot configuration, by id (only non-default
    /// ones are stored).
    pub fn hot_collections(&self) -> impl Iterator<Item = (CollectionId, HotConfig)> + '_ {
        self.collection_hot.iter().map(|(id, hot)| (*id, *hot))
    }

    /// Every lease whose key starts with `prefix`, in key order, released
    /// and expired ones included (a read-only query, Ruling 12).
    pub fn leases_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a Lease)> {
        self.leases
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, lease)| (key.as_str(), lease))
    }

    /// The hot configuration map, for the snapshot codec.
    pub(crate) fn collection_hot_map(&self) -> &BTreeMap<CollectionId, HotConfig> {
        &self.collection_hot
    }

    /// Replaces the hot configuration map (the snapshot codec's decode).
    pub(crate) fn set_collection_hot_map(&mut self, map: BTreeMap<CollectionId, HotConfig>) {
        self.collection_hot = map;
    }
}
