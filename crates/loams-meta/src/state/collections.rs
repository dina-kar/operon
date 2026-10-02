//! The collection catalog: collections with their implicit streams and links,
//! aliases, and schema evolution (M1 overview §6.1).

use std::collections::BTreeMap;

use loams_common::meta::{
    AliasAction, AliasTargetAction, AliasTargets, ApplyError, COLLECTION_KIND, Collection, LinkId,
    MAX_ALIAS_TARGETS, MAX_COLLECTION_NAME_LEN, NameTarget, Retention, TargetRef, WalClass,
    collection_pk_prefix, collection_pointer_key, collection_prefix, implicit_name,
};
use loams_common::schema::{CollectionSchema, SchemaError};
use loams_common::{CollectionId, NamespaceId};

use super::catalog::check_partitions;
use super::{MetaState, refuse_reserved, validate_name};
use crate::command::Reply;

/// Most actions one `UpdateAliases` may carry.
const MAX_ALIAS_ACTIONS: usize = 100;

fn schema_message(err: SchemaError) -> String {
    match err {
        SchemaError::Invalid(message) | SchemaError::Incompatible(message) => message,
    }
}

impl MetaState {
    pub(super) fn create_collection(
        &mut self,
        namespace: NamespaceId,
        name: String,
        schema: CollectionSchema,
        partitions: u32,
    ) -> Result<Reply, ApplyError> {
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        validate_name("collection", &name)?;
        if name.len() > MAX_COLLECTION_NAME_LEN {
            return Err(ApplyError::InvalidArgument(format!(
                "a collection name is at most {MAX_COLLECTION_NAME_LEN} bytes, got {}",
                name.len()
            )));
        }
        refuse_reserved(&name)?;
        check_partitions(partitions)?;
        schema
            .validate()
            .map_err(|e| ApplyError::InvalidArgument(e.to_string()))?;
        if schema.version != 1 {
            return Err(ApplyError::InvalidArgument(format!(
                "a new collection's schema is at version 1, got {}",
                schema.version
            )));
        }
        let key = (namespace, name);
        if let Some(existing) = self
            .collection_names
            .get(&key)
            .and_then(|id| self.collections.get(id))
        {
            return Err(
                if existing.schema == schema && existing.partitions == partitions {
                    ApplyError::CollectionExists(existing.id)
                } else {
                    ApplyError::NameTaken(key.1)
                },
            );
        }
        if self.aliases.contains_key(&key) || self.alias_targets.contains_key(&key) {
            return Err(ApplyError::NameTaken(key.1));
        }

        // Everything is checked: apply.
        let (namespace, name) = key;
        let id = CollectionId(self.last_collection_id + 1);
        self.last_collection_id = id.0;
        let implicit = implicit_name(&name, id);
        let stream = self.insert_stream(
            namespace,
            implicit.clone(),
            partitions,
            WalClass::Standard,
            Retention::default(),
        );
        let link = self.insert_link(
            namespace,
            implicit,
            stream,
            TargetRef {
                kind: COLLECTION_KIND.to_string(),
                name: name.clone(),
            },
            BTreeMap::new(),
        );
        self.collection_names.insert((namespace, name.clone()), id);
        self.collections.insert(
            id,
            Collection {
                id,
                namespace,
                name,
                schema,
                partitions,
                stream,
                link,
            },
        );
        Ok(Reply::CollectionCreated { id, stream, link })
    }

    pub(super) fn drop_collection(
        &mut self,
        namespace: NamespaceId,
        name: String,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        self.clock_ms = self.clock_ms.max(now_ms);
        let Some(id) = self.collection_names.remove(&(namespace, name)) else {
            return Ok(Reply::CollectionDropped(None));
        };
        let Some(collection) = self.collections.remove(&id) else {
            // `collection_names` and `collections` agree (an invariant).
            return Ok(Reply::CollectionDropped(None));
        };
        self.aliases.retain(|_, target| *target != id);
        // Rule 3: leave every alias with several members, then put each
        // changed one in its canonical map.
        let changed: Vec<(NamespaceId, String)> = self
            .alias_targets
            .iter()
            .filter(|(_, targets)| targets.members.contains_key(&id))
            .map(|(key, _)| key.clone())
            .collect();
        for (ns, alias) in changed {
            let mut members = self.alias_members(ns, &alias);
            members.remove(&id);
            self.put_alias(ns, alias, members);
        }
        self.collection_hot.remove(&id);
        self.remove_stream(collection.stream);
        self.remove_link(collection.link);
        self.pointers
            .remove(&(namespace, collection_pointer_key(id)));
        for prefix in [
            collection_prefix(namespace, id),
            collection_pk_prefix(namespace, id),
        ] {
            self.retired.insert(prefix, self.clock_ms);
        }
        Ok(Reply::CollectionDropped(Some(id)))
    }

    pub(super) fn update_collection_schema(
        &mut self,
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    ) -> Result<Reply, ApplyError> {
        let current = &self
            .collections
            .get(&collection)
            .ok_or(ApplyError::CollectionNotFound(collection))?
            .schema;
        schema
            .validate()
            .map_err(|e| ApplyError::InvalidArgument(e.to_string()))?;
        // A retry of an update that was applied.
        if expected_version.checked_add(1) == Some(current.version)
            && current.same_ignoring_version(&schema)
        {
            return Ok(Reply::SchemaUpdated {
                version: current.version,
            });
        }
        if current.version != expected_version {
            return Err(ApplyError::SchemaVersionMismatch {
                collection,
                current: current.version,
            });
        }
        current
            .check_additive(&schema)
            .map_err(|e| ApplyError::IncompatibleSchema(schema_message(e)))?;
        let version = expected_version.checked_add(1).ok_or_else(|| {
            ApplyError::InvalidArgument("the schema version cannot grow past u64::MAX".to_string())
        })?;
        let stored = &mut self
            .collections
            .get_mut(&collection)
            .ok_or(ApplyError::CollectionNotFound(collection))?
            .schema;
        *stored = CollectionSchema { version, ..schema };
        Ok(Reply::SchemaUpdated { version })
    }

    pub(super) fn update_aliases(
        &mut self,
        namespace: NamespaceId,
        actions: Vec<AliasAction>,
    ) -> Result<Reply, ApplyError> {
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        if !(1..=MAX_ALIAS_ACTIONS).contains(&actions.len()) {
            return Err(ApplyError::InvalidArgument(format!(
                "an alias update takes 1..={MAX_ALIAS_ACTIONS} actions, got {}",
                actions.len()
            )));
        }
        // The actions' effect, applied only once every action succeeded:
        // alias → its new target, or `None` to remove it.
        let mut changes: BTreeMap<String, Option<CollectionId>> = BTreeMap::new();
        for action in actions {
            match action {
                AliasAction::Create { alias, collection } => {
                    validate_name("alias", &alias)?;
                    refuse_reserved(&alias)?;
                    if self
                        .collection_names
                        .contains_key(&(namespace, alias.clone()))
                    {
                        return Err(ApplyError::NameTaken(alias));
                    }
                    let Some(&id) = self.collection_names.get(&(namespace, collection.clone()))
                    else {
                        return Err(ApplyError::UnknownCollection(collection));
                    };
                    changes.insert(alias, Some(id));
                }
                AliasAction::Delete { alias } => {
                    changes.insert(alias, None);
                }
            }
        }
        for (alias, target) in changes {
            // Either map may hold the alias (M1.5 Task 0a rule 3): `Create`
            // re-points it to exactly one collection, `Delete` removes it.
            let key = (namespace, alias);
            self.alias_targets.remove(&key);
            match target {
                Some(id) => self.aliases.insert(key, id),
                None => self.aliases.remove(&key),
            };
        }
        Ok(Reply::AliasesUpdated)
    }

    /// M1.5 Task 0a rule 2: alias-target actions over a working copy of
    /// each touched alias, checked, then applied at once.
    pub(super) fn update_alias_targets(
        &mut self,
        namespace: NamespaceId,
        actions: Vec<AliasTargetAction>,
    ) -> Result<Reply, ApplyError> {
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        if !(1..=MAX_ALIAS_ACTIONS).contains(&actions.len()) {
            return Err(ApplyError::InvalidArgument(format!(
                "an alias update takes 1..={MAX_ALIAS_ACTIONS} actions, got {}",
                actions.len()
            )));
        }
        let mut working: BTreeMap<String, BTreeMap<CollectionId, Option<bool>>> = BTreeMap::new();
        for action in actions {
            match action {
                AliasTargetAction::Add {
                    alias,
                    collection,
                    is_write_index,
                } => {
                    validate_name("alias", &alias)?;
                    refuse_reserved(&alias)?;
                    if self
                        .collection_names
                        .contains_key(&(namespace, alias.clone()))
                    {
                        return Err(ApplyError::NameTaken(alias));
                    }
                    let Some(&id) = self.collection_names.get(&(namespace, collection.clone()))
                    else {
                        return Err(ApplyError::UnknownCollection(collection));
                    };
                    let members = self.working_members(&mut working, namespace, alias);
                    members.insert(id, is_write_index);
                }
                AliasTargetAction::Remove { alias, collection } => {
                    let id = self.collection_names.get(&(namespace, collection)).copied();
                    let members = self.working_members(&mut working, namespace, alias);
                    if let Some(id) = id {
                        members.remove(&id);
                    }
                }
                AliasTargetAction::RemoveAlias { alias } => {
                    self.working_members(&mut working, namespace, alias).clear();
                }
            }
        }
        for (alias, members) in &working {
            if members.len() > MAX_ALIAS_TARGETS {
                return Err(ApplyError::InvalidArgument(format!(
                    "alias [{alias}] would name {} collections; the limit is {MAX_ALIAS_TARGETS}",
                    members.len()
                )));
            }
            let mut writers: Vec<&str> = members
                .iter()
                .filter(|(_, w)| **w == Some(true))
                .filter_map(|(id, _)| self.collections.get(id).map(|c| c.name.as_str()))
                .collect();
            if writers.len() > 1 {
                writers.sort_unstable();
                return Err(ApplyError::InvalidArgument(format!(
                    "alias [{alias}] has more than one write index [{}]",
                    writers.join(",")
                )));
            }
        }
        for (alias, members) in working {
            self.put_alias(namespace, alias, members);
        }
        Ok(Reply::AliasesUpdated)
    }

    /// The working copy of `alias`'s members, seeded from the state the
    /// first time the command touches it.
    fn working_members<'w>(
        &self,
        working: &'w mut BTreeMap<String, BTreeMap<CollectionId, Option<bool>>>,
        namespace: NamespaceId,
        alias: String,
    ) -> &'w mut BTreeMap<CollectionId, Option<bool>> {
        working
            .entry(alias)
            .or_insert_with_key(|alias| self.alias_members(namespace, alias))
    }

    /// The members of `alias` from either map (an M1.1 alias is one unset
    /// member); empty when there is no such alias.
    fn alias_members(
        &self,
        namespace: NamespaceId,
        alias: &str,
    ) -> BTreeMap<CollectionId, Option<bool>> {
        let key = (namespace, alias.to_string());
        if let Some(id) = self.aliases.get(&key) {
            return BTreeMap::from([(*id, None)]);
        }
        self.alias_targets
            .get(&key)
            .map(|targets| targets.members.clone())
            .unwrap_or_default()
    }

    /// Puts `alias` in its canonical map (M1.5 Task 0a rule 1): none when
    /// `members` is empty, the M1.1 map when it is exactly one unset
    /// member, `alias_targets` otherwise.
    fn put_alias(
        &mut self,
        namespace: NamespaceId,
        alias: String,
        members: BTreeMap<CollectionId, Option<bool>>,
    ) {
        let key = (namespace, alias);
        self.aliases.remove(&key);
        self.alias_targets.remove(&key);
        let mut iter = members.iter();
        match (iter.next(), iter.next()) {
            (None, _) => {}
            (Some((id, None)), None) => {
                self.aliases.insert(key, *id);
            }
            _ => {
                self.alias_targets.insert(key, AliasTargets { members });
            }
        }
    }

    /// Looks up a collection by id.
    pub fn collection(&self, id: CollectionId) -> Option<&Collection> {
        self.collections.get(&id)
    }

    /// Looks up a collection by namespace and name (not an alias).
    pub fn collection_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Collection> {
        self.collection_names
            .get(&(namespace, name.to_string()))
            .and_then(|id| self.collections.get(id))
    }

    /// The collections of one namespace, in id order.
    pub fn collections(&self, namespace: NamespaceId) -> impl Iterator<Item = &Collection> {
        self.collections
            .values()
            .filter(move |c| c.namespace == namespace)
    }

    /// Every collection, in id order.
    pub fn all_collections(&self) -> impl Iterator<Item = &Collection> {
        self.collections.values()
    }

    /// The collection named `name_or_alias` in `namespace`, directly or
    /// through an alias with exactly one member; `None` for an alias with
    /// several members (M1.5 Task 0a).
    pub fn resolve_collection(
        &self,
        namespace: NamespaceId,
        name_or_alias: &str,
    ) -> Option<&Collection> {
        let key = (namespace, name_or_alias.to_string());
        self.collection_names
            .get(&key)
            .or_else(|| self.aliases.get(&key))
            .or_else(|| {
                let targets = self.alias_targets.get(&key)?;
                let mut members = targets.members.keys();
                match (members.next(), members.next()) {
                    (Some(id), None) => Some(id),
                    _ => None,
                }
            })
            .and_then(|id| self.collections.get(id))
    }

    /// What `name` names in `namespace`: a collection, or an alias of
    /// either map with its member records; `None` if neither.
    pub fn resolve_name(&self, namespace: NamespaceId, name: &str) -> Option<NameTarget> {
        if let Some(collection) = self.collection_by_name(namespace, name) {
            return Some(NameTarget::Collection(collection.clone()));
        }
        let members = self.alias_members(namespace, name);
        if members.is_empty() {
            return None;
        }
        let targets = AliasTargets { members };
        Some(NameTarget::Alias {
            write_target: targets.write_target(),
            members: targets
                .members
                .iter()
                .filter_map(|(id, w)| self.collections.get(id).map(|c| (c.clone(), *w)))
                .collect(),
        })
    }

    /// The aliases of one namespace with the collections they point at: one
    /// pair per member, from both maps, by alias name and then collection
    /// id.
    pub fn aliases(&self, namespace: NamespaceId) -> impl Iterator<Item = (&str, CollectionId)> {
        let mut pairs: Vec<(&str, CollectionId)> = self
            .aliases
            .range((namespace, String::new())..)
            .take_while(|((ns, _), _)| *ns == namespace)
            .map(|((_, alias), id)| (alias.as_str(), *id))
            .collect();
        pairs.extend(
            self.alias_targets
                .range((namespace, String::new())..)
                .take_while(|((ns, _), _)| *ns == namespace)
                .flat_map(|((_, alias), targets)| {
                    targets.members.keys().map(|id| (alias.as_str(), *id))
                }),
        );
        pairs.sort_unstable();
        pairs.into_iter()
    }

    /// Every alias of one namespace with its members, by alias name; an
    /// alias of the M1.1 map is one unset member.
    pub fn alias_targets(
        &self,
        namespace: NamespaceId,
    ) -> impl Iterator<Item = (&str, AliasTargets)> {
        let mut out: Vec<(&str, AliasTargets)> = self
            .aliases
            .range((namespace, String::new())..)
            .take_while(|((ns, _), _)| *ns == namespace)
            .map(|((_, alias), id)| {
                (
                    alias.as_str(),
                    AliasTargets {
                        members: BTreeMap::from([(*id, None)]),
                    },
                )
            })
            .collect();
        out.extend(
            self.alias_targets
                .range((namespace, String::new())..)
                .take_while(|((ns, _), _)| *ns == namespace)
                .map(|((_, alias), targets)| (alias.as_str(), targets.clone())),
        );
        out.sort_unstable_by(|a, b| a.0.cmp(b.0));
        out.into_iter()
    }

    /// The multi-target alias map, for the snapshot codec.
    pub(crate) fn alias_targets_map(&self) -> &BTreeMap<(NamespaceId, String), AliasTargets> {
        &self.alias_targets
    }

    /// Replaces the multi-target alias map (the snapshot codec's decode).
    pub(crate) fn set_alias_targets_map(
        &mut self,
        map: BTreeMap<(NamespaceId, String), AliasTargets>,
    ) {
        self.alias_targets = map;
    }

    /// The collection whose implicit link is `link`.
    pub fn collection_for_link(&self, link: LinkId) -> Option<&Collection> {
        let link = self.links.get(&link)?;
        if link.target.kind != COLLECTION_KIND {
            return None;
        }
        self.collection_by_name(link.namespace, &link.target.name)
            .filter(|c| c.link == link.id)
    }
}
