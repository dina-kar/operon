//! The collection catalog: collections with their implicit streams and links,
//! aliases, and schema evolution (M1 overview §6.1).

use std::collections::BTreeMap;

use operon_common::meta::{
    AliasAction, ApplyError, COLLECTION_KIND, Collection, LinkId, MAX_COLLECTION_NAME_LEN,
    Retention, TargetRef, WalClass, collection_pk_prefix, collection_pointer_key,
    collection_prefix, implicit_name,
};
use operon_common::schema::{CollectionSchema, SchemaError};
use operon_common::{CollectionId, NamespaceId};

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
        if self.aliases.contains_key(&key) {
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
            match target {
                Some(id) => self.aliases.insert((namespace, alias), id),
                None => self.aliases.remove(&(namespace, alias)),
            };
        }
        Ok(Reply::AliasesUpdated)
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
    /// through an alias.
    pub fn resolve_collection(
        &self,
        namespace: NamespaceId,
        name_or_alias: &str,
    ) -> Option<&Collection> {
        let key = (namespace, name_or_alias.to_string());
        self.collection_names
            .get(&key)
            .or_else(|| self.aliases.get(&key))
            .and_then(|id| self.collections.get(id))
    }

    /// The aliases of one namespace with the collections they point at, in
    /// alias name order.
    pub fn aliases(&self, namespace: NamespaceId) -> impl Iterator<Item = (&str, CollectionId)> {
        self.aliases
            .range((namespace, String::new())..)
            .take_while(move |((ns, _), _)| *ns == namespace)
            .map(|((_, alias), id)| (alias.as_str(), *id))
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
