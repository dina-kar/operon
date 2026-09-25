//! The link catalog (design §09 §1).

use std::collections::BTreeMap;

use operon_common::{NamespaceId, StreamId};

use super::{MAX_KEY_LEN, MetaState, refuse_reserved, validate_name};
use crate::command::{ApplyError, Reply};
use crate::types::{COLLECTION_KIND, Link, LinkId, TargetRef};

/// Most options one link may carry.
const MAX_LINK_OPTIONS: usize = 64;

impl MetaState {
    pub(super) fn create_link(
        &mut self,
        namespace: NamespaceId,
        name: String,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    ) -> Result<Reply, ApplyError> {
        validate_name("link", &name)?;
        refuse_reserved(&name)?;
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        if let Some(&id) = self.link_names.get(&(namespace, name.clone())) {
            return Err(ApplyError::LinkExists(id));
        }
        match self.streams.get(&source) {
            None => return Err(ApplyError::StreamNotFound(source)),
            Some(stream) if stream.namespace != namespace => {
                return Err(ApplyError::InvalidArgument(format!(
                    "stream {source} is not in namespace {namespace}"
                )));
            }
            Some(_) => {}
        }
        validate_name("link target kind", &target.kind)?;
        validate_name("link target name", &target.name)?;
        if target.kind == COLLECTION_KIND {
            return Err(ApplyError::InvalidArgument(
                "a collection's link is created with the collection".to_string(),
            ));
        }
        if options.len() > MAX_LINK_OPTIONS
            || options
                .iter()
                .any(|(k, v)| k.is_empty() || k.len() > MAX_KEY_LEN || v.len() > MAX_KEY_LEN)
        {
            return Err(ApplyError::InvalidArgument(format!(
                "a link takes at most {MAX_LINK_OPTIONS} options, with keys of 1..={MAX_KEY_LEN} \
                 bytes and values of at most {MAX_KEY_LEN} bytes"
            )));
        }
        let id = self.insert_link(namespace, name, source, target, options);
        Ok(Reply::LinkCreated(id))
    }

    /// Adds a link, validated by the caller (the namespace and source exist
    /// and the name is free).
    pub(super) fn insert_link(
        &mut self,
        namespace: NamespaceId,
        name: String,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    ) -> LinkId {
        self.last_link_id += 1;
        let id = LinkId(self.last_link_id);
        self.link_names.insert((namespace, name.clone()), id);
        self.links.insert(
            id,
            Link {
                id,
                namespace,
                name,
                source,
                target,
                options,
            },
        );
        id
    }

    /// Removes a link and its name.
    pub(super) fn remove_link(&mut self, id: LinkId) {
        if let Some(link) = self.links.remove(&id) {
            self.link_names.remove(&(link.namespace, link.name));
        }
    }

    /// Looks up a link by id.
    pub fn link(&self, id: LinkId) -> Option<&Link> {
        self.links.get(&id)
    }

    /// Looks up a link by namespace and name.
    pub fn link_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Link> {
        self.link_names
            .get(&(namespace, name.to_string()))
            .and_then(|id| self.links.get(id))
    }

    /// The links of one namespace, in id order.
    pub fn links(&self, namespace: NamespaceId) -> impl Iterator<Item = &Link> {
        self.links
            .values()
            .filter(move |link| link.namespace == namespace)
    }

    /// Every link, in id order.
    pub fn all_links(&self) -> impl Iterator<Item = &Link> {
        self.links.values()
    }
}
