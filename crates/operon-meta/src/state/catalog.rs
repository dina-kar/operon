//! Namespaces and streams.

use operon_common::meta::{ApplyError, MAX_PARTITIONS, Namespace, Retention, Stream, WalClass};
use operon_common::{NamespaceId, StreamId};

use super::{MetaState, refuse_reserved, validate_name};
use crate::command::Reply;
use crate::types::PartitionState;

impl MetaState {
    pub(super) fn create_namespace(&mut self, name: String) -> Result<Reply, ApplyError> {
        validate_name("namespace", &name)?;
        if let Some(&id) = self.namespace_names.get(&name) {
            return Err(ApplyError::NamespaceExists(id));
        }
        self.last_namespace_id += 1;
        let id = NamespaceId(self.last_namespace_id);
        self.namespace_names.insert(name.clone(), id);
        self.namespaces.insert(id, Namespace { id, name });
        Ok(Reply::NamespaceCreated(id))
    }

    pub(super) fn create_stream(
        &mut self,
        namespace: NamespaceId,
        name: String,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> Result<Reply, ApplyError> {
        validate_name("stream", &name)?;
        refuse_reserved(&name)?;
        check_partitions(partitions)?;
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        if let Some(&id) = self.stream_names.get(&(namespace, name.clone())) {
            return Err(ApplyError::StreamExists(id));
        }
        let id = self.insert_stream(namespace, name, partitions, class, retention);
        Ok(Reply::StreamCreated(id))
    }

    /// Adds a stream, validated by the caller (the namespace exists and the
    /// name is free), with `partitions` empty partitions.
    pub(super) fn insert_stream(
        &mut self,
        namespace: NamespaceId,
        name: String,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    ) -> StreamId {
        self.last_stream_id += 1;
        let id = StreamId(self.last_stream_id);
        for partition in 0..partitions {
            self.partitions
                .insert((id, partition), PartitionState::default());
        }
        self.stream_names.insert((namespace, name.clone()), id);
        self.streams.insert(
            id,
            Stream {
                id,
                namespace,
                name,
                partitions,
                class,
                retention,
            },
        );
        id
    }

    /// Removes a stream, its name and its partitions. Every index entry goes
    /// through `release_entry`, so objects no other entry references retire.
    pub(super) fn remove_stream(&mut self, id: StreamId) {
        let Some(stream) = self.streams.remove(&id) else {
            return;
        };
        self.stream_names.remove(&(stream.namespace, stream.name));
        for partition in 0..stream.partitions {
            let Some(state) = self.partitions.remove(&(id, partition)) else {
                continue;
            };
            for entry in state.index.into_values() {
                self.release_entry(entry);
            }
        }
    }

    /// Looks up a namespace by id.
    pub fn namespace(&self, id: NamespaceId) -> Option<&Namespace> {
        self.namespaces.get(&id)
    }

    /// Looks up a namespace by name.
    pub fn namespace_by_name(&self, name: &str) -> Option<&Namespace> {
        self.namespace_names
            .get(name)
            .and_then(|id| self.namespaces.get(id))
    }

    /// All namespaces, in id order.
    pub fn namespaces(&self) -> impl Iterator<Item = &Namespace> {
        self.namespaces.values()
    }

    /// Looks up a stream by id.
    pub fn stream(&self, id: StreamId) -> Option<&Stream> {
        self.streams.get(&id)
    }

    /// Looks up a stream by namespace and name.
    pub fn stream_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Stream> {
        self.stream_names
            .get(&(namespace, name.to_string()))
            .and_then(|id| self.streams.get(id))
    }

    /// The streams of one namespace, in id order.
    pub fn streams(&self, namespace: NamespaceId) -> impl Iterator<Item = &Stream> {
        self.streams
            .values()
            .filter(move |s| s.namespace == namespace)
    }

    /// Every stream, in id order.
    pub fn all_streams(&self) -> impl Iterator<Item = &Stream> {
        self.streams.values()
    }

    /// Sequencer state of one partition, if the stream and partition exist.
    pub fn partition(&self, stream: StreamId, partition: u32) -> Option<&PartitionState> {
        self.partitions.get(&(stream, partition))
    }
}

/// A stream has 1..=[`MAX_PARTITIONS`] partitions.
pub(super) fn check_partitions(partitions: u32) -> Result<(), ApplyError> {
    if !(1..=MAX_PARTITIONS).contains(&partitions) {
        return Err(ApplyError::InvalidArgument(format!(
            "partitions must be 1..={MAX_PARTITIONS}, got {partitions}"
        )));
    }
    Ok(())
}
