//! Namespaces and streams.

use operon_common::{NamespaceId, StreamId};

use super::{MAX_PARTITIONS, MetaState, validate_name};
use crate::command::{ApplyError, Reply};
use crate::types::{Namespace, PartitionState, Stream, WalClass};

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
    ) -> Result<Reply, ApplyError> {
        validate_name("stream", &name)?;
        if !(1..=MAX_PARTITIONS).contains(&partitions) {
            return Err(ApplyError::InvalidArgument(format!(
                "partitions must be 1..={MAX_PARTITIONS}, got {partitions}"
            )));
        }
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        let key = (namespace, name);
        if let Some(&id) = self.stream_names.get(&key) {
            return Err(ApplyError::StreamExists(id));
        }
        self.last_stream_id += 1;
        let id = StreamId(self.last_stream_id);
        for partition in 0..partitions {
            self.partitions
                .insert((id, partition), PartitionState::default());
        }
        let (namespace, name) = key.clone();
        self.stream_names.insert(key, id);
        self.streams.insert(
            id,
            Stream {
                id,
                namespace,
                name,
                partitions,
                class,
            },
        );
        Ok(Reply::StreamCreated(id))
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

    /// Sequencer state of one partition, if the stream and partition exist.
    pub fn partition(&self, stream: StreamId, partition: u32) -> Option<&PartitionState> {
        self.partitions.get(&(stream, partition))
    }
}
