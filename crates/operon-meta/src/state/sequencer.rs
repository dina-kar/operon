//! The stream sequencer: dense offsets and the offset index (design §02 §3).

use super::{MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{IndexEntry, WalChunk};

impl MetaState {
    pub(super) fn commit_wal(
        &mut self,
        object: String,
        chunks: Vec<WalChunk>,
    ) -> Result<Reply, ApplyError> {
        validate_key("WAL object path", &object)?;
        if let Some(base_offsets) = self.wal_commits.get(&object) {
            return Ok(Reply::WalCommitted {
                base_offsets: base_offsets.clone(),
            });
        }
        if chunks.is_empty() {
            return Err(ApplyError::InvalidArgument(
                "a WAL commit needs at least one chunk".to_string(),
            ));
        }
        // Validate every chunk before changing anything, so a commit is all or nothing.
        for chunk in &chunks {
            self.validate_chunk(chunk)?;
        }

        let mut base_offsets = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            // Present: `validate_chunk` checked every chunk above.
            let partition = self
                .partitions
                .get_mut(&(chunk.stream, chunk.partition))
                .ok_or(ApplyError::PartitionNotFound {
                    stream: chunk.stream,
                    partition: chunk.partition,
                })?;
            let base_offset = partition.next_offset;
            partition.next_offset += u64::from(chunk.records);
            partition.index.insert(
                base_offset,
                IndexEntry {
                    base_offset,
                    records: chunk.records,
                    object: object.clone(),
                    byte_range: chunk.byte_range,
                    max_timestamp_ms: chunk.max_timestamp_ms,
                },
            );
            base_offsets.push(base_offset);
        }
        self.wal_commits.insert(object, base_offsets.clone());
        Ok(Reply::WalCommitted { base_offsets })
    }

    fn validate_chunk(&self, chunk: &WalChunk) -> Result<(), ApplyError> {
        if !self.streams.contains_key(&chunk.stream) {
            return Err(ApplyError::StreamNotFound(chunk.stream));
        }
        if !self
            .partitions
            .contains_key(&(chunk.stream, chunk.partition))
        {
            return Err(ApplyError::PartitionNotFound {
                stream: chunk.stream,
                partition: chunk.partition,
            });
        }
        if chunk.records == 0 {
            return Err(ApplyError::InvalidArgument(
                "a WAL chunk needs at least one record".to_string(),
            ));
        }
        if chunk.byte_range.start >= chunk.byte_range.end {
            return Err(ApplyError::InvalidArgument(format!(
                "a WAL chunk needs a non-empty byte range, got {:?}",
                chunk.byte_range
            )));
        }
        Ok(())
    }
}
