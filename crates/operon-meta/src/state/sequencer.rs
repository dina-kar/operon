//! The stream sequencer: dense offsets and the offset index (design §02 §3).

use super::{MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{EntryKind, IndexEntry, WAL_COMMIT_WINDOW_MS, WalChunk, WalCommitRecord};

impl MetaState {
    pub(super) fn commit_wal(
        &mut self,
        object: String,
        created_at_ms: u64,
        chunks: Vec<WalChunk>,
    ) -> Result<Reply, ApplyError> {
        validate_key("WAL object path", &object)?;
        if let Some(record) = self.wal_commits.get(&object) {
            return Ok(Reply::WalCommitted {
                base_offsets: record.base_offsets.clone(),
            });
        }
        if created_at_ms.saturating_add(WAL_COMMIT_WINDOW_MS) < self.clock_ms {
            return Err(ApplyError::StaleCommit { object });
        }
        if chunks.is_empty() {
            return Err(ApplyError::InvalidArgument(
                "a WAL commit needs at least one chunk".to_string(),
            ));
        }
        let live_chunks = u32::try_from(chunks.len()).map_err(|_| {
            ApplyError::InvalidArgument(format!("too many chunks: {}", chunks.len()))
        })?;
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
            partition.insert_entry(IndexEntry {
                kind: EntryKind::Wal,
                base_offset,
                records: chunk.records,
                object: object.clone(),
                byte_range: chunk.byte_range,
                max_timestamp_ms: chunk.max_timestamp_ms,
            });
            base_offsets.push(base_offset);
        }
        self.wal_live_chunks.insert(object.clone(), live_chunks);
        self.wal_commits.insert(
            object,
            WalCommitRecord {
                base_offsets: base_offsets.clone(),
                created_at_ms,
            },
        );
        Ok(Reply::WalCommitted { base_offsets })
    }

    /// What the metastore remembers about a committed WAL object, until it is
    /// pruned.
    pub fn wal_commit(&self, object: &str) -> Option<&WalCommitRecord> {
        self.wal_commits.get(object)
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
