//! Parquet slices (Ruling 6): one row group per slice, read through an
//! `AsyncFileReader` whose every range read is conditional on the planned
//! etag (`ParquetObjectReader` sets no `If-Match`, T0-5).

use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use operon_store::Store;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use super::slice::read_range;
use super::{Attempt, ImportEnv, PlannedFile, retrying};

/// Footer bytes read in the first request.
const FOOTER_HINT: usize = 64 * 1024;

/// A Parquet file on the source store, read with `If-Match`.
struct Reader {
    store: Store,
    file: PlannedFile,
    /// The store failure behind the last read error, which the Parquet
    /// error cannot carry.
    failure: Arc<Mutex<Option<Attempt>>>,
}

impl Reader {
    fn new(store: &Store, file: &PlannedFile) -> Self {
        Self {
            store: store.clone(),
            file: file.clone(),
            failure: Arc::new(Mutex::new(None)),
        }
    }
}

impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            match read_range(&self.store, &self.file, range).await {
                Ok(bytes) => Ok(bytes),
                Err(attempt) => {
                    let message = match &attempt {
                        Attempt::Retry { message, .. } | Attempt::Fail { message, .. } => {
                            message.clone()
                        }
                    };
                    *self
                        .failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(attempt);
                    Err(ParquetError::General(message))
                }
            }
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let size = self.file.size;
            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(FOOTER_HINT))
                .load_and_finish(&mut *self, size)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

/// `e` as an attempt: the store failure behind it, else a bad file.
fn attempt_of(failure: &Mutex<Option<Attempt>>, key: &str, e: &ParquetError) -> Attempt {
    failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .unwrap_or_else(|| Attempt::Fail {
            code: "invalid_file".into(),
            message: format!("{key} is not a readable Parquet file: {e}"),
        })
}

/// The file's slices: its row groups. A row group over
/// `max_row_group_bytes` uncompressed fails the file (Ruling 6).
pub(super) async fn slices(
    env: &ImportEnv,
    store: &Store,
    file: &PlannedFile,
) -> Result<u64, (String, String)> {
    let metadata = retrying(env, &file.key, || async {
        let mut reader = Reader::new(store, file);
        let failure = Arc::clone(&reader.failure);
        reader
            .get_metadata(None)
            .await
            .map_err(|e| attempt_of(&failure, &file.key, &e))
    })
    .await?;
    for (i, group) in metadata.row_groups().iter().enumerate() {
        let size = u64::try_from(group.total_byte_size()).unwrap_or(0);
        if size > env.config.max_row_group_bytes {
            return Err((
                "row_group_too_large".into(),
                format!(
                    "{}: row group {i} is {size} bytes uncompressed; an import takes row groups \
                     of at most {} bytes. Rewrite the file with smaller row groups",
                    file.key, env.config.max_row_group_bytes
                ),
            ));
        }
    }
    Ok(metadata.num_row_groups() as u64)
}

/// Row group `n` of `file`, in batches of `chunk_rows`, and its compressed
/// size.
pub(super) async fn read_slice(
    env: &ImportEnv,
    store: &Store,
    file: &PlannedFile,
    n: u64,
) -> Result<(Vec<RecordBatch>, u64), Attempt> {
    let reader = Reader::new(store, file);
    let failure = Arc::clone(&reader.failure);
    let fail = |e: ParquetError| attempt_of(&failure, &file.key, &e);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(fail)?;
    let index = usize::try_from(n).unwrap_or(usize::MAX);
    let Some(group) = builder.metadata().row_groups().get(index) else {
        return Err(Attempt::Fail {
            code: "file_changed".into(),
            message: format!("{} has no row group {n}", file.key),
        });
    };
    let bytes = u64::try_from(group.compressed_size()).unwrap_or(0);
    let stream = builder
        .with_row_groups(vec![index])
        .with_batch_size(env.config.chunk_rows.max(1))
        .build()
        .map_err(fail)?;
    let batches = stream.try_collect::<Vec<_>>().await.map_err(fail)?;
    Ok((batches, bytes))
}
