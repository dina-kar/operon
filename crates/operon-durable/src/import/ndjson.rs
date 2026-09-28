//! NDJSON slices (Ruling 6): ranges of `ndjson_slice_bytes` split at
//! newlines. Slice `n` owns the lines that start in
//! `[n × size, (n + 1) × size)`: it skips its partial first line (which the
//! previous slice finishes) and reads past its end to finish its last one.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::{Bytes, BytesMut};
use operon_store::Store;

use super::slice::read_range;
use super::{Attempt, ImportEnv, PlannedFile};

/// How far past a slice's end one read looks for the newline.
const EXTEND: u64 = 64 * 1024;

/// The file's slices.
pub(super) fn slices(env: &ImportEnv, file: &PlannedFile) -> u64 {
    file.size.div_ceil(env.config.ndjson_slice_bytes.max(1))
}

/// The bytes of the lines slice `n` owns (see the module comment).
async fn owned_lines(
    env: &ImportEnv,
    store: &Store,
    file: &PlannedFile,
    n: u64,
) -> Result<Bytes, Attempt> {
    let step = env.config.ndjson_slice_bytes.max(1);
    let start = n.saturating_mul(step).min(file.size);
    let end = start.saturating_add(step).min(file.size);
    if start >= end {
        return Ok(Bytes::new());
    }
    // From one byte early, so a line starting exactly at `start` is seen.
    let from = start.saturating_sub(1);
    let head = read_range(store, file, from..end).await?;
    let skip = if n == 0 {
        0
    } else {
        match head.iter().position(|&b| b == b'\n') {
            Some(i) => i + 1,
            // No line starts in this slice.
            None => return Ok(Bytes::new()),
        }
    };
    if skip >= head.len() {
        return Ok(Bytes::new());
    }
    let mut out = BytesMut::from(&head[skip..]);
    let mut pos = end;
    while pos < file.size && out.last() != Some(&b'\n') {
        let more = read_range(store, file, pos..(pos + EXTEND).min(file.size)).await?;
        match more.iter().position(|&b| b == b'\n') {
            Some(i) => {
                out.extend_from_slice(&more[..=i]);
                break;
            }
            None => {
                out.extend_from_slice(&more);
                pos += more.len() as u64;
            }
        }
    }
    Ok(out.freeze())
}

/// Slice `n` of `file` as batches of `chunk_rows`, and the bytes it covers.
/// The schema is inferred from the slice's own lines.
pub(super) async fn read_slice(
    env: &ImportEnv,
    store: &Store,
    file: &PlannedFile,
    n: u64,
) -> Result<(Vec<RecordBatch>, u64), Attempt> {
    let data = owned_lines(env, store, file, n).await?;
    let bytes = data.len() as u64;
    if data.iter().all(u8::is_ascii_whitespace) {
        return Ok((Vec::new(), bytes));
    }
    let invalid = |e: arrow_schema::ArrowError| Attempt::Fail {
        code: "invalid_file".into(),
        message: format!("{} slice {n} is not NDJSON: {e}", file.key),
    };
    let (schema, _) =
        arrow_json::reader::infer_json_schema(Cursor::new(&data[..]), None).map_err(invalid)?;
    let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
        .with_batch_size(env.config.chunk_rows.max(1))
        .build(Cursor::new(&data[..]))
        .map_err(invalid)?;
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(invalid)?;
    Ok((batches, bytes))
}
