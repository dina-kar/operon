// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-storage/src/split.rs); modified for Operon: hyper, AWS and pin_project code replaced by PutPayload::range_bytes; finalize always writes the footer trailer; finalize_with_footer_trailer made pub; Debug impls.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};

use tantivy::directory::OwnedBytes;

use crate::shim::consts::{SPLIT_FIELDS_FILE_NAME, SPLIT_RECOVERY_METADATA_FILE_NAME};
use crate::storage::bundle_storage::{BundleFileRangesVersions, serialize_split_footer_trailer};
use crate::storage::{BundleFileRanges, PutPayload, VersionedComponent};

/// Payload of a split which builds the split bundle and hotcache on the fly and streams it to the
/// storage.
#[derive(Clone)]
pub struct SplitPayload {
    payloads: Vec<Box<dyn PutPayload>>,
    /// bytes range of the footer (hotcache + bundle metadata)
    pub footer_range: Range<u64>,
}

impl std::fmt::Debug for SplitPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitPayload")
            .field("len", &self.len())
            .field("footer_range", &self.footer_range)
            .finish()
    }
}

fn range_bytes_from_payloads(
    payloads: &[Box<dyn PutPayload>],
    range: Range<u64>,
) -> io::Result<OwnedBytes> {
    let mut bytes: Vec<u8> = Vec::with_capacity((range.end - range.start) as usize);

    let payloads_and_ranges =
        chunk_payload_ranges(payloads, range.start as usize..range.end as usize);

    for (payload, range) in payloads_and_ranges {
        bytes.extend_from_slice(
            payload
                .range_bytes(range.start as u64..range.end as u64)?
                .as_slice(),
        );
    }

    Ok(OwnedBytes::new(bytes))
}

impl PutPayload for SplitPayload {
    fn len(&self) -> u64 {
        self.payloads.iter().map(|payload| payload.len()).sum()
    }

    fn range_bytes(&self, range: Range<u64>) -> io::Result<OwnedBytes> {
        range_bytes_from_payloads(&self.payloads, range)
    }
}

#[derive(Clone)]
struct FilePayload {
    len: u64,
    path: PathBuf,
}

impl PutPayload for FilePayload {
    fn len(&self) -> u64 {
        self.len
    }

    fn range_bytes(&self, range: Range<u64>) -> io::Result<OwnedBytes> {
        assert!(!range.is_empty());
        assert!(range.end <= self.len);

        let len = range.end - range.start;
        let mut file = std::fs::File::open(&self.path)?;
        file.seek(SeekFrom::Start(range.start))?;
        let mut bytes = vec![0u8; len as usize];
        file.read_exact(&mut bytes)?;
        Ok(OwnedBytes::new(bytes))
    }
}

/// SplitPayloadBuilder is used to create a `SplitPayload`.
#[derive(Default)]
pub struct SplitPayloadBuilder {
    /// File name, payload, and range of the payload in the bundle file
    /// Range could be computed on the fly, and is just kept here for convenience.
    payloads: Vec<(String, Box<dyn PutPayload>, Range<u64>)>,
    current_offset: usize,
}

impl std::fmt::Debug for SplitPayloadBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let files: Vec<(&str, &Range<u64>)> = self
            .payloads
            .iter()
            .map(|(file_name, _, range)| (file_name.as_str(), range))
            .collect();
        f.debug_struct("SplitPayloadBuilder")
            .field("files", &files)
            .field("current_offset", &self.current_offset)
            .finish()
    }
}

impl SplitPayloadBuilder {
    /// Creates a new SplitPayloadBuilder for given files, recovery metadata, and hotcache.
    pub fn get_split_payload(
        split_files: &[PathBuf],
        serialized_split_fields: &[u8],
        serialized_recovery_metadata: Option<&[u8]>,
        hotcache: &[u8],
    ) -> anyhow::Result<SplitPayload> {
        let mut split_payload_builder = SplitPayloadBuilder::default();
        for file in split_files {
            split_payload_builder.add_file(file)?;
        }
        split_payload_builder.add_payload(
            SPLIT_FIELDS_FILE_NAME.to_string(),
            Box::new(serialized_split_fields.to_vec()),
        );
        if let Some(serialized_recovery_metadata) = serialized_recovery_metadata {
            split_payload_builder.add_payload(
                SPLIT_RECOVERY_METADATA_FILE_NAME.to_string(),
                Box::new(serialized_recovery_metadata.to_vec()),
            );
        }
        let offsets = split_payload_builder.finalize(hotcache)?;
        Ok(offsets)
    }

    /// Adds the payload to the bundle file.
    pub fn add_payload(&mut self, file_name: String, payload: Box<dyn PutPayload>) {
        let range = self.current_offset as u64..self.current_offset as u64 + payload.len();
        self.current_offset += payload.len() as usize;
        self.payloads.push((file_name, payload, range));
    }

    /// Adds the file to the bundle file.
    pub fn add_file(&mut self, path: &Path) -> io::Result<()> {
        let file = std::fs::metadata(path)?;
        let file_name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid file name in path {path:?}"),
                )
            })?;

        let file_payload = FilePayload {
            path: path.to_owned(),
            len: file.len(),
        };

        self.add_payload(file_name, Box::new(file_payload));

        Ok(())
    }

    /// Writes the bundle file ranges at the end of the bundle file.
    pub fn finalize(self, hotcache: &[u8]) -> anyhow::Result<SplitPayload> {
        self.finalize_with_footer_trailer(hotcache, true)
    }

    /// Writes the bundle file ranges at the end of the bundle file, followed by the
    /// 16-byte footer trailer if `enable_footer_trailer` is set.
    pub fn finalize_with_footer_trailer(
        self,
        hotcache: &[u8],
        enable_footer_trailer: bool,
    ) -> anyhow::Result<SplitPayload> {
        // Build the footer.
        let file_ranges = self
            .payloads
            .iter()
            .map(|(file_name, _, range)| {
                let file_name = PathBuf::from(file_name);
                Ok((file_name, range.start..range.end))
            })
            .collect::<Result<HashMap<_, _>, anyhow::Error>>()?;

        let bundle_file_ranges = BundleFileRanges { files: file_ranges };
        let bundle_metadata = BundleFileRangesVersions::serialize(&bundle_file_ranges);

        // The hotcache needs to be next to the bundle metadata in order to read both
        // in one continuous read.
        let mut footer_bytes = Vec::new();
        footer_bytes.extend(&bundle_metadata);
        footer_bytes.extend((bundle_metadata.len() as u32).to_le_bytes());
        footer_bytes.extend(hotcache);
        footer_bytes.extend((hotcache.len() as u32).to_le_bytes());
        if enable_footer_trailer {
            footer_bytes.extend(serialize_split_footer_trailer(self.current_offset as u64));
        }

        let mut payloads: Vec<Box<dyn PutPayload>> = self
            .payloads
            .into_iter()
            .map(|(_, payload, _)| payload)
            .collect();

        payloads.push(Box::new(footer_bytes.to_vec()));

        Ok(SplitPayload {
            payloads,
            footer_range: self.current_offset as u64
                ..self.current_offset as u64 + footer_bytes.len() as u64,
        })
    }
}

/// Returns the payloads with their absolute ranges.
fn get_payloads_with_absolute_range(
    payloads: &[Box<dyn PutPayload>],
) -> Vec<(Box<dyn PutPayload>, Range<usize>)> {
    let mut current = 0;
    payloads
        .iter()
        .map(|payload| {
            let start = current;
            current += payload.len();
            (payload.clone(), start as usize..current as usize)
        })
        .collect()
}

fn get_ranges_overlap(range1: &Range<usize>, range2: &Range<usize>) -> Range<usize> {
    range1.start.max(range2.start)..range1.end.min(range2.end)
}

// Returns payloads and their relative ranges for an absolute range.
fn chunk_payload_ranges(
    payloads: &[Box<dyn PutPayload>],
    range: Range<usize>,
) -> Vec<(Box<dyn PutPayload>, Range<usize>)> {
    let mut ranges = Vec::new();
    for (payload, payload_absolute_range) in get_payloads_with_absolute_range(payloads) {
        let absolute_range_overlap = get_ranges_overlap(&payload_absolute_range, &range);
        if !absolute_range_overlap.is_empty() {
            // Push the range relative to this payload as we will read from it.
            ranges.push((
                payload.clone(),
                (absolute_range_overlap.start - payload_absolute_range.start)
                    ..(absolute_range_overlap.end - payload_absolute_range.start),
            ));
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use tantivy::directory::FileSlice;

    use super::*;

    #[tokio::test]
    async fn test_split_offset_computer() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(b"hello")?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(b"world")?;

        let split_payload = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1, test_filepath2],
            &[],
            None,
            b"abc",
        )?;

        // 128 bytes plus the 16-byte footer trailer, which `finalize` always writes.
        assert_eq!(split_payload.len(), 144);

        Ok(())
    }

    #[tokio::test]
    async fn test_split_payload_embeds_recovery_metadata() {
        let recovery_metadata = b"recovery-protobuf";
        let split_payload = SplitPayloadBuilder::get_split_payload(
            &[],
            b"fields",
            Some(recovery_metadata),
            b"hotcache",
        )
        .unwrap();

        let recovery_range =
            b"fields".len() as u64..(b"fields".len() + recovery_metadata.len()) as u64;
        assert_eq!(
            fetch_data(&split_payload, recovery_range).await.unwrap(),
            recovery_metadata
        );
    }

    #[cfg(test)]
    async fn fetch_data(
        split_streamer: &SplitPayload,
        range: Range<u64>,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(split_streamer.range_bytes(range)?.as_slice().to_vec())
    }

    #[test]
    fn test_chunk_payloads() -> anyhow::Result<()> {
        let payloads: Vec<Box<dyn PutPayload>> = vec![
            Box::new(vec![1, 2, 3]),
            Box::new(vec![4, 5, 6]),
            Box::new(vec![7, 8, 9, 10]),
        ];

        assert_eq!(
            chunk_payload_ranges(&payloads, 0..1)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..1]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 0..2)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..2]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 1..2)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![1..2]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 2..3)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 0..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..3, 0..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 0..5)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..3, 0..2]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 3..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![0..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 4..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![1..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 5..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 2..6)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3, 0..3]
        );
        assert_eq!(
            chunk_payload_ranges(&payloads, 2..5)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![2..3, 0..2]
        );

        assert_eq!(
            chunk_payload_ranges(&payloads, 7..8)
                .iter()
                .map(|el| el.1.clone())
                .collect::<Vec<_>>(),
            vec![1..2]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_split_streamer() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("a");
        let test_filepath2 = temp_dir.path().join("b");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[],
            None,
            &[1, 2, 3],
        )?;

        // border case 1 exact start of first block
        assert_eq!(fetch_data(&split_streamer, 0..1).await?, vec![123]);
        assert_eq!(fetch_data(&split_streamer, 0..2).await?, vec![123, 76]);
        assert_eq!(fetch_data(&split_streamer, 0..3).await?, vec![123, 76, 99]);

        // border 2 case skip and take cross adjacent blocks
        assert_eq!(fetch_data(&split_streamer, 1..3).await?, vec![76, 99]);

        // border 3 case skip and take in separate blocks with full block between
        assert_eq!(
            fetch_data(&split_streamer, 1..6).await?,
            vec![76, 99, 55, 44, 174]
        );

        // border case 4 exact middle block
        assert_eq!(fetch_data(&split_streamer, 2..5).await?, vec![99, 55, 44]);

        // border case 5, no skip but take in middle block
        assert_eq!(fetch_data(&split_streamer, 2..4).await?, vec![99, 55]);

        // border case 6 skip and take in middle block
        assert_eq!(fetch_data(&split_streamer, 3..4).await?, vec![55]);

        // border case 7 start exact last block - footer
        assert_eq!(
            fetch_data(&split_streamer, 5..10).await?,
            vec![174, 190, 18, 24, 1]
        );
        // border case 8 skip and take in last block  - footer
        assert_eq!(
            fetch_data(&split_streamer, 6..10).await?,
            vec![190, 18, 24, 1]
        );

        let total_len = split_streamer.len();
        let all_data = fetch_data(&split_streamer, 0..total_len).await?;

        let split_without_trailer =
            crate::storage::strip_split_footer_trailer(FileSlice::from(all_data))?.read_bytes()?;
        assert_eq!(
            split_without_trailer[split_without_trailer.len() - 4..],
            3_u32.to_le_bytes()
        );
        Ok(())
    }
}
