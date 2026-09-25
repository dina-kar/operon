// Copyright 2026 The Operon Authors
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

//! [`PutPayload`] for in-memory buffers.

use std::io;
use std::ops::Range;

use tantivy::directory::OwnedBytes;

use crate::storage::PutPayload;

fn check_range(len: u64, range: &Range<u64>) -> io::Result<Range<usize>> {
    if range.start > range.end || range.end > len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("range {range:?} is out of bounds for a payload of {len} bytes"),
        ));
    }
    Ok(range.start as usize..range.end as usize)
}

impl PutPayload for Vec<u8> {
    fn len(&self) -> u64 {
        self.len() as u64
    }

    fn range_bytes(&self, range: Range<u64>) -> io::Result<OwnedBytes> {
        let range = check_range(PutPayload::len(self), &range)?;
        Ok(OwnedBytes::new(self[range].to_vec()))
    }
}

impl PutPayload for OwnedBytes {
    fn len(&self) -> u64 {
        OwnedBytes::len(self) as u64
    }

    fn range_bytes(&self, range: Range<u64>) -> io::Result<OwnedBytes> {
        let range = check_range(PutPayload::len(self), &range)?;
        Ok(self.slice(range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn vec_and_owned_bytes_payloads_serve_ranges() {
        let vec_payload: Box<dyn PutPayload> = Box::new(b"hello world".to_vec());
        let bytes_payload: Box<dyn PutPayload> = Box::new(OwnedBytes::new(b"hello world".to_vec()));
        for payload in [vec_payload, bytes_payload] {
            assert_eq!(payload.len(), 11);
            assert_eq!(payload.range_bytes(6..11).unwrap().as_slice(), b"world");
            assert_eq!(payload.read_all().await.unwrap().as_slice(), b"hello world");
            assert!(payload.range_bytes(6..12).is_err());
        }
    }
}
