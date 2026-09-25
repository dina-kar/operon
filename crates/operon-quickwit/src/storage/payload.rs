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
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-storage/src/payload.rs); modified for Operon: AWS ByteStream methods replaced by range_bytes; Vec<u8> impl moved to shim::payload.

use std::io;
use std::ops::Range;

use async_trait::async_trait;
use tantivy::directory::OwnedBytes;

#[async_trait]
/// PutPayload is used to upload data and support multipart.
pub trait PutPayload: PutPayloadClone + Send + Sync {
    /// Return the total length of the payload.
    fn len(&self) -> u64;

    /// Retrieve the bytes of the specified range.
    fn range_bytes(&self, range: Range<u64>) -> io::Result<OwnedBytes>;

    /// Load the whole Payload into memory.
    async fn read_all(&self) -> io::Result<OwnedBytes> {
        let total_len = self.len();
        let range = 0..total_len;
        self.range_bytes(range)
    }
}

pub trait PutPayloadClone {
    fn box_clone(&self) -> Box<dyn PutPayload>;
}

impl<T> PutPayloadClone for T
where
    T: 'static + PutPayload + Clone,
{
    fn box_clone(&self) -> Box<dyn PutPayload> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn PutPayload> {
    fn clone(&self) -> Box<dyn PutPayload> {
        self.box_clone()
    }
}
