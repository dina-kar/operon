//! Region pre-splits through PD's HTTP API (`POST /pd/api/v1/regions/split`).
//!
//! A write-hot key range, such as one WAL timeline (design §28 §6.5), gets its
//! own region so that its Raft log, leader and store are its own. TiKV's
//! load-based split only reacts to reads, so the range is split up front.
//!
//! On API v2 PD sees every key, raw or transactional, as the keyspace-prefixed
//! key (`'r'` or `'x'`, then the 3-byte keyspace id) in memcomparable form,
//! which is what [`raw_region_key`] builds.

use serde::Serialize;

use crate::{Tikv, TikvError};

/// PD's view of the API v2 raw key `key` in keyspace `keyspace_id`: the
/// memcomparable encoding of `'r' ‖ id (3 bytes, big-endian) ‖ key`.
pub fn raw_region_key(keyspace_id: u32, key: &[u8]) -> Vec<u8> {
    let mut full = Vec::with_capacity(4 + key.len());
    full.push(b'r');
    full.extend_from_slice(&keyspace_id.to_be_bytes()[1..]);
    full.extend_from_slice(key);
    memcomparable(&full)
}

/// TiKV's `encode_bytes`: groups of 8 bytes, each followed by a marker byte
/// `0xFF - padding`; the last group is zero-padded.
fn memcomparable(key: &[u8]) -> Vec<u8> {
    const GROUP: usize = 8;
    let mut out = Vec::with_capacity((key.len() / GROUP + 1) * (GROUP + 1));
    for chunk in key.chunks(GROUP) {
        out.extend_from_slice(chunk);
        let pad = GROUP - chunk.len();
        out.extend(std::iter::repeat_n(0u8, pad));
        #[allow(clippy::cast_possible_truncation)] // pad <= 8
        out.push(0xFF - pad as u8);
    }
    if key.len().is_multiple_of(GROUP) {
        out.extend_from_slice(&[0u8; GROUP]);
        out.push(0xFF - GROUP as u8);
    }
    out
}

#[derive(Serialize)]
struct SplitRequest {
    split_keys: Vec<String>,
    retry_limit: u32,
}

impl Tikv {
    /// Asks PD to split regions at the raw keys `keys` (keys of this handle's
    /// keyspace, already under the handle's root if they should be). PD
    /// schedules the splits and answers once they are done or `retry_limit`
    /// rounds passed; a key that is already a region boundary is a no-op.
    pub async fn split_raw_regions(&self, keys: &[Vec<u8>]) -> Result<(), TikvError> {
        const OP: &str = "POST regions/split";
        if keys.is_empty() {
            return Ok(());
        }
        let id = self.keyspace_meta().await?.id;
        let body = SplitRequest {
            split_keys: keys
                .iter()
                .map(|k| hex_upper(&raw_region_key(id, k)))
                .collect(),
            retry_limit: 3,
        };
        let url = format!("{}/pd/api/v1/regions/split", self.pd_http());
        let response =
            self.http
                .post(url)
                .json(&body)
                .send()
                .await
                .map_err(|e| TikvError::Http {
                    op: OP,
                    message: e.to_string(),
                })?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(TikvError::Pd {
            op: OP,
            status,
            body,
        })
    }
}

fn hex_upper(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02X}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memcomparable_matches_tikv() {
        // tikv_util::codec::bytes::encode_bytes test vectors.
        assert_eq!(memcomparable(b""), [0, 0, 0, 0, 0, 0, 0, 0, 0xF7]);
        assert_eq!(
            memcomparable(b"\x01\x02\x03"),
            [1, 2, 3, 0, 0, 0, 0, 0, 0xFA]
        );
        assert_eq!(
            memcomparable(b"\x01\x02\x03\x04\x05\x06\x07\x08"),
            [1, 2, 3, 4, 5, 6, 7, 8, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0xF7]
        );
    }

    #[test]
    fn raw_keys_carry_the_keyspace_prefix() {
        let k = raw_region_key(0x01_02_03, b"a");
        assert_eq!(&k[..5], b"r\x01\x02\x03a");
        assert_eq!(k.len(), 9);
        // Order is kept: the encoding is memcomparable.
        assert!(raw_region_key(7, b"ab") < raw_region_key(7, b"abc"));
        assert!(raw_region_key(7, b"abc") < raw_region_key(7, b"abd"));
    }
}
