//! Vitess's `hash` and `xxhash` vindexes (§31 §6.1). Behaviour from Vitess
//! `go/vt/vtgate/vindexes/{hash,xxhash}.go` (Apache-2.0); the test vectors in
//! `tests/fixtures/vitess_hash_vectors.json` come from their tests with the
//! notice.

use des::Des;
use des::cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};

use crate::ranges::KeyspaceId;

fn cipher() -> Des {
    // Vitess's `hash` vindex encrypts with DES under an all-zero key.
    Des::new(&Array::from([0u8; 8]))
}

/// The `hash` vindex: DES of the big-endian bytes of `id`. The keyspace id is
/// the ciphertext read big-endian.
pub fn vitess_hash(id: u64) -> KeyspaceId {
    let mut block = Array::from(id.to_be_bytes());
    cipher().encrypt_block(&mut block);
    u64::from_be_bytes(block.into())
}

/// The inverse of [`vitess_hash`] (Vitess's `ReverseMap`).
pub fn vitess_unhash(ksid: KeyspaceId) -> u64 {
    let mut block = Array::from(ksid.to_be_bytes());
    cipher().decrypt_block(&mut block);
    u64::from_be_bytes(block.into())
}

/// The `xxhash` vindex: XXH64 (seed 0) of the value's raw bytes, written
/// little-endian into the 8-byte keyspace id.
pub fn vitess_xxhash(bytes: &[u8]) -> KeyspaceId {
    let h = xxhash_rust::xxh64::xxh64(bytes, 0);
    u64::from_be_bytes(h.to_le_bytes())
}
