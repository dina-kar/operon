//! Postgres hash partitioning, ported from PostgreSQL 17 (`REL_17_STABLE`):
//! `src/common/hashfn.c` (`hash_bytes_extended`, `hash_bytes_uint32_extended`,
//! the `mix` and `final` macros), `src/include/common/hashfn.h`
//! (`hash_combine64`), `src/backend/access/hash/hashfunc.c`
//! (`hashint4extended`, `hashint8extended`) and
//! `src/backend/partitioning/partbounds.c` (`compute_partition_hash_value`).
//! Ported from PostgreSQL's own sources, never from PgDog's copy (D318).
//!
//! PostgreSQL Database Management System
//! (formerly known as Postgres, then as Postgres95)
//!
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
//!
//! Portions Copyright (c) 1994, The Regents of the University of California
//!
//! Permission to use, copy, modify, and distribute this software and its
//! documentation for any purpose, without fee, and without a written agreement
//! is hereby granted, provided that the above copyright notice and this
//! paragraph and the following two paragraphs appear in all copies.
//!
//! IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
//! DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
//! LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
//! DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
//! POSSIBILITY OF SUCH DAMAGE.
//!
//! THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
//! INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
//! AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
//! ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
//! PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//!
//! `hash_bytes_extended` reads the key as little-endian 32-bit words, which is
//! what PostgreSQL computes on little-endian hosts (x86-64, ARM64): every
//! server Loams runs. On a big-endian host PostgreSQL computes different
//! hashes, and these functions would not match it.

use serde::{Deserialize, Serialize};

/// `HASH_PARTITION_SEED` from `src/include/catalog/partition.h`.
pub const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;

/// The initial state of `hash_bytes` and its variants: `0x9e3779b9 + len + 3923095`.
const fn init_state(len: u32) -> u32 {
    0x9e37_79b9u32.wrapping_add(len).wrapping_add(3_923_095)
}

#[inline]
fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(4);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(6);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(8);
    *b = b.wrapping_add(*a);
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(16);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(19);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(4);
    *b = b.wrapping_add(*a);
}

#[inline]
fn final_mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(14));
    *a ^= *c;
    *a = a.wrapping_sub(c.rotate_left(11));
    *b ^= *a;
    *b = b.wrapping_sub(a.rotate_left(25));
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(16));
    *a ^= *c;
    *a = a.wrapping_sub(c.rotate_left(4));
    *b ^= *a;
    *b = b.wrapping_sub(a.rotate_left(14));
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(24));
}

#[inline]
fn perturb(a: &mut u32, b: &mut u32, c: &mut u32, seed: u64) {
    if seed != 0 {
        *a = a.wrapping_add((seed >> 32) as u32);
        *b = b.wrapping_add(seed as u32);
        mix(a, b, c);
    }
}

/// `hash_bytes_extended`: Bob Jenkins' lookup3 over `bytes`, seeded, as a `u64`.
pub fn hash_bytes_extended(bytes: &[u8], seed: u64) -> u64 {
    let len = bytes.len() as u32;
    let (mut a, mut b, mut c) = (init_state(len), init_state(len), init_state(len));
    perturb(&mut a, &mut b, &mut c, seed);
    let word = |k: &[u8]| u32::from_le_bytes([k[0], k[1], k[2], k[3]]);
    let mut k = bytes;
    while k.len() >= 12 {
        a = a.wrapping_add(word(&k[0..4]));
        b = b.wrapping_add(word(&k[4..8]));
        c = c.wrapping_add(word(&k[8..12]));
        mix(&mut a, &mut b, &mut c);
        k = &k[12..];
    }
    // The last 0..=11 bytes, little-endian; the lowest byte of c is left for the length.
    let byte = |i: usize, shift: u32| u32::from(k[i]) << shift;
    let rest = k.len();
    if rest >= 11 {
        c = c.wrapping_add(byte(10, 24));
    }
    if rest >= 10 {
        c = c.wrapping_add(byte(9, 16));
    }
    if rest >= 9 {
        c = c.wrapping_add(byte(8, 8));
    }
    if rest >= 8 {
        b = b.wrapping_add(byte(7, 24));
    }
    if rest >= 7 {
        b = b.wrapping_add(byte(6, 16));
    }
    if rest >= 6 {
        b = b.wrapping_add(byte(5, 8));
    }
    if rest >= 5 {
        b = b.wrapping_add(byte(4, 0));
    }
    if rest >= 4 {
        a = a.wrapping_add(byte(3, 24));
    }
    if rest >= 3 {
        a = a.wrapping_add(byte(2, 16));
    }
    if rest >= 2 {
        a = a.wrapping_add(byte(1, 8));
    }
    if rest >= 1 {
        a = a.wrapping_add(byte(0, 0));
    }
    final_mix(&mut a, &mut b, &mut c);
    (u64::from(b) << 32) | u64::from(c)
}

/// `hash_bytes_uint32_extended` (`hash_uint32_extended`).
pub fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
    let init = init_state(4);
    let (mut a, mut b, mut c) = (init, init, init);
    perturb(&mut a, &mut b, &mut c, seed);
    a = a.wrapping_add(k);
    final_mix(&mut a, &mut b, &mut c);
    (u64::from(b) << 32) | u64::from(c)
}

/// `hashint4extended`.
pub fn hashint4extended(v: i32, seed: u64) -> u64 {
    hash_uint32_extended(v as u32, seed)
}

/// `hashint8extended`: fold the high half into the low half (complemented for
/// negative values, so int2, int4 and int8 hash alike), then hash 32 bits.
pub fn hashint8extended(v: i64, seed: u64) -> u64 {
    let lo = v as u32;
    let hi = (v >> 32) as u32;
    hash_uint32_extended(lo ^ if v >= 0 { hi } else { !hi }, seed)
}

/// `hash_combine64`.
pub fn hash_combine64(a: u64, b: u64) -> u64 {
    a ^ b
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7)
}

/// A value of a hash-partitioning key column, in the types Loams supports.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PgKey {
    Int8(i64),
    Int4(i32),
    /// `text` or `varchar` under a deterministic collation: hashed by its bytes.
    Text(String),
    Uuid([u8; 16]),
}

/// The column types of [`PgKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PgKeyType {
    Int8,
    Int4,
    Text,
    Uuid,
}

impl PgKey {
    pub fn key_type(&self) -> PgKeyType {
        match self {
            PgKey::Int8(_) => PgKeyType::Int8,
            PgKey::Int4(_) => PgKeyType::Int4,
            PgKey::Text(_) => PgKeyType::Text,
            PgKey::Uuid(_) => PgKeyType::Uuid,
        }
    }

    /// The type's extended hash support function, as Postgres calls it.
    pub fn hash_extended(&self, seed: u64) -> u64 {
        match self {
            PgKey::Int8(v) => hashint8extended(*v, seed),
            PgKey::Int4(v) => hashint4extended(*v, seed),
            PgKey::Text(s) => hash_bytes_extended(s.as_bytes(), seed),
            PgKey::Uuid(u) => hash_bytes_extended(u, seed),
        }
    }
}

/// The partition Postgres's `PARTITION BY HASH` puts a one-column key in, when
/// every partition has modulus `modulus`: `compute_partition_hash_value`
/// (`hash_combine64(0, hash(value, HASH_PARTITION_SEED))`) modulo `modulus`.
/// This is the remainder of the partition, and PgDog's shard number.
pub fn pg_partition_index(key: &PgKey, modulus: u32) -> u32 {
    assert!(modulus > 0, "a hash partition modulus is positive");
    let row_hash = hash_combine64(0, key.hash_extended(HASH_PARTITION_SEED));
    (row_hash % u64::from(modulus)) as u32
}
