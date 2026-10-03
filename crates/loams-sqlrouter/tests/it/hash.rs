use loams_sqlrouter::hash::{PgKey, pg_partition_index, vitess_hash, vitess_unhash, vitess_xxhash};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn parse_uuid(s: &str) -> [u8; 16] {
    let hexed: String = s.chars().filter(|c| *c != '-').collect();
    hex::decode(hexed).unwrap().try_into().unwrap()
}

/// Ground truth from real `PARTITION BY HASH` in Postgres 17.11 (ruling 4):
/// for every key and modulus, the partition Postgres chose.
#[test]
fn pg_hash_matches_postgres_partitioning() {
    let fx = fixture("pg_hash_vectors.json");
    let mut checked = 0;
    for ty in ["int8", "int4", "text", "uuid"] {
        let rows = fx[ty].as_array().unwrap();
        assert!(rows.len() > 1000, "{ty}: {} rows", rows.len());
        for row in rows {
            let k = row["key"].as_str().unwrap();
            let key = match ty {
                "int8" => PgKey::Int8(k.parse().unwrap()),
                "int4" => PgKey::Int4(k.parse().unwrap()),
                "text" => PgKey::Text(k.to_owned()),
                _ => PgKey::Uuid(parse_uuid(k)),
            };
            for (modulus, remainder) in row["remainders"].as_object().unwrap() {
                let m: u32 = modulus.parse().unwrap();
                let want = remainder.as_u64().unwrap() as u32;
                assert_eq!(
                    pg_partition_index(&key, m),
                    want,
                    "{ty} key {k:?} modulus {m}"
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 35_000, "checked {checked}");
}

/// The vectors of Vitess's own `hash_test.go` and `xxhash_test.go` (Apache-2.0; notice in the fixture).
#[test]
fn vitess_hash_matches_vitess_vectors() {
    let fx = fixture("vitess_hash_vectors.json");
    for case in fx["hash"].as_array().unwrap() {
        let input = case["input"].as_u64().unwrap();
        let want = u64::from_str_radix(case["ksid"].as_str().unwrap(), 16).unwrap();
        assert_eq!(vitess_hash(input), want, "hash({input})");
    }
    for case in fx["xxhash"].as_array().unwrap() {
        let input = case["input_utf8"].as_str().unwrap();
        let want = u64::from_str_radix(case["ksid"].as_str().unwrap(), 16).unwrap();
        assert_eq!(vitess_xxhash(input.as_bytes()), want, "xxhash({input:?})");
    }
}

#[test]
fn vitess_unhash_inverts() {
    for v in [0, 1, 2, 42, u64::MAX, 1 << 63, 0x0123_4567_89ab_cdef] {
        assert_eq!(vitess_unhash(vitess_hash(v)), v);
    }
}
