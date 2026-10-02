use loams_sqlrouter::hash::{PgKey, PgKeyType};
use loams_sqlrouter::ranges::parse_vitess_shard;
use loams_sqlrouter::record::*;
use loams_sqlrouter::{KeyRange, PartitionError};
use proptest::prelude::*;

fn shard(i: usize) -> ShardEntry {
    ShardEntry {
        name: format!("s{i}"),
        backend: BackendRef {
            host: format!("shard{i}.local"),
            port: 5432,
            database: "app".into(),
        },
        fence: FenceState::Open,
    }
}

fn pg_hash(n: u32) -> ShardMapRecord {
    ShardMapRecord {
        version: 7,
        generation: 3,
        engine: Engine::Postgres,
        router: RouterKind::PgDog {
            database: "app".into(),
        },
        scheme: Scheme::PgHash {
            column: "user_id".into(),
            data_type: PgKeyType::Int8,
            shards: n,
        },
        shards: (0..n as usize).map(shard).collect(),
        state: MapState::Serving,
    }
}

fn vindex(names: &[&str]) -> ShardMapRecord {
    ShardMapRecord {
        version: 1,
        generation: 1,
        engine: Engine::MySql,
        router: RouterKind::Vitess {
            keyspace: "commerce".into(),
        },
        scheme: Scheme::Vindex {
            vindex: VindexKind::Hash,
            ranges: names
                .iter()
                .map(|n| parse_vitess_shard(n).unwrap())
                .collect(),
        },
        shards: (0..names.len()).map(shard).collect(),
        state: MapState::CuttingOver {
            step: CutoverStep::Fenced,
        },
    }
}

#[test]
fn unknown_format_byte_is_refused() {
    let mut bytes = pg_hash(2).encode();
    assert_eq!(bytes[0], RECORD_FORMAT);
    bytes[0] = 2;
    assert_eq!(
        ShardMapRecord::decode(&bytes),
        Err(RecordError::UnknownFormat(2))
    );
    assert!(matches!(
        ShardMapRecord::decode(&[]),
        Err(RecordError::Corrupt(_))
    ));
    assert!(matches!(
        ShardMapRecord::decode(&[RECORD_FORMAT, 0xff]),
        Err(RecordError::Corrupt(_))
    ));
}

#[test]
fn validate_checks_counts_and_partitions() {
    assert_eq!(pg_hash(4).validate(), Ok(()));
    let mut r = pg_hash(4);
    r.shards.pop();
    assert_eq!(
        r.validate(),
        Err(RecordError::ShardCount {
            scheme: 4,
            listed: 3
        })
    );
    assert_eq!(vindex(&["-80", "80-"]).validate(), Ok(()));
    assert_eq!(
        vindex(&["-40", "80-"]).validate(),
        Err(RecordError::Partition(PartitionError::Gap {
            at: 0x4000_0000_0000_0000
        }))
    );
}

#[test]
fn shard_for_routes_by_scheme() {
    let r = vindex(&["-80", "80-"]);
    // Vitess's hash of 1 is 0x166b40b44aba4bd6, in -80.
    assert_eq!(r.shard_for(&KeyValue::Uint(1)), Ok(0));
    // Vitess's hash of 3 is 0x4eb190c9a2fa169c (-80); of 4, 0xd2fd8867d50d2dfe (80-).
    assert_eq!(r.shard_for(&KeyValue::Uint(4)), Ok(1));
    assert_eq!(
        r.shard_for(&KeyValue::Bytes(vec![1])),
        Err(RecordError::UnsupportedKeyType)
    );
    let h = pg_hash(4);
    assert!(h.shard_for(&KeyValue::Pg(PgKey::Int8(42))).unwrap() < 4);
    assert_eq!(
        h.shard_for(&KeyValue::Pg(PgKey::Text("x".into()))),
        Err(RecordError::UnsupportedKeyType)
    );
}

fn arb_record() -> impl Strategy<Value = ShardMapRecord> {
    (
        any::<u64>(),
        any::<u64>(),
        1u32..16,
        prop::collection::vec(1u64.., 0..6),
        any::<bool>(),
    )
        .prop_map(|(version, generation, n, cuts, vitess)| {
            let mut r = if vitess {
                let ranges: Vec<KeyRange> = crate::partition_from_cuts(cuts);
                ShardMapRecord {
                    shards: (0..ranges.len()).map(shard).collect(),
                    scheme: Scheme::Vindex {
                        vindex: VindexKind::XxHash,
                        ranges,
                    },
                    ..vindex(&["-"])
                }
            } else {
                pg_hash(n)
            };
            r.version = version;
            r.generation = generation;
            r
        })
}

proptest! {
    #[test]
    fn record_round_trips(r in arb_record()) {
        let bytes = r.encode();
        prop_assert_eq!(ShardMapRecord::decode(&bytes).unwrap(), r.clone());
        prop_assert_eq!(r.validate(), Ok(()));
    }
}

#[test]
fn zero_hash_shards_are_refused() {
    let mut r = pg_hash(1);
    r.scheme = Scheme::PgHash {
        column: "id".into(),
        data_type: PgKeyType::Int8,
        shards: 0,
    };
    r.shards.clear();
    assert_eq!(
        r.validate(),
        Err(RecordError::ShardCount {
            scheme: 0,
            listed: 0
        })
    );
}
