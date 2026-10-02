use loams_sqlrouter::ranges::{
    EditError, lookup, merge, parse_vitess_shard, split, validate_partition, vitess_shard_name,
};
use loams_sqlrouter::{KeyRange, PartitionError};
use proptest::prelude::*;

use crate::partition_from_cuts;

fn kr(lo: u64, hi: Option<u64>) -> KeyRange {
    KeyRange { lo, hi }
}

#[test]
fn vitess_shard_names_parse_and_print() {
    let cases = [
        ("-", kr(0, None)),
        ("-80", kr(0, Some(0x8000_0000_0000_0000))),
        ("80-", kr(0x8000_0000_0000_0000, None)),
        (
            "40-80",
            kr(0x4000_0000_0000_0000, Some(0x8000_0000_0000_0000)),
        ),
        (
            "4000-8000",
            kr(0x4000_0000_0000_0000, Some(0x8000_0000_0000_0000)),
        ),
        (
            "0c-0d",
            kr(0x0c00_0000_0000_0000, Some(0x0d00_0000_0000_0000)),
        ),
        ("A0B1-", kr(0xa0b1_0000_0000_0000, None)),
        ("0102030405060708-", kr(0x0102_0304_0506_0708, None)),
    ];
    for (name, range) in cases {
        assert_eq!(parse_vitess_shard(name), Ok(range), "{name}");
    }
    // Printing is canonical: lowercase, trailing zero bytes dropped.
    assert_eq!(vitess_shard_name(&kr(0, None)), "-");
    assert_eq!(
        vitess_shard_name(&parse_vitess_shard("4000-8000").unwrap()),
        "40-80"
    );
    assert_eq!(
        vitess_shard_name(&parse_vitess_shard("A0B1-").unwrap()),
        "a0b1-"
    );
    assert_eq!(
        vitess_shard_name(&parse_vitess_shard("0102030405060708-").unwrap()),
        "0102030405060708-"
    );
    for bad in [
        "",
        "80",
        "8-",
        "zz-",
        "80-40",
        "80-80",
        "012345678901234567-",
    ] {
        assert!(parse_vitess_shard(bad).is_err(), "{bad:?} must not parse");
    }
}

#[test]
fn validate_rejects_gap_and_overlap() {
    let half = 0x8000_0000_0000_0000;
    assert_eq!(
        validate_partition(&[kr(0, Some(half)), kr(half, None)]),
        Ok(())
    );
    assert_eq!(validate_partition(&[kr(0, None)]), Ok(()));
    assert_eq!(validate_partition(&[]), Err(PartitionError::Empty));
    assert_eq!(
        validate_partition(&[kr(1, None)]),
        Err(PartitionError::Gap { at: 0 })
    );
    assert_eq!(
        validate_partition(&[kr(0, Some(10)), kr(12, None)]),
        Err(PartitionError::Gap { at: 10 })
    );
    assert_eq!(
        validate_partition(&[kr(0, Some(10)), kr(8, None)]),
        Err(PartitionError::Overlap { at: 8 })
    );
    assert_eq!(
        validate_partition(&[kr(0, Some(10))]),
        Err(PartitionError::Gap { at: 10 })
    );
    assert_eq!(
        validate_partition(&[kr(0, None), kr(5, None)]),
        Err(PartitionError::Overlap { at: 5 })
    );
    assert_eq!(
        validate_partition(&[kr(0, Some(0)), kr(0, None)]),
        Err(PartitionError::NotSorted)
    );
    // Vitess's two-shard layout, from names.
    let two: Vec<_> = ["-80", "80-"]
        .iter()
        .map(|n| parse_vitess_shard(n).unwrap())
        .collect();
    assert_eq!(validate_partition(&two), Ok(()));
}

#[test]
fn split_and_merge_refuse_bad_edits() {
    let p = vec![kr(0, Some(100)), kr(100, None)];
    assert_eq!(split(&p, 2, 5), Err(EditError::BadIndex(2)));
    assert_eq!(split(&p, 0, 0), Err(EditError::NotInside(0)));
    assert_eq!(split(&p, 0, 100), Err(EditError::NotInside(100)));
    assert_eq!(merge(&p, 1), Err(EditError::BadIndex(1)));
    assert_eq!(
        merge(&[kr(0, Some(5)), kr(6, None)], 0),
        Err(EditError::NotAdjacent { index: 0 })
    );
}

proptest! {
    #[test]
    fn split_then_merge_is_identity(cuts in prop::collection::vec(1u64.., 0..12), pick in any::<usize>(), at in any::<u64>()) {
        let p = partition_from_cuts(cuts);
        let i = pick % p.len();
        let r = p[i];
        let hi = r.hi.unwrap_or(u64::MAX);
        // A range [u64::MAX, end) has no interior point; skip it without overflowing.
        let Some(first) = r.lo.checked_add(1) else { return Ok(()) };
        prop_assume!(first < hi);
        let at = first + at % (hi - first);
        let s = split(&p, i, at).unwrap();
        prop_assert_eq!(validate_partition(&s), Ok(()));
        prop_assert_eq!(merge(&s, i).unwrap(), p);
    }

    #[test]
    fn lookup_is_total_and_unique(cuts in prop::collection::vec(1u64.., 0..12), id in any::<u64>()) {
        let p = partition_from_cuts(cuts);
        prop_assert_eq!(validate_partition(&p), Ok(()));
        let i = lookup(&p, id).expect("a partition covers every id");
        prop_assert!(p[i].contains(id));
        prop_assert_eq!(p.iter().filter(|r| r.contains(id)).count(), 1);
    }
}
