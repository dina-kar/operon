//! The linearizability checker against hand-written histories.

use operon_sim::linearizability::{
    CasInput, CasOutput, CasRegisterModel, Op, Outcome, SequencerInput, SequencerModel,
    SequencerOutput, check,
};

fn cas(
    client: u32,
    invoke: u64,
    complete: u64,
    expected: Option<u64>,
    value: &str,
    out: Option<CasOutput>,
) -> Op<CasInput, CasOutput> {
    let indeterminate = out.is_none();
    Op {
        client,
        invoke,
        complete: if indeterminate { u64::MAX } else { complete },
        input: CasInput::Cas {
            expected,
            value: value.to_string(),
        },
        outcome: out.map_or(Outcome::Indeterminate, Outcome::Ok),
    }
}

fn read(
    client: u32,
    invoke: u64,
    complete: u64,
    seen: Option<(u64, &str)>,
) -> Op<CasInput, CasOutput> {
    Op {
        client,
        invoke,
        complete,
        input: CasInput::Read,
        outcome: Outcome::Ok(CasOutput::Read(seen.map(|(v, s)| (v, s.to_string())))),
    }
}

fn commit(
    client: u32,
    invoke: u64,
    complete: u64,
    object: &str,
    records: u32,
    base: Option<u64>,
) -> Op<SequencerInput, SequencerOutput> {
    Op {
        client,
        invoke,
        complete: if base.is_some() { complete } else { u64::MAX },
        input: SequencerInput::Commit {
            object: object.to_string(),
            records,
        },
        outcome: base.map_or(Outcome::Indeterminate, |b| {
            Outcome::Ok(SequencerOutput::BaseOffset(b))
        }),
    }
}

fn hwm(client: u32, invoke: u64, complete: u64, value: u64) -> Op<SequencerInput, SequencerOutput> {
    Op {
        client,
        invoke,
        complete,
        input: SequencerInput::ReadHwm,
        outcome: Outcome::Ok(SequencerOutput::Hwm(value)),
    }
}

#[test]
fn an_empty_history_is_linearizable() {
    assert!(check(CasRegisterModel::default(), &[]).is_ok());
}

#[test]
fn sequential_register_operations_are_linearizable() {
    let history = [
        read(1, 0, 1, None),
        cas(1, 2, 3, None, "a", Some(CasOutput::Ok(1))),
        read(2, 4, 5, Some((1, "a"))),
        cas(2, 6, 7, Some(1), "b", Some(CasOutput::Ok(2))),
        cas(
            1,
            8,
            9,
            Some(1),
            "c",
            Some(CasOutput::Mismatch(Some((2, "b".to_string())))),
        ),
        read(1, 10, 11, Some((2, "b"))),
    ];
    assert!(check(CasRegisterModel::default(), &history).is_ok());
}

#[test]
fn concurrent_operations_may_take_effect_in_either_order() {
    // Two overlapping CASes from version 0; one wins. The read overlaps both
    // and sees the loser's attempt fail after the winner.
    let history = [
        cas(
            1,
            0,
            10,
            None,
            "a",
            Some(CasOutput::Mismatch(Some((1, "b".to_string())))),
        ),
        cas(2, 1, 9, None, "b", Some(CasOutput::Ok(1))),
        read(3, 2, 8, None),
        read(3, 11, 12, Some((1, "b"))),
    ];
    assert!(check(CasRegisterModel::default(), &history).is_ok());
}

#[test]
fn a_stale_read_is_not_linearizable() {
    let history = [
        cas(1, 0, 1, None, "a", Some(CasOutput::Ok(1))),
        // Starts after the write completed, but sees nothing.
        read(2, 2, 3, None),
    ];
    let err = check(CasRegisterModel::default(), &history).unwrap_err();
    assert!(err.message.contains("not linearizable"), "{err}");
}

#[test]
fn a_lost_update_is_not_linearizable() {
    // Both CASes from version 0 claim success.
    let history = [
        cas(1, 0, 5, None, "a", Some(CasOutput::Ok(1))),
        cas(2, 1, 6, None, "b", Some(CasOutput::Ok(1))),
    ];
    assert!(check(CasRegisterModel::default(), &history).is_err());
}

#[test]
fn a_duplicate_offset_is_not_linearizable() {
    let history = [
        commit(1, 0, 5, "w1", 3, Some(0)),
        commit(2, 1, 6, "w2", 2, Some(0)),
    ];
    assert!(check(SequencerModel::default(), &history).is_err());
}

#[test]
fn a_retried_commit_gets_its_first_offsets_back() {
    let history = [
        commit(1, 0, 1, "w1", 3, Some(0)),
        commit(1, 2, 3, "w1", 3, Some(0)),
        commit(2, 4, 5, "w2", 2, Some(3)),
        hwm(3, 6, 7, 5),
    ];
    assert!(check(SequencerModel::default(), &history).is_ok());
    // A retry that got new offsets committed the object twice.
    let doubled = [
        commit(1, 0, 1, "w1", 3, Some(0)),
        commit(1, 2, 3, "w1", 3, Some(3)),
    ];
    assert!(check(SequencerModel::default(), &doubled).is_err());
}

#[test]
fn a_gap_in_offsets_is_not_linearizable() {
    let history = [commit(1, 0, 1, "w1", 3, Some(0)), hwm(2, 2, 3, 4)];
    assert!(check(SequencerModel::default(), &history).is_err());
}

/// Review focus 5: an indeterminate operation that must have taken effect.
#[test]
fn an_indeterminate_operation_may_have_been_applied() {
    let history = [cas(1, 0, 0, None, "a", None), read(2, 5, 6, Some((1, "a")))];
    assert!(check(CasRegisterModel::default(), &history).is_ok());
    let history = [
        commit(1, 0, 0, "w1", 3, None),
        hwm(2, 5, 6, 3),
        commit(2, 7, 8, "w2", 1, Some(3)),
    ];
    assert!(check(SequencerModel::default(), &history).is_ok());
}

/// Review focus 5: an indeterminate operation that must not have taken
/// effect (yet).
#[test]
fn an_indeterminate_operation_may_have_not_been_applied() {
    let history = [
        cas(1, 0, 0, None, "a", None),
        cas(2, 5, 6, None, "b", Some(CasOutput::Ok(1))),
        read(2, 7, 8, Some((1, "b"))),
    ];
    assert!(check(CasRegisterModel::default(), &history).is_ok());
    let history = [
        commit(1, 0, 0, "w1", 3, None),
        hwm(2, 5, 6, 0),
        commit(2, 7, 8, "w2", 2, Some(0)),
    ];
    assert!(check(SequencerModel::default(), &history).is_ok());
}

/// An indeterminate operation takes effect at most once and no earlier than
/// its invocation; it cannot explain everything.
#[test]
fn indeterminate_operations_do_not_hide_real_violations() {
    // A value nobody wrote.
    let history = [cas(1, 0, 0, None, "a", None), read(2, 5, 6, Some((1, "c")))];
    assert!(check(CasRegisterModel::default(), &history).is_err());
    // Seen before it was invoked.
    let history = [read(2, 0, 1, Some((1, "a"))), cas(1, 5, 0, None, "a", None)];
    assert!(check(CasRegisterModel::default(), &history).is_err());
    // Applied, then un-applied.
    let history = [
        commit(1, 0, 0, "w1", 3, None),
        hwm(2, 5, 6, 3),
        hwm(2, 7, 8, 0),
    ];
    assert!(check(SequencerModel::default(), &history).is_err());
    // Applied twice.
    let history = [commit(1, 0, 0, "w1", 3, None), hwm(2, 5, 6, 6)];
    assert!(check(SequencerModel::default(), &history).is_err());
}

#[test]
fn a_few_hundred_concurrent_operations_check_quickly() {
    // Four clients, each committing 60 objects of one record, in overlapping
    // windows; offsets in completion order.
    let mut history = Vec::new();
    let mut offset = 0;
    for i in 0..240u64 {
        let invoke = i * 10;
        history.push(commit(
            u32::try_from(i % 4).unwrap(),
            invoke,
            invoke + 35,
            &format!("w{i}"),
            1,
            Some(offset),
        ));
        offset += 1;
        if i % 20 == 0 {
            history.push(hwm(9, invoke + 1, invoke + 2, offset - 1));
        }
    }
    let started = std::time::Instant::now();
    assert!(check(SequencerModel::default(), &history).is_ok());
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}
