use operon_common::{NamespaceId, StreamId};
use operon_meta::{ApplyError, Command, MAX_NAME_LEN, MAX_PARTITIONS, MetaState, Reply, WalClass};

fn create_namespace(state: &mut MetaState, name: &str) -> Result<Reply, ApplyError> {
    state.apply(Command::CreateNamespace {
        name: name.to_string(),
    })
}

fn create_stream(
    state: &mut MetaState,
    namespace: NamespaceId,
    name: &str,
    partitions: u32,
) -> Result<Reply, ApplyError> {
    state.apply(Command::CreateStream {
        namespace,
        name: name.to_string(),
        partitions,
        class: WalClass::Standard,
    })
}

#[test]
fn namespaces_get_increasing_ids_starting_at_one() {
    let mut state = MetaState::default();
    assert_eq!(
        create_namespace(&mut state, "acme"),
        Ok(Reply::NamespaceCreated(NamespaceId(1)))
    );
    assert_eq!(
        create_namespace(&mut state, "globex"),
        Ok(Reply::NamespaceCreated(NamespaceId(2)))
    );

    let acme = state.namespace_by_name("acme").unwrap();
    assert_eq!(acme.id, NamespaceId(1));
    assert_eq!(state.namespace(NamespaceId(2)).unwrap().name, "globex");
    let names: Vec<&str> = state.namespaces().map(|n| n.name.as_str()).collect();
    assert_eq!(names, ["acme", "globex"]);
}

#[test]
fn duplicate_namespace_is_rejected_with_the_existing_id() {
    let mut state = MetaState::default();
    create_namespace(&mut state, "acme").unwrap();
    let before = state.clone();

    assert_eq!(
        create_namespace(&mut state, "acme"),
        Err(ApplyError::NamespaceExists(NamespaceId(1)))
    );
    assert_eq!(state, before);
}

#[test]
fn invalid_names_are_rejected_and_leave_state_unchanged() {
    let mut state = MetaState::default();
    let too_long = "a".repeat(MAX_NAME_LEN + 1);
    for bad in [
        "",
        ".",
        "..",
        "a/b",
        "a b",
        "ünïcode",
        "tab\t",
        too_long.as_str(),
    ] {
        let err = create_namespace(&mut state, bad).unwrap_err();
        assert!(
            matches!(err, ApplyError::InvalidArgument(_)),
            "{bad:?}: {err:?}"
        );
    }
    assert_eq!(state, MetaState::default());

    let longest = "a".repeat(MAX_NAME_LEN);
    for good in ["a", "A-z_0.9", "..a", longest.as_str()] {
        create_namespace(&mut state, good).unwrap();
    }
}

#[test]
fn streams_start_with_every_partition_at_offset_zero() {
    let mut state = MetaState::default();
    create_namespace(&mut state, "acme").unwrap();
    let ns = NamespaceId(1);

    assert_eq!(
        create_stream(&mut state, ns, "events", 3),
        Ok(Reply::StreamCreated(StreamId(1)))
    );

    let stream = state.stream_by_name(ns, "events").unwrap();
    assert_eq!(stream.id, StreamId(1));
    assert_eq!(stream.namespace, ns);
    assert_eq!(stream.partitions, 3);
    assert_eq!(stream.class, WalClass::Standard);
    for p in 0..3 {
        assert_eq!(state.partition(StreamId(1), p).unwrap().next_offset(), 0);
    }
    assert!(state.partition(StreamId(1), 3).is_none());
}

#[test]
fn stream_names_are_scoped_to_their_namespace_but_ids_are_global() {
    let mut state = MetaState::default();
    create_namespace(&mut state, "acme").unwrap();
    create_namespace(&mut state, "globex").unwrap();

    create_stream(&mut state, NamespaceId(1), "events", 1).unwrap();
    assert_eq!(
        create_stream(&mut state, NamespaceId(2), "events", 1),
        Ok(Reply::StreamCreated(StreamId(2)))
    );
    assert_eq!(
        create_stream(&mut state, NamespaceId(1), "events", 1),
        Err(ApplyError::StreamExists(StreamId(1)))
    );

    let acme: Vec<StreamId> = state.streams(NamespaceId(1)).map(|s| s.id).collect();
    assert_eq!(acme, [StreamId(1)]);
    assert_eq!(state.stream(StreamId(2)).unwrap().namespace, NamespaceId(2));
}

#[test]
fn invalid_streams_are_rejected_and_leave_state_unchanged() {
    let mut state = MetaState::default();
    create_namespace(&mut state, "acme").unwrap();
    let before = state.clone();

    assert_eq!(
        create_stream(&mut state, NamespaceId(9), "events", 1),
        Err(ApplyError::NamespaceNotFound(NamespaceId(9)))
    );
    for partitions in [0, MAX_PARTITIONS + 1] {
        let err = create_stream(&mut state, NamespaceId(1), "events", partitions).unwrap_err();
        assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    }
    let err = create_stream(&mut state, NamespaceId(1), "bad/name", 1).unwrap_err();
    assert!(matches!(err, ApplyError::InvalidArgument(_)), "{err:?}");
    assert_eq!(state, before);
}
