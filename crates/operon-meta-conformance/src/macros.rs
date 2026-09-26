//! The one list of cases, and the macros generated from it.

/// Calls the macro named in brackets with its extra arguments in
/// parentheses, followed by every case name in suite order. [`CASES`] and
/// [`metastore_conformance!`] are both generated through it, so they cannot
/// drift apart.
///
/// [`CASES`]: crate::CASES
#[doc(hidden)]
#[macro_export]
macro_rules! for_each_case {
    ([$($callback:tt)+] $($args:tt)*) => {
        $($callback)+! {
            ($($args)*)
            // catalog
            namespace_create_then_lookup,
            namespace_create_retry_reports_namespace_exists,
            stream_create_validates_names_partitions_and_reserved_prefix,
            stream_state_reports_bounds_per_partition,
            set_retention_round_trips,
            link_create_lookup_and_retry,
            lists_are_ordered_by_id,
            // sequencer
            commit_wal_assigns_dense_offsets_per_partition,
            commit_wal_retry_returns_the_first_offsets,
            a_wal_object_older_than_the_commit_window_is_stale,
            partition_index_pages_by_bytes,
            swap_segment_replaces_wal_entries_and_retires_objects,
            swap_segment_against_a_moved_index_is_index_mismatch,
            a_segment_past_its_freshness_is_stale_object,
            trim_moves_the_log_start_and_retires_whole_entries,
            // leases
            acquire_renew_release,
            a_held_lease_refuses_another_owner,
            renew_at_a_stale_epoch_is_lease_lost,
            reacquire_after_expiry_keeps_the_epoch,
            takeover_bumps_the_epoch_and_fences_the_old_holder,
            a_ttl_above_the_limit_is_invalid,
            leases_with_prefix_lists_only_that_prefix,
            // pointers
            cas_create_then_update,
            cas_mismatch_carries_the_current_pointer,
            a_fenced_cas_is_refused,
            a_stale_cas_is_refused,
            a_collection_pointer_needs_its_collection,
            a_key_over_the_limit_is_invalid,
            // collections
            create_collection_makes_its_stream_and_link,
            create_collection_retry_is_collection_exists,
            drop_frees_the_name_and_retires_both_prefixes,
            schema_updates_are_additive_and_versioned,
            aliases_apply_atomically_and_resolve,
            collection_head_reads_pointer_bounds_and_clock,
            collection_for_link_finds_the_implicit_link,
            // hot
            collection_hot_defaults_and_set_is_retry_safe,
            collection_hot_needs_the_collection_in_its_namespace,
            a_dropped_collection_forgets_its_hot_config,
            // gc
            retired_expired_respects_grace,
            forget_objects_removes_retired_entries,
            orphan_wal_objects_skips_live_retired_and_young,
            orphan_segments_skips_indexed_retired_and_young,
            segment_referenced_sees_the_index_and_retired_set,
            collection_roots_lists_prefixes_under_a_path,
            prune_wal_commits_is_fenced,
            // changes
            changes_wake_a_waiter_armed_before_a_write,
            is_ready_after_start,
            a_linearizable_read_through_another_client_sees_an_acknowledged_write,
            // faults
            a_lost_ack_on_commit_wal_returns_the_first_offsets_and_flags_it,
            a_lost_ack_on_create_collection_returns_the_same_ids,
            a_lost_ack_on_cas_reports_a_mismatch_with_the_callers_value,
            // linearizable
            concurrent_cas_on_two_keys_is_linearizable,
            concurrent_wal_commits_are_linearizable,
        }
    };
}

/// [`for_each_case!`] callback: the case names as a `&[&str]`.
#[doc(hidden)]
#[macro_export]
macro_rules! __case_names {
    (() $($case:ident),* $(,)?) => {
        &[$(stringify!($case)),*]
    };
}

/// [`for_each_case!`] callback: one test per case against `$backend`.
#[doc(hidden)]
#[macro_export]
macro_rules! __case_tests {
    (($backend:expr) $($case:ident),* $(,)?) => {
        $(
            #[::tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $case() {
                $crate::bounded(stringify!($case), $crate::suite::$case(&$backend)).await
            }
        )*
    };
}

/// Expands the whole suite against `$backend` (an expression of a type
/// implementing [`Backend`](crate::Backend)): one
/// `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` per entry of
/// [`CASES`](crate::CASES), named after the case. Expand it inside a module
/// of its own per backend.
#[macro_export]
macro_rules! metastore_conformance {
    ($backend:expr) => {
        $crate::for_each_case!([$crate::__case_tests] $backend);
    };
}
