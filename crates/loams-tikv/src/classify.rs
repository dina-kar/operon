//! Error classification (R1 plan Task 2 semantics 1, row R9) and key scrubbing
//! (semantics 6).
//!
//! Only `Error::UndeterminedError` is an unknown outcome here; the runner adds
//! the other source, a commit it drops at its deadline. In the pinned
//! `tikv-client` every other `commit()` error means the transaction did not
//! commit (row R9).

use std::fmt::Write as _;

use tikv_client::Error;

/// The class of a `tikv-client` error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Class {
    // Declared from the weakest to the strongest: a compound error takes the
    // strongest class of its parts.
    /// Invalid arguments, misuse and unknown kinds.
    Fatal,
    /// The transaction did not commit (region, TSO, busy, network, …).
    NotApplied,
    /// The TSO stream is gone: not applied, and the client must be rebuilt.
    TsoClosed,
    /// gRPC's decoding limit (`OutOfRange`): the response was too large.
    TooLarge,
    /// A write conflict, `PessimisticRetry`, a lock the client's backoff did
    /// not outwait, or a deadlock: restart the transaction.
    Conflict,
    /// `KeyError.already_exist` from an insert.
    AlreadyExists,
    /// `Error::UndeterminedError`.
    Undetermined,
}

/// The `tikv-client` text of a closed TSO stream (`pd/timestamp.rs`).
pub(crate) const TSO_CLOSED: &str = "TimestampRequest channel is closed";

/// Classifies `e`.
pub(crate) fn classify(e: &Error) -> Class {
    match e {
        Error::UndeterminedError(_) => Class::Undetermined,
        Error::PessimisticLockError { inner, .. } => classify(inner),
        Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
            errors.iter().map(classify).max().unwrap_or(Class::Fatal)
        }
        Error::KeyError(ke) => {
            if ke.already_exist.is_some() {
                Class::AlreadyExists
            } else if ke.conflict.is_some()
                || ke.locked.is_some()
                || ke.deadlock.is_some()
                || !ke.retryable.is_empty()
            {
                Class::Conflict
            } else if ke.assertion_failed.is_some() {
                Class::Fatal
            } else if !ke.abort.is_empty()
                || ke.commit_ts_expired.is_some()
                || ke.txn_not_found.is_some()
                || ke.txn_lock_not_found.is_some()
                || ke.commit_ts_too_large.is_some()
                || ke.primary_mismatch.is_some()
            {
                Class::NotApplied
            } else {
                Class::Fatal
            }
        }
        Error::DuplicateKeyInsertion => Class::AlreadyExists,
        Error::ResolveLockError(_) => Class::Conflict,
        Error::GrpcAPI(status) if i32::from(status.code()) == GRPC_OUT_OF_RANGE => Class::TooLarge,
        Error::InternalError { message } | Error::StringError(message)
            if message.contains(TSO_CLOSED) =>
        {
            Class::TsoClosed
        }
        Error::GrpcAPI(_)
        | Error::Grpc(_)
        | Error::RegionError(_)
        | Error::Io(_)
        | Error::Channel(_)
        | Error::Canceled(_)
        | Error::JoinError(_)
        | Error::LeaderNotFound { .. }
        | Error::RegionForKeyNotFound { .. }
        | Error::RegionForRangeNotFound { .. }
        | Error::RegionNotFoundInResponse { .. }
        | Error::NoCurrentRegions
        | Error::EntryNotFoundInRegionCache
        | Error::KvError { .. }
        | Error::TxnNotFound(_)
        | Error::OnePcFailure
        | Error::InternalError { .. }
        | Error::StringError(_) => Class::NotApplied,
        _ => Class::Fatal,
    }
}

/// gRPC's `OutOfRange` status code (the decoding limit), compared as a number
/// because `tikv-client` uses its own `tonic`.
const GRPC_OUT_OF_RANGE: i32 = 11;

/// The error text of `e` with every key the scrubber recognises shown
/// relative to `root` (the keyspace prefix and root removed) and cut to 64
/// bytes.
pub(crate) fn describe(e: &Error, root: &[u8]) -> String {
    let text = match e {
        Error::UndeterminedError(inner) => {
            format!("commit outcome undetermined: {}", describe(inner, root))
        }
        Error::PessimisticLockError { inner, .. } => {
            format!("pessimistic lock: {}", describe(inner, root))
        }
        Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
            let mut out = format!("{} errors: ", errors.len());
            for (i, err) in errors.iter().take(3).enumerate() {
                if i > 0 {
                    out.push_str("; ");
                }
                out.push_str(&describe(err, root));
            }
            out
        }
        Error::KeyError(ke) => {
            if let Some(ae) = &ke.already_exist {
                format!("key already exists: {}", scrub_key(&ae.key, root))
            } else if let Some(c) = &ke.conflict {
                format!(
                    "write conflict (reason {}) on {}",
                    c.reason,
                    scrub_key(&c.key, root)
                )
            } else if let Some(lock) = &ke.locked {
                format!("key is locked: {}", scrub_key(&lock.key, root))
            } else if ke.deadlock.is_some() {
                "deadlock".to_string()
            } else if !ke.retryable.is_empty() {
                format!("retryable: {}", scrub_text(&ke.retryable, root))
            } else if !ke.abort.is_empty() {
                format!("aborted: {}", scrub_text(&ke.abort, root))
            } else if ke.commit_ts_expired.is_some() {
                "commit ts expired".to_string()
            } else if ke.txn_not_found.is_some() || ke.txn_lock_not_found.is_some() {
                "transaction not found".to_string()
            } else if ke.commit_ts_too_large.is_some() {
                "commit ts too large".to_string()
            } else if ke.assertion_failed.is_some() {
                "assertion failed".to_string()
            } else if ke.primary_mismatch.is_some() {
                "primary mismatch".to_string()
            } else {
                "key error".to_string()
            }
        }
        Error::RegionError(re) => format!("region error: {}", region_error_kind(re)),
        Error::RegionForKeyNotFound { key } => {
            format!("no region for key {}", scrub_key(key, root))
        }
        Error::RegionForRangeNotFound { .. } => "no region for a range".to_string(),
        Error::GrpcAPI(status) => format!(
            "gRPC {:?}: {}",
            status.code(),
            scrub_text(status.message(), root)
        ),
        other => scrub_text(&other.to_string(), root),
    };
    truncate_chars(text, 400)
}

fn region_error_kind(re: &tikv_client::ProtoRegionError) -> &'static str {
    if re.not_leader.is_some() {
        "not leader"
    } else if re.region_not_found.is_some() {
        "region not found"
    } else if re.key_not_in_region.is_some() {
        "key not in region"
    } else if re.epoch_not_match.is_some() {
        "epoch not match"
    } else if re.server_is_busy.is_some() {
        "server is busy"
    } else if re.stale_command.is_some() {
        "stale command"
    } else if re.store_not_match.is_some() {
        "store not match"
    } else if re.raft_entry_too_large.is_some() {
        "raft entry too large"
    } else if re.max_timestamp_not_synced.is_some() {
        "max timestamp not synced"
    } else if re.read_index_not_ready.is_some() {
        "read index not ready"
    } else if re.proposal_in_merging_mode.is_some() {
        "proposal in merging mode"
    } else if re.data_is_not_ready.is_some() {
        "data is not ready"
    } else if re.region_not_initialized.is_some() {
        "region not initialized"
    } else if re.disk_full.is_some() {
        "disk full"
    } else if re.undetermined_result.is_some() {
        "undetermined result"
    } else {
        "other"
    }
}

/// The largest key prefix shown.
const KEY_SHOWN: usize = 64;

/// A key as errors show it: without the API v2 keyspace prefix (`x` and the
/// 3-byte keyspace id) and the root, escaped, and cut to 64 bytes.
pub(crate) fn scrub_key(key: &[u8], root: &[u8]) -> String {
    let mut k = key;
    if k.len() >= 4 && k[0] == b'x' {
        k = &k[4..];
    }
    if !root.is_empty()
        && let Some(rest) = k.strip_prefix(root)
    {
        k = rest;
    }
    let shown = &k[..k.len().min(KEY_SHOWN)];
    let mut out = format!("\"{}\"", shown.escape_ascii());
    if k.len() > KEY_SHOWN {
        let _ = write!(out, "… ({} bytes)", k.len());
    }
    out
}

/// Replaces every `Debug`-printed byte list of at least four bytes in `text`
/// (`[120, 0, 0, 4, …]`, how `tikv-client` prints keys) with [`scrub_key`].
pub(crate) fn scrub_text(text: &str, root: &[u8]) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find('[') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after
            .find(']')
            .and_then(|close| parse_byte_list(&after[..close]).map(|bytes| (close, bytes)))
        {
            Some((close, bytes)) if bytes.len() >= 4 => {
                out.push_str(&scrub_key(&bytes, root));
                rest = &after[close + 1..];
            }
            _ => {
                out.push('[');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

fn parse_byte_list(s: &str) -> Option<Vec<u8>> {
    s.split(',')
        .map(|part| part.trim().parse::<u8>().ok())
        .collect()
}

fn truncate_chars(text: String, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &text[..i]),
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_error(f: impl FnOnce(&mut tikv_client::ProtoKeyError)) -> Error {
        let mut ke = tikv_client::ProtoKeyError::default();
        f(&mut ke);
        Error::KeyError(Box::new(ke))
    }

    #[test]
    fn classification_follows_row_r9() {
        let conflict = key_error(|ke| ke.conflict = Some(Default::default()));
        assert_eq!(classify(&conflict), Class::Conflict);
        let retry = key_error(|ke| {
            ke.conflict = Some(Default::default());
            if let Some(c) = ke.conflict.as_mut() {
                c.reason = 2; // PessimisticRetry
            }
        });
        assert_eq!(
            classify(&Error::PessimisticLockError {
                inner: Box::new(retry),
                success_keys: vec![],
            }),
            Class::Conflict
        );
        assert_eq!(
            classify(&key_error(|ke| ke.locked = Some(Default::default()))),
            Class::Conflict
        );
        assert_eq!(
            classify(&key_error(|ke| ke.already_exist = Some(Default::default()))),
            Class::AlreadyExists
        );
        assert_eq!(
            classify(&Error::UndeterminedError(Box::new(Error::StringError(
                "x".into()
            )))),
            Class::Undetermined
        );
        assert_eq!(
            classify(&Error::InternalError {
                message: format!("[pd/timestamp.rs:72]: {TSO_CLOSED}")
            }),
            Class::TsoClosed
        );
        assert_eq!(
            classify(&Error::RegionError(Box::default())),
            Class::NotApplied
        );
        assert_eq!(
            classify(&Error::InternalError {
                message: "pd unavailable".into()
            }),
            Class::NotApplied
        );
        assert_eq!(classify(&Error::NoPrimaryKey), Class::Fatal);
        assert_eq!(classify(&Error::InvalidTransactionType), Class::Fatal);
        assert_eq!(
            classify(&Error::MultipleKeyErrors(vec![
                Error::StringError("busy".into()),
                key_error(|ke| ke.conflict = Some(Default::default())),
            ])),
            Class::Conflict
        );
    }

    #[test]
    fn keys_are_scrubbed_of_keyspace_and_root() {
        let root = b"ROOT".as_slice();
        let mut key = vec![b'x', 0, 0, 4];
        key.extend_from_slice(root);
        key.extend_from_slice(b"k/1");
        assert_eq!(scrub_key(&key, root), "\"k/1\"");
        let long = vec![b'a'; 100];
        let shown = scrub_key(&long, root);
        assert!(
            shown.starts_with(&format!("\"{}\"", "a".repeat(64))),
            "{shown}"
        );
        assert!(shown.ends_with("(100 bytes)"), "{shown}");

        let listed = format!("Region is not found for key: {key:?}");
        let text = scrub_text(&listed, root);
        assert_eq!(text, "Region is not found for key: \"k/1\"");
        assert_eq!(
            scrub_text("keep [1, 2] and [a]", root),
            "keep [1, 2] and [a]"
        );

        let e = key_error(|ke| {
            ke.already_exist = Some(Default::default());
            if let Some(ae) = ke.already_exist.as_mut() {
                ae.key = key.clone();
            }
        });
        assert_eq!(describe(&e, root), "key already exists: \"k/1\"");
        let e = Error::RegionForKeyNotFound { key };
        assert!(!describe(&e, root).contains("ROOT"));
    }
}
