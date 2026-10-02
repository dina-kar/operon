//! The mock's fake credentials (documented in the crate docs).
//!
//! `Bearer mock-access-<principal>` is a session authenticated now;
//! `Bearer mock-stale-<principal>` one authenticated [`STALE_AGE`] ago.
//! The principal must exist in the seed.

use std::time::{Duration, SystemTime};

use connectrpc::{ConnectError, ErrorCode, RequestContext};

use crate::proto::loams::instance::v1::Principal;
use crate::refuse;
use crate::seed::Seed;

/// How old a `mock-stale-*` session is: past the 5-minute step-up window.
pub(crate) const STALE_AGE: Duration = Duration::from_secs(10 * 60);

/// The authenticated caller of an RPC.
#[derive(Debug, Clone)]
pub(crate) struct Caller {
    pub(crate) principal: Principal,
    pub(crate) authenticated_at: SystemTime,
}

/// Resolves the caller from the request's bearer token.
pub(crate) fn caller(seed: &Seed, ctx: &RequestContext) -> Result<Caller, ConnectError> {
    let header = ctx
        .header(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ConnectError::unauthenticated("a bearer token is required"))?;
    caller_from_header(seed, header, SystemTime::now())
}

pub(crate) fn caller_from_header(
    seed: &Seed,
    header: &str,
    now: SystemTime,
) -> Result<Caller, ConnectError> {
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| ConnectError::unauthenticated("expected a bearer token"))?;
    let (id, authenticated_at) = if let Some(id) = token.strip_prefix("mock-access-") {
        (id, now)
    } else if let Some(id) = token.strip_prefix("mock-stale-") {
        (id, now.checked_sub(STALE_AGE).unwrap_or(now))
    } else {
        return Err(ConnectError::unauthenticated(
            "unknown token: the mock accepts mock-access-<principal> and mock-stale-<principal>",
        ));
    };
    let principal = seed
        .principal(id)
        .ok_or_else(|| ConnectError::unauthenticated(format!("no seed principal `{id}`")))?;
    if seed.revoked_principals.iter().any(|p| p == id) {
        return Err(refuse(
            ErrorCode::Unauthenticated,
            "device_revoked",
            "this device was revoked",
        ));
    }
    Ok(Caller {
        principal,
        authenticated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_and_stale_tokens_resolve_seed_principals() {
        let seed = Seed::demo();
        let now = SystemTime::now();
        let fresh = caller_from_header(&seed, "Bearer mock-access-usr_omar", now).unwrap();
        assert_eq!(fresh.principal.id, "usr_omar");
        assert_eq!(fresh.authenticated_at, now);
        let stale = caller_from_header(&seed, "Bearer mock-stale-usr_omar", now).unwrap();
        assert_eq!(
            now.duration_since(stale.authenticated_at).unwrap(),
            STALE_AGE
        );
    }

    #[test]
    fn unknown_tokens_and_principals_are_unauthenticated() {
        let seed = Seed::demo();
        let now = SystemTime::now();
        for header in [
            "Bearer real-looking-token",
            "Basic abc",
            "Bearer mock-access-nobody",
        ] {
            let err = caller_from_header(&seed, header, now).unwrap_err();
            assert_eq!(err.code, ErrorCode::Unauthenticated, "{header}");
        }
    }
}
