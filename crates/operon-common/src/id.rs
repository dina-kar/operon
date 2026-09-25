use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Error returned when parsing an id that is not a canonical decimal number.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid id {0:?}: expected a decimal number without sign or leading zeros")]
pub struct ParseIdError(String);

/// Parses a canonical decimal `u64`: digits only, no sign, no leading zeros
/// (except `"0"` itself). Anything else is rejected so that every id has exactly
/// one spelling in object paths.
fn parse_canonical(s: &str) -> Result<u64, ParseIdError> {
    let canonical =
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && (s == "0" || !s.starts_with('0'));
    if !canonical {
        return Err(ParseIdError(s.to_string()));
    }
    s.parse().map_err(|_| ParseIdError(s.to_string()))
}

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                parse_canonical(s).map(Self)
            }
        }
    };
}

id_type!(
    /// Identifies a namespace, the unit of tenancy (design §01 §1).
    NamespaceId
);

id_type!(
    /// Identifies a stream. Unique across the cluster, not only within its namespace.
    StreamId
);

id_type!(
    /// Identifies a collection. Dense, allocated by the state machine (D18), never reused.
    CollectionId
);
