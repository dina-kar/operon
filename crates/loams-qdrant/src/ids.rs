//! Point ids (Ruling 18): Qdrant's `ExtendedPointId` and Loams's
//! `PrimaryKey`.

use loams_collection::PrimaryKey;
use serde_json::Value;
use uuid::Uuid;

use crate::error::GatewayError;
use crate::proto::qdrant as pb;

/// A Qdrant point id. The derived order (numbers before UUIDs, then the
/// value) is Qdrant's `ExtendedPointId` order and the order of the
/// `PrimaryKey` canonical bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PointId {
    Num(u64),
    Uuid(Uuid),
}

impl PointId {
    /// A JSON id: an unsigned integer, or a string `Uuid::parse_str`
    /// accepts (hyphenated, simple, braced or URN, any case). Anything else,
    /// including a digit string or `1.0`, is Qdrant's format error.
    pub fn from_json(v: &Value) -> Result<Self, GatewayError> {
        match v {
            Value::Number(n) => n
                .as_u64()
                .map(PointId::Num)
                .ok_or_else(|| invalid_json_id(v)),
            Value::String(s) => Uuid::parse_str(s)
                .map(PointId::Uuid)
                .map_err(|_| invalid_json_id(v)),
            _ => Err(invalid_json_id(v)),
        }
    }

    /// A path id: digits are a number, else a UUID, else
    /// `Can not recognize "<s>" as point id`.
    pub fn parse_path(s: &str) -> Result<Self, GatewayError> {
        if let Ok(num) = s.parse::<u64>() {
            return Ok(PointId::Num(num));
        }
        Uuid::parse_str(s)
            .map(PointId::Uuid)
            .map_err(|_| GatewayError::BadRequest(format!("Can not recognize \"{s}\" as point id")))
    }

    pub fn to_pk(self) -> PrimaryKey {
        match self {
            PointId::Num(n) => PrimaryKey::U64(n),
            PointId::Uuid(uuid) => PrimaryKey::Uuid(*uuid.as_bytes()),
        }
    }
}

/// Qdrant's text for an invalid JSON id; the value is shown the way
/// Qdrant's `SerdeValue` shows it (strings unquoted, `null` as `Unit`).
fn invalid_json_id(v: &Value) -> GatewayError {
    let shown = match v {
        Value::String(s) => s.clone(),
        Value::Null => "Unit".to_string(),
        other => other.to_string(),
    };
    GatewayError::json(format!(
        "value {shown} is not a valid point ID, valid values are either an unsigned integer or a UUID"
    ))
}

/// A key as a JSON id: a number, a lowercase hyphenated UUID, or a string
/// key's own text.
pub fn pk_to_json(pk: &PrimaryKey) -> Value {
    match pk {
        PrimaryKey::U64(n) => Value::from(*n),
        PrimaryKey::Uuid(bytes) => Value::String(Uuid::from_bytes(*bytes).hyphenated().to_string()),
        PrimaryKey::Str(s) => Value::String(s.clone()),
    }
}

/// A key as a gRPC id; a string key travels as its text in `uuid`.
pub fn pk_to_grpc(pk: &PrimaryKey) -> pb::PointId {
    use pb::point_id::PointIdOptions;
    let options = match pk {
        PrimaryKey::U64(n) => PointIdOptions::Num(*n),
        PrimaryKey::Uuid(bytes) => {
            PointIdOptions::Uuid(Uuid::from_bytes(*bytes).hyphenated().to_string())
        }
        PrimaryKey::Str(s) => PointIdOptions::Uuid(s.clone()),
    };
    pb::PointId {
        point_id_options: Some(options),
    }
}

/// A gRPC id, with Qdrant's messages (`qdrant:lib/api/src/grpc/conversions.rs:1288-1305`).
pub fn point_id_from_grpc(id: &pb::PointId) -> Result<PointId, GatewayError> {
    use pb::point_id::PointIdOptions;
    match &id.point_id_options {
        Some(PointIdOptions::Num(n)) => Ok(PointId::Num(*n)),
        Some(PointIdOptions::Uuid(s)) => Uuid::parse_str(s)
            .map(PointId::Uuid)
            .map_err(|_| GatewayError::BadRequest(format!("Unable to parse UUID: {s}"))),
        None => Err(GatewayError::BadRequest(
            "No ID options provided".to_string(),
        )),
    }
}

/// The largest U64 or Uuid key below `pk` in canonical order: `U64(0)` has
/// none, the smallest UUID follows `U64(u64::MAX)`. String keys are never
/// point ids, so they get `None`.
pub fn pk_predecessor(pk: &PrimaryKey) -> Option<PrimaryKey> {
    match pk {
        PrimaryKey::U64(0) => None,
        PrimaryKey::U64(n) => Some(PrimaryKey::U64(n - 1)),
        PrimaryKey::Uuid(bytes) => match u128::from_be_bytes(*bytes) {
            0 => Some(PrimaryKey::U64(u64::MAX)),
            n => Some(PrimaryKey::Uuid((n - 1).to_be_bytes())),
        },
        PrimaryKey::Str(_) => None,
    }
}
