//! Qdrant's `Filter` and its conditions (Task 4). Unlike the rest of the
//! model, every struct here refuses unknown fields (Ruling 4): a typo in a
//! filter must not silently match everything.

use serde::Deserialize;
use serde_json::Value;

/// `{must?, should?, must_not?, min_should?}`; each list may also be a
/// single condition.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    #[serde(default)]
    pub must: Option<OneOrMany<Condition>>,
    #[serde(default)]
    pub should: Option<OneOrMany<Condition>>,
    #[serde(default)]
    pub must_not: Option<OneOrMany<Condition>>,
    #[serde(default)]
    pub min_should: Option<MinShould>,
}

/// A list, or one item standing for a list of one. `Many` is tried first:
/// serde would otherwise read a list as a struct in sequence form (row
/// T4-2).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    Many(Vec<T>),
    One(T),
}

impl<T> OneOrMany<T> {
    /// The items as a slice.
    pub fn as_slice(&self) -> &[T] {
        match self {
            OneOrMany::Many(items) => items,
            OneOrMany::One(item) => std::slice::from_ref(item),
        }
    }
}

/// At least `min_count` (≥ 1) of `conditions` match.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MinShould {
    pub conditions: Vec<Condition>,
    pub min_count: usize,
}

/// One condition, in Qdrant's untagged order.
#[allow(clippy::large_enum_variant)] // As Qdrant's own `Condition`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(
    untagged,
    deny_unknown_fields,
    expecting = "Expected some form of condition, which can be a field condition (like {\"key\": ..., \"match\": ... }), or some other mentioned in the documentation: https://qdrant.tech/documentation/concepts/filtering/#filtering-conditions"
)]
pub enum Condition {
    Field(FieldCondition),
    IsEmpty { is_empty: PayloadField },
    IsNull { is_null: PayloadField },
    HasId { has_id: Vec<Value> },
    HasVector { has_vector: String },
    Nested { nested: Value },
    Slice { slice: Value },
    Filter(Box<Filter>),
}

/// A condition on one payload key; several sub-conditions are OR-ed.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldCondition {
    pub key: String,
    #[serde(default)]
    pub r#match: Option<Match>,
    #[serde(default)]
    pub range: Option<RangeInterface>,
    #[serde(default)]
    pub values_count: Option<ValuesCount>,
    #[serde(default)]
    pub is_empty: Option<bool>,
    #[serde(default)]
    pub is_null: Option<bool>,
    #[serde(default)]
    pub geo_bounding_box: Option<Value>,
    #[serde(default)]
    pub geo_radius: Option<Value>,
    #[serde(default)]
    pub geo_polygon: Option<Value>,
}

/// The `match` forms.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Match {
    Value { value: MatchValue },
    Text { text: String },
    TextAny { text_any: String },
    Phrase { phrase: String },
    Prefix { prefix: String },
    Any { any: AnyVariants },
    Except { except: AnyVariants },
}

/// `match.value`: type-strict.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum MatchValue {
    Bool(bool),
    Int(i64),
    Str(String),
}

/// The list of `match.any` and `match.except`; `[]` is `Ints([])`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum AnyVariants {
    Ints(Vec<i64>),
    Strs(Vec<String>),
}

impl AnyVariants {
    /// Whether the list holds no value.
    pub fn is_empty(&self) -> bool {
        match self {
            AnyVariants::Ints(v) => v.is_empty(),
            AnyVariants::Strs(v) => v.is_empty(),
        }
    }
}

/// Numeric bounds, else datetime bounds.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum RangeInterface {
    Number(Range<f64>),
    Datetime(Range<String>),
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range<T> {
    #[serde(default = "none")]
    pub lt: Option<T>,
    #[serde(default = "none")]
    pub gt: Option<T>,
    #[serde(default = "none")]
    pub gte: Option<T>,
    #[serde(default = "none")]
    pub lte: Option<T>,
}

/// `None`, for `#[serde(default = …)]`.
fn none<T>() -> Option<T> {
    None
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValuesCount {
    #[serde(default)]
    pub lt: Option<u64>,
    #[serde(default)]
    pub gt: Option<u64>,
    #[serde(default)]
    pub gte: Option<u64>,
    #[serde(default)]
    pub lte: Option<u64>,
}

/// `{"key": …}` of `is_empty` and `is_null`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadField {
    pub key: String,
}
