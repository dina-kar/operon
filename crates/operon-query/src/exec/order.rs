//! The effective sort (plan M1.2 Task 5 rule 1; Ruling 6, R10): the total
//! order every path ranks hits by, and `search_after` paging.

use std::cmp::Ordering;

use crate::error::ServiceError;
use crate::exec::schema::Ranked;
use crate::ir::{MissingOrder, Retriever, SearchRequest, SortKey, SortOrder, SortValue};

/// How candidates are ranked (Ruling 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RankMode {
    /// The retrievers' top-k lists, fused; the first key is the score.
    Score,
    /// Every match of `Text ∧ filter`, ordered by field or PK values.
    Field,
}

/// The effective sort of a request: its keys, ending with a `Pk` key, and
/// the ranking mode.
///
/// A [`Ranked`] hit carries one value in `sort` per `Field` key, in key
/// order; the score and PK keys read the hit's own score and key.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectiveSort {
    /// Ends with `Pk` (rule 1).
    pub keys: Vec<SortKey>,
    pub mode: RankMode,
}

fn apply(order: SortOrder, ordering: Ordering) -> Ordering {
    match order {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
}

/// The rank of a value's kind between kinds that do not compare.
fn kind_rank(value: &SortValue) -> u8 {
    match value {
        SortValue::Null => 0,
        SortValue::Bool(_) => 1,
        SortValue::I64(_) | SortValue::U64(_) | SortValue::F64(_) => 2,
        SortValue::Str(_) => 3,
        SortValue::Uuid(_) => 4,
    }
}

/// An integer value as `i128`.
fn int_of(value: &SortValue) -> Option<i128> {
    match value {
        SortValue::I64(n) => Some(i128::from(*n)),
        SortValue::U64(n) => Some(i128::from(*n)),
        _ => None,
    }
}

/// The ascending order of two non-null values: `Bool` false < true; numbers
/// numerically (integers exactly, a float against an integer through
/// `f64::total_cmp`); `Str` and `Uuid` bytewise.
pub fn compare_values(a: &SortValue, b: &SortValue) -> Ordering {
    match (a, b) {
        (SortValue::Bool(a), SortValue::Bool(b)) => a.cmp(b),
        (SortValue::Str(a), SortValue::Str(b)) => a.as_bytes().cmp(b.as_bytes()),
        (SortValue::Uuid(a), SortValue::Uuid(b)) => a.cmp(b),
        (SortValue::F64(a), SortValue::F64(b)) => a.total_cmp(b),
        (SortValue::F64(a), other) => match int_of(other) {
            Some(n) => a.total_cmp(&(n as f64)),
            None => kind_rank(&SortValue::F64(*a)).cmp(&kind_rank(other)),
        },
        (other, SortValue::F64(b)) => match int_of(other) {
            Some(n) => (n as f64).total_cmp(b),
            None => kind_rank(other).cmp(&kind_rank(&SortValue::F64(*b))),
        },
        _ => match (int_of(a), int_of(b)) {
            (Some(a), Some(b)) => a.cmp(&b),
            _ => kind_rank(a).cmp(&kind_rank(b)),
        },
    }
}

/// One field key's order: `Null` first or last per `missing` whatever the
/// order, the rest by value, reversed for `desc`.
fn compare_field(
    a: &SortValue,
    b: &SortValue,
    order: SortOrder,
    missing: MissingOrder,
) -> Ordering {
    let null_first = match missing {
        MissingOrder::First => Ordering::Less,
        MissingOrder::Last => Ordering::Greater,
    };
    match (a, b) {
        (SortValue::Null, SortValue::Null) => Ordering::Equal,
        (SortValue::Null, _) => null_first,
        (_, SortValue::Null) => null_first.reverse(),
        _ => apply(order, compare_values(a, b)),
    }
}

/// The score a `search_after` value at a score position names.
fn score_of(value: &SortValue) -> Option<f32> {
    match value {
        SortValue::F64(x) => Some(*x as f32),
        SortValue::I64(n) => Some(*n as f32),
        SortValue::U64(n) => Some(*n as f32),
        _ => None,
    }
}

impl EffectiveSort {
    /// The effective sort of `request` (rule 1):
    /// - an empty sort is `[Score desc]` with retrievers, else `[Pk asc]`;
    /// - a trailing `Pk asc` is appended unless the last key is `Pk`;
    /// - field mode allows at most one retriever, a text one.
    pub fn of(request: &SearchRequest) -> Result<Self, ServiceError> {
        let mut keys = request.sort.clone();
        let mode = match keys.first() {
            None if request.retrievers.is_empty() => RankMode::Field,
            None | Some(SortKey::Score { .. }) => RankMode::Score,
            Some(SortKey::Field { .. } | SortKey::Pk { .. }) => RankMode::Field,
        };
        if request.retrievers.is_empty() && keys.iter().any(|k| matches!(k, SortKey::Score { .. }))
        {
            return Err(ServiceError::InvalidArgument(
                "a score sort needs a retriever".to_string(),
            ));
        }
        if mode == RankMode::Field
            && (request.retrievers.len() > 1
                || request
                    .retrievers
                    .iter()
                    .any(|r| !matches!(r, Retriever::Text { .. })))
        {
            return Err(ServiceError::InvalidArgument(
                "a field sort allows at most one retriever, and it must be text".to_string(),
            ));
        }
        if keys.is_empty() {
            keys.push(match mode {
                RankMode::Score => SortKey::Score {
                    order: SortOrder::Desc,
                },
                RankMode::Field => SortKey::Pk {
                    order: SortOrder::Asc,
                },
            });
        }
        if !matches!(keys.last(), Some(SortKey::Pk { .. })) {
            keys.push(SortKey::Pk {
                order: SortOrder::Asc,
            });
        }
        Ok(Self { keys, mode })
    }

    /// `[Score desc, Pk asc]`: the order of a scored retriever.
    pub fn by_score() -> Self {
        Self {
            keys: vec![
                SortKey::Score {
                    order: SortOrder::Desc,
                },
                SortKey::Pk {
                    order: SortOrder::Asc,
                },
            ],
            mode: RankMode::Score,
        }
    }

    /// Whether some key reads the score.
    pub fn has_score(&self) -> bool {
        self.keys
            .iter()
            .any(|key| matches!(key, SortKey::Score { .. }))
    }

    /// The `Field` keys, in order: (field, order, missing).
    pub fn field_keys(&self) -> impl Iterator<Item = (&str, SortOrder, MissingOrder)> {
        self.keys.iter().filter_map(|key| match key {
            SortKey::Field {
                field,
                order,
                missing,
            } => Some((field.as_str(), *order, *missing)),
            _ => None,
        })
    }

    /// The total order of R10: key by key, the PK last.
    pub fn compare(&self, a: &Ranked, b: &Ranked) -> Ordering {
        self.compare_keys(a, b, true)
    }

    /// [`EffectiveSort::compare`] with every `Pk` key equal.
    pub fn compare_ignoring_pk(&self, a: &Ranked, b: &Ranked) -> Ordering {
        self.compare_keys(a, b, false)
    }

    fn compare_keys(&self, a: &Ranked, b: &Ranked, pk: bool) -> Ordering {
        let mut field = 0;
        for key in &self.keys {
            let ordering = match key {
                SortKey::Score { order } => apply(*order, a.score.total_cmp(&b.score)),
                SortKey::Pk { .. } if !pk => Ordering::Equal,
                SortKey::Pk { order } => apply(*order, a.pk.cmp(&b.pk)),
                SortKey::Field { order, missing, .. } => {
                    let ordering = compare_field(
                        a.sort.get(field).unwrap_or(&SortValue::Null),
                        b.sort.get(field).unwrap_or(&SortValue::Null),
                        *order,
                        *missing,
                    );
                    field += 1;
                    ordering
                }
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }

    /// Refuses a `search_after` that names more values than there are keys,
    /// or a PK position whose value is not a key.
    pub fn check_search_after(&self, search_after: &[SortValue]) -> Result<(), ServiceError> {
        if search_after.len() > self.keys.len() {
            return Err(ServiceError::InvalidArgument(format!(
                "search_after has {} values but the sort has {} keys",
                search_after.len(),
                self.keys.len()
            )));
        }
        for (key, value) in self.keys.iter().zip(search_after) {
            let valid = match key {
                SortKey::Pk { .. } => value.as_pk().is_some(),
                SortKey::Score { .. } => score_of(value).is_some(),
                SortKey::Field { .. } => true,
            };
            if !valid {
                return Err(ServiceError::InvalidArgument(format!(
                    "search_after value {value:?} does not fit its sort key"
                )));
            }
        }
        Ok(())
    }

    /// Whether `hit` comes after `search_after` (prefix semantics): the
    /// first `search_after.len()` keys of the hit compare `Greater` than the
    /// values. Equal prefixes are not after.
    pub fn is_after(&self, hit: &Ranked, search_after: &[SortValue]) -> bool {
        let mut field = 0;
        for (key, value) in self.keys.iter().zip(search_after) {
            let ordering = match key {
                SortKey::Score { order } => match score_of(value) {
                    Some(score) => apply(*order, hit.score.total_cmp(&score)),
                    None => Ordering::Greater,
                },
                SortKey::Pk { order } => match value.as_pk() {
                    Some(pk) => apply(*order, hit.pk.cmp(&pk)),
                    None => Ordering::Greater,
                },
                SortKey::Field { order, missing, .. } => {
                    let ordering = compare_field(
                        hit.sort.get(field).unwrap_or(&SortValue::Null),
                        value,
                        *order,
                        *missing,
                    );
                    field += 1;
                    ordering
                }
            };
            match ordering {
                Ordering::Greater => return true,
                Ordering::Less => return false,
                Ordering::Equal => {}
            }
        }
        false
    }

    /// One value per key, including the score and the PK.
    pub fn sort_values(&self, hit: &Ranked) -> Vec<SortValue> {
        let mut field = 0;
        self.keys
            .iter()
            .map(|key| match key {
                SortKey::Score { .. } => SortValue::F64(f64::from(hit.score)),
                SortKey::Pk { .. } => SortValue::from_pk(&hit.pk),
                SortKey::Field { .. } => {
                    let value = hit.sort.get(field).cloned().unwrap_or(SortValue::Null);
                    field += 1;
                    value
                }
            })
            .collect()
    }
}
