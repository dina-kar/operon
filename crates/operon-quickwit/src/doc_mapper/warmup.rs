// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-doc-mapper/src/doc_mapper/mod.rs lines 65-183 and tests 619-827); modified for Operon: cut into a module of its own, with its imports and a test module.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use tantivy::Term;
use tantivy::schema::Field;

/// Bounds for a range of terms, with an optional max count of terms being matched.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TermRange {
    /// Start of the range
    pub start: Bound<Term>,
    /// End of the range
    pub end: Bound<Term>,
    /// Max number of matched terms
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Supported automaton types to warmup
pub enum Automaton {
    /// A regex in it's str representation as tantivy_fst::Regex isn't PartialEq, and the path if
    /// inside a json field
    Regex(Option<Vec<u8>>, String),
    // we could add termset query here, instead of downloading the whole dictionary
}

/// Description of how a fast field should be warmed up
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FastFieldWarmupInfo {
    /// Name of the fast field
    pub name: String,
    /// Whether subfields should also be loaded for warmup
    pub with_subfields: bool,
}

/// Information about what a DocMapper think should be warmed up before
/// running the query.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WarmupInfo {
    /// Name of fields from the term dictionary and posting list which needs to
    /// be entirely loaded
    pub term_dict_fields: HashSet<Field>,
    /// Fast fields which needs to be loaded
    pub fast_fields: HashSet<FastFieldWarmupInfo>,
    /// Whether to warmup field norms. Used mostly for scoring.
    pub field_norms: bool,
    /// Terms to warmup, and whether their position is needed too.
    pub terms_grouped_by_field: HashMap<Field, HashMap<Term, bool>>,
    /// Term ranges to warmup, and whether their position is needed too.
    pub term_ranges_grouped_by_field: HashMap<Field, HashMap<TermRange, bool>>,
    /// Automatons to warmup
    pub automatons_grouped_by_field: HashMap<Field, HashSet<Automaton>>,
    /// Terms that must all be present for the query to match any document.
    ///
    /// If any of these terms has an empty posting list in a split, the query
    /// provably matches nothing there, so the leaf search can abort warmup
    /// early. This is a conservative subset (see
    /// `crate::query::query_ast::required_terms`).
    pub required_terms: HashSet<Term>,
}

impl WarmupInfo {
    /// Merge other WarmupInfo into self.
    pub fn merge(&mut self, other: WarmupInfo) {
        self.term_dict_fields.extend(other.term_dict_fields);
        self.field_norms |= other.field_norms;

        for fast_field_warmup_info in other.fast_fields.into_iter() {
            // avoid overwriting with a less demanding warmup
            if !self.fast_fields.contains(&FastFieldWarmupInfo {
                name: fast_field_warmup_info.name.clone(),
                with_subfields: true,
            }) {
                self.fast_fields.insert(fast_field_warmup_info);
            }
        }

        for (field, term_and_pos) in other.terms_grouped_by_field.into_iter() {
            let sub_map = self.terms_grouped_by_field.entry(field).or_default();

            for (term, include_position) in term_and_pos.into_iter() {
                *sub_map.entry(term).or_default() |= include_position;
            }
        }

        // this merge is suboptimal in case of overlapping range with no limit.
        for (field, term_range_and_pos) in other.term_ranges_grouped_by_field.into_iter() {
            let sub_map = self.term_ranges_grouped_by_field.entry(field).or_default();

            for (term_range, include_position) in term_range_and_pos.into_iter() {
                *sub_map.entry(term_range).or_default() |= include_position;
            }
        }

        for (field, automatons) in other.automatons_grouped_by_field.into_iter() {
            let sub_map = self.automatons_grouped_by_field.entry(field).or_default();
            sub_map.extend(automatons);
        }

        // Required terms come from the query; a collector's `WarmupInfo` carries
        // none, so this union simply preserves the query's set.
        self.required_terms.extend(other.required_terms);
    }

    /// Simplify a WarmupInfo, removing some redundant tasks
    pub fn simplify(&mut self) {
        self.terms_grouped_by_field.retain(|field, terms| {
            if self.term_dict_fields.contains(field) {
                // we are already about to full-load this dictionary. We only care about terms
                // which needs additional position
                terms.retain(|_term, include_position| *include_position);
            }
            // if no term is left, remove the entry from the hashmap
            !terms.is_empty()
        });
        self.term_ranges_grouped_by_field.retain(|field, terms| {
            if self.term_dict_fields.contains(field) {
                terms.retain(|_term, include_position| *include_position);
            }
            !terms.is_empty()
        });
        // TODO we could remove from terms_grouped_by_field for ranges with no `limit` in
        // term_ranges_grouped_by_field
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hashset_fast(elements: &[&str]) -> HashSet<FastFieldWarmupInfo> {
        elements
            .iter()
            .map(|elem| FastFieldWarmupInfo {
                name: elem.to_string(),
                with_subfields: false,
            })
            .collect()
    }

    fn automaton_hashset(elements: &[&str]) -> HashSet<Automaton> {
        elements
            .iter()
            .map(|elem| Automaton::Regex(None, elem.to_string()))
            .collect()
    }

    fn hashset_field(elements: &[u32]) -> HashSet<Field> {
        elements
            .iter()
            .map(|elem| Field::from_field_id(*elem))
            .collect()
    }

    fn hashmap(elements: &[(u32, &str, bool)]) -> HashMap<Field, HashMap<Term, bool>> {
        let mut result: HashMap<Field, HashMap<Term, bool>> = HashMap::new();
        for (field, term, pos) in elements {
            let field = Field::from_field_id(*field);
            *result
                .entry(field)
                .or_default()
                .entry(Term::from_field_text(field, term))
                .or_default() |= pos;
        }

        result
    }

    fn hashmap_ranges(elements: &[(u32, &str, bool)]) -> HashMap<Field, HashMap<TermRange, bool>> {
        let mut result: HashMap<Field, HashMap<TermRange, bool>> = HashMap::new();
        for (field, term, pos) in elements {
            let field = Field::from_field_id(*field);
            let term = Term::from_field_text(field, term);
            // this is a 1 element bound, but it's enough for testing.
            let range = TermRange {
                start: Bound::Included(term.clone()),
                end: Bound::Included(term),
                limit: None,
            };
            *result.entry(field).or_default().entry(range).or_default() |= pos;
        }

        result
    }

    #[test]
    fn test_warmup_info_merge() {
        let wi_base = WarmupInfo {
            term_dict_fields: hashset_field(&[1, 2]),
            fast_fields: hashset_fast(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[(1, "term1", false), (1, "term2", false)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (2, "term1", false),
                (2, "term2", false),
            ]),
            automatons_grouped_by_field: [(
                Field::from_field_id(1),
                automaton_hashset(&["my_reg.*ex"]),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        // merging with default has no impact
        let mut wi_cloned = wi_base.clone();
        wi_cloned.merge(WarmupInfo::default());
        assert_eq!(wi_cloned, wi_base);

        let mut wi_base = wi_base;
        let wi_2 = WarmupInfo {
            term_dict_fields: hashset_field(&[2, 3]),
            fast_fields: hashset_fast(&["fast2", "fast3"]),
            field_norms: true,
            terms_grouped_by_field: hashmap(&[(2, "term1", false), (1, "term2", true)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (3, "term1", false),
                (2, "term2", true),
            ]),
            automatons_grouped_by_field: [
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(2), automaton_hashset(&["my_reg.*ex"])),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        wi_base.merge(wi_2.clone());

        assert_eq!(wi_base.term_dict_fields, hashset_field(&[1, 2, 3]));
        assert_eq!(
            wi_base.fast_fields,
            hashset_fast(&["fast1", "fast2", "fast3"])
        );
        assert!(wi_base.field_norms);

        let expected_terms = [(1, "term1", false), (1, "term2", true), (2, "term1", false)];
        for (field, term, pos) in expected_terms {
            let field = Field::from_field_id(field);
            let term = Term::from_field_text(field, term);

            assert_eq!(
                *wi_base
                    .terms_grouped_by_field
                    .get(&field)
                    .unwrap()
                    .get(&term)
                    .unwrap(),
                pos
            );
        }

        let expected_ranges = [(2, "term1", false), (2, "term2", true), (3, "term1", false)];
        for (field, term, pos) in expected_ranges {
            let field = Field::from_field_id(field);
            let term = Term::from_field_text(field, term);
            let range = TermRange {
                start: Bound::Included(term.clone()),
                end: Bound::Included(term),
                limit: None,
            };

            assert_eq!(
                *wi_base
                    .term_ranges_grouped_by_field
                    .get(&field)
                    .unwrap()
                    .get(&range)
                    .unwrap(),
                pos
            );
        }

        let expected_automatons = [(1, "my_reg.*ex"), (1, "other-re.ex"), (2, "my_reg.*ex")];
        for (field, regex) in expected_automatons {
            let field = Field::from_field_id(field);
            let automaton = Automaton::Regex(None, regex.to_string());
            assert!(
                wi_base
                    .automatons_grouped_by_field
                    .get(&field)
                    .unwrap()
                    .contains(&automaton)
            );
        }

        // merge is idempotent
        let mut wi_cloned = wi_base.clone();
        wi_cloned.merge(wi_2);
        assert_eq!(wi_cloned, wi_base);
    }

    #[test]
    fn test_warmup_info_simplify() {
        let mut warmup_info = WarmupInfo {
            term_dict_fields: hashset_field(&[1]),
            fast_fields: hashset_fast(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[
                (1, "term1", false),
                (1, "term2", true),
                (2, "term3", false),
            ]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (1, "term1", false),
                (1, "term2", true),
                (2, "term3", false),
            ]),
            automatons_grouped_by_field: [
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(2), automaton_hashset(&["my_reg.ex"])),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let expected = WarmupInfo {
            term_dict_fields: hashset_field(&[1]),
            fast_fields: hashset_fast(&["fast1", "fast2"]),
            field_norms: false,
            terms_grouped_by_field: hashmap(&[(1, "term2", true), (2, "term3", false)]),
            term_ranges_grouped_by_field: hashmap_ranges(&[
                (1, "term2", true),
                (2, "term3", false),
            ]),
            automatons_grouped_by_field: [
                (Field::from_field_id(1), automaton_hashset(&["other-re.ex"])),
                (Field::from_field_id(2), automaton_hashset(&["my_reg.ex"])),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        warmup_info.simplify();
        assert_eq!(warmup_info, expected);
    }
}
