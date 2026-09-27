//! Qdrant's payload key paths (`qdrant:lib/segment/src/json_path/`): the
//! grammar, `value_get`, the `with_payload` include/exclude selectors, and
//! `value_set`/`value_remove` for `set_payload` with `key` (Ruling 12).
//!
//! Payload maps keep their key order (E5), so removals use `shift_remove`.

use std::str::FromStr;

use serde_json::{Map, Value};

use crate::error::GatewayError;

/// One step after the first key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathItem {
    /// `.key` or `."quoted key"`.
    Key(String),
    /// `[n]`.
    Index(usize),
    /// `[]`: every array element.
    Wildcard,
}

/// `key(.key | ."key" | [n] | [])*`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonPath {
    pub first: String,
    pub rest: Vec<PathItem>,
}

/// A raw key character (`qdrant:…/json_path/parse.rs`, `raw_str`).
fn raw_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

/// Whether a key needs quotes to be written in a path.
fn needs_quoting(key: &str) -> bool {
    key.is_empty() || !key.chars().all(raw_char)
}

impl FromStr for JsonPath {
    type Err = GatewayError;

    /// Qdrant's grammar; an invalid path is `Format error in JSON body:
    /// Invalid json path: '<s>'`, as Qdrant's serde says.
    fn from_str(s: &str) -> Result<Self, GatewayError> {
        let invalid = || GatewayError::json(format!("Invalid json path: '{s}'"));
        let mut p = Parser { s, at: 0 };
        let first = p.key().ok_or_else(invalid)?;
        let mut rest = Vec::new();
        while p.at < s.len() {
            if p.eat('.') {
                rest.push(PathItem::Key(p.key().ok_or_else(invalid)?));
            } else if p.eat('[') {
                if p.eat(']') {
                    rest.push(PathItem::Wildcard);
                } else {
                    let digits = p.take_while(|c| c.is_ascii_digit());
                    let index = digits.parse().map_err(|_| invalid())?;
                    if !p.eat(']') {
                        return Err(invalid());
                    }
                    rest.push(PathItem::Index(index));
                }
            } else {
                return Err(invalid());
            }
        }
        Ok(JsonPath { first, rest })
    }
}

struct Parser<'a> {
    s: &'a str,
    at: usize,
}

impl Parser<'_> {
    /// Consumes `c` when it is next.
    fn eat(&mut self, c: char) -> bool {
        if self.s[self.at..].starts_with(c) {
            self.at += c.len_utf8();
            true
        } else {
            false
        }
    }

    /// Consumes the longest prefix whose characters satisfy `f`.
    fn take_while(&mut self, f: impl Fn(char) -> bool) -> &str {
        let start = self.at;
        let len: usize = self.s[start..]
            .chars()
            .take_while(|c| f(*c))
            .map(char::len_utf8)
            .sum();
        self.at += len;
        &self.s[start..start + len]
    }

    /// A raw key (one or more raw characters) or a quoted one (no `\` or
    /// `"` inside).
    fn key(&mut self) -> Option<String> {
        if self.eat('"') {
            let key = self.take_while(|c| c != '"' && c != '\\').to_string();
            return self.eat('"').then_some(key);
        }
        let key = self.take_while(raw_char);
        (!key.is_empty()).then(|| key.to_string())
    }
}

impl std::fmt::Display for JsonPath {
    /// The path in Qdrant's syntax, quoting keys that need it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = |f: &mut std::fmt::Formatter<'_>, k: &str| {
            if needs_quoting(k) {
                write!(f, "\"{k}\"")
            } else {
                f.write_str(k)
            }
        };
        key(f, &self.first)?;
        for item in &self.rest {
            match item {
                PathItem::Key(k) => {
                    f.write_str(".")?;
                    key(f, k)?;
                }
                PathItem::Index(i) => write!(f, "[{i}]")?,
                PathItem::Wildcard => f.write_str("[]")?,
            }
        }
        Ok(())
    }
}

impl JsonPath {
    /// The keys joined by `.`, wildcards dropped: the IR path under the
    /// `payload` field, which flattens arrays (Ruling 5). An index, or a
    /// key containing `.` (only a quoted key can), is unsupported
    /// (Ruling 15).
    pub fn normalized(&self) -> Result<String, GatewayError> {
        let key_ok = |k: &str| {
            if k.contains('.') {
                Err(GatewayError::Unsupported(format!(
                    "payload key {self} (a key containing '.')"
                )))
            } else {
                Ok(())
            }
        };
        key_ok(&self.first)?;
        let mut out = self.first.clone();
        for item in &self.rest {
            match item {
                PathItem::Key(k) => {
                    key_ok(k)?;
                    out.push('.');
                    out.push_str(k);
                }
                PathItem::Wildcard => {}
                PathItem::Index(_) => {
                    return Err(GatewayError::Unsupported(format!(
                        "payload key {self} (an array index)"
                    )));
                }
            }
        }
        Ok(out)
    }

    /// Every value at the path, as Qdrant's `value_get`: a plain key does
    /// not descend into arrays; `[]` does, `[n]` takes one element.
    pub fn value_get<'a>(&self, map: &'a Map<String, Value>) -> Vec<&'a Value> {
        let mut out = Vec::new();
        if let Some(value) = map.get(&self.first) {
            value_get(&self.rest, value, &mut out);
        }
        out
    }

    /// The path as a list of items, the first key included.
    fn items(&self) -> Vec<PathItem> {
        let mut items = vec![PathItem::Key(self.first.clone())];
        items.extend(self.rest.iter().cloned());
        items
    }
}

/// Collects the values at `path` under `value` into `out`.
fn value_get<'a>(path: &[PathItem], value: &'a Value, out: &mut Vec<&'a Value>) {
    let Some((head, tail)) = path.split_first() else {
        out.push(value);
        return;
    };
    match (head, value) {
        (PathItem::Key(k), Value::Object(map)) => {
            if let Some(v) = map.get(k) {
                value_get(tail, v, out);
            }
        }
        (PathItem::Index(i), Value::Array(items)) => {
            if let Some(v) = items.get(*i) {
                value_get(tail, v, out);
            }
        }
        (PathItem::Wildcard, Value::Array(items)) => {
            for v in items {
                value_get(tail, v, out);
            }
        }
        _ => {}
    }
}

/// Qdrant's `value_filter`: keeps each value whose path `keep` accepts,
/// recursing into the kept objects and arrays (array elements have the
/// path `…[]`).
fn value_filter(
    payload: &Map<String, Value>,
    keep: &dyn Fn(&[PathItem]) -> bool,
) -> Map<String, Value> {
    let mut path = Vec::new();
    let mut out = Map::new();
    for (key, value) in payload {
        path.push(PathItem::Key(key.clone()));
        if keep(&path) {
            out.insert(key.clone(), run_filter(&mut path, value, keep));
        }
        path.pop();
    }
    out
}

/// [`value_filter`] below `path`: an array's elements (path `…[]`) and an
/// object's keys are kept when `keep` accepts their path.
fn run_filter(
    path: &mut Vec<PathItem>,
    value: &Value,
    keep: &dyn Fn(&[PathItem]) -> bool,
) -> Value {
    match value {
        Value::Array(items) => {
            path.push(PathItem::Wildcard);
            let mut kept = Vec::new();
            for v in items {
                if keep(path) {
                    kept.push(run_filter(path, v, keep));
                }
            }
            path.pop();
            Value::Array(kept)
        }
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, v) in map {
                path.push(PathItem::Key(key.clone()));
                if keep(path) {
                    out.insert(key.clone(), run_filter(path, v, keep));
                }
                path.pop();
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// `with_payload: {include}`: the values whose path is a prefix of some
/// pattern or has one as a prefix, so ancestors are kept and descendants
/// of a match are kept whole (`check_include_pattern`).
pub fn select_include(payload: &Map<String, Value>, include: &[JsonPath]) -> Map<String, Value> {
    let patterns: Vec<Vec<PathItem>> = include.iter().map(JsonPath::items).collect();
    value_filter(payload, &|path| {
        patterns
            .iter()
            .any(|p| path.iter().zip(p).all(|(a, b)| a == b))
    })
}

/// `with_payload: {exclude}`: drops each value that some pattern is a
/// prefix of (`check_exclude_pattern`).
pub fn select_exclude(payload: &Map<String, Value>, exclude: &[JsonPath]) -> Map<String, Value> {
    let patterns: Vec<Vec<PathItem>> = exclude.iter().map(JsonPath::items).collect();
    value_filter(payload, &|path| {
        !patterns.iter().any(|p| path.starts_with(p))
    })
}

/// Qdrant's `merge_map`: top-level keys of `src` replace those of `dest`,
/// and a `null` removes its key.
fn merge_map(dest: &mut Map<String, Value>, src: &Map<String, Value>) {
    for (key, value) in src {
        if value.is_null() {
            dest.shift_remove(key);
        } else {
            dest.insert(key.clone(), value.clone());
        }
    }
}

/// Qdrant's `value_set`: merges `src` into the object at `path` (creating
/// missing objects, replacing non-objects), or into `dest` itself without
/// a path.
pub fn value_set(path: Option<&JsonPath>, dest: &mut Map<String, Value>, src: &Map<String, Value>) {
    match path {
        None => merge_map(dest, src),
        Some(path) => set_in_map(&path.first, &path.rest, dest, src, false),
    }
}

/// `overwrite_payload` with `key`: as [`value_set`], but the object at
/// `path` is replaced by `src` instead of merged with it (Task 5 step 4).
pub fn value_overwrite(path: &JsonPath, dest: &mut Map<String, Value>, src: &Map<String, Value>) {
    set_in_map(&path.first, &path.rest, dest, src, true);
}

/// [`value_set`] (or [`value_overwrite`] with `replace`) under `dest[key]`,
/// creating it when missing.
fn set_in_map(
    key: &str,
    rest: &[PathItem],
    dest: &mut Map<String, Value>,
    src: &Map<String, Value>,
    replace: bool,
) {
    match dest.get_mut(key) {
        Some(value) => set_in_value(rest, value, src, replace),
        None => {
            let mut value = Value::Null;
            set_in_value(rest, &mut value, src, replace);
            dest.insert(key.to_string(), value);
        }
    }
}

/// [`value_set`] (or [`value_overwrite`] with `replace`) at `path` under
/// `dest`: non-objects on the way are replaced.
fn set_in_value(path: &[PathItem], dest: &mut Value, src: &Map<String, Value>, replace: bool) {
    let Some((head, rest)) = path.split_first() else {
        if replace {
            *dest = Value::Object(src.clone());
            return;
        }
        if !dest.is_object() {
            *dest = Value::Object(Map::new());
        }
        if let Value::Object(map) = dest {
            merge_map(map, src);
        }
        return;
    };
    match head {
        PathItem::Key(k) => {
            if !dest.is_object() {
                *dest = Value::Object(Map::new());
            }
            if let Value::Object(map) = dest {
                set_in_map(k, rest, map, src, replace);
            }
        }
        PathItem::Index(i) => {
            if !dest.is_array() {
                *dest = Value::Array(Vec::new());
            }
            if let Some(v) = dest.as_array_mut().and_then(|a| a.get_mut(*i)) {
                set_in_value(rest, v, src, replace);
            }
        }
        PathItem::Wildcard => match dest {
            Value::Array(items) => items
                .iter_mut()
                .for_each(|v| set_in_value(rest, v, src, replace)),
            other => *other = Value::Array(Vec::new()),
        },
    }
}

/// Qdrant's `value_remove`: removes the values at `path`. A trailing `[]`
/// empties the arrays; a trailing `[n]` removes nothing (not idempotent).
pub fn value_remove(path: &JsonPath, dest: &mut Map<String, Value>) {
    match path.rest.split_first() {
        None => {
            dest.shift_remove(&path.first);
        }
        Some((head, rest)) => {
            if let Some(value) = dest.get_mut(&path.first) {
                remove_in(head, rest, value);
            }
        }
    }
}

/// [`value_remove`] of `head` then `rest` under `value`.
fn remove_in(head: &PathItem, rest: &[PathItem], value: &mut Value) {
    if let Some((next, tail)) = rest.split_first() {
        match (head, value) {
            (PathItem::Key(k), Value::Object(map)) => {
                if let Some(v) = map.get_mut(k) {
                    remove_in(next, tail, v);
                }
            }
            (PathItem::Index(i), Value::Array(items)) => {
                if let Some(v) = items.get_mut(*i) {
                    remove_in(next, tail, v);
                }
            }
            (PathItem::Wildcard, Value::Array(items)) => {
                for v in items {
                    remove_in(next, tail, v);
                }
            }
            _ => {}
        }
        return;
    }
    match (head, value) {
        (PathItem::Key(k), Value::Object(map)) => {
            map.shift_remove(k);
        }
        (PathItem::Wildcard, Value::Array(items)) => items.clear(),
        _ => {}
    }
}
