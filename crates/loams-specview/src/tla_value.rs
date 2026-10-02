//! A parser for the TLA+ value syntax TLC prints in traces, into JSON.
//!
//! Records `[a |-> 1]` and functions `[k |-> "s"]` become objects (TLC prints
//! both alike); `(0 :> 1 @@ 1 :> 2)` (functions over non-string domains)
//! becomes an object with the keys rendered as strings; sets `{..}` and
//! sequences `<<..>>` become arrays; `TRUE`/`FALSE`, integers and strings map
//! directly; a bare identifier (a TLC model value) becomes a string.

use serde_json::{Map, Number, Value};

pub fn parse_value(src: &str) -> Result<Value, String> {
    let mut p = Parser {
        s: src.as_bytes(),
        i: 0,
    };
    let v = p.expr()?;
    p.ws();
    if p.i != p.s.len() {
        return Err(format!("trailing input at byte {}: {:?}", p.i, &src[p.i..]));
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn eat(&mut self, tok: &str) -> bool {
        self.ws();
        if self.s[self.i..].starts_with(tok.as_bytes()) {
            self.i += tok.len();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: &str) -> Result<(), String> {
        if self.eat(tok) {
            Ok(())
        } else {
            Err(format!("expected {tok:?} at byte {}", self.i))
        }
    }

    /// `a :> b @@ c :> d` or a plain value.
    fn expr(&mut self) -> Result<Value, String> {
        let first = self.atom()?;
        if self.eat(":>") {
            let mut map = Map::new();
            let v = self.atom()?;
            map.insert(key_of(&first), v);
            while self.eat("@@") {
                let k = self.atom()?;
                self.expect(":>")?;
                let v = self.atom()?;
                map.insert(key_of(&k), v);
            }
            return Ok(Value::Object(map));
        }
        Ok(first)
    }

    fn atom(&mut self) -> Result<Value, String> {
        self.ws();
        match self.s.get(self.i).copied() {
            None => Err("unexpected end of input".into()),
            Some(b'"') => self.string(),
            Some(b'{') => {
                self.i += 1;
                Ok(Value::Array(self.list("}")?))
            }
            Some(b'<') if self.s[self.i..].starts_with(b"<<") => {
                self.i += 2;
                Ok(Value::Array(self.list(">>")?))
            }
            Some(b'[') => self.record(),
            Some(b'(') => {
                self.i += 1;
                let v = self.expr()?;
                self.expect(")")?;
                Ok(v)
            }
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            Some(c) if c.is_ascii_alphabetic() || c == b'_' => Ok(self.ident()),
            Some(c) => Err(format!("unexpected {:?} at byte {}", c as char, self.i)),
        }
    }

    fn list(&mut self, close: &str) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        if self.eat(close) {
            return Ok(out);
        }
        loop {
            out.push(self.expr()?);
            if self.eat(",") {
                continue;
            }
            self.expect(close)?;
            return Ok(out);
        }
    }

    fn record(&mut self) -> Result<Value, String> {
        self.i += 1; // [
        let mut map = Map::new();
        if self.eat("]") {
            return Ok(Value::Object(map));
        }
        loop {
            self.ws();
            let key = if self.s.get(self.i) == Some(&b'"') {
                key_of(&self.string()?)
            } else {
                self.ident_str()
            };
            self.expect("|->")?;
            map.insert(key, self.expr()?);
            if self.eat(",") {
                continue;
            }
            self.expect("]")?;
            return Ok(Value::Object(map));
        }
    }

    fn string(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut out = String::new();
        let start = self.i;
        let mut seg = start;
        while self.i < self.s.len() {
            match self.s[self.i] {
                b'"' => {
                    out.push_str(
                        std::str::from_utf8(&self.s[seg..self.i]).map_err(|e| e.to_string())?,
                    );
                    self.i += 1;
                    return Ok(Value::String(out));
                }
                b'\\' => {
                    out.push_str(
                        std::str::from_utf8(&self.s[seg..self.i]).map_err(|e| e.to_string())?,
                    );
                    let esc = *self.s.get(self.i + 1).ok_or("dangling escape")?;
                    out.push(match esc {
                        b'n' => '\n',
                        b't' => '\t',
                        b'r' => '\r',
                        other => other as char,
                    });
                    self.i += 2;
                    seg = self.i;
                }
                _ => self.i += 1,
            }
        }
        Err("unterminated string".into())
    }

    fn number(&mut self) -> Result<Value, String> {
        let start = self.i;
        if self.s[self.i] == b'-' {
            self.i += 1;
        }
        while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
            self.i += 1;
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).map_err(|e| e.to_string())?;
        text.parse::<i64>()
            .map(|n| Value::Number(Number::from(n)))
            .map_err(|e| format!("bad integer {text:?}: {e}"))
    }

    fn ident_str(&mut self) -> String {
        self.ws();
        let start = self.i;
        while self.i < self.s.len()
            && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_')
        {
            self.i += 1;
        }
        String::from_utf8_lossy(&self.s[start..self.i]).into_owned()
    }

    fn ident(&mut self) -> Value {
        match self.ident_str().as_str() {
            "TRUE" => Value::Bool(true),
            "FALSE" => Value::Bool(false),
            other => Value::String(other.to_owned()),
        }
    }
}

/// A JSON object key for a function argument: strings as they are, anything
/// else as its compact JSON.
fn key_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scalars() {
        assert_eq!(parse_value("TRUE").unwrap(), json!(true));
        assert_eq!(parse_value("FALSE").unwrap(), json!(false));
        assert_eq!(parse_value("-12").unwrap(), json!(-12));
        assert_eq!(parse_value(r#""a b\"c""#).unwrap(), json!("a b\"c"));
        assert_eq!(parse_value("d").unwrap(), json!("d"));
    }

    #[test]
    fn collections() {
        assert_eq!(parse_value("{}").unwrap(), json!([]));
        assert_eq!(parse_value("<<>>").unwrap(), json!([]));
        assert_eq!(parse_value("{1, 2}").unwrap(), json!([1, 2]));
        assert_eq!(parse_value(r#"<<"a", TRUE>>"#).unwrap(), json!(["a", true]));
        assert_eq!(
            parse_value(r#"[gen |-> 1, owner |-> [k1 |-> "s1", k2 |-> "s2"]]"#).unwrap(),
            json!({"gen": 1, "owner": {"k1": "s1", "k2": "s2"}})
        );
        assert_eq!(
            parse_value(r#"[s1 |-> {[id |-> 1, key |-> "k1"]}, s2 |-> {}]"#).unwrap(),
            json!({"s1": [{"id": 1, "key": "k1"}], "s2": []})
        );
    }

    #[test]
    fn functions_over_other_domains() {
        assert_eq!(
            parse_value(r#"(0 :> "a" @@ 1 :> "b")"#).unwrap(),
            json!({"0": "a", "1": "b"})
        );
        assert_eq!(parse_value("(1 :> TRUE)").unwrap(), json!({"1": true}));
        assert_eq!(
            parse_value(r#"<<"k", 1>> :> 2"#).unwrap(),
            json!({"[\"k\",1]": 2})
        );
    }

    #[test]
    fn errors_are_reported() {
        assert!(parse_value("[a |-> ").is_err());
        assert!(parse_value("{1,").is_err());
        assert!(parse_value("1 2").is_err());
        assert!(parse_value("\"x").is_err());
        assert!(parse_value("").is_err());
    }
}
