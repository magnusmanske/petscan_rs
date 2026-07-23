pub mod database;
pub mod manual;
pub mod pagepile;
pub mod search;
pub mod sitelinks;
pub mod sparql;
pub mod wikidata;

use crate::{pagelist::PageList, platform::Platform};
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::Value as MyValue;
use rayon::prelude::*;

/// An SQL fragment plus the values bound to its `?` placeholders.
///
/// Previously a bare `(String, Vec<MyValue>)` type alias — any stray
/// string/vec pair was accepted, and nothing tied the placeholder count
/// to the value count. A tuple struct keeps the existing `.0`/`.1` call
/// sites working while giving the pair a nominal type that can carry
/// invariant helpers like [`SQLtuple::placeholders_balanced`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SQLtuple(pub String, pub Vec<MyValue>);

impl SQLtuple {
    pub const fn new() -> Self {
        Self(String::new(), Vec::new())
    }

    /// True iff the number of `?` placeholders in the SQL text matches the
    /// number of bound values. Only meaningful on a *complete* fragment —
    /// mid-construction, SQL text and its values are often pushed in two
    /// steps. None of the generated SQL uses `?` inside string literals,
    /// so a plain character count is exact.
    pub fn placeholders_balanced(&self) -> bool {
        self.0.matches('?').count() == self.1.len()
    }
}

#[async_trait]
pub trait DataSource {
    fn can_run(&self, platform: &Platform) -> bool;
    async fn run(&mut self, platform: &Platform) -> Result<PageList>;
    fn name(&self) -> String;
}

// ─── SQL utilities ────────────────────────────────────────────────────────────

/// Generates a string with `len` comma-separated question marks
pub fn get_placeholders(len: usize) -> String {
    vec!["?"; len].join(",")
}

/// Returns an empty SQL tuple `("", vec![])`
pub const fn sql_tuple() -> SQLtuple {
    SQLtuple::new()
}

/// Returns a tuple with a comma-separated placeholder string and the (non-empty, trimmed) values
pub fn prep_quote(strings: &[String]) -> SQLtuple {
    let escaped: Vec<MyValue> = strings
        .par_iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| MyValue::Bytes(s.into()))
        .collect();
    SQLtuple(get_placeholders(escaped.len()), escaped)
}

/// Strips the leading entity-type character (Q/P/L) and returns numeric IDs as SQL values
pub fn full_entity_id_to_number(strings: &[String]) -> SQLtuple {
    let escaped: Vec<MyValue> = strings
        .par_iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s[1..].to_string())
        .map(|s| MyValue::Bytes(s.into()))
        .collect();
    SQLtuple(get_placeholders(escaped.len()), escaped)
}

/// Appends `sub` (both SQL string and parameters) onto `sql`
pub fn append_sql(sql: &mut SQLtuple, mut sub: SQLtuple) {
    sql.0 += &sub.0;
    sql.1.append(&mut sub.1);
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_placeholders_zero() {
        assert_eq!(get_placeholders(0), "");
    }

    #[test]
    fn test_get_placeholders_one() {
        assert_eq!(get_placeholders(1), "?");
    }

    #[test]
    fn test_get_placeholders_three() {
        assert_eq!(get_placeholders(3), "?,?,?");
    }

    #[test]
    fn test_get_placeholders_five() {
        assert_eq!(get_placeholders(5), "?,?,?,?,?");
    }

    #[test]
    fn test_sql_tuple_empty() {
        let t = sql_tuple();
        assert_eq!(t.0, "");
        assert!(t.1.is_empty());
    }

    #[test]
    fn test_prep_quote_basic() {
        let strings = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let SQLtuple(placeholders, values) = prep_quote(&strings);
        assert_eq!(placeholders, "?,?,?");
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_prep_quote_empty_filtered() {
        let strings = vec![
            "foo".to_string(),
            "".to_string(),
            "  ".to_string(),
            "bar".to_string(),
        ];
        let SQLtuple(placeholders, values) = prep_quote(&strings);
        assert_eq!(placeholders, "?,?");
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_prep_quote_empty_input() {
        let strings: Vec<String> = vec![];
        let SQLtuple(placeholders, values) = prep_quote(&strings);
        assert_eq!(placeholders, "");
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_prep_quote_whitespace_only_filtered() {
        let strings = vec!["   ".to_string(), "\t".to_string()];
        let SQLtuple(placeholders, values) = prep_quote(&strings);
        assert_eq!(placeholders, "");
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_append_sql_basic() {
        let mut sql = SQLtuple("SELECT * FROM page WHERE ".to_string(), vec![]);
        let sub = SQLtuple(
            "page_title=?".to_string(),
            vec![mysql_async::Value::Bytes("Foo".into())],
        );
        append_sql(&mut sql, sub);
        assert_eq!(sql.0, "SELECT * FROM page WHERE page_title=?");
        assert_eq!(sql.1.len(), 1);
        assert!(sql.placeholders_balanced());
    }

    #[test]
    fn test_append_sql_empty_sub() {
        let mut sql = SQLtuple("SELECT 1".to_string(), vec![]);
        append_sql(&mut sql, SQLtuple("".to_string(), vec![]));
        assert_eq!(sql.0, "SELECT 1");
        assert!(sql.1.is_empty());
    }

    #[test]
    fn test_placeholders_balanced() {
        assert!(sql_tuple().placeholders_balanced());
        assert!(prep_quote(&["a".to_string(), "b".to_string()]).placeholders_balanced());
        let missing_value = SQLtuple("x=?".to_string(), vec![]);
        assert!(!missing_value.placeholders_balanced());
        let extra_value = SQLtuple(
            "x=1".to_string(),
            vec![mysql_async::Value::Bytes("y".into())],
        );
        assert!(!extra_value.placeholders_balanced());
    }

    #[test]
    fn test_full_entity_id_to_number_basic() {
        let strings = vec!["Q123".to_string(), "P456".to_string(), "L789".to_string()];
        let SQLtuple(placeholders, values) = full_entity_id_to_number(&strings);
        assert_eq!(placeholders, "?,?,?");
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], mysql_async::Value::Bytes("123".into()));
        assert_eq!(values[1], mysql_async::Value::Bytes("456".into()));
        assert_eq!(values[2], mysql_async::Value::Bytes("789".into()));
    }

    #[test]
    fn test_full_entity_id_to_number_empty_filtered() {
        let strings = vec!["Q1".to_string(), "".to_string(), "  ".to_string()];
        let SQLtuple(placeholders, values) = full_entity_id_to_number(&strings);
        assert_eq!(placeholders, "?");
        assert_eq!(values.len(), 1);
    }
}
