use crate::datasource::SQLtuple;
use crate::platform::Platform;
use mysql_async::Value as MyValue;
use rayon::prelude::*;

impl Platform {
    /// Returns a tuple with a string containing comma-separated question marks, and the (non-empty) Vec elements
    pub fn prep_quote(strings: &[String]) -> SQLtuple {
        let escaped: Vec<MyValue> = strings
            .par_iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| MyValue::Bytes(s.into()))
            .collect();
        (Platform::get_placeholders(escaped.len()), escaped)
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
        (Platform::get_placeholders(escaped.len()), escaped)
    }

    /// Generates a string with `len` comma-separated question marks
    pub fn get_placeholders(len: usize) -> String {
        let mut questionmarks: Vec<String> = Vec::new();
        questionmarks.resize(len, "?".to_string());
        questionmarks.join(",")
    }

    /// Returns an empty SQL tuple `("", vec![])`
    pub const fn sql_tuple() -> SQLtuple {
        (String::new(), vec![])
    }

    /// Appends `sub` (both SQL string and parameters) onto `sql`
    pub fn append_sql(sql: &mut SQLtuple, mut sub: SQLtuple) {
        sql.0 += &sub.0;
        sql.1.append(&mut sub.1);
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

    #[test]
    fn test_prep_quote_basic() {
        let strings = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let (placeholders, values) = Platform::prep_quote(&strings);
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
        let (placeholders, values) = Platform::prep_quote(&strings);
        assert_eq!(placeholders, "?,?");
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_prep_quote_empty_input() {
        let strings: Vec<String> = vec![];
        let (placeholders, values) = Platform::prep_quote(&strings);
        assert_eq!(placeholders, "");
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_prep_quote_whitespace_only_filtered() {
        let strings = vec!["   ".to_string(), "\t".to_string()];
        let (placeholders, values) = Platform::prep_quote(&strings);
        assert_eq!(placeholders, "");
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_get_placeholders_zero() {
        assert_eq!(Platform::get_placeholders(0), "");
    }

    #[test]
    fn test_get_placeholders_one() {
        assert_eq!(Platform::get_placeholders(1), "?");
    }

    #[test]
    fn test_get_placeholders_three() {
        assert_eq!(Platform::get_placeholders(3), "?,?,?");
    }

    #[test]
    fn test_get_placeholders_five() {
        assert_eq!(Platform::get_placeholders(5), "?,?,?,?,?");
    }

    #[test]
    fn test_sql_tuple_empty() {
        let t = Platform::sql_tuple();
        assert_eq!(t.0, "");
        assert!(t.1.is_empty());
    }

    #[test]
    fn test_append_sql_basic() {
        let mut sql = ("SELECT * FROM page WHERE ".to_string(), vec![]);
        let sub = (
            "page_title=?".to_string(),
            vec![mysql_async::Value::Bytes("Foo".into())],
        );
        Platform::append_sql(&mut sql, sub);
        assert_eq!(sql.0, "SELECT * FROM page WHERE page_title=?");
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn test_append_sql_empty_sub() {
        let mut sql = ("SELECT 1".to_string(), vec![]);
        Platform::append_sql(&mut sql, ("".to_string(), vec![]));
        assert_eq!(sql.0, "SELECT 1");
        assert!(sql.1.is_empty());
    }

    #[test]
    fn test_full_entity_id_to_number_basic() {
        let strings = vec!["Q123".to_string(), "P456".to_string(), "L789".to_string()];
        let (placeholders, values) = Platform::full_entity_id_to_number(&strings);
        assert_eq!(placeholders, "?,?,?");
        assert_eq!(values.len(), 3);
        // Verify the leading letter is stripped
        assert_eq!(values[0], mysql_async::Value::Bytes("123".into()));
        assert_eq!(values[1], mysql_async::Value::Bytes("456".into()));
        assert_eq!(values[2], mysql_async::Value::Bytes("789".into()));
    }

    #[test]
    fn test_full_entity_id_to_number_empty_filtered() {
        let strings = vec!["Q1".to_string(), "".to_string(), "  ".to_string()];
        let (placeholders, values) = Platform::full_entity_id_to_number(&strings);
        assert_eq!(placeholders, "?");
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_platform_not_used_directly_in_sql_utils() {
        // Just ensure Platform can still be constructed without panic
        let p = make_platform(vec![]);
        assert!(!p.has_param("anything"));
    }
}
