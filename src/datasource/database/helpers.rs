//! Pure helpers extracted from `SourceDatabase`.
//!
//! These previously lived inside `impl SourceDatabase` but never touched
//! `&self`, so they're moved out as free functions to keep `SourceDatabase`
//! focused on stateful query construction and to make the helpers
//! unit-testable without a database.

use crate::datasource::{SQLtuple, append_sql, prep_quote};
use std::collections::HashMap;
use wikimisc::mediawiki::api::{Api, NamespaceID};
use wikimisc::mediawiki::title::Title;

use super::SourceDatabaseCatDepth;

/// Maximum number of categories sent in one `WHERE category IN (…)` IN-list.
/// Wikimedia replicas reject very large IN-lists with `Got a packet bigger
/// than 'max_allowed_packet'`; 2 500 keeps us safely under that bound while
/// minimising round-trips for deep category traversals.
pub(super) const MAX_CATEGORY_BATCH_SIZE: usize = 2500;

/// Parse the `Foo|3` / `Foo` / `Foo|-1` category-with-depth syntax used in
/// `PetScan`'s `categories=` field.
///
/// - Bare names use `default_depth`.
/// - Negative depths are interpreted as "unbounded" and capped at 999.
/// - Non-integer depths are dropped (filter, not error).
pub(super) fn parse_category_depth(
    cats: &[String],
    default_depth: u16,
) -> Vec<SourceDatabaseCatDepth> {
    cats.iter()
        .filter_map(|c| {
            let mut parts = c.split('|');
            let name = parts.next()?.to_string();
            let depth = match parts.next() {
                Some(depth) => {
                    let depth_signed = depth.parse::<i32>().ok()?;
                    if depth_signed < 0 {
                        999
                    } else {
                        depth_signed as u16
                    }
                }
                None => default_depth,
            };
            Some(SourceDatabaseCatDepth { name, depth })
        })
        .collect()
}

/// Convert spaces to underscores; optionally capitalise the first letter for
/// case-insensitive namespaces (templates, categories on most wikis).
pub(super) fn s2u_ucfirst(s: &str, is_case_insensitive: bool) -> String {
    if is_case_insensitive {
        Title::spaces_to_underscores(&Title::first_letter_uppercase(s))
    } else {
        Title::spaces_to_underscores(s)
    }
}

pub(super) fn vec_to_ucfirst(input: Vec<String>, is_case_insensitive: bool) -> Vec<String> {
    input
        .iter()
        .map(|s| s2u_ucfirst(s, is_case_insensitive))
        .collect()
}

/// Append an SQL `=v` or `IN (v1, v2, …)` clause, picking the shorter form
/// when there's only one value.
pub(super) fn sql_in(input: &[String], sql: &mut SQLtuple) {
    if input.len() == 1 {
        sql.0 += "=";
        append_sql(sql, prep_quote(input));
    } else {
        sql.0 += " IN (";
        append_sql(sql, prep_quote(input));
        sql.0 += ")";
    }
}

pub(super) fn group_link_list_by_namespace(
    input: &[String],
    api: &Api,
) -> HashMap<NamespaceID, Vec<String>> {
    let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
    for title in input {
        let title = Title::new_from_full(title, api);
        ret.entry(title.namespace_id())
            .or_default()
            .push(title.with_underscores());
    }
    ret
}

pub(super) fn links_from_subquery(input: &[String], api: &Api) -> SQLtuple {
    let mut sql: SQLtuple = ("(".to_string(), vec![]);
    let nslist = group_link_list_by_namespace(input, api);
    for nsgroup in &nslist {
        if !sql.1.is_empty() {
            sql.0 += " ) OR ( ";
        }
        sql.0 += "( SELECT p_to.page_id FROM page p_to,page p_from,pagelinks,linktarget WHERE pl_target_id=lt_id AND p_from.page_namespace=";
        sql.0 += &nsgroup.0.to_string();
        sql.0 += "  AND p_from.page_id=pl_from AND lt_namespace=p_to.page_namespace AND lt_title=p_to.page_title AND p_from.page_title";
        sql_in(nsgroup.1, &mut sql);
        sql.0 += " )";
    }
    sql.0 += ")";
    sql
}

pub(super) fn links_to_subquery(input: &[String], api: &Api) -> SQLtuple {
    let mut sql: SQLtuple = ("(".to_string(), vec![]);
    let nslist = group_link_list_by_namespace(input, api);
    for nsgroup in &nslist {
        if !sql.1.is_empty() {
            sql.0 += " ) OR ( ";
        }
        sql.0 += "( SELECT DISTINCT pl_from FROM pagelinks,linktarget WHERE pl_target_id=lt_id AND lt_namespace=";
        sql.0 += &nsgroup.0.to_string();
        sql.0 += " AND lt_title";
        sql_in(nsgroup.1, &mut sql);
        sql.0 += " )";
    }
    sql.0 += ")";
    sql
}

/// Build a cross-product of category batches, chunked by
/// [`MAX_CATEGORY_BATCH_SIZE`] × 10 to stay under `MySQL`'s packet limit.
/// Recursive: each call peels one positional slot off `categories` and
/// combines its chunks with the recursive result of the remainder.
pub(super) fn iterate_category_batches(
    categories: &[Vec<String>],
    start: usize,
) -> Vec<Vec<Vec<String>>> {
    let mut ret: Vec<Vec<Vec<String>>> = vec![];
    categories[start]
        .chunks(MAX_CATEGORY_BATCH_SIZE * 10)
        .for_each(|c| {
            if start + 1 >= categories.len() {
                ret.push(vec![c.to_vec()]);
                return;
            }
            let tmp = iterate_category_batches(categories, start + 1);
            for t in &tmp {
                let mut to_add = vec![c.to_vec()];
                to_add.append(&mut t.clone());
                ret.push(to_add);
            }
        });
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cd(name: &str, depth: u16) -> SourceDatabaseCatDepth {
        SourceDatabaseCatDepth {
            name: name.to_string(),
            depth,
        }
    }

    #[test]
    fn parse_category_depth_bare_uses_default() {
        let cats = vec!["Foo".to_string()];
        assert_eq!(parse_category_depth(&cats, 3), vec![cd("Foo", 3)]);
    }

    #[test]
    fn parse_category_depth_explicit_overrides_default() {
        let cats = vec!["Foo|7".to_string()];
        assert_eq!(parse_category_depth(&cats, 3), vec![cd("Foo", 7)]);
    }

    #[test]
    fn parse_category_depth_negative_caps_at_999() {
        let cats = vec!["Foo|-1".to_string(), "Bar|-50".to_string()];
        assert_eq!(
            parse_category_depth(&cats, 3),
            vec![cd("Foo", 999), cd("Bar", 999)]
        );
    }

    #[test]
    fn parse_category_depth_zero_is_explicit() {
        // Zero must remain zero — not collapsed to default.
        let cats = vec!["Foo|0".to_string()];
        assert_eq!(parse_category_depth(&cats, 5), vec![cd("Foo", 0)]);
    }

    #[test]
    fn parse_category_depth_non_integer_dropped() {
        let cats = vec!["Foo|abc".to_string(), "Bar|2".to_string()];
        // "Foo|abc" is silently dropped (filter_map), "Bar|2" parses normally.
        assert_eq!(parse_category_depth(&cats, 9), vec![cd("Bar", 2)]);
    }

    #[test]
    fn parse_category_depth_empty_input() {
        assert_eq!(parse_category_depth(&[], 3), vec![]);
    }

    #[test]
    fn s2u_ucfirst_case_sensitive() {
        assert_eq!(s2u_ucfirst("foo bar", false), "foo_bar");
        assert_eq!(s2u_ucfirst("Foo bar", false), "Foo_bar");
    }

    #[test]
    fn s2u_ucfirst_case_insensitive_uppercases_first() {
        assert_eq!(s2u_ucfirst("foo bar", true), "Foo_bar");
        assert_eq!(s2u_ucfirst("Foo bar", true), "Foo_bar");
    }

    #[test]
    fn vec_to_ucfirst_maps_each_item() {
        let input = vec!["foo".to_string(), "bar baz".to_string()];
        assert_eq!(
            vec_to_ucfirst(input, true),
            vec!["Foo".to_string(), "Bar_baz".to_string()]
        );
    }

    #[test]
    fn sql_in_single_value_uses_equality() {
        let mut sql: SQLtuple = ("WHERE x".to_string(), vec![]);
        sql_in(&["alpha".to_string()], &mut sql);
        assert_eq!(sql.0, "WHERE x=?");
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn sql_in_multiple_values_uses_in_list() {
        let mut sql: SQLtuple = ("WHERE x".to_string(), vec![]);
        sql_in(
            &["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
            &mut sql,
        );
        assert_eq!(sql.0, "WHERE x IN (?,?,?)");
        assert_eq!(sql.1.len(), 3);
    }

    #[test]
    fn iterate_category_batches_single_slot() {
        let categories = vec![vec!["a".to_string(), "b".to_string()]];
        let batches = iterate_category_batches(&categories, 0);
        // Single positional slot, both fit in one chunk → one outer batch
        // containing one chunk of two items.
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], vec![vec!["a".to_string(), "b".to_string()]]);
    }

    #[test]
    fn iterate_category_batches_cross_product_of_two_slots() {
        // Two positional slots, two items each → 2×2 = 4 combinations.
        let categories = vec![
            vec!["x1".to_string()],
            vec!["y1".to_string(), "y2".to_string()],
        ];
        let batches = iterate_category_batches(&categories, 0);
        assert_eq!(batches.len(), 1);
        // Both ys fit in the same chunk; one outer combo:
        //   [ chunk_of_x: [x1], chunk_of_y: [y1, y2] ]
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[0][0], vec!["x1".to_string()]);
        assert_eq!(batches[0][1], vec!["y1".to_string(), "y2".to_string()]);
    }
}
