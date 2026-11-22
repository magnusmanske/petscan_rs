use anyhow::Result;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::LazyLock;
use url::Url;

static RE_NS_FROM_PARAMS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^ns\[(\d+)\]$"#).expect("FormParameters::ns_from_params:RE"));

#[derive(Debug, Clone, Default)]
pub struct FormParameters {
    pub params: HashMap<String, String>,
    pub ns: HashSet<usize>,
}

impl fmt::Display for FormParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.query_string_except(|_| false))
    }
}

impl FormParameters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_from_pairs(parameter_pairs: HashMap<String, String>) -> Self {
        Self::from_params(parameter_pairs)
    }

    /// Builds from raw parameters, applying legacy aliases before deriving
    /// the namespace set, so aliases that add `ns[…]` entries
    /// (`QuickIntersection`) are reflected in [`Self::ns`].
    fn from_params(params: HashMap<String, String>) -> Self {
        let mut ret = Self {
            params,
            ns: HashSet::new(),
        };
        ret.legacy_parameters();
        ret.recompute_namespaces();
        ret
    }

    fn recompute_namespaces(&mut self) {
        self.ns = Self::ns_from_params(&self.params);
    }

    /// Extracts namespaces from parameter list
    fn ns_from_params(params: &HashMap<String, String>) -> HashSet<usize> {
        params
            .iter()
            .filter(|(_k, v)| v.as_str() == "1")
            .filter_map(|(k, _v)| RE_NS_FROM_PARAMS.captures(k))
            .filter_map(|cap| cap[1].parse::<usize>().ok())
            .collect()
    }

    /// Parses a query string into a new object
    pub fn outcome_from_query(query: &str) -> Result<Self> {
        let parsed_url = Url::parse(&("https://127.0.0.1/?".to_string() + query))?;
        let params: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
        Ok(Self::from_params(params))
    }

    /// Amends a an object based on a previous one (used for PSID in main.rs)
    pub fn rebase(&mut self, base: &FormParameters) {
        for (k, v) in &base.params {
            let current = self.params.entry(k.clone()).or_default();
            if current.is_empty() {
                *current = v.clone();
            }
        }
        self.legacy_parameters();
        self.recompute_namespaces();
    }

    pub fn to_string_no_doit(&self) -> String {
        self.query_string_except(|key| key == "doit" || key == "format")
    }

    /// A percent-encoded query string with keys sorted, so equal parameter
    /// sets serialize identically across processes. PSIDs deduplicate on the
    /// serialized string, and `HashMap` iteration order is not stable between
    /// processes.
    fn query_string_except(&self, skip: impl Fn(&str) -> bool) -> String {
        let mut entries: Vec<(&str, &str)> = self
            .params
            .iter()
            .filter(|(k, _)| !skip(k))
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        entries.sort_unstable_by_key(|(k, _)| *k);
        entries
            .iter()
            .map(|(k, v)| Self::percent_encode(k) + "=" + &Self::percent_encode(v))
            .collect::<Vec<String>>()
            .join("&")
    }

    pub fn percent_encode(s: &str) -> String {
        utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
    }

    fn has_param(&self, key: &str) -> bool {
        self.params.contains_key(key)
    }

    fn has_param_with_value(&self, key: &str) -> bool {
        match self.params.get(key) {
            Some(s) => !s.trim().is_empty(),
            None => false,
        }
    }

    pub fn set_param(&mut self, key: &str, value: &str) {
        self.params.insert(key.to_string(), value.to_string());
    }

    fn fallback(&mut self, key_primary: &str, key_fallback: &str) {
        if !self.has_param(key_fallback) {
            return;
        }
        if !self.has_param(key_primary) || self.params.get(key_primary) == Some(&String::new()) {
            let value = self
                .params
                .get(key_fallback)
                .expect("FormParameters::fallback")
                .to_owned();
            self.set_param(key_primary, &value);
        }
    }

    /// Legacy parameter support
    fn legacy_parameters(&mut self) {
        self.fallback("language", "lang");
        self.fallback("categories", "cats");
        self.fallback("rxp_filter", "regexp_filter");

        // Old hack using manual wiki with no pages as "common wiki"
        if self.has_param_with_value("manual_list_wiki")
            && !self.has_param_with_value("manual_list")
            && !self.has_param_with_value("common_wiki_other")
            && let Some(wiki) = self.params.get("manual_list_wiki")
        {
            let wiki = wiki.to_owned();
            self.set_param("common_wiki_other", &wiki);
            self.set_param("manual_list_wiki", "");
        }

        // query originally from QuickIntersection
        if self.has_param("max") {
            if self.params.get("format").unwrap_or(&String::new()) == "jsonfm" {
                self.set_param("json-pretty", "1");
            }
            self.set_param("output_compatability", "quick-intersection");
            match self.params.get("ns") {
                None => {}
                Some(num) => {
                    if num == "*" {
                        self.set_param("ns[0]", "1");
                    } else if let Ok(ns_num) = num.parse::<usize>() {
                        self.set_param(format!("ns[{ns_num}]").as_str(), "1");
                    }
                }
            }
        }

        if self.has_param("comb_subset") {
            self.set_param("combination", "subset");
        }
        if self.has_param("comb_union") {
            self.set_param("combination", "union");
        }
        if self.has_param("get_q") {
            self.set_param("wikidata_item", "any");
        }
        if self.has_param("wikidata") {
            self.set_param("wikidata_item", "any");
        }
        if self.has_param("wikidata_no_item") {
            self.set_param("wikidata_item", "without");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legacy_parameters() {
        let mut form_params = FormParameters::new();
        form_params.set_param("manual_list_wiki", "enwiki");
        form_params.legacy_parameters();
        assert_eq!(
            form_params.params.get("common_wiki_other"),
            Some(&"enwiki".to_string())
        );
        assert_eq!(
            form_params.params.get("manual_list_wiki"),
            Some(&"".to_string())
        );
    }

    #[test]
    fn test_has_param_with_value() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        assert!(form_params.has_param_with_value("test"));
        assert!(!form_params.has_param_with_value("test2"));
    }

    #[test]
    fn test_to_string_no_doit() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        form_params.set_param("doit", "1");
        assert_eq!(form_params.to_string_no_doit(), "test=value".to_string());
    }

    #[test]
    fn test_rebase() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        let mut form_params2 = FormParameters::new();
        form_params2.set_param("test2", "value2");
        form_params2.rebase(&form_params);
        assert_eq!(form_params2.params.get("test"), Some(&"value".to_string()));
        assert_eq!(
            form_params2.params.get("test2"),
            Some(&"value2".to_string())
        );
    }

    #[test]
    fn test_outcome_from_query() {
        let form_params = FormParameters::outcome_from_query("test=value&test2=value2");
        assert!(form_params.is_ok());
        let form_params = form_params.unwrap();
        assert_eq!(form_params.params.get("test"), Some(&"value".to_string()));
        assert_eq!(form_params.params.get("test2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_has_param() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        assert!(form_params.has_param("test"));
        assert!(!form_params.has_param("test2"));
    }

    #[test]
    fn test_set_param() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        assert_eq!(form_params.params.get("test"), Some(&"value".to_string()));
    }

    #[test]
    fn test_ns_from_params() {
        let mut params = HashMap::new();
        params.insert("ns[0]".to_string(), "1".to_string());
        params.insert("ns[1]".to_string(), "1".to_string());
        params.insert("ns[2]".to_string(), "1".to_string());
        let form_params = FormParameters::new_from_pairs(params);
        assert!(form_params.ns.contains(&0));
        assert!(form_params.ns.contains(&1));
        assert!(form_params.ns.contains(&2));
    }

    #[test]
    fn test_percent_encode() {
        assert_eq!(
            FormParameters::percent_encode("test value"),
            "test%20value".to_string()
        );
    }

    #[test]
    fn test_fallback() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        form_params.fallback("test2", "test");
        assert_eq!(form_params.params.get("test2"), Some(&"value".to_string()));
    }

    #[test]
    fn test_new_from_pairs() {
        let mut params = HashMap::new();
        params.insert("test".to_string(), "value".to_string());
        let form_params = FormParameters::new_from_pairs(params);
        assert_eq!(form_params.params.get("test"), Some(&"value".to_string()));
    }

    #[test]
    fn test_new() {
        let form_params = FormParameters::new();
        assert_eq!(form_params.params.len(), 0);
    }

    #[test]
    fn test_to_string() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        assert_eq!(form_params.to_string(), "test=value".to_string());
    }

    #[test]
    fn test_rebase_empty() {
        let mut form_params = FormParameters::new();
        let form_params2 = FormParameters::new();
        form_params.rebase(&form_params2);
        assert_eq!(form_params.params.len(), 0);
    }

    #[test]
    fn test_rebase_no_overwrite() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        let mut form_params2 = FormParameters::new();
        form_params2.set_param("test", "value2");
        form_params.rebase(&form_params2);
        assert_eq!(form_params.params.get("test"), Some(&"value".to_string()));
    }

    #[test]
    fn test_rebase_overwrite() {
        let mut form_params = FormParameters::new();
        form_params.set_param("test", "value");
        let mut form_params2 = FormParameters::new();
        form_params2.set_param("test", "value2");
        form_params2.set_param("test2", "value3");
        form_params.rebase(&form_params2);
        assert_eq!(form_params.params.get("test"), Some(&"value".to_string()));
        assert_eq!(form_params.params.get("test2"), Some(&"value3".to_string()));
    }

    // ── rebase edge cases ────────────────────────────────────────────────────

    #[test]
    fn test_rebase_overwrites_empty_existing_value() {
        // An empty string in self should be replaced by base's value
        let mut current = FormParameters::new();
        current.set_param("lang", "");
        let mut base = FormParameters::new();
        base.set_param("lang", "de");
        current.rebase(&base);
        assert_eq!(current.params.get("lang"), Some(&"de".to_string()));
    }

    // ── ns_from_params ───────────────────────────────────────────────────────

    #[test]
    fn test_ns_from_params_only_includes_value_1() {
        let mut params = HashMap::new();
        params.insert("ns[0]".to_string(), "1".to_string());
        params.insert("ns[1]".to_string(), "0".to_string()); // value is not "1"
        params.insert("ns[2]".to_string(), "yes".to_string()); // value is not "1"
        let fp = FormParameters::new_from_pairs(params);
        assert!(fp.ns.contains(&0));
        assert!(!fp.ns.contains(&1));
        assert!(!fp.ns.contains(&2));
    }

    #[test]
    fn legacy_namespace_aliases_reach_the_namespace_set() {
        // QuickIntersection encodes the namespace as `ns=4`; `legacy_parameters`
        // turns that into `ns[4]=1`, which must also update `ns`.
        let fp = FormParameters::new_from_pairs(
            [
                ("max".to_string(), "500".to_string()),
                ("ns".to_string(), "4".to_string()),
            ]
            .into(),
        );
        assert!(fp.ns.contains(&4), "{:?}", fp.ns);
    }

    #[test]
    fn serialization_is_independent_of_hash_map_order() {
        let a = FormParameters::new_from_pairs(
            [
                ("z".to_string(), "1".to_string()),
                ("a".to_string(), "2".to_string()),
                ("m".to_string(), "3".to_string()),
            ]
            .into(),
        );
        let b = FormParameters::new_from_pairs(
            [
                ("m".to_string(), "3".to_string()),
                ("z".to_string(), "1".to_string()),
                ("a".to_string(), "2".to_string()),
            ]
            .into(),
        );
        assert_eq!(a.to_string(), "a=2&m=3&z=1");
        assert_eq!(a.to_string(), b.to_string());
    }

    // ── has_param_with_value ─────────────────────────────────────────────────

    #[test]
    fn test_has_param_with_value_whitespace_returns_false() {
        let mut fp = FormParameters::new();
        fp.set_param("key", "   ");
        assert!(!fp.has_param_with_value("key"));
    }

    // ── legacy_parameters: comb_subset / comb_union ──────────────────────────

    #[test]
    fn test_legacy_comb_subset_sets_combination() {
        let mut fp = FormParameters::new();
        fp.set_param("comb_subset", "1");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("combination"), Some(&"subset".to_string()));
    }

    #[test]
    fn test_legacy_comb_union_sets_combination() {
        let mut fp = FormParameters::new();
        fp.set_param("comb_union", "1");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("combination"), Some(&"union".to_string()));
    }

    // ── legacy_parameters: wikidata_item aliases ─────────────────────────────

    #[test]
    fn test_legacy_get_q_sets_wikidata_item_any() {
        let mut fp = FormParameters::new();
        fp.set_param("get_q", "1");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("wikidata_item"), Some(&"any".to_string()));
    }

    #[test]
    fn test_legacy_wikidata_param_sets_wikidata_item_any() {
        let mut fp = FormParameters::new();
        fp.set_param("wikidata", "1");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("wikidata_item"), Some(&"any".to_string()));
    }

    #[test]
    fn test_legacy_wikidata_no_item_sets_without() {
        let mut fp = FormParameters::new();
        fp.set_param("wikidata_no_item", "1");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("wikidata_item"), Some(&"without".to_string()));
    }

    // ── legacy_parameters: QuickIntersection ────────────────────────────────

    #[test]
    fn test_legacy_quick_intersection_sets_output_compatability() {
        let mut fp = FormParameters::new();
        fp.set_param("max", "500");
        fp.legacy_parameters();
        assert_eq!(
            fp.params.get("output_compatability"),
            Some(&"quick-intersection".to_string())
        );
    }

    #[test]
    fn test_legacy_quick_intersection_ns_star_adds_ns0() {
        let mut fp = FormParameters::new();
        fp.set_param("max", "500");
        fp.set_param("ns", "*");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("ns[0]"), Some(&"1".to_string()));
    }

    #[test]
    fn test_legacy_quick_intersection_ns_numeric_adds_bracket_key() {
        let mut fp = FormParameters::new();
        fp.set_param("max", "500");
        fp.set_param("ns", "4");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("ns[4]"), Some(&"1".to_string()));
    }

    #[test]
    fn test_legacy_quick_intersection_jsonfm_sets_json_pretty() {
        let mut fp = FormParameters::new();
        fp.set_param("max", "100");
        fp.set_param("format", "jsonfm");
        fp.legacy_parameters();
        assert_eq!(fp.params.get("json-pretty"), Some(&"1".to_string()));
    }
}
