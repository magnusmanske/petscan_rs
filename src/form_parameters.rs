use anyhow::Result;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use url::Url;

#[derive(Debug, Clone, Default)]
pub struct FormParameters {
    pub params: HashMap<String, String>,
    pub ns: HashSet<usize>,
}

impl fmt::Display for FormParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ret = self
            .params
            .iter()
            .map(|(k, v)| Self::percent_encode(k) + "=" + &Self::percent_encode(v))
            .collect::<Vec<String>>()
            .join("&");
        write!(f, "{ret}")
    }
}

impl FormParameters {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn new_from_pairs(parameter_pairs: HashMap<String, String>) -> Self {
        let mut ret = Self::new();
        ret.params = parameter_pairs;
        ret.ns = Self::ns_from_params(&ret.params);
        ret.legacy_parameters();
        ret
    }

    /// Extracts namespaces from parameter list
    fn ns_from_params(params: &HashMap<String, String>) -> HashSet<usize> {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r#"^ns\[(\d+)\]$"#).expect("FormParameters::ns_from_params:RE");
        }
        let mut ns: HashSet<usize> = HashSet::new();
        params
            .iter()
            .filter(|(_k, v)| *v == "1")
            .for_each(|(k, v)| {
                if k == "ns" && v == "*" {
                    // Backwards compat
                    ns.insert(0);
                }
                for cap in RE.captures_iter(k) {
                    if let Ok(ns_num) = cap[1].parse::<usize>() {
                        ns.insert(ns_num);
                    }
                }
            });
        ns
    }

    /// Parses a query string into a new object
    pub fn outcome_from_query(query: &str) -> Result<Self> {
        let parsed_url = Url::parse(&("https://127.0.0.1/?".to_string() + query))?;
        let params: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
        let ns = Self::ns_from_params(&params);
        let mut ret = FormParameters { params, ns };
        ret.legacy_parameters();
        Ok(ret)
    }

    /// Amends a an object based on a previous one (used for PSID in main.rs)
    pub fn rebase(&mut self, base: &FormParameters) {
        base.params.iter().for_each(|(k, v)| {
            if self.params.contains_key(k) {
                if self
                    .params
                    .get(k)
                    .expect("FormParameters::rebase")
                    .is_empty()
                {
                    self.params.insert(k.to_string(), v.to_string());
                }
            } else {
                self.params.insert(k.to_string(), v.to_string());
            }
        });
        self.legacy_parameters();
        self.ns = Self::ns_from_params(&self.params);
    }

    pub fn to_string_no_doit(&self) -> String {
        self.params
            .iter()
            .filter(|(k, _v)| *k != "doit")
            .filter(|(k, _v)| *k != "format")
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
        {
            if let Some(wiki) = self.params.get("manual_list_wiki") {
                let wiki = wiki.to_owned();
                self.set_param("common_wiki_other", &wiki);
                self.set_param("manual_list_wiki", "");
            }
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
}
