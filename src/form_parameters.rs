use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use url::*;

#[derive(Debug, Clone)]
pub struct FormParameters {
    pub params: HashMap<String, String>,
    pub ns: HashSet<usize>,
}

impl FormParameters {
    pub fn new() -> Self {
        Self {
            params: HashMap::new(),
            ns: HashSet::new(),
        }
    }

    pub fn new_from_pairs(parameter_pairs: Vec<(&str, &str)>) -> Self {
        let mut ret = Self::new();
        ret.params = parameter_pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string().replace("+", " ")))
            .collect();
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
                    match cap[1].parse::<usize>() {
                        Ok(ns_num) => {
                            ns.insert(ns_num);
                        }
                        _ => {}
                    }
                }
            });
        ns
    }

    /// Parses a query string into a new object
    pub fn outcome_from_query(query: &str) -> Result<Self, String> {
        let parsed_url = match Url::parse(&("https://127.0.0.1/?".to_string() + query)) {
            Ok(url) => url,
            Err(e) => return Err(format!("{:?}", &e)),
        };
        let params: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
        let ns = Self::ns_from_params(&params);
        let mut ret = FormParameters {
            params: params,
            ns: ns,
        };
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

    pub fn to_string(&self) -> String {
        self.params
            .iter()
            .map(|(k, v)| Self::percent_encode(k) + "=" + &Self::percent_encode(v))
            .collect::<Vec<String>>()
            .join("&")
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
        self.params.contains_key(&key.to_string())
    }

    fn has_param_with_value(&self, key: &str) -> bool {
        match self.params.get(&key.to_string()) {
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
        if !self.has_param(key_primary) || self.params.get(key_primary) == Some(&"".to_string()) {
            let value = self
                .params
                .get(key_fallback)
                .expect("FormParameters::fallback")
                .to_owned();
            self.set_param(key_primary, &value);
        }
    }

    fn legacy_parameters(&mut self) {
        self.fallback("language", "lang");
        self.fallback("categories", "cats");

        // Old hack using manual wiki with no pages as "common wiki"
        if self.has_param_with_value("manual_list_wiki")
            && !self.has_param_with_value("manual_list")
            && !self.has_param_with_value("common_wiki_other")
        {
            match self.params.get("manual_list_wiki") {
                Some(wiki) => {
                    let wiki = wiki.to_owned();
                    self.set_param(&"common_wiki_other".to_string(), &wiki);
                    self.set_param(&"manual_list_wiki".to_string(), &"".to_string());
                }
                None => {}
            }
        }

        // query originally from QuickIntersection
        if self.has_param("max") {
            if self.params.get("format").unwrap_or(&"".to_string()) == "jsonfm" {
                self.set_param("json-pretty", "1");
            }
            self.set_param("output_compatability", "quick-intersection");
            match self.params.get("ns") {
                None => {}
                Some(num) => {
                    if num == "*" {
                        self.set_param("ns[0]", "1");
                    } else {
                        match num.parse::<usize>() {
                            Ok(ns_num) => {
                                self.set_param(format!("ns[{}]", ns_num).as_str(), "1");
                            }
                            Err(_) => {}
                        }
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
