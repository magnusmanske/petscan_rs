use crate::platform::Platform;
use wikimisc::mediawiki::title::Title;

impl Platform {
    /// Checks if a parameter is set and non-blank
    pub fn has_param(&self, param: &str) -> bool {
        match self.form_parameters().params.get(param) {
            Some(s) => !s.is_empty(),
            None => false,
        }
    }

    /// Returns the parameter value if present and non-empty, otherwise None
    pub fn get_param(&self, param: &str) -> Option<String> {
        if self.has_param(param) {
            self.form_parameters()
                .params
                .get(param)
                .map(|s| s.to_string())
        } else {
            None
        }
    }

    /// Returns the parameter value or empty string
    pub fn get_param_blank(&self, param: &str) -> String {
        self.get_param(param).unwrap_or_default()
    }

    /// Returns the parameter value, falling back to `default` if absent or empty
    pub fn get_param_default(&self, param: &str, default: &str) -> String {
        let ret = self.get_param(param).unwrap_or_else(|| default.to_string());
        if ret.is_empty() {
            default.to_string()
        } else {
            ret
        }
    }

    /// Returns true if the parameter is blank (absent or only whitespace)
    pub fn is_param_blank(&self, param: &str) -> bool {
        self.get_param_blank(param).trim().is_empty()
    }

    /// Returns the parameter split by `separator`, trimmed, filtered and with spaces→underscores
    pub fn get_param_as_vec(&self, param: &str, separator: &str) -> Vec<String> {
        match self.get_param(param) {
            Some(s) => s
                .split(separator)
                .map(|s| s.trim().trim_matches('\u{200E}').trim_matches('\u{200F}'))
                .filter(|s| !s.is_empty())
                .map(Title::spaces_to_underscores)
                .collect(),
            None => vec![],
        }
    }

    /// Returns a `usize` parsed from the given parameter, or `None`
    pub fn usize_option_from_param(&self, key: &str) -> Option<usize> {
        self.get_param(key)?.parse::<usize>().ok()
    }

    /// Returns the main wiki derived from `language`/`lang` + `project` parameters
    pub fn get_main_wiki(&self) -> Option<String> {
        let language = self.get_param_default("lang", "en"); // Fallback
        let language = self
            .get_param_default("language", &language)
            .replace('_', "-");
        let project = self.get_param_default("project", "wikipedia");
        self.get_wiki_for_language_project(&language, &project)
            .map(|wiki| self.state().fix_wiki_name(&wiki))
    }

    /// Maps a language + project pair to a wiki database name
    pub fn get_wiki_for_language_project(&self, language: &str, project: &str) -> Option<String> {
        match (language, project) {
            (language, "wikipedia") => Some(language.to_owned() + "wiki"),
            ("commons", _) => Some("commonswiki".to_string()),
            ("wikidata", _) => Some("wikidatawiki".to_string()),
            (_, "wikidata") => Some("wikidatawiki".to_string()),
            (l, p) => {
                let url = format!("https://{}.{}.org", &l, &p);
                self.state().site_matrix().get_wiki_for_server_url(&url)
            }
        }
    }

    /// Returns (sortby, descending) for output ordering
    pub(super) fn get_sorting_parameters(&self) -> (String, bool) {
        let mut sortby = self.get_param_blank("sortby");
        let sort_order = if self.do_output_redlinks() && (sortby.is_empty() || sortby == "none") {
            sortby = "redlinks".to_string();
            true
        } else {
            self.get_param_blank("sortorder") == "descending"
        };
        (sortby, sort_order)
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use std::sync::Arc;

    fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = std::collections::HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

    #[test]
    fn test_has_param_present() {
        let p = make_platform(vec![("format", "json"), ("empty_key", "")]);
        assert!(p.has_param("format"));
        assert!(!p.has_param("empty_key"));
        assert!(!p.has_param("nonexistent"));
    }

    #[test]
    fn test_get_param_present() {
        let p = make_platform(vec![("format", "json")]);
        assert_eq!(p.get_param("format"), Some("json".to_string()));
    }

    #[test]
    fn test_get_param_missing() {
        let p = make_platform(vec![]);
        assert_eq!(p.get_param("nonexistent"), None);
    }

    #[test]
    fn test_get_param_empty_value_treated_as_missing() {
        let p = make_platform(vec![("somekey", "")]);
        assert_eq!(p.get_param("somekey"), None);
    }

    #[test]
    fn test_get_param_blank() {
        let p = make_platform(vec![("format", "json")]);
        assert_eq!(p.get_param_blank("format"), "json".to_string());
        assert_eq!(p.get_param_blank("missing"), "".to_string());
    }

    #[test]
    fn test_get_param_default() {
        let p = make_platform(vec![("sortby", "title")]);
        assert_eq!(
            p.get_param_default("sortby", "default"),
            "title".to_string()
        );
        assert_eq!(
            p.get_param_default("missing", "fallback"),
            "fallback".to_string()
        );
        // Empty value should fall back to default
        let p2 = make_platform(vec![("key", "")]);
        assert_eq!(
            p2.get_param_default("key", "mydefault"),
            "mydefault".to_string()
        );
    }

    #[test]
    fn test_is_param_blank() {
        let p = make_platform(vec![("key", "value"), ("blank", "")]);
        assert!(!p.is_param_blank("key"));
        assert!(p.is_param_blank("blank"));
        assert!(p.is_param_blank("nonexistent"));
    }

    #[test]
    fn test_get_param_as_vec() {
        let p = make_platform(vec![("cats", "Cat1\nCat2\nCat3")]);
        let v = p.get_param_as_vec("cats", "\n");
        assert_eq!(v.len(), 3);
        assert!(v.contains(&"Cat1".to_string()));
    }

    #[test]
    fn test_get_param_as_vec_empty() {
        let p = make_platform(vec![]);
        let v = p.get_param_as_vec("cats", "\n");
        assert!(v.is_empty());
    }

    #[test]
    fn test_usize_option_from_param() {
        let p = make_platform(vec![("count", "42"), ("bad", "nope")]);
        assert_eq!(p.usize_option_from_param("count"), Some(42));
        assert_eq!(p.usize_option_from_param("bad"), None);
        assert_eq!(p.usize_option_from_param("missing"), None);
    }

    #[test]
    fn test_get_wiki_for_language_project_wikipedia() {
        let p = make_platform(vec![("language", "en"), ("project", "wikipedia")]);
        assert_eq!(
            p.get_wiki_for_language_project("en", "wikipedia"),
            Some("enwiki".to_string())
        );
    }

    #[test]
    fn test_get_wiki_for_language_project_commons() {
        let p = make_platform(vec![]);
        assert_eq!(
            p.get_wiki_for_language_project("commons", "anything"),
            Some("commonswiki".to_string())
        );
    }

    #[test]
    fn test_get_wiki_for_language_project_wikidata_language() {
        let p = make_platform(vec![]);
        assert_eq!(
            p.get_wiki_for_language_project("wikidata", "wikipedia"),
            Some("wikidatawiki".to_string())
        );
    }

    #[test]
    fn test_get_wiki_for_language_project_wikidata_project() {
        let p = make_platform(vec![]);
        assert_eq!(
            p.get_wiki_for_language_project("en", "wikidata"),
            Some("wikidatawiki".to_string())
        );
    }

    #[test]
    fn test_get_sorting_parameters_default() {
        let p = make_platform(vec![("sortby", "title"), ("sortorder", "descending")]);
        let (sortby, desc) = p.get_sorting_parameters();
        assert_eq!(sortby, "title");
        assert!(desc);
    }

    #[test]
    fn test_get_sorting_parameters_ascending() {
        let p = make_platform(vec![("sortby", "title")]);
        let (sortby, desc) = p.get_sorting_parameters();
        assert_eq!(sortby, "title");
        assert!(!desc);
    }
}
