use crate::app_state::AppState;
use crate::combination::Combination;
use crate::content_type::ContentType;
use crate::datasource::DataSource;
use crate::datasource::database::{SourceDatabase, SourceDatabaseParameters};

use crate::datasource::manual::SourceManual;
use crate::datasource::pagepile::SourcePagePile;
use crate::datasource::search::SourceSearch;
use crate::datasource::sitelinks::SourceSitelinks;
use crate::datasource::sparql::SourceSparql;
use crate::datasource::wikidata::SourceWikidata;
use crate::form_parameters::FormParameters;
use crate::pagelist::PageList;
use crate::pagelist_entry::PageListSort;
use crate::render::Render;
use crate::render::html::RenderHTML;
use crate::render::json::RenderJSON;
use crate::render::jsonl::RenderJSONL;
use crate::render::kml::RenderKML;
use crate::render::pagepile::RenderPagePile;
use crate::render::plaintext::RenderPlainText;
use crate::render::tsv::RenderTSV;
use crate::render::wikitext::RenderWiki;
use crate::wdfist::WDfist;
use anyhow::{Result, anyhow};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tracing::{debug, instrument};
use wikimisc::mediawiki::api::NamespaceID;

pub static PAGE_BATCH_SIZE: usize = 15000;

mod combine;

mod params;
mod process;

#[derive(Debug, Clone, PartialEq)]
pub struct MyResponse {
    pub s: String,
    pub content_type: ContentType,
}

#[derive(Debug)]
pub struct Platform {
    pub(super) form_parameters: FormParameters,
    pub(super) state: Arc<AppState>,
    pub(super) result: Option<PageList>,
    pub psid: Option<u64>,
    pub(super) existing_labels: RwLock<HashSet<String>>,
    pub(super) combination: Combination,
    pub(super) output_redlinks: bool,
    pub(super) query_time: Option<Duration>,
    pub(super) wiki_by_source: HashMap<String, String>,
    pub(super) wdfist_result: Option<serde_json::Value>,
    pub(super) warnings: RwLock<Vec<String>>,
    pub(super) namespace_case_sensitivity_cache: RwLock<HashMap<(String, NamespaceID), bool>>,
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: Arc<AppState>) -> Self {
        Self {
            form_parameters: (*form_parameters).clone(),
            state,
            result: None,
            psid: None,
            existing_labels: RwLock::new(HashSet::new()),
            combination: Combination::None,
            output_redlinks: false,
            query_time: None,
            wiki_by_source: HashMap::new(),
            wdfist_result: None,
            warnings: RwLock::new(vec![]),
            namespace_case_sensitivity_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn warnings(&self) -> Result<Vec<String>> {
        Ok(self.warnings.read().map_err(|e| anyhow!("{e}"))?.clone())
    }

    pub fn warn(&self, s: String) -> Result<()> {
        self.warnings.write().map_err(|e| anyhow!("{e}"))?.push(s);
        Ok(())
    }

    pub fn label_exists(&self, label: &str) -> bool {
        match self.existing_labels.read() {
            Ok(el) => el.contains(label),
            _ => false,
        }
    }

    pub fn combination(&self) -> Combination {
        self.combination.clone()
    }

    pub const fn do_output_redlinks(&self) -> bool {
        self.output_redlinks
    }

    pub fn query_time(&self) -> Option<Duration> {
        self.query_time.to_owned()
    }

    /// Returns `true` if "case" in namespace info is "case-sensitive", `false` otherwise (default)
    pub async fn get_namespace_case_sensitivity(&self, namespace_id: NamespaceID) -> bool {
        let wiki = match self.get_main_wiki() {
            Some(wiki) => wiki,
            None => return false,
        };

        match self.namespace_case_sensitivity_cache.read() {
            Ok(ncsc) => {
                if let Some(ret) = ncsc.get(&(wiki.to_owned(), namespace_id)) {
                    return *ret;
                }
            }
            _ => return false,
        }
        let api = match self.state().get_api_for_wiki(wiki.to_owned()).await {
            Ok(api) => api,
            _ => {
                match self.namespace_case_sensitivity_cache.write() {
                    Ok(mut ncsc) => {
                        ncsc.insert((wiki.to_owned(), namespace_id), false);
                    }
                    _ => return false,
                }
                return false;
            }
        };
        let namespace_info = api.get_site_info_value("namespaces", &namespace_id.to_string());
        let ret = match namespace_info["case"].as_str() {
            Some(c) => c == "case-sensitive",
            None => false,
        };
        match self.namespace_case_sensitivity_cache.write() {
            Ok(mut ncsc) => {
                ncsc.insert((wiki.to_owned(), namespace_id), ret);
            }
            _ => return false,
        }
        ret
    }

    #[instrument(skip_all, err(level = tracing::Level::INFO))]
    #[allow(clippy::default_constructed_unit_structs)]
    pub async fn run(&mut self) -> Result<()> {
        Platform::profile("begin run", None);
        let start_time = SystemTime::now();
        self.output_redlinks = self.has_param("show_redlinks");

        let mut s_db = SourceDatabase::new(SourceDatabaseParameters::db_params(self).await);
        let mut s_sparql = SourceSparql::default();
        let mut s_manual = SourceManual::default();
        let mut s_pagepile = SourcePagePile::default();
        let mut s_search = SourceSearch::default();
        let mut s_wikidata = SourceWikidata::default();

        let mut s_sitelinks = SourceSitelinks::new();

        let mut futures = vec![];
        let mut available_sources = vec![];

        if s_db.can_run(self) {
            available_sources.push(s_db.name());
            futures.push(s_db.run(self));
        }
        if s_sparql.can_run(self) {
            available_sources.push(s_sparql.name());
            futures.push(s_sparql.run(self));
        }
        if s_manual.can_run(self) {
            available_sources.push(s_manual.name());
            futures.push(s_manual.run(self));
        }
        if s_pagepile.can_run(self) {
            available_sources.push(s_pagepile.name());
            futures.push(s_pagepile.run(self));
        }
        if s_search.can_run(self) {
            available_sources.push(s_search.name());
            futures.push(s_search.run(self));
        }
        if s_wikidata.can_run(self) {
            available_sources.push(s_wikidata.name());
            futures.push(s_wikidata.run(self));
        }
        if futures.is_empty() && s_sitelinks.can_run(self) {
            available_sources.push(s_sitelinks.name());
            futures.push(s_sitelinks.run(self));
        }

        if futures.is_empty() {
            return Err(anyhow!("No possible data source found in parameters"));
        }

        Platform::profile("begin futures 1", None);

        let mut tmp_results = join_all(futures).await;

        let mut results: HashMap<String, PageList> = HashMap::new();
        let mut names = available_sources.clone();
        while !tmp_results.is_empty() {
            let result = tmp_results.remove(0);
            if names.is_empty() {
                return Err(anyhow!("Platform::run: names list is empty unexpectedly"));
            }
            let name = names.remove(0);
            results.insert(name, result?);
        }
        drop(tmp_results);

        self.wiki_by_source = results
            .iter()
            .filter_map(|(name, data)| data.wiki().map(|wiki| (name.to_string(), wiki)))
            .collect();
        Platform::profile("end futures 1", None);

        self.combination = self.get_combination(&available_sources);

        Platform::profile("before combine_results", None);
        let serialized_combination = Self::serialize_combine_results(&self.combination)?;
        let result = self
            .combine_results(&mut results, serialized_combination)
            .await?;
        drop(results);

        self.result = Some(result);
        Platform::profile("after combine_results", None);
        self.post_process_result(&available_sources).await?;
        Platform::profile("after post_process_result", None);

        if self.has_param("wdf_main") {
            match &self.result {
                Some(pagelist) => pagelist.convert_to_wiki("wikidatawiki", self).await?,
                None => return Err(anyhow!("No result set for WDfist")),
            }
            let mut wdfist =
                WDfist::new(self, &self.result).ok_or_else(|| anyhow!("Cannot create WDfist"))?;
            self.result = None; // Safe space
            self.wdfist_result = Some(wdfist.run().await?);
        }

        self.query_time = start_time.elapsed().ok();
        Platform::profile("after run", None);

        Ok(())
    }

    pub fn profile(label: &str, num: Option<usize>) {
        debug!(num, "{}", label);
    }

    pub fn state(&self) -> Arc<AppState> {
        self.state.clone()
    }

    pub async fn get_response(&self) -> Result<MyResponse> {
        // Shortcut: WDFIST
        if let Some(j) = &self.wdfist_result {
            return Ok(self
                .state
                .output_json(j, self.form_parameters.params.get("callback")));
        }

        let result = match &self.result {
            Some(result) => result,
            None => return Err(anyhow!("Platform::get_response: No result")),
        };
        let wiki = match result.wiki() {
            Some(wiki) => wiki,
            None => return Err(anyhow!("Platform::get_response: No wiki in result")),
        };

        let (sortby, sort_order) = self.get_sorting_parameters();
        let mut pages =
            result.drain_into_sorted_vec(PageListSort::new_from_params(&sortby, sort_order));
        self.apply_results_limit(&mut pages);

        match self.get_param_blank("format").as_str() {
            "wiki" => RenderWiki::new().response(self, &wiki, pages).await,
            "csv" => RenderTSV::new(",").response(self, &wiki, pages).await,
            "tsv" => RenderTSV::new("\t").response(self, &wiki, pages).await,
            "json" => RenderJSON::new().response(self, &wiki, pages).await,
            "jsonl" => RenderJSONL::new().response(self, &wiki, pages).await,
            "pagepile" => RenderPagePile::new().response(self, &wiki, pages).await,
            "kml" => RenderKML::new().response(self, &wiki, pages).await,
            "plain" => RenderPlainText::new().response(self, &wiki, pages).await,
            _ => RenderHTML::new().response(self, &wiki, pages).await,
        }
    }

    pub const fn result(&self) -> &Option<PageList> {
        &self.result
    }

    pub const fn form_parameters(&self) -> &FormParameters {
        &self.form_parameters
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use crate::pagelist_entry::{PageListEntry, PageListSort};
    use serde_json::Value;
    use std::env;
    use std::fs::File;
    use wikimisc::mediawiki::title::Title;

    // ─── helpers ─────────────────────────────────────────────────────────────

    async fn get_new_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let file = File::open(path).expect("Can not open config file");
        let petscan_config: Value =
            serde_json::from_reader(file).expect("Can not parse JSON from config file");
        Arc::new(
            AppState::new_from_config(&petscan_config)
                .await
                .expect("AppState::new_from_config failed in test"),
        )
    }

    async fn get_state() -> Arc<AppState> {
        get_new_state().await
    }

    async fn run_psid_ext(psid: usize, addendum: &str) -> Result<Platform> {
        let state = get_state().await;
        let form_parameters = match state.get_query_from_psid(&format!("{}", &psid)).await {
            Ok(psid_query) => {
                let query = psid_query + addendum;
                FormParameters::outcome_from_query(&query)?
            }
            Err(e) => return Err(e),
        };
        let mut platform = Platform::new_from_parameters(&form_parameters, state);
        platform.run().await?;
        Ok(platform)
    }

    async fn run_psid(psid: usize) -> Platform {
        run_psid_ext(psid, "").await.unwrap()
    }

    async fn check_results_for_psid_ext(
        psid: usize,
        addendum: &str,
        wiki: &str,
        expected: Vec<Title>,
    ) {
        let mut platform = run_psid_ext(psid, addendum).await.unwrap();
        let s1 = platform.get_param_blank("sortby");
        let s2 = platform.get_param_blank("sortorder");

        let result = platform.result.unwrap();
        let some_wiki = result.wiki();
        assert_eq!(some_wiki.unwrap(), wiki);

        // Sort/crop results
        let mut entries =
            result.drain_into_sorted_vec(PageListSort::new_from_params(&s1, s2 == "descending"));
        platform.result = Some(result);
        platform.apply_results_limit(&mut entries);

        assert_eq!(entries.len(), expected.len());
        let titles: Vec<Title> = entries.iter().map(|e| e.title()).cloned().collect();
        assert_eq!(titles, expected);
    }

    async fn check_results_for_psid(psid: usize, wiki: &str, expected: Vec<Title>) {
        check_results_for_psid_ext(psid, "", wiki, expected).await;
    }

    pub(super) fn make_platform_with_params(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = std::collections::HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

    fn entries_from_result(result: PageList) -> Vec<PageListEntry> {
        result.as_vec()
    }

    // ─── unit tests ───────────────────────────────────────────────────────────

    #[test]
    fn test_warnings() {
        let p = make_platform_with_params(vec![]);
        assert!(p.warnings().unwrap().is_empty());
        p.warn("test warning".to_string()).unwrap();
        let warnings = p.warnings().unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], "test warning");
    }

    #[test]
    fn test_do_output_redlinks_default() {
        let p = make_platform_with_params(vec![]);
        assert!(!p.do_output_redlinks());
    }

    #[test]
    fn test_label_exists_empty() {
        let p = make_platform_with_params(vec![]);
        assert!(!p.label_exists("anything"));
    }

    #[test]
    fn test_label_exists_after_warn_does_not_add_label() {
        let p = make_platform_with_params(vec![]);
        p.warn("foo".to_string()).unwrap();
        // warn() adds to warnings, not existing_labels
        assert!(!p.label_exists("foo"));
    }

    #[test]
    fn test_combination_default_is_none() {
        let p = make_platform_with_params(vec![]);
        assert_eq!(p.combination(), Combination::None);
    }

    #[test]
    fn test_query_time_default_is_none() {
        let p = make_platform_with_params(vec![]);
        assert!(p.query_time().is_none());
    }

    #[test]
    fn test_result_default_is_none() {
        let p = make_platform_with_params(vec![]);
        assert!(p.result().is_none());
    }

    // ─── integration tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_manual_list_enwiki_use_props() {
        check_results_for_psid(10087995, "enwiki", vec![Title::new("Magnus_Manske", 0)]).await;
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_sitelinks() {
        // This assumes [[en:Count von Count]] has no lvwiki article
        check_results_for_psid(10123257, "wikidatawiki", vec![Title::new("Q13520818", 0)]).await;
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_min_max_sitelinks() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        check_results_for_psid(10123897, "wikidatawiki", vec![Title::new("Q13520818", 0)]).await; // Min 15
        check_results_for_psid(10124667, "wikidatawiki", vec![Title::new("Q12345", 0)]).await;
        // Max 15
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_label_filter() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        check_results_for_psid(10125089, "wikidatawiki", vec![Title::new("Q12345", 0)]).await;
        // Label "Count%" in en
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_neg_cat_filter() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        // Manual list on enwiki, minus [[Category:Fictional vampires]]
        check_results_for_psid(10126217, "enwiki", vec![Title::new("Magnus Manske", 0)]).await;
    }

    #[tokio::test]
    async fn test_manual_list_commons_file_info() {
        // Manual list [[File:KingsCollegeChapelWest.jpg]] on commons
        let platform = run_psid(10137125).await;
        let result = platform.result.unwrap();
        let entries = result.as_vec();
        assert_eq!(entries.len(), 1);
        let entry = entries.first().unwrap();
        assert_eq!(entry.page_id(), Some(1340715));
        let fi = entry.get_file_info();
        assert!(fi.is_some());
        let fi = fi.unwrap();
        assert!(fi.file_usage.len() > 10);
        assert_eq!(fi.img_size, Some(223131));
        assert_eq!(fi.img_width, Some(1025));
        assert_eq!(fi.img_height, Some(768));
        assert_eq!(fi.img_user_text, Some("Solipsist~commonswiki".to_string()));
        assert_eq!(
            fi.img_sha1,
            Some("sypcaey3hmlhjky46x0nhiwhiivx6yj".to_string())
        );
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_page_info() {
        // Manual list [[Cambridge]] on enwiki
        let platform = run_psid(10136716).await;
        let result = platform.result.unwrap();
        let entries = result.as_vec();
        assert_eq!(entries.len(), 1);
        let entry = entries.first().unwrap();
        assert_eq!(entry.page_id(), Some(36995));
        assert!(entry.page_bytes().is_some());
        assert!(entry.get_page_timestamp().is_some());
        assert_eq!(
            entry.get_page_image(),
            Some("Cambridge_-_Kings_College_vue_des_backs.jpg".to_string())
        );
        assert_eq!(*entry.disambiguation(), crate::pagelist_entry::TriState::No);
        assert!(entry.incoming_links().unwrap() > 7500);
        assert!(entry.get_coordinates().is_some());
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_annotate_wikidata_item() {
        // Manual list [[Count von Count]] on enwiki
        let platform = run_psid(10137767).await;
        let result = platform.result.unwrap();
        let entries = result.as_vec();
        assert_eq!(entries.len(), 1);
        let entry = entries.first().unwrap();
        assert_eq!(entry.page_id(), Some(239794));
        assert_eq!(entry.get_wikidata_item(), Some("Q12345".to_string()));
    }

    #[tokio::test]
    async fn test_manual_list_enwiki_subpages() {
        // Manual list [[User:Magnus Manske]] on enwiki, subpages, not "root page"
        let platform = run_psid(10138030).await;
        let result = platform.result.unwrap();
        let entries = result.as_vec();
        assert!(entries.len() > 100);
        // Try to find pages with no '/'
        assert!(
            !entries
                .iter()
                .any(|entry| { entry.title().pretty().find('/').is_none() })
        );
    }

    #[tokio::test]
    async fn test_manual_list_wikidata_labels() {
        // Manual list [[Q12345]], nl label/desc
        let platform = run_psid(10138979).await;
        let result = platform.result.unwrap();
        let entries = result.as_vec();
        assert_eq!(entries.len(), 1);
        let entry = entries.first().unwrap();
        assert_eq!(entry.page_id(), Some(13925));
        assert_eq!(entry.get_wikidata_label(), Some("Graaf Tel".to_string()));
        assert_eq!(
            entry.get_wikidata_description(),
            Some("figuur van Sesamstraat".to_string())
        );
    }

    #[tokio::test]
    async fn test_regexp_filter_fallback() {
        // Old parameter
        check_results_for_psid_ext(
            10140344,
            "&regexp_filter=.*Manske",
            "wikidatawiki",
            vec![Title::new("Q13520818", 0)],
        )
        .await;

        // New parameter
        check_results_for_psid_ext(
            10140344,
            "&rxp_filter=.*Manske",
            "wikidatawiki",
            vec![Title::new("Q13520818", 0)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_manual_list_wikidata_regexp() {
        check_results_for_psid_ext(
            10140344,
            "&rxp_filter=.*Manske",
            "wikidatawiki",
            vec![Title::new("Q13520818", 0)],
        )
        .await;
        check_results_for_psid_ext(
            10140344,
            "&rxp_filter=Graaf.*",
            "wikidatawiki",
            vec![Title::new("Q12345", 0)],
        )
        .await;
        check_results_for_psid_ext(
            10140616,
            "&rxp_filter=&rxp_filter=Jimbo.*",
            "enwiki",
            vec![Title::new("Jimbo Wales", 0)],
        )
        .await;
        check_results_for_psid_ext(
            10140616,
            "&rxp_filter=&rxp_filter=.*Sanger",
            "enwiki",
            vec![Title::new("Larry Sanger", 0)],
        )
        .await;
    }

    // Deactivated: connection to frwiki_p required
    // #[tokio::test]
    // async fn test_en_categories_sparql_common_wiki_other() {
    //     check_results_for_psid(15960820, "frwiki", vec![Title::new("Magnus Manske", 0)]).await;
    // }

    // Deactivated: connection to enwikiquote_p required
    // #[tokio::test]
    // async fn test_trim_extended_whitespace() {
    //     let platform = run_psid(15015735).await; // The categories contain a left-to-right mark
    //     let result = platform.result.unwrap();
    //     let entries = entries_from_result(result);
    //     assert!(entries.len() > 20);
    // }

    #[tokio::test]
    async fn test_template_talk_pages() {
        let platform = run_psid(43089908).await;
        let result = platform.result.unwrap();
        let entries = entries_from_result(result);
        assert!(!entries.is_empty());
        for entry in entries {
            assert_eq!(entry.title().namespace_id(), 0);
        }
    }

    #[tokio::test]
    async fn test_sort_by_defaultsort() {
        check_results_for_psid(
            18604332,
            "enwiki",
            vec![Title::new("Earth", 0), Title::new("Ayn Rand", 0)],
        )
        .await;
    }
}
