use crate::app_state::AppState;
use crate::datasource::*;
use crate::datasource_database::{SourceDatabase, SourceDatabaseParameters};
use crate::form_parameters::FormParameters;
use crate::pagelist::*;
use crate::render::*;
use crate::wdfist::*;
use actix_web::{Error, HttpResponse};
use chrono::Local;
use mysql as my;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};
use wikibase::mediawiki::api::NamespaceID;
use wikibase::mediawiki::title::Title;

pub static PAGE_BATCH_SIZE: usize = 20000;

#[derive(Debug, Clone, PartialEq)]
pub enum ContentType {
    HTML,
    Plain,
    JSON,
    JSONP,
    CSV,
    TSV,
}

impl ContentType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::HTML => "text/html; charset=utf-8",
            Self::Plain => "text/plain; charset=utf-8",
            Self::JSON => " application/json",
            Self::JSONP => "application/javascript",
            Self::CSV => "text/csv; charset=utf-8",
            Self::TSV => "text/tab-separated-values; charset=utf-8",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MyResponse {
    pub s: String,
    pub content_type: ContentType,
}

impl MyResponse {
    pub fn respond(&self) -> Result<HttpResponse, Error> {
        Ok(HttpResponse::Ok()
            .content_type(self.content_type.as_str())
            .body(self.s.to_owned())) // TODO FIXME duplication of output
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Combination {
    None,
    Source(String),
    Intersection((Box<Combination>, Box<Combination>)),
    Union((Box<Combination>, Box<Combination>)),
    Not((Box<Combination>, Box<Combination>)),
}

impl Combination {
    pub fn to_string(&self) -> String {
        match self {
            Combination::None => "nothing".to_string(),
            Combination::Source(s) => s.to_string(),
            Combination::Intersection((a, b)) => {
                "(".to_string() + &a.to_string() + " AND " + &b.to_string() + ")"
            }
            Combination::Union((a, b)) => {
                "(".to_string() + &a.to_string() + " OR " + &b.to_string() + ")"
            }
            Combination::Not((a, b)) => {
                "(".to_string() + &a.to_string() + " NOT " + &b.to_string() + ")"
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Platform {
    form_parameters: FormParameters,
    state: Arc<AppState>,
    result: Option<PageList>,
    pub psid: Option<u64>,
    existing_labels: Arc<RwLock<HashSet<String>>>,
    combination: Combination,
    output_redlinks: bool,
    query_time: Option<Duration>,
    wiki_by_source: HashMap<String, String>,
    wdfist_result: Option<Value>,
    warnings: Arc<RwLock<Vec<String>>>,
    namespace_case_sensitivity_cache: Arc<RwLock<HashMap<(String, NamespaceID), bool>>>,
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: &AppState) -> Self {
        Self {
            form_parameters: (*form_parameters).clone(),
            state: Arc::new(state.clone()),
            result: None,
            psid: None,
            existing_labels: Arc::new(RwLock::new(HashSet::new())),
            combination: Combination::None,
            output_redlinks: false,
            query_time: None,
            wiki_by_source: HashMap::new(),
            wdfist_result: None,
            warnings: Arc::new(RwLock::new(vec![])),
            namespace_case_sensitivity_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn warnings(&self) -> Vec<String> {
        self.warnings.read().unwrap().clone()
    }

    pub fn warn(&self, s: String) {
        self.warnings.write().unwrap().push(s);
    }

    pub fn label_exists(&self, label: &String) -> bool {
        // TODO normalization?
        self.existing_labels.read().unwrap().contains(label)
    }

    pub fn combination(&self) -> Combination {
        self.combination.clone()
    }

    pub fn do_output_redlinks(&self) -> bool {
        self.output_redlinks
    }

    pub fn query_time(&self) -> Option<Duration> {
        self.query_time.to_owned()
    }

    // Returns true if "case" in namespace info is "case-sensitive", false otherwise (default)
    pub fn get_namespace_case_sensitivity(&self, namespace_id: NamespaceID) -> bool {
        let wiki = match self.get_main_wiki() {
            Some(wiki) => wiki,
            None => return false,
        };
        match self
            .namespace_case_sensitivity_cache
            .read()
            .unwrap()
            .get(&(wiki.to_owned(), namespace_id))
        {
            Some(ret) => return *ret,
            None => {}
        }
        let state = self.state();
        let api = match state.get_api_for_wiki(wiki.to_owned()) {
            Ok(api) => api,
            _ => {
                self.namespace_case_sensitivity_cache
                    .write()
                    .unwrap()
                    .insert((wiki.to_owned(), namespace_id), false);
                return false;
            }
        };
        let namespace_info =
            api.get_site_info_value("namespaces", format!("{}", namespace_id).as_str());
        let ret = match namespace_info["case"].as_str() {
            Some(c) => (c == "case-sensitive"),
            None => false,
        };
        self.namespace_case_sensitivity_cache
            .write()
            .unwrap()
            .insert((wiki.to_owned(), namespace_id), ret);
        ret
    }

    pub fn run(&mut self) -> Result<(), String> {
        Platform::profile("begin run", None);
        let start_time = SystemTime::now();
        self.output_redlinks = self.has_param("show_redlinks");
        let mut candidate_sources: Vec<Arc<RwLock<Box<dyn DataSource + Send + Sync>>>> = vec![];
        candidate_sources.push(Arc::new(RwLock::new(Box::new(SourceDatabase::new(
            SourceDatabaseParameters::db_params(self),
        )))));
        candidate_sources.push(Arc::new(RwLock::new(Box::new(SourceSparql::new()))));
        candidate_sources.push(Arc::new(RwLock::new(Box::new(SourceManual::new()))));
        candidate_sources.push(Arc::new(RwLock::new(Box::new(SourcePagePile::new()))));
        candidate_sources.push(Arc::new(RwLock::new(Box::new(SourceSearch::new()))));
        candidate_sources.push(Arc::new(RwLock::new(Box::new(SourceWikidata::new()))));

        if !candidate_sources
            .par_iter()
            .any(|source| (*source.read().unwrap()).can_run(&self))
        {
            candidate_sources = vec![];
            candidate_sources.push(Arc::new(RwLock::new(Box::new(SourceLabels::new()))));
            if !candidate_sources
                .par_iter()
                .any(|source| (*source.read().unwrap()).can_run(&self))
            {
                return Err(format!("No possible data source found in parameters"));
            }
        }

        Platform::profile("begin threads 1", None);
        let results: HashMap<String, Arc<RwLock<PageList>>> = candidate_sources
            .par_iter()
            .filter(|ds| ds.read().unwrap().can_run(&self))
            .filter_map(|ds| {
                let mut ds = ds.write().unwrap();
                match ds.run(&self) {
                    Ok(data) => Some((ds.name(), Arc::new(RwLock::new(data)))),
                    _ => None,
                }
            })
            .collect();
        self.wiki_by_source = results
            .iter()
            .filter_map(|(name, data)| match data.read().unwrap().wiki() {
                Some(wiki) => Some((name.to_string(), wiki.to_string())),
                None => None,
            })
            .collect();
        Platform::profile("end threads 1", None);

        let available_sources = candidate_sources
            .par_iter()
            .filter(|s| (*s.read().unwrap()).can_run(&self))
            .map(|s| (*s.read().unwrap()).name())
            .collect();
        self.combination = self.get_combination(&available_sources);
        Platform::profile("before combine_results", None);
        let result = self.combine_results(&results, &self.combination)?;
        drop(results);

        let result = match Arc::try_unwrap(result) {
            Ok(result) => result,
            _ => return Err(format!("Can't unwrap arc")),
        };
        let result = match result.into_inner() {
            Ok(result) => result,
            _ => return Err(format!("Can't into_inner")),
        };
        self.result = Some(result);
        Platform::profile("after combine_results", None);
        self.post_process_result(&available_sources)?;
        Platform::profile("after post_process_result", None);

        if self.has_param("wdf_main") {
            let pagelist = self
                .result
                .as_ref()
                .ok_or(format!("No result set for WDfist"))?;
            pagelist
                .convert_to_wiki("wikidatawiki", self)
                .map_err(|e| format!("Failed to convert result to Wikidata for WDfist: {}", e))?;
            self.result = Some(pagelist.clone());
            let mut wdfist =
                WDfist::new(&self, &self.result).ok_or(format!("Cannot create WDfist"))?;
            self.result = None; // Safe space
            self.wdfist_result = Some(wdfist.run()?);
        }

        self.query_time = start_time.elapsed().ok();
        Platform::profile("after run", None);

        Ok(())
    }

    pub fn profile(label: &str, num: Option<usize>) {
        if false {
            println!(
                "{} [{}]: {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                num.unwrap_or(0),
                label,
            );
        }
    }

    fn post_process_result(&self, available_sources: &Vec<String>) -> Result<(), String> {
        Platform::profile("post_process_result begin", None);
        let result = match self.result.as_ref() {
            Some(res) => res,
            None => return Ok(()),
        };

        // Filter and post-process
        Platform::profile("before filter_wikidata", Some(result.len()));
        self.filter_wikidata(&result)?;
        Platform::profile("after filter_wikidata", Some(result.len()));
        self.process_sitelinks(&result)?;
        Platform::profile("after process_sitelinks", None);
        if *available_sources != vec!["labels".to_string()] {
            self.process_labels(&result)?;
            Platform::profile("after process_labels", Some(result.len()));
        }

        self.convert_to_common_wiki(&result)?;
        Platform::profile("after convert_to_common_wiki", Some(result.len()));

        if !available_sources.contains(&"categories".to_string()) {
            self.process_missing_database_filters(&result)?;
            Platform::profile("after process_missing_database_filters", Some(result.len()));
        }
        self.process_by_wikidata_item(&result)?;
        Platform::profile("after process_by_wikidata_item", Some(result.len()));
        self.process_files(&result)?;
        Platform::profile("after process_files", Some(result.len()));
        self.process_pages(&result)?;
        Platform::profile("after process_pages", Some(result.len()));
        self.process_subpages(&result)?;
        Platform::profile("after process_subpages", Some(result.len()));
        self.annotate_with_wikidata_item(result)?;
        Platform::profile("after annotate_with_wikidata_item [2]", Some(result.len()));

        let wikidata_label_language = self.get_param_default(
            "wikidata_label_language",
            &self.get_param_default("interface_language", "en"),
        );
        result.load_missing_metadata(Some(wikidata_label_language), &self)?;
        Platform::profile("after load_missing_metadata", Some(result.len()));
        match self.get_param("regexp_filter") {
            Some(regexp) => result.regexp_filter(&regexp),
            None => {}
        }
        self.process_redlinks(&result)?;
        Platform::profile("after process_redlinks", Some(result.len()));
        self.process_creator(&result)?;
        Platform::profile("after process_creator", Some(result.len()));

        Ok(())
    }

    pub fn state(&self) -> Arc<AppState> {
        self.state.clone()
    }

    fn convert_to_common_wiki(&self, result: &PageList) -> Result<(), String> {
        // Find best wiki to convert to
        match self.get_param_default("common_wiki", "auto").as_str() {
            "auto" => {}
            "cats" => result.convert_to_wiki(
                &self
                    .wiki_by_source
                    .get("categories")
                    .ok_or(format!("categories wiki requested as output, but not set"))?,
                &self,
            )?,
            "pagepile" => result.convert_to_wiki(
                &self
                    .wiki_by_source
                    .get("pagepile")
                    .ok_or(format!("pagepile wiki requested as output, but not set"))?,
                &self,
            )?,
            "manual" => result.convert_to_wiki(
                &self
                    .wiki_by_source
                    .get("manual")
                    .map(|s| s.to_string())
                    .or_else(|| self.get_param("common_wiki_other"))
                    .ok_or(format!("manual wiki requested as output, but not set"))?,
                &self,
            )?,
            "wikidata" => result.convert_to_wiki("wikidatawiki", &self)?,
            "other" => result.convert_to_wiki(
                &self.get_param("common_wiki_other").ok_or(format!(
                    "Other wiki for output expected, but not given in text field"
                ))?,
                &self,
            )?,
            unknown => return Err(format!("Unknown output wiki type '{}'", &unknown)),
        }
        Ok(())
    }

    fn apply_results_limit(&self, pages: &mut Vec<PageListEntry>) {
        let limit = self
            .get_param_default("output_limit", "0")
            .parse::<usize>()
            .unwrap_or(0);
        if limit != 0 && limit < pages.len() {
            pages.resize(limit, PageListEntry::new(Title::new("", 0)));
        }
    }

    // Prepares for JS "creator" mode
    // Chackes which labels already exist on Wikidata
    fn process_creator(&self, result: &PageList) -> Result<(), String> {
        if result.is_empty() || result.is_wikidata() {
            return Ok(());
        }
        if !self.has_param("show_redlinks")
            && self.get_param_blank("wikidata_item") != "without".to_string()
        {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = result
                .to_sql_batches(PAGE_BATCH_SIZE)
                .par_iter_mut()
                .map(|mut sql_batch| {
                    // Text for any label or alias used in an item
                    sql_batch.0 = "SELECT wbx_text FROM wbt_text WHERE EXISTS (SELECT * FROM wbt_item_terms,wbt_type,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id = wbtl_id AND wbtl_type_id = wby_id AND wby_name IN ('label','alias') AND wbtl_text_in_lang_id = wbxl_id AND wbxl_text_id = wbx_id) AND wbx_text IN (".to_string() ;
                    // One of these
                    sql_batch.0 += &Platform::get_questionmarks(sql_batch.1.len()) ;
                    sql_batch.0 += ")";
                    // Looking for labels, so spaces instead of underscores
                    for element in sql_batch.1.iter_mut() {
                        *element = Title::underscores_to_spaces(element);
                    }
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

        let state = self.state();
        let db_user_pass = match state.get_db_mutex().lock() {
            Ok(db) => db,
            Err(e) => return Err(format!("Bad mutex: {:?}", e)),
        };
        let mut conn = self
            .state
            .get_wiki_db_connection(&db_user_pass, &"wikidatawiki".to_string())?;

        let mut error: Option<String> = None;
        batches.iter().for_each(|sql| {
            let result = match conn.prep_exec(&sql.0, &sql.1) {
                Ok(r) => r,
                Err(e) => {
                    error = Some(format!("{:?}", e));
                    return;
                }
            };
            for row in result {
                match row {
                    Ok(row) => match my::from_row_opt::<Vec<u8>>(row) {
                        Ok(wbx_text) => {
                            let wbx_text = String::from_utf8_lossy(&wbx_text).into_owned();
                            self.existing_labels.write().unwrap().insert(wbx_text);
                        }
                        Err(_e) => {}
                    },
                    _ => {} // Ignore error
                }
            }
        });
        match error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn process_redlinks(&self, result: &PageList) -> Result<(), String> {
        if result.is_empty() || !self.do_output_redlinks() || result.is_wikidata() {
            return Ok(());
        }
        let ns0_only = self.has_param("article_redlinks_only");
        let remove_template_redlinks = self.has_param("remove_template_redlinks");

        let batches: Vec<SQLtuple> = result
                .to_sql_batches(PAGE_BATCH_SIZE/20) // ???
                .par_iter_mut()
                .map(|mut sql_batch| {
                    let mut sql = "SELECT pl_title,pl_namespace,(SELECT COUNT(*) FROM page p1 WHERE p1.page_title=pl0.pl_title AND p1.page_namespace=pl0.pl_namespace) AS cnt from page p0,pagelinks pl0 WHERE pl_from=p0.page_id AND ".to_string() ;
                    sql += &sql_batch.0 ;
                    if ns0_only {sql += " AND pl_namespace=0" ;}
                    else {sql += " AND pl_namespace>=0" ;}
                    if remove_template_redlinks {
                        sql += " AND NOT EXISTS (SELECT * FROM pagelinks pl1 WHERE pl1.pl_from_namespace=10 AND pl0.pl_namespace=pl1.pl_namespace AND pl0.pl_title=pl1.pl_title LIMIT 1)" ;
                    }
                    sql += " GROUP BY page_id,pl_namespace,pl_title" ;
                    sql += " HAVING cnt=0" ;

                    sql_batch.0 = sql ;
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

        let redlink_counter: HashMap<Title, LinkCount> = HashMap::new();
        let redlink_counter = Arc::new(RwLock::new(redlink_counter));

        let wiki = match result.wiki() {
            Some(wiki) => wiki.to_owned(),
            None => return Err(format!("Platform::process_redlinks: no wiki set in result")),
        };

        let error: Option<String> = None;
        let error = Mutex::new(error);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(5) // TODO More? Less?
            .build()
            .expect("process_redlinks: can't build ThreadPool");
        pool.install(|| {
            batches.par_iter().for_each(|sql| {
                let db_user_pass = match self.state.get_db_mutex().lock() {
                    Ok(db) => db,
                    Err(e) => {
                        *error.lock().unwrap() = Some(format!("Bad mutex: {:?}", e));
                        return;
                    }
                };
                let mut conn = match self.state.get_wiki_db_connection(&db_user_pass, &wiki) {
                    Ok(conn) => conn,
                    Err(e) => {
                        *error.lock().unwrap() = Some(format!("Bad mutex: {:?}", e));
                        return;
                    }
                };

                let new_result = match conn.prep_exec(&sql.0, &sql.1) {
                    Ok(r) => r,
                    Err(e) => {
                        *error.lock().unwrap() = Some(format!("{:?}", e));
                        return;
                    }
                };
                for row in new_result {
                    match row {
                        Ok(row) => {
                            let (page_title, namespace_id, _count) =
                                my::from_row::<(String, NamespaceID, u8)>(row);
                            let title = Title::new(&page_title, namespace_id);
                            let new_value = match &redlink_counter.read().unwrap().get(&title) {
                                Some(x) => *x + 1,
                                None => 1,
                            };
                            redlink_counter.write().unwrap().insert(title, new_value);
                        }
                        _ => {} // Ignore error
                    }
                }
            });
        });

        let error = &*error.lock().unwrap();
        match error {
            Some(e) => return Err(e.to_string()),
            None => {}
        }

        let min_redlinks = self
            .get_param_default("min_redlink_count", "1")
            .parse::<LinkCount>()
            .unwrap_or(1);
        let mut redlink_counter = redlink_counter.write().unwrap();
        redlink_counter.retain(|_, &mut v| v >= min_redlinks);
        result.set_entries(
            redlink_counter
                .par_iter()
                .map(|(k, redlink_count)| {
                    let mut ret = PageListEntry::new(k.to_owned());
                    ret.redlink_count = Some(*redlink_count);
                    ret
                })
                .collect(),
        );
        Ok(())
    }

    fn process_subpages(&self, result: &PageList) -> Result<(), String> {
        let add_subpages = self.has_param("add_subpages");
        let subpage_filter = self.get_param_default("subpage_filter", "either");
        if !add_subpages && subpage_filter != "subpages" && subpage_filter != "no_subpages" {
            return Ok(());
        }

        if add_subpages {
            let title_ns: Vec<(String, NamespaceID)> = result
                .entries()
                .read()
                .unwrap()
                .par_iter()
                .map(|entry| {
                    (
                        entry.title().with_underscores().to_owned(),
                        entry.title().namespace_id(),
                    )
                })
                .collect();

            let wiki = match result.wiki() {
                Some(wiki) => wiki.to_owned(),
                None => return Err(format!("Platform::process_redlinks: no wiki set in result")),
            };
            let db_user_pass = match self.state.get_db_mutex().lock() {
                Ok(db) => db,
                Err(e) => return Err(format!("Bad mutex: {:?}", e)),
            };
            let mut conn = self.state.get_wiki_db_connection(&db_user_pass, &wiki)?;

            let mut error: Option<String> = None;
            title_ns.iter().for_each(|(title, namespace_id)| {
                let sql: SQLtuple = (
                    "SELECT page_title,page_namespace FROM page WHERE page_namespace=? AND page_title LIKE ?"
                        .to_string(),
                    vec![namespace_id.to_string(), format!("{}/%", &title)],
                );
                let db_result = match conn.prep_exec(&sql.0, &sql.1) {
                    Ok(r) => r,
                    Err(e) => {
                        error = Some(format!("{:?}", e));
                        return;
                    }
                };
                db_result.filter_map(|row_result|row_result.ok())
                .filter_map(|row| my::from_row_opt::<(Vec<u8>,NamespaceID)>(row).ok() )
                .for_each(|(page_title,page_namespace)|{
                    let page_title = String::from_utf8_lossy(&page_title).into_owned();
                    result.add_entry(PageListEntry::new(Title::new(&page_title,page_namespace)));
                });
            });
            match error {
                Some(e) => return Err(e),
                None => {}
            }
            // TODO if new pages were added, they should get some of the post_process_result treatment as well
        }

        if subpage_filter != "subpages" && subpage_filter != "no_subpages" {
            return Ok(());
        }

        let keep_subpages = subpage_filter == "subpages";
        result.retain_entries(&|entry: &PageListEntry| {
            let has_slash = entry.title().pretty().find('/').is_some();
            has_slash == keep_subpages
        });
        /*
        result.entries.retain(|entry| {
            let has_slash = entry.title().pretty().find('/').is_some();
            has_slash == keep_subpages
        });
        */
        Ok(())
    }

    fn process_pages(&self, result: &PageList) -> Result<(), String> {
        let add_coordinates = self.has_param("add_coordinates");
        let add_image = self.has_param("add_image");
        let add_defaultsort = self.has_param("add_defaultsort");
        let add_disambiguation = self.has_param("add_disambiguation");
        let add_incoming_links = self.get_param_blank("sortby") == "incoming_links";
        if !add_coordinates
            && !add_image
            && !add_defaultsort
            && !add_disambiguation & !add_incoming_links
        {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = result
                .to_sql_batches(PAGE_BATCH_SIZE)
                .par_iter_mut()
                .map(|mut sql_batch| {
                    let mut sql ="SELECT page_title,page_namespace".to_string();
                    if add_image {sql += ",(SELECT pp_value FROM page_props WHERE pp_page=page_id AND pp_propname IN ('page_image','page_image_free') LIMIT 1) AS image" ;}
                    if add_coordinates {sql += ",(SELECT concat(gt_lat,',',gt_lon) FROM geo_tags WHERE gt_primary=1 AND gt_globe='earth' AND gt_page_id=page_id LIMIT 1) AS coord" ;}
                    if add_defaultsort {sql += ",(SELECT pp_value FROM page_props WHERE pp_page=page_id AND pp_propname='defaultsort' LIMIT 1) AS defaultsort" ;}
                    if add_disambiguation {sql += ",(SELECT pp_value FROM page_props WHERE pp_page=page_id AND pp_propname='disambiguation' LIMIT 1) AS disambiguation" ;}
                    if add_incoming_links {sql += ",(SELECT count(*) FROM pagelinks WHERE pl_namespace=page_namespace AND pl_title=page_title AND pl_from_namespace=0) AS incoming_links" ;}
                    sql += " FROM page WHERE " ;
                    sql_batch.0 = sql + &sql_batch.0 ;
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

        result.annotate_batch_results(
            &self.state(),
            batches,
            0,
            1,
            &|row: my::Row, entry: &mut PageListEntry| {
                let mut parts = row.unwrap(); // Unwrap into vector, should be safe
                parts.remove(0); // page_title
                parts.remove(0); // page_namespace
                if add_image {
                    entry.set_page_image(match parts.remove(0) {
                        my::Value::Bytes(s) => String::from_utf8(s).ok(),
                        _ => None,
                    });
                }
                if add_coordinates {
                    let coordinates = match parts.remove(0) {
                        my::Value::Bytes(s) => match String::from_utf8(s) {
                            Ok(lat_lon) => PageCoordinates::new_from_lat_lon(&lat_lon),
                            _ => None,
                        },
                        _ => None,
                    };
                    entry.set_coordinates(coordinates);
                }
                if add_defaultsort {
                    entry.set_defaultsort(match parts.remove(0) {
                        my::Value::Bytes(s) => String::from_utf8(s).ok(),
                        _ => None,
                    });
                }
                if add_disambiguation {
                    entry.disambiguation = match parts.remove(0) {
                        my::Value::NULL => TriState::No,
                        _ => TriState::Yes,
                    }
                }
                if add_incoming_links {
                    entry.incoming_links = match parts.remove(0) {
                        my::Value::Int(i) => Some(i as LinkCount),
                        _ => None,
                    };
                }
            },
        )
    }

    fn process_files(&self, result: &PageList) -> Result<(), String> {
        let giu = self.has_param("giu");
        let file_data = self.has_param("ext_image_data")
            || self.get_param("sortby") == Some("filesize".to_string())
            || self.get_param("sortby") == Some("uploaddate".to_string());
        let file_usage = giu || self.has_param("file_usage_data");
        let file_usage_data_ns0 = self.has_param("file_usage_data_ns0");

        if file_usage {
            let batches: Vec<SQLtuple> = result
                .to_sql_batches_namespace(PAGE_BATCH_SIZE,6)
                .par_iter_mut()
                .map(|mut sql_batch| {
                    let tmp = Platform::prep_quote(&sql_batch.1);
                    sql_batch.0 = "SELECT gil_to,6 AS namespace_id,GROUP_CONCAT(gil_wiki,':',gil_page_namespace_id,':',gil_page_namespace,':',gil_page_title SEPARATOR '|') AS gil_group FROM globalimagelinks WHERE gil_to IN (".to_string() ;
                    sql_batch.0 += &tmp.0 ;
                    sql_batch.0 += ")";
                    if file_usage_data_ns0  {sql_batch.0 += " AND gil_page_namespace_id=0" ;}
                    sql_batch.0 += " GROUP BY gil_to" ;
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

            result.annotate_batch_results(
                &self.state(),
                batches,
                0,
                1,
                &|row: my::Row, entry: &mut PageListEntry| match PageList::string_from_row(&row, 2)
                {
                    Some(gil_group) => {
                        let fi = FileInfo::new_from_gil_group(&gil_group);
                        entry.set_file_info(Some(fi));
                    }
                    None => {}
                },
            )?;
        }

        if file_data {
            let batches: Vec<SQLtuple> = result
                .to_sql_batches(PAGE_BATCH_SIZE)
                .par_iter_mut()
                .map(|mut sql_batch| {
                    let tmp = Platform::prep_quote(&sql_batch.1);
                    sql_batch.0 = "SELECT img_name,6 AS namespace_id,img_size,img_width,img_height,img_media_type,img_major_mime,img_minor_mime,img_user_text,img_timestamp,img_sha1 FROM image_compat WHERE img_name IN (".to_string() ;
                    sql_batch.0 += &tmp.0 ;
                    sql_batch.0 += ")";
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

            result.annotate_batch_results(
                &self.state(),
                batches,
                0,
                1,
                &|row: my::Row, entry: &mut PageListEntry| {
                    let (
                        _img_name,
                        _namespace_id,
                        img_size,
                        img_width,
                        img_height,
                        img_media_type,
                        img_major_mime,
                        img_minor_mime,
                        img_user_text,
                        img_timestamp,
                        img_sha1,
                    ) = my::from_row::<(
                        String,
                        usize,
                        usize,
                        usize,
                        usize,
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                    )>(row);
                    let mut file_info = match entry.get_file_info() {
                        Some(fi) => fi,
                        None => FileInfo::new(),
                    };
                    file_info.img_size = Some(img_size);
                    file_info.img_width = Some(img_width);
                    file_info.img_height = Some(img_height);
                    file_info.img_media_type = Some(img_media_type);
                    file_info.img_major_mime = Some(img_major_mime);
                    file_info.img_minor_mime = Some(img_minor_mime);
                    file_info.img_user_text = Some(img_user_text);
                    file_info.img_timestamp = Some(img_timestamp);
                    file_info.img_sha1 = Some(img_sha1);
                    entry.set_file_info(Some(file_info));
                },
            )?;
        }
        Ok(())
    }

    fn annotate_with_wikidata_item(&self, result: &PageList) -> Result<(), String> {
        if result.is_wikidata() {
            return Ok(());
        }

        let wiki = match result.wiki().clone() {
            Some(wiki) => wiki.to_string(),
            None => return Ok(()), // TODO is it OK to just ignore? Error for "no wiki set"?
        };
        let api = self.state.get_api_for_wiki(wiki.to_owned())?;

        // Using Wikidata
        let titles: Vec<String> = result
            .entries()
            .read()
            .unwrap()
            .par_iter()
            .filter_map(|entry| entry.title().full_pretty(&api))
            .collect();

        let mut batches: Vec<SQLtuple> = vec![];
        titles.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {

            let escaped: Vec<String> = chunk
                .par_iter()
                .filter_map(|s| match s.trim() {
                    "" => None,
                    other => Some(other.to_string()),
                })
                .collect();
            let mut sql = (Platform::get_questionmarks(escaped.len()), escaped);

            sql.0 = format!("SELECT ips_site_page,ips_item_id FROM wb_items_per_site WHERE ips_site_id='{}' and ips_site_page IN ({})", &wiki,&sql.0);
            batches.push(sql);
        });

        // Duplicated from Patelist::annotate_batch_results
        let rows: Mutex<Vec<my::Row>> = Mutex::new(vec![]);
        let error: Mutex<Option<String>> = Mutex::new(None);

        batches.par_iter().for_each(|sql| {
            // Get DB connection
            let db_user_pass = match self.state.get_db_mutex().lock() {
                Ok(db) => db,
                Err(e) => {
                    *error.lock().unwrap() = Some(format!("Bad mutex: {:?}", e));
                    return;
                }
            };
            let mut conn = match self.state.get_wiki_db_connection(&db_user_pass, &"wikidatawiki".to_string()) {
                Ok(conn) => conn,
                Err(e) => {
                    *error.lock().unwrap() = Some(format!("Bad mutex: {:?}", e));
                    return;
                }
            };

            // Run query
            let mut result = match conn.prep_exec(&sql.0, &sql.1) {
                Ok(r) => r.filter_map(|row| row.ok()).collect(),
                Err(e) => {
                    *error.lock().unwrap() = Some(format!("Platform::annotate_with_wikidata_item: Can't connect to wikidatawiki: {:?}", e));
                    return;
                }
            };

            rows.lock().unwrap().append(&mut result);
        });

        // Check error
        match &*error.lock().unwrap() {
            Some(e) => return Err(e.to_string()),
            None => {}
        }

        // Rows to entries
        rows.lock().unwrap().iter().for_each(|row| {
            let full_page_title = match row.get(0) {
                Some(title) => match title {
                    my::Value::Bytes(uv) => match String::from_utf8(uv) {
                        Ok(s) => s,
                        Err(_) => return,
                    },
                    _ => return,
                },
                None => return,
            };
            let ips_item_id = match row.get(1) {
                Some(title) => match title {
                    my::Value::Int(i) => i,
                    _ => return,
                },
                None => return,
            };
            let title = Title::new_from_full(&full_page_title, &api);
            let tmp_entry = PageListEntry::new(title);
            let mut entry = match result.entries().read().unwrap().get(&tmp_entry) {
                Some(e) => (*e).clone(),
                None => return,
            };

            //f(row.clone(), &mut entry);
            //let (ips_site_page,ips_item_id) = my::from_row::<(String, u64)>(*row);
            let q = "Q".to_string() + &ips_item_id.to_string();
            entry.set_wikidata_item(Some(q));

            result.add_entry(entry);
        });
        Ok(())

        /*
        // THIS WOULD BE NICE BUT page_props HAS DAYS OF DATA LAG OR IS FAULTY
        // Batches
        let batches: Vec<SQLtuple> = result.to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql|{
                sql.0 = "SELECT page_title,page_namespace,pp_value FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ".to_owned()+&sql.0;
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.annotate_batch_results(
            self.state(),
            batches,
            0,
            1,
            &|row: my::Row, entry: &mut PageListEntry| {
                let (_page_title, _page_namespace, pp_value) =
                    my::from_row::<(String, NamespaceID, String)>(row);
                entry.wikidata_item = Some(pp_value);
            },
        )
        */
    }

    /// Filters on whether a page has a Wikidata item, depending on the "wikidata_item"
    fn process_by_wikidata_item(&self, result: &PageList) -> Result<(), String> {
        if result.is_wikidata() {
            return Ok(());
        }
        let wdi = self.get_param_default("wikidata_item", "no");
        if wdi != "any" && wdi != "with" && wdi != "without" {
            return Ok(());
        }
        self.annotate_with_wikidata_item(result)?;
        if wdi == "with" {
            result.retain_entries(&|entry| entry.get_wikidata_item().is_some());
        }
        if wdi == "without" {
            result.retain_entries(&|entry| entry.get_wikidata_item().is_none());
        }
        Ok(())
    }

    /// Adds page properties that might be missing if none of the original sources was "categories"
    fn process_missing_database_filters(&self, result: &PageList) -> Result<(), String> {
        let mut params = SourceDatabaseParameters::db_params(self);
        params.set_wiki(Some(result.wiki().ok_or(format!(
            "Platform::process_missing_database_filters: result has no wiki"
        ))?));
        let mut db = SourceDatabase::new(params);
        let new_result = db.get_pages(&self.state, Some(result))?;
        result.set_from(new_result);
        Ok(())
    }

    fn process_labels_old(&self, result: &PageList) -> Result<(), String> {
        let mut sql = self.get_label_sql();
        if sql.1.is_empty() {
            return Ok(());
        }
        result.convert_to_wiki("wikidatawiki", &self)?;
        if result.is_empty() {
            return Ok(());
        }
        sql.0 += " AND term_full_entity_id IN (";

        // Batches
        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql_batch| {
                let tmp = Platform::prep_quote(&sql_batch.1);
                sql_batch.0 = sql.0.to_owned() + &tmp.0 + ")";
                sql_batch.1.splice(..0, sql.1.to_owned());
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        result.process_batch_results(&self.state(), batches, &|row: my::Row| {
            let term_full_entity_id = my::from_row::<String>(row);
            Platform::entry_from_entity(&term_full_entity_id)
        })
    }

    //________________________________________________________________________________________________

    fn get_label_sql_helper_new(&self, ret: &mut SQLtuple, part1: &str) {
        let mut wbt_type: Vec<String> = vec![];
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_l")) {
            wbt_type.push("1".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_a")) {
            wbt_type.push("3".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_d")) {
            wbt_type.push("2".to_string());
        }
        if !wbt_type.is_empty() {
            if wbt_type.len() == 1 {
                ret.0 += &format!(" AND wbtl_type_id={}", wbt_type.join(","));
            } else {
                ret.0 += &format!(" AND wbtl_type_id IN ({})", wbt_type.join(","));
            }
        }
    }

    fn get_label_sql_subquery_new(
        &self,
        ret: &mut SQLtuple,
        key: &str,
        languages: &Vec<String>,
        s: &String,
    ) {
        let has_pattern = !s.is_empty() && s != "%";
        let has_languages = !languages.is_empty();
        ret.0 += "SELECT * FROM wbt_term_in_lang,wbt_item_terms t2";
        if has_languages || has_pattern {
            ret.0 += ",wbt_text_in_lang";
        }
        if has_pattern {
            ret.0 += ",wbt_text";
        }
        ret.0 += " WHERE t2.wbit_item_id=t1.wbit_item_id AND wbtl_id=t2.wbit_term_in_lang_id";
        self.get_label_sql_helper_new(ret, key);
        if has_languages || has_pattern {
            let mut tmp = Self::prep_quote(&languages);
            ret.0 += " AND wbtl_text_in_lang_id=wbxl_id";
            if !tmp.1.is_empty() {
                if tmp.1.len() == 1 {
                    ret.0 += &(" AND wbxl_language=".to_owned() + &tmp.0);
                } else {
                    ret.0 += &(" AND wbxl_language IN (".to_owned() + &tmp.0 + ")");
                }
                ret.1.append(&mut tmp.1);
            }
            if has_pattern {
                ret.0 += " AND wbxl_text_id=wbx_id AND wbx_text LIKE ?";
                ret.1.push(s.to_string());
            }
        }
    }

    fn get_label_sql_new(&self, namespace_id: &NamespaceID) -> Option<SQLtuple> {
        lazy_static! {
            static ref RE1: Regex =
                Regex::new(r#"[^a-z,]"#).expect("Platform::get_label_sql Regex is invalid");
        }
        let mut ret: SQLtuple = ("".to_string(), vec![]);
        let yes = self.get_param_as_vec("labels_yes", "\n");
        let any = self.get_param_as_vec("labels_any", "\n");
        let no = self.get_param_as_vec("labels_no", "\n");
        if yes.len() + any.len() + no.len() == 0 {
            return None;
        }

        let langs_yes = self.get_param_as_vec("langs_labels_yes", ",");
        let langs_any = self.get_param_as_vec("langs_labels_any", ",");
        let langs_no = self.get_param_as_vec("langs_labels_no", ",");

        if *namespace_id == 0 {
            ret.0 =
                "SELECT DISTINCT CONCAT('Q',wbit_item_id) AS term_full_entity_id FROM wbt_item_terms t1 WHERE 1=1".to_string();
        } else if *namespace_id == 120 {
            ret.0 = "SELECT DISTINCT CONACT('P',wbit_property_id) AS term_full_entity_id FROM wbt_property_terms t1 WHERE 1=1"
                .to_string();
        } else {
            return None;
        }

        yes.iter().for_each(|s| {
            ret.0 += " AND EXISTS (";
            self.get_label_sql_subquery_new(&mut ret, "yes", &langs_yes, s);
            ret.0 += ")";
        });

        if !langs_any.is_empty() {
            ret.0 += " AND (0=1";
            any.iter().for_each(|s| {
                ret.0 += " OR EXISTS (";
                self.get_label_sql_subquery_new(&mut ret, "any", &langs_any, s);
                ret.0 += ")";
            });
            ret.0 += ")";
        }

        no.iter().for_each(|s| {
            ret.0 += " AND NOT EXISTS (";
            self.get_label_sql_subquery_new(&mut ret, "no", &langs_no, s);
            ret.0 += ")";
        });
        Some(ret)
    }

    /// Using new wbt_item_terms
    fn process_labels_new(&self, result: &PageList) -> Result<(), String> {
        if self.get_label_sql_new(&0).is_none() {
            return Ok(());
        }
        result.convert_to_wiki("wikidatawiki", &self)?;
        if result.is_empty() {
            return Ok(());
        }

        // Batches
        let batches: Vec<SQLtuple> = result
            .group_by_namespace()
            .par_iter()
            .filter_map(|(namespace_id, titles)| {
                let mut sql = self.get_label_sql_new(namespace_id)?;
                if *namespace_id == 0 {
                    sql.0 += " AND wbit_item_id IN (";
                } else if *namespace_id == 120 {
                    sql.0 += " AND wbit_property_id IN (";
                } else {
                    return None;
                }
                sql.0 += &titles
                    .par_iter()
                    .map(|title| title[1..].to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                sql.0 += ")";
                Some(sql)
            })
            .collect();

        result.clear_entries();
        result.process_batch_results(&self.state(), batches, &|row: my::Row| {
            let term_full_entity_id = my::from_row::<String>(row);
            Platform::entry_from_entity(&term_full_entity_id)
        })
    }

    fn process_labels(&self, result: &PageList) -> Result<(), String> {
        if false {
            self.process_labels_old(result)
        } else {
            self.process_labels_new(result)
        }
    }

    fn process_sitelinks(&self, result: &PageList) -> Result<(), String> {
        if result.is_empty() {
            return Ok(());
        }

        let sitelinks_yes = self.get_param_as_vec("sitelinks_yes", "\n");
        let sitelinks_any = self.get_param_as_vec("sitelinks_any", "\n");
        let sitelinks_no = self.get_param_as_vec("sitelinks_no", "\n");
        let sitelinks_min = self.get_param_blank("min_sitelink_count");
        let sitelinks_max = self.get_param_blank("max_sitelink_count");

        if sitelinks_yes.is_empty()
            && sitelinks_any.is_empty()
            && sitelinks_no.is_empty()
            && sitelinks_min.is_empty()
            && sitelinks_max.is_empty()
        {
            return Ok(());
        }
        let old_wiki = result.wiki().to_owned();
        result.convert_to_wiki("wikidatawiki", &self)?;
        if result.is_empty() {
            return Ok(());
        }

        let use_min_max = !sitelinks_min.is_empty() || !sitelinks_max.is_empty();

        let mut sql: SQLtuple = ("".to_string(), vec![]);
        sql.0 += "SELECT ";
        if use_min_max {
            sql.0 += "page_title,(SELECT count(*) FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1) AS sitelink_count" ;
        } else {
            sql.0 += "DISTINCT page_title,0";
        }
        sql.0 += " FROM page WHERE page_namespace=0";

        sitelinks_yes.iter().for_each(|site|{
            sql.0 += " AND EXISTS (SELECT * FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1 AND ips_site_id=? LIMIT 1)" ;
            sql.1.push(site.to_string());
        });
        if !sitelinks_any.is_empty() {
            sql.0 += " AND EXISTS (SELECT * FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1 AND ips_site_id IN (" ;
            let tmp = Platform::prep_quote(&sitelinks_any);
            Platform::append_sql(&mut sql, tmp);
            sql.0 += ") LIMIT 1)";
        }
        sitelinks_no.iter().for_each(|site|{
            sql.0 += " AND NOT EXISTS (SELECT * FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1 AND ips_site_id=? LIMIT 1)" ;
            sql.1.push(site.to_string());
        });
        sql.0 += " AND ";

        let mut having: Vec<String> = vec![];
        match sitelinks_min.parse::<usize>() {
            Ok(s) => having.push(format!("sitelink_count>={}", s)),
            _ => {}
        }
        match sitelinks_max.parse::<usize>() {
            Ok(s) => having.push(format!("sitelink_count<={}", s)),
            _ => {}
        }

        let mut sql_post = "".to_string();
        if use_min_max {
            sql_post += " GROUP BY page_title";
        }
        if !having.is_empty() {
            sql_post += " HAVING ";
            sql_post += &having.join(" AND ");
        }

        // Batches
        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql_batch| {
                sql_batch.0 = sql.0.to_owned() + &sql_batch.0 + &sql_post;
                sql_batch.1.splice(..0, sql.1.to_owned());
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        result.process_batch_results(&self.state(), batches, &|row: my::Row| {
            let (page_title, _sitelinks_count) = my::from_row::<(String, usize)>(row);
            Some(PageListEntry::new(Title::new(&page_title, 0)))
        })?;

        match old_wiki {
            Some(wiki) => result.convert_to_wiki(&wiki, &self)?,
            None => {}
        }
        Ok(())
    }

    fn filter_wikidata(&self, result: &PageList) -> Result<(), String> {
        if result.is_empty() {
            return Ok(());
        }
        let no_statements = self.has_param("wpiu_no_statements");
        let no_sitelinks = self.has_param("wpiu_no_sitelinks");
        let wpiu = self.get_param_default("wpiu", "any");
        let list = self.get_param_blank("wikidata_prop_item_use");
        let list = list.trim();
        if list.is_empty() && !no_statements && !no_sitelinks {
            return Ok(());
        }
        let original_wiki = result.wiki();
        Platform::profile("before filter_wikidata:convert_to_wiki", Some(result.len()));
        result.convert_to_wiki("wikidatawiki", &self)?;
        Platform::profile("after filter_wikidata:convert_to_wiki", Some(result.len()));
        if result.is_empty() {
            match original_wiki {
                Some(wiki) => result.convert_to_wiki(&wiki, &self)?,
                None => {}
            }
            return Ok(());
        }
        // For all/any/none
        let parts = list
            .split_terminator(',')
            .filter_map(|s| match s.chars().nth(0) {
                Some('Q') => Some((
                    "(SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=0 AND pl_title=?)".to_string(),
                    vec![s.to_string()],
                )),
                Some('P') => Some((
                    "(SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=120 AND pl_title=?)".to_string(),
                    vec![s.to_string()],
                )),
                _ => None,
            })
            .collect::<Vec<SQLtuple>>();

        let mut sql_post: SQLtuple = ("".to_string(), vec![]);
        if no_statements {
            sql_post.0 += " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-claims' AND pp_sortkey=0)" ;
        }
        if no_sitelinks {
            sql_post.0 += " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-sitelinks' AND pp_sortkey=0)" ;
        }
        if !parts.is_empty() {
            match wpiu.as_str() {
                "all" => {
                    parts.iter().for_each(|sql| {
                        sql_post.0 += &(" AND EXISTS ".to_owned() + &sql.0);
                        sql_post.1.append(&mut sql.1.to_owned());
                    });
                }
                "any" => {
                    sql_post.0 += " AND (0";
                    parts.iter().for_each(|sql| {
                        sql_post.0 += &(" OR EXISTS ".to_owned() + &sql.0);
                        sql_post.1.append(&mut sql.1.to_owned());
                    });
                    sql_post.0 += ")";
                }
                "none" => {
                    parts.iter().for_each(|sql| {
                        sql_post.0 += &(" AND NOT EXISTS ".to_owned() + &sql.0);
                        sql_post.1.append(&mut sql.1.to_owned());
                    });
                }
                _ => {}
            }
        }

        // Batches
        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql| {
                sql.0 = "SELECT DISTINCT page_title FROM page WHERE ".to_owned()
                    + &sql.0
                    + &sql_post.0.to_owned();
                sql.1.append(&mut sql_post.1.to_owned());
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        let ret = result.process_batch_results(&self.state(), batches, &|row: my::Row| {
            let pp_value: String = my::from_row(row);
            Some(PageListEntry::new(Title::new(&pp_value, 0)))
        });
        match original_wiki {
            Some(wiki) => result.convert_to_wiki(&wiki, &self)?,
            None => {}
        }
        ret
    }

    pub fn entry_from_entity(entity: &str) -> Option<PageListEntry> {
        // TODO media-info?
        match entity.chars().next() {
            Some('Q') => Some(PageListEntry::new(Title::new(&entity.to_string(), 0))),
            Some('P') => Some(PageListEntry::new(Title::new(&entity.to_string(), 120))),
            Some('L') => Some(PageListEntry::new(Title::new(&entity.to_string(), 146))),
            _ => None,
        }
    }

    pub fn usize_option_from_param(&self, key: &str) -> Option<usize> {
        self.get_param(key)?.parse::<usize>().ok()
    }

    pub fn get_main_wiki(&self) -> Option<String> {
        let language = self.get_param_default("lang", "en"); // Fallback
        let language = self
            .get_param_default("language", &language)
            .replace("_", "-");
        let project = self.get_param_default("project", "wikipedia");
        self.get_wiki_for_language_project(&language, &project)
    }

    pub fn get_wiki_for_language_project(
        &self,
        language: &String,
        project: &String,
    ) -> Option<String> {
        match (language.as_str(), project.as_str()) {
            (language, "wikipedia") => Some(language.to_owned() + "wiki"),
            ("commons", _) => Some("commonswiki".to_string()),
            ("wikidata", _) => Some("wikidatawiki".to_string()),
            (_, "wikidata") => Some("wikidatawiki".to_string()),
            (l, p) => {
                let url = "https://".to_string() + &l + "." + &p + ".org";
                self.state.get_wiki_for_server_url(&url)
            }
        }
    }

    pub fn get_response(&self) -> Result<MyResponse, String> {
        // Shortcut: WDFIST
        match &self.wdfist_result {
            Some(j) => {
                return Ok(self
                    .state
                    .output_json(j, self.form_parameters.params.get("callback")));
            }
            None => {}
        }

        let result = match &self.result {
            Some(result) => result,
            None => return Err(format!("Platform::get_response: No result")),
        };
        let wiki = match result.wiki() {
            Some(wiki) => wiki,
            None => return Err(format!("Platform::get_response: No wiki in result")),
        };

        let mut sortby = self.get_param_blank("sortby");
        let mut sort_order = self.get_param_blank("sortorder") == "descending".to_string();
        if self.do_output_redlinks() && (sortby.is_empty() || sortby == "none") {
            sortby = "redlinks".to_string();
            sort_order = true;
        }
        let mut pages =
            result.drain_into_sorted_vec(PageListSort::new_from_params(&sortby, sort_order));
        drop(result);
        self.apply_results_limit(&mut pages);

        let renderer: Box<dyn Render> = match self.get_param_blank("format").as_str() {
            "wiki" => RenderWiki::new(),
            "csv" => RenderTSV::new(","),
            "tsv" => RenderTSV::new("\t"),
            "json" => RenderJSON::new(),
            "pagepile" => RenderPagePile::new(),
            _ => RenderHTML::new(),
        };
        renderer.response(&self, &wiki, pages)
    }

    pub fn get_param_as_vec(&self, param: &str, separator: &str) -> Vec<String> {
        match self.get_param(param) {
            Some(s) => s
                .split(separator)
                .map(|s| s.trim().trim_matches('\u{200E}').trim_matches('\u{200F}')) // See https://doc.rust-lang.org/reference/whitespace.html
                .filter(|s| !s.is_empty())
                .map(|s| Title::spaces_to_underscores(&s.to_string()))
                .collect(),
            None => vec![],
        }
    }

    pub fn get_param_blank(&self, param: &str) -> String {
        self.get_param(param).unwrap_or("".to_string())
    }

    pub fn get_param_default(&self, param: &str, default: &str) -> String {
        let ret = self.get_param(param).unwrap_or(default.to_string());
        if ret.is_empty() {
            default.to_string()
        } else {
            ret
        }
    }

    pub fn append_sql(sql: &mut SQLtuple, mut sub: SQLtuple) {
        sql.0 += &sub.0;
        sql.1.append(&mut sub.1);
    }

    /// Returns a tuple with a string containing comma-separated question marks, and the (non-empty) Vec elements
    pub fn prep_quote(strings: &[String]) -> SQLtuple {
        let escaped: Vec<String> = strings
            .par_iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        (Platform::get_questionmarks(escaped.len()), escaped)
    }

    pub fn full_entity_id_to_number(strings: &[String]) -> SQLtuple {
        let escaped: Vec<String> = strings
            .par_iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s[1..].to_string())
            .collect();
        (Platform::get_questionmarks(escaped.len()), escaped)
    }

    // Generates a string with `len` comma-separated question marks
    pub fn get_questionmarks(len: usize) -> String {
        let mut questionmarks: Vec<String> = Vec::new();
        questionmarks.resize(len, "?".to_string());
        questionmarks.join(",")
    }

    pub fn sql_tuple() -> SQLtuple {
        ("".to_string(), vec![])
    }

    fn get_label_sql_helper(&self, ret: &mut SQLtuple, part1: &str, part2: &str) {
        let mut types: Vec<String> = vec![];
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_l")) {
            types.push("label".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_a")) {
            types.push("alias".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_d")) {
            types.push("description".to_string());
        }
        if !types.is_empty() {
            let mut tmp = Self::prep_quote(&types);
            ret.0 += &(" AND ".to_owned() + part2 + &" IN (".to_owned() + &tmp.0 + ")");
            ret.1.append(&mut tmp.1);
        }
    }

    pub fn get_label_sql(&self) -> SQLtuple {
        lazy_static! {
            static ref RE1: Regex =
                Regex::new(r#"[^a-z,]"#).expect("Platform::get_label_sql Regex is invalid");
        }
        let mut ret: SQLtuple = ("".to_string(), vec![]);
        let yes = self.get_param_as_vec("labels_yes", "\n");
        let any = self.get_param_as_vec("labels_any", "\n");
        let no = self.get_param_as_vec("labels_no", "\n");
        if yes.len() + any.len() + no.len() == 0 {
            return ret;
        }

        let langs_yes = self.get_param_as_vec("langs_labels_yes", ",");
        let langs_any = self.get_param_as_vec("langs_labels_any", ",");
        let langs_no = self.get_param_as_vec("langs_labels_no", ",");

        ret.0 = "SELECT DISTINCT concat('Q',wbit_item_id) AS term_full_entity_id 
            FROM wbt_text,wbt_item_terms wbt_item_terms1,wbt_type,wbt_term_in_lang,wbt_text_in_lang 
            WHERE wbit_term_in_lang_id = wbtl_id 
            AND wbtl_type_id = wby_id 
            AND wbtl_text_in_lang_id = wbxl_id 
            AND wbxl_text_id = wbx_id"
            .to_string();

        yes.iter().for_each(|s| {
            if s != "%" {
                ret.0 += &(" AND wbx_text LIKE ?".to_owned());
                ret.1.push(s.to_string());
            }
            if !langs_yes.is_empty() {
                let mut tmp = Self::prep_quote(&langs_yes);
                ret.0 += &(" AND wbxl_language IN (".to_owned() + &tmp.0 + ")");
                ret.1.append(&mut tmp.1);
                self.get_label_sql_helper(&mut ret, "yes", "wby_name");
            }
        });

        if !langs_any.is_empty() {
            ret.0 += " AND (";
            let mut first = true;
            any.iter().for_each(|s| {
                if first {
                    first = false;
                } else {
                    ret.0 += " OR "
                }
                if s != "%" {
                    ret.0 += &(" ( wbx_text LIKE ?".to_owned());
                    ret.1.push(s.to_string());
                }
                if !langs_any.is_empty() {
                    let mut tmp = Self::prep_quote(&langs_any);
                    ret.0 += &(" AND wbxl_language IN (".to_owned() + &tmp.0 + ")");
                    ret.1.append(&mut tmp.1);
                    self.get_label_sql_helper(&mut ret, "any", "wby_name");
                }
                ret.0 += ")";
            });
            ret.0 += ")";
        }

        no.iter().for_each(|s| {
            ret.0 += " AND NOT EXISTS (
                SELECT * FROM 
                wbt_text wbt_text2,
                wbt_item_terms wbt_item_terms2,
                wbt_type wbt_type2,
                wbt_term_in_lang wbt_term_in_lang2,
                wbt_text_in_lang wbt_text_in_lang2
                WHERE wbt_item_terms2.wbit_term_in_lang_id = wbt_term_in_lang2.wbtl_id 
                AND wbt_term_in_lang2.wbtl_type_id = wbt_type2.wby_id 
                AND wbt_term_in_lang2.wbtl_text_in_lang_id = wbt_text_in_lang2.wbxl_id 
                AND wbt_text_in_lang2.wbxl_text_id = wbt_text2.wbx_id
                AND wbt_item_terms1.wbit_item_id=wbt_item_terms2.wbit_item_id 
                AND wbt_type2.wby_name='item'";
            if s != "%" {
                ret.0 += &(" AND wbt_text2.wbx_text LIKE ?".to_owned());
                ret.1.push(s.to_string());
            }
            if !langs_no.is_empty() {
                let mut tmp = Self::prep_quote(&langs_no);
                ret.0 += &(" AND wbt_type2.wbxl_language IN (".to_owned() + &tmp.0 + ")");
                ret.1.append(&mut tmp.1);
                self.get_label_sql_helper(&mut ret, "no", "wbt_type2.wby_name");
            }
            ret.0 += ")";
        });
        println!("{:?}", &ret);
        ret
    }

    fn parse_combination_string(s: &String) -> Combination {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"\w+(?:'\w+)?|[^\w\s]")
                .expect("Platform::parse_combination_string: Regex is invalid");
        }
        match s.trim().to_lowercase().as_str() {
            "" => return Combination::None,
            "categories" | "sparql" | "manual" | "pagepile" | "wikidata" | "search" => {
                return Combination::Source(s.to_string())
            }
            _ => {}
        }
        let mut parts: Vec<String> = RE
            .captures_iter(s)
            .filter_map(|cap| cap.get(0))
            .map(|s| s.as_str().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // Problem?
        if parts.len() < 3 {
            return Combination::None;
        }

        let first_part = match parts.get(0) {
            Some(part) => part.to_owned(),
            None => "".to_string(),
        };
        let left = if first_part == "(" {
            let mut cnt = 0;
            let mut new_left: Vec<String> = vec![];
            loop {
                if parts.is_empty() {
                    return Combination::None; // Failure to parse
                }
                let x = parts.remove(0);
                if x == "(" {
                    if cnt > 0 {
                        new_left.push(x.to_string());
                    }
                    cnt += 1;
                } else if x == ")" {
                    cnt -= 1;
                    if cnt == 0 {
                        break;
                    } else {
                        new_left.push(x.to_string());
                    }
                } else {
                    new_left.push(x.to_string());
                }
            }
            new_left.join(" ")
        } else {
            parts.remove(0)
        };
        if parts.is_empty() {
            return Self::parse_combination_string(&left);
        }
        let comb = parts.remove(0);
        let left = Box::new(Self::parse_combination_string(&left));
        let rest = Box::new(Self::parse_combination_string(&parts.join(" ")));
        match comb.trim().to_lowercase().as_str() {
            "and" => Combination::Intersection((left, rest)),
            "or" => Combination::Union((left, rest)),
            "not" => Combination::Not((left, rest)),
            _ => Combination::None,
        }
    }

    /// Checks is the parameter is set, and non-blank
    pub fn has_param(&self, param: &str) -> bool {
        match self.form_parameters().params.get(&param.to_string()) {
            Some(s) => s != "",
            None => false,
        }
    }

    pub fn get_param(&self, param: &str) -> Option<String> {
        if self.has_param(param) {
            self.form_parameters()
                .params
                .get(&param.to_string())
                .map(|s| s.to_string())
        } else {
            None
        }
    }

    fn get_combination(&self, available_sources: &Vec<String>) -> Combination {
        match self.get_param("source_combination") {
            Some(combination_string) => Self::parse_combination_string(&combination_string),
            None => {
                let mut comb = Combination::None;
                for source in available_sources {
                    if comb == Combination::None {
                        comb = Combination::Source(source.to_string());
                    } else {
                        comb = Combination::Intersection((
                            Box::new(Combination::Source(source.to_string())),
                            Box::new(comb),
                        ));
                    }
                }
                comb
            }
        }
    }

    fn combine_results(
        &self,
        results: &HashMap<String, Arc<RwLock<PageList>>>,
        combination: &Combination,
    ) -> Result<Arc<RwLock<PageList>>, String> {
        match combination {
            Combination::Source(s) => match results.get(s) {
                Some(r) => Ok(r.clone()),
                None => Err(format!("No result for source {}", &s)),
            },
            Combination::Union((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, c) => self.combine_results(results, c),
                (c, Combination::None) => self.combine_results(results, c),
                (c, d) => {
                    let r1 = self.combine_results(results, c)?;
                    let r2 = self.combine_results(results, d)?;
                    r1.write()
                        .unwrap()
                        .union(&r2.read().unwrap(), Some(&self))?;
                    Ok(r1)
                }
            },
            Combination::Intersection((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => {
                    Err(format!("Intersection with Combination::None found"))
                }
                (_c, Combination::None) => {
                    Err(format!("Intersection with Combination::None found"))
                }
                (c, d) => {
                    let r1 = self.combine_results(results, c)?;
                    let r2 = self.combine_results(results, d)?;
                    r1.write()
                        .unwrap()
                        .intersection(&r2.read().unwrap(), Some(&self))?;
                    Ok(r1)
                }
            },
            Combination::Not((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => Err(format!("Not with Combination::None found")),
                (c, Combination::None) => self.combine_results(results, c),
                (c, d) => {
                    let r1 = self.combine_results(results, c)?;
                    let r2 = self.combine_results(results, d)?;
                    r1.write()
                        .unwrap()
                        .difference(&r2.read().unwrap(), Some(&self))?;
                    Ok(r1)
                }
            },
            Combination::None => Err(format!("Combination::None found")),
        }
    }

    pub fn result(&self) -> &Option<PageList> {
        &self.result
    }

    pub fn form_parameters(&self) -> &FormParameters {
        &self.form_parameters
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use serde_json::Value;
    use std::env;
    use std::fs::File;

    fn get_new_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let file = File::open(path).expect("Can not open config file");
        let petscan_config: Value =
            serde_json::from_reader(file).expect("Can not parse JSON from config file");
        Arc::new(AppState::new_from_config(&petscan_config))
    }

    fn get_state() -> Arc<AppState> {
        lazy_static! {
            static ref STATE: Arc<AppState> = get_new_state();
        }
        STATE.clone()
    }

    fn run_psid_ext(psid: usize, addendum: &str) -> Result<Platform, String> {
        let state = get_state();
        let form_parameters = match state.get_query_from_psid(&format!("{}", &psid)) {
            Ok(psid_query) => {
                let query = psid_query + addendum;
                FormParameters::outcome_from_query(&query)?
            }
            Err(e) => return Err(e),
        };
        let mut platform = Platform::new_from_parameters(&form_parameters, &state);
        platform.run().unwrap();
        Ok(platform)
    }

    fn run_psid(psid: usize) -> Platform {
        run_psid_ext(psid, "").unwrap()
    }

    fn check_results_for_psid_ext(psid: usize, addendum: &str, wiki: &str, expected: Vec<Title>) {
        let platform = run_psid_ext(psid, addendum).unwrap();
        let result = platform.result.clone().unwrap();
        assert_eq!(result.wiki(), Some(wiki.to_string()));

        // Sort/crop results
        let mut entries = result.drain_into_sorted_vec(PageListSort::new_from_params(
            &platform.get_param_blank("sortby"),
            platform.get_param_blank("sortorder") == "descending".to_string(),
        ));
        platform.apply_results_limit(&mut entries);

        assert_eq!(entries.len(), expected.len());
        let titles: Vec<Title> = entries.iter().map(|e| e.title()).cloned().collect();
        assert_eq!(titles, expected);
    }

    fn check_results_for_psid(psid: usize, wiki: &str, expected: Vec<Title>) {
        check_results_for_psid_ext(psid, "", wiki, expected)
    }

    #[test]
    fn test_parse_combination_string() {
        let res =
            Platform::parse_combination_string(&"categories NOT (sparql OR pagepile)".to_string());
        let expected = Combination::Not((
            Box::new(Combination::Source("categories".to_string())),
            Box::new(Combination::Union((
                Box::new(Combination::Source("sparql".to_string())),
                Box::new(Combination::Source("pagepile".to_string())),
            ))),
        ));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_manual_list_enwiki_use_props() {
        check_results_for_psid(10087995, "enwiki", vec![Title::new("Magnus_Manske", 0)]);
    }

    #[test]
    fn test_manual_list_enwiki_sitelinks() {
        // This assumes [[en:Count von Count]] has no lvwiki article
        check_results_for_psid(10123257, "wikidatawiki", vec![Title::new("Q13520818", 0)]);
    }

    #[test]
    fn test_manual_list_enwiki_min_max_sitelinks() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        check_results_for_psid(10123897, "wikidatawiki", vec![Title::new("Q13520818", 0)]); // Min 15
        check_results_for_psid(10124667, "wikidatawiki", vec![Title::new("Q12345", 0)]);
        // Max 15
    }

    #[test]
    fn test_manual_list_enwiki_label_filter() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        check_results_for_psid(10125089, "wikidatawiki", vec![Title::new("Q12345", 0)]);
        // Label "Count%" in en
    }

    #[test]
    fn test_manual_list_enwiki_neg_cat_filter() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        // Manual list on enwiki, minus [[Category:Fictional vampires]]
        check_results_for_psid(10126217, "enwiki", vec![Title::new("Magnus Manske", 0)]);
    }

    #[test]
    fn test_source_labels() {
        check_results_for_psid(
            10225056,
            "wikidatawiki",
            vec![Title::new("Q13520818", 0), Title::new("Q10995651", 0)],
        );
    }

    #[test]
    fn test_manual_list_commons_file_info() {
        // Manual list [[File:KingsCollegeChapelWest.jpg]] on commons
        let platform = run_psid(10137125);
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(0).unwrap();
        assert_eq!(entry.page_id, Some(1340715));
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

    #[test]
    fn test_manual_list_enwiki_page_info() {
        // Manual list [[Cambridge]] on enwiki
        let platform = run_psid(10136716);
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(0).unwrap();
        assert_eq!(entry.page_id, Some(36995));
        assert!(entry.page_bytes.is_some());
        assert!(entry.get_page_timestamp().is_some());
        assert_eq!(
            entry.get_page_image(),
            Some("KingsCollegeChapelWest.jpg".to_string())
        );
        assert_eq!(entry.disambiguation, TriState::No);
        assert!(entry.incoming_links.is_some());
        assert!(entry.incoming_links.unwrap() > 7500);
        assert!(entry.get_coordinates().is_some());
    }

    #[test]
    fn test_manual_list_enwiki_annotate_wikidata_item() {
        // Manual list [[Count von Count]] on enwiki
        let platform = run_psid(10137767);
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(0).unwrap();
        assert_eq!(entry.page_id, Some(239794));
        assert_eq!(entry.get_wikidata_item(), Some("Q12345".to_string()));
    }

    #[test]
    fn test_manual_list_enwiki_subpages() {
        // Manual list [[User:Magnus Manske]] on enwiki, subpages, not "root page"
        let platform = run_psid(10138030);
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert!(entries.len() > 100);
        // Try to find pages with no '/'
        assert!(!entries
            .iter()
            .any(|entry| { entry.title().pretty().find('/').is_none() }));
    }

    #[test]
    fn test_manual_list_wikidata_labels() {
        // Manual list [[Q12345]], nl label/desc
        let platform = run_psid(10138979);
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(0).unwrap();
        assert_eq!(entry.page_id, Some(13925));
        assert_eq!(entry.get_wikidata_label(), Some("Graaf Tel".to_string()));
        assert_eq!(
            entry.get_wikidata_description(),
            Some("figuur van Sesamstraat".to_string())
        );
    }

    #[test]
    fn test_manual_list_wikidata_regexp() {
        check_results_for_psid_ext(
            10140344,
            "&regexp_filter=.*Manske",
            "wikidatawiki",
            vec![Title::new("Q13520818", 0)],
        );
        check_results_for_psid_ext(
            10140344,
            "&regexp_filter=Graaf.*",
            "wikidatawiki",
            vec![Title::new("Q12345", 0)],
        );
        check_results_for_psid_ext(
            10140616,
            "&regexp_filter=&regexp_filter=Jimbo.*",
            "enwiki",
            vec![Title::new("Jimbo Wales", 0)],
        );
        check_results_for_psid_ext(
            10140616,
            "&regexp_filter=&regexp_filter=.*Sanger",
            "enwiki",
            vec![Title::new("Larry Sanger", 0)],
        );
    }

    #[test]
    fn test_en_categories_sparql_common_wiki_other() {
        check_results_for_psid(15960820, "frwiki", vec![Title::new("Magnus Manske", 0)]);
    }

    #[test]
    fn test_trim_extended_whitespace() {
        let platform = run_psid(15015735); // The categories contain a left-to-right mark
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert!(entries.len() > 20);
    }

    #[test]
    fn test_template_talk_pages() {
        let platform = run_psid(15059382);
        let result = platform.result.unwrap();
        let entries = result
            .entries()
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert!(entries.len() > 0);
        for entry in entries {
            assert_eq!(entry.title().namespace_id(), 0);
        }
    }
}
