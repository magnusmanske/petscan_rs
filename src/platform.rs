use crate::app_state::AppState;
use crate::datasource::*;
use crate::datasource_database::{SourceDatabase, SourceDatabaseParameters};
use crate::form_parameters::FormParameters;
use crate::pagelist::{PageList, PageListEntry};
use mediawiki::api::NamespaceID;
use mediawiki::title::Title;
use mysql as my;
use regex::Regex;
//use rayon::prelude::*;
use rocket::http::ContentType;
use rocket::http::Status;
use rocket::response::Responder;
use rocket::Request;
use rocket::Response;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

pub static PAGE_BATCH_SIZE: usize = 20000;

pub struct MyResponse {
    pub s: String,
    pub content_type: ContentType,
}

impl Responder<'static> for MyResponse {
    fn respond_to(self, _: &Request) -> Result<Response<'static>, Status> {
        Response::build()
            .header(self.content_type)
            .sized_body(Cursor::new(self.s))
            .ok()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Combination {
    None,
    Source(String),
    Intersection((Box<Combination>, Box<Combination>)),
    Union((Box<Combination>, Box<Combination>)),
    Not((Box<Combination>, Box<Combination>)),
}

#[derive(Debug, Clone)]
pub struct Platform {
    form_parameters: Arc<FormParameters>,
    pub state: Arc<AppState>,
    result: Option<PageList>,
    pub psid: Option<u64>,
}

impl Platform {
    pub fn new_from_parameters(form_parameters: &FormParameters, state: &AppState) -> Self {
        Self {
            form_parameters: Arc::new((*form_parameters).clone()),
            state: Arc::new(state.clone()),
            result: None,
            psid: None,
        }
    }

    pub fn run(&mut self) {
        // TODO legacy parameters

        let mut candidate_sources: Vec<Box<dyn DataSource>> = vec![];
        candidate_sources.push(Box::new(SourceDatabase::new(self.db_params())));
        candidate_sources.push(Box::new(SourceSparql::new()));
        candidate_sources.push(Box::new(SourceManual::new()));
        candidate_sources.push(Box::new(SourcePagePile::new()));
        candidate_sources.push(Box::new(SourceSearch::new()));
        candidate_sources.push(Box::new(SourceWikidata::new()));

        if !candidate_sources.iter().any(|source| source.can_run(&self)) {
            candidate_sources = vec![];
            candidate_sources.push(Box::new(SourceLabels::new()));
            if !candidate_sources.iter().any(|source| source.can_run(&self)) {
                return;
            }
        }

        let mut results: HashMap<String, Option<PageList>> = HashMap::new();
        // TODO threads

        for source in &mut candidate_sources {
            if source.can_run(&self) {
                results.insert(source.name(), source.run(&self));
            }
        }

        let available_sources = candidate_sources
            .iter()
            .filter(|s| s.can_run(&self))
            .map(|s| s.name())
            .collect();
        let combination = self.get_combination(&available_sources);

        println!("{:#?}", &combination);

        self.result = self.combine_results(&mut results, &combination);
        self.post_process_result(&available_sources);
    }

    fn post_process_result(&mut self, available_sources: &Vec<String>) {
        if self.result.is_none() {
            return;
        }

        let mut result = self.result.as_ref().unwrap().to_owned();

        // Filter and post-process
        self.filter_wikidata(&mut result);
        self.process_sitelinks(&mut result);
        if *available_sources != vec!["labels".to_string()] {
            self.process_labels(&mut result);
        }
        //if ( !common_wiki.empty() && pagelist.wiki != common_wiki ) pagelist.convertToWiki ( common_wiki ) ; // TODO
        if !available_sources.contains(&"categories".to_string()) {
            self.process_missing_database_filters(&mut result);
        }
        self.process_by_wikidata_item(&mut result);

        self.result = Some(result);

        /*
        // TODO
        processWikidata ( pagelist ) ;
        processFiles ( pagelist ) ;
        processPages ( pagelist ) ;
        processSubpages ( pagelist ) ;

        gettimeofday(&after , NULL);
        querytime = time_diff(before , after)/1000000 ;

        string wikidata_label_language = getParam ( "wikidata_label_language" , "" ) ;
        if ( wikidata_label_language.empty() ) wikidata_label_language = getParam("interface_language","en") ;
        pagelist.loadMissingMetadata ( wikidata_label_language , this ) ;

        pagelist.regexpFilter ( getParam("regexp_filter","") ) ;

        sortResults ( pagelist ) ;
        processRedlinks ( pagelist ) ; // Supersedes sort
        params["format"] = getParam ( "format" , "html" , true ) ;

        processCreator ( pagelist ) ;
        applyResultsLimit ( pagelist ) ;

        string wdf_main = getParam ( "wdf_main" , "" ) ;
        if ( !wdf_main.empty() ) {
            TWDFIST wdfist ( &pagelist , this ) ;
            return wdfist.run() ;
        }
        */
    }

    fn annotate_with_wikidata_item(&self, result: &mut PageList) {
        if result.wiki == Some("wikidatawiki".to_string()) {
            return;
        }

        // Batches
        let batches: Vec<SQLtuple> = result.to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql|{
                sql.0 = "SELECT pp_value,page_title,page_namespace FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ".to_owned()+&sql.0;
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        let mut tmp = PageList::new_from_wiki(result.wiki.as_ref().unwrap().as_str());
        tmp.process_batch_results(self, batches, &|row: my::Row| {
            let (pp_value, page_title, page_namespace) =
                my::from_row::<(String, String, NamespaceID)>(row);
            let mut entry = PageListEntry::new(Title::new(&page_title, page_namespace));
            entry.wikidata_item = Some(pp_value);
            Some(entry)
        });
        tmp.entries
            .iter()
            .for_each(|new_entry| match result.entries.get(new_entry) {
                Some(entry) => {
                    let mut entry = entry.clone();
                    entry.wikidata_item = new_entry.wikidata_item.clone();
                    result.entries.replace(entry);
                }
                None => println!("Could not find entry {:?}", &new_entry),
            });
    }

    fn process_by_wikidata_item(&mut self, result: &mut PageList) {
        // TEST: http://127.0.0.1:3000/?psid=10126830
        if result.wiki == Some("wikidatawiki".to_string()) {
            return;
        }
        let wdi = self.get_param_default("wikidata_item", "no");
        if wdi != "any" && wdi != "with" && wdi != "without" {
            return;
        }
        println!("A: {:?}", &result);
        self.annotate_with_wikidata_item(result);
        println!("B: {:?}", &result);
        if wdi == "with" {
            result.entries.retain(|entry| entry.wikidata_item.is_some());
        }
        if wdi == "without" {
            result.entries.retain(|entry| entry.wikidata_item.is_none());
        }
        println!("C: {:?}", &result);
    }

    fn process_missing_database_filters(&mut self, result: &mut PageList) {
        let mut params = self.db_params();
        params.wiki = match &result.wiki {
            Some(wiki) => Some(wiki.to_string()),
            None => return,
        };
        let mut db = SourceDatabase::new(params);
        match db.get_pages(&self.state, Some(result)) {
            Some(new_result) => *result = new_result,
            None => {}
        }
    }

    fn process_labels(&mut self, result: &mut PageList) {
        //println!("{:?}", &self.form_parameters);
        let mut sql = self.get_label_sql();
        if sql.1.is_empty() {
            return;
        }
        result.convert_to_wiki("wikidatawiki", &self);
        if result.is_empty() {
            return;
        }
        sql.0 += " AND term_full_entity_id IN (";

        // Batches
        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql_batch| {
                let tmp = Platform::prep_quote(&sql_batch.1);
                sql_batch.0 = sql.0.to_owned() + &tmp.0 + ")";
                sql_batch.1.splice(..0, sql.1.to_owned());
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        result.process_batch_results(self, batches, &|row: my::Row| {
            let term_full_entity_id = my::from_row::<String>(row);
            Platform::entry_from_entity(&term_full_entity_id)
        });
    }

    fn process_sitelinks(&mut self, result: &mut PageList) {
        if result.is_empty() {
            return;
        }

        let sitelinks_yes = self.get_param_as_vec("sitelinks_yes", "\n");
        let sitelinks_any = self.get_param_as_vec("sitelinks_any", "\n");
        let sitelinks_no = self.get_param_as_vec("sitelinks_no", "\n");
        let sitelinks_min = self.get_param_blank("min_sitelink_count");
        let sitelinks_max = self.get_param_blank("max_sitelink_count");

        //if ( trim(sitelinks_min) == "0" ) sitelinks_min.clear() ;
        if sitelinks_yes.is_empty()
            && sitelinks_any.is_empty()
            && sitelinks_no.is_empty()
            && sitelinks_min.is_empty()
            && sitelinks_max.is_empty()
        {
            return;
        }
        result.convert_to_wiki("wikidatawiki", &self);
        if result.is_empty() {
            return;
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
            let mut tmp = Platform::prep_quote(&sitelinks_any);
            Platform::append_sql(&mut sql, &mut tmp);
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
            .iter_mut()
            .map(|sql_batch| {
                sql_batch.0 = sql.0.to_owned() + &sql_batch.0 + &sql_post;
                sql_batch.1.splice(..0, sql.1.to_owned());
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        result.process_batch_results(self, batches, &|row: my::Row| {
            let (page_title, _sitelinks_count) = my::from_row::<(String, usize)>(row);
            Some(PageListEntry::new(Title::new(&page_title, 0)))
        });
    }

    fn filter_wikidata(&mut self, result: &mut PageList) {
        if result.is_empty() {
            return;
        }
        let no_statements = self.has_param("wpiu_no_statements");
        let no_sitelinks = self.has_param("wpiu_no_sitelinks");
        let wpiu = self.get_param_default("wpiu", "any");
        let list = self.get_param_blank("wikidata_prop_item_use");
        let list = list.trim();
        if list.is_empty() && !no_statements && !no_sitelinks {
            return;
        }
        result.convert_to_wiki("wikidatawiki", &self);
        if result.is_empty() {
            return;
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
        result.process_batch_results(self, batches, &|row: my::Row| {
            let pp_value: String = my::from_row(row);
            Some(PageListEntry::new(Title::new(&pp_value, 0)))
        });
    }

    pub fn entry_from_entity(entity: &str) -> Option<PageListEntry> {
        match entity.chars().next() {
            Some('Q') => Some(PageListEntry::new(Title::new(&entity.to_string(), 0))),
            Some('P') => Some(PageListEntry::new(Title::new(&entity.to_string(), 120))),
            Some('L') => Some(PageListEntry::new(Title::new(&entity.to_string(), 146))),
            _ => None,
        }
    }

    pub fn db_params(&self) -> SourceDatabaseParameters {
        /*
        // TODO Legacy parameters
        if ( params.find("comb_subset") != params.end() ) params["combination"] = "subset" ;
        if ( params.find("comb_union") != params.end() ) params["combination"] = "union" ;
        if ( params.find("get_q") != params.end() ) params["wikidata_item"] = "any" ;
        if ( params.find("wikidata") != params.end() ) params["wikidata_item"] = "any" ;
        if ( params.find("wikidata_no_item") != params.end() ) params["wikidata_item"] = "without" ;
        */

        let depth: u16 = self
            .get_param("depth")
            .unwrap_or("0".to_string())
            .parse::<u16>()
            .unwrap_or(0);
        let ret = SourceDatabaseParameters {
            combine: match self.form_parameters.params.get("combination") {
                Some(x) => {
                    if x == "union" {
                        x.to_string()
                    } else {
                        "subset".to_string()
                    }
                }
                None => "subset".to_string(),
            },
            only_new_since: self.has_param("only_new_since"),
            max_age: self
                .get_param("max_age")
                .map(|x| x.parse::<i64>().unwrap_or(0)),
            before: self.get_param_blank("before"),
            after: self.get_param_blank("after"),
            templates_yes: self.get_param_as_vec("templates_yes", "\n"),
            templates_any: self.get_param_as_vec("templates_any", "\n"),
            templates_no: self.get_param_as_vec("templates_no", "\n"),
            templates_yes_talk_page: self.has_param("templates_use_talk_yes"),
            templates_any_talk_page: self.has_param("templates_use_talk_any"),
            templates_no_talk_page: self.has_param("templates_use_talk_no"),
            linked_from_all: self.get_param_as_vec("outlinks_yes", "\n"),
            linked_from_any: self.get_param_as_vec("outlinks_any", "\n"),
            linked_from_none: self.get_param_as_vec("outlinks_no", "\n"),
            links_to_all: self.get_param_as_vec("links_to_all", "\n"),
            links_to_any: self.get_param_as_vec("links_to_any", "\n"),
            links_to_none: self.get_param_as_vec("links_to_no", "\n"),
            last_edit_bot: self.get_param_default("edits[bots]", "both"),
            last_edit_anon: self.get_param_default("edits[anons]", "both"),
            last_edit_flagged: self.get_param_default("edits[flagged]", "both"),
            gather_link_count: self.has_param("minlinks") || self.has_param("maxlinks"),
            page_image: self.get_param_default("page_image", "any"),
            page_wikidata_item: self.get_param_default("wikidata_item", "any"),
            ores_type: self.get_param_blank("ores_type"),
            ores_prediction: self.get_param_default("ores_prediction", "any"),
            depth: depth,
            cat_pos: self.get_param_as_vec("categories", "\n"),
            cat_neg: self.get_param_as_vec("negcats", "\n"),
            ores_prob_from: self
                .get_param("ores_prob_from")
                .map(|x| x.parse::<f32>().unwrap_or(0.0)),
            ores_prob_to: self
                .get_param("ores_prob_to")
                .map(|x| x.parse::<f32>().unwrap_or(1.0)),
            redirects: self.get_param_blank("show_redirects"),
            minlinks: self
                .get_param("minlinks")
                .map(|i| i.parse::<usize>().unwrap()),
            maxlinks: self
                .get_param("maxlinks")
                .map(|i| i.parse::<usize>().unwrap()),
            larger: self
                .get_param("larger")
                .map(|i| i.parse::<usize>().unwrap()),
            smaller: self
                .get_param("smaller")
                .map(|i| i.parse::<usize>().unwrap()),
            wiki: self.get_main_wiki(),
            namespace_ids: self
                .form_parameters
                .ns
                .iter()
                .cloned()
                .collect::<Vec<usize>>(),
            use_new_category_mode: true,
        };
        ret
    }

    pub fn get_main_wiki(&self) -> Option<String> {
        // TODO
        let language = self.get_param("language")?;
        let project = self.get_param("project")?;
        if project == "wikipedia" {
            Some(language.to_owned() + "wiki")
        } else {
            None
        }
    }

    pub fn get_response(&self) -> MyResponse {
        MyResponse {
            s: format!("{:#?}", self.result()),
            content_type: ContentType::Plain,
        }
    }

    pub fn get_param_as_vec(&self, param: &str, separator: &str) -> Vec<String> {
        match self.get_param(param) {
            Some(s) => s
                .split(separator)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
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

    pub fn append_sql(sql: &mut SQLtuple, sub: &mut SQLtuple) {
        sql.0 += &sub.0;
        sql.1.append(&mut sub.1);
    }

    /// Returns a tuple with a string containing comma-separated question marks, and the (non-empty) Vec elements
    pub fn prep_quote(strings: &Vec<String>) -> SQLtuple {
        let escaped: Vec<String> = strings
            .iter()
            .filter_map(|s| match s.trim() {
                "" => None,
                other => Some(other.to_string()),
            })
            .collect();
        let mut questionmarks: Vec<String> = Vec::new();
        questionmarks.resize(escaped.len(), "?".to_string());
        (questionmarks.join(","), escaped)
    }

    pub fn sql_tuple() -> SQLtuple {
        ("".to_string(), vec![])
    }

    fn get_label_sql_helper(&self, ret: &mut SQLtuple, part1: &str, part2: &str) {
        let mut types = vec![];
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_l")) {
            types.push("label");
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_a")) {
            types.push("alias");
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_d")) {
            types.push("description");
        }
        if !types.is_empty() {
            let mut tmp = Self::prep_quote(&types.iter().map(|s| s.to_string()).collect());
            ret.0 += &(" AND ".to_owned() + part2 + &" IN (".to_owned() + &tmp.0 + ")");
            ret.1.append(&mut tmp.1);
        }
    }

    pub fn get_label_sql(&self) -> SQLtuple {
        lazy_static! {
            static ref RE1: Regex = Regex::new(r#"[^a-z,]"#).unwrap();
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

        ret.0 =
            "SELECT DISTINCT term_full_entity_id FROM wb_terms t1 WHERE term_entity_type='item'"
                .to_string();
        let field = "term_text".to_string(); // term_search_key case-sensitive; term_text case-insensitive?

        yes.iter().for_each(|s| {
            ret.0 += &(" AND ".to_owned() + &field + " LIKE ?");
            ret.1.push(s.to_string());
            if !langs_yes.is_empty() {
                let mut tmp = Self::prep_quote(&langs_yes);
                ret.0 += &(" AND term_language IN (".to_owned() + &tmp.0 + ")");
                ret.1.append(&mut tmp.1);
                self.get_label_sql_helper(&mut ret, "yes", "term_type");
            }
        });

        if !langs_any.is_empty() {
            ret.0 += " AND (";
            let mut first = true;
            yes.iter().for_each(|s| {
                if first {
                    first = false;
                } else {
                    ret.0 += " OR "
                }
                ret.0 += &(" ( ".to_owned() + &field + " LIKE ?");
                ret.1.push(s.to_string());
                if !langs_any.is_empty() {
                    let mut tmp = Self::prep_quote(&langs_any);
                    ret.0 += &(" AND term_language IN (".to_owned() + &tmp.0 + ")");
                    ret.1.append(&mut tmp.1);
                    self.get_label_sql_helper(&mut ret, "any", "term_type");
                }
                ret.0 += ")";
            });
            ret.0 += ")";
        }

        no.iter().for_each(|s| {
            ret.0 += " AND NOT EXISTS (SELECT t2.term_full_entity_id FROM wb_terms t2 WHERE";
            ret.0 +=
                " t2.term_full_entity_id=t1.term_full_entity_id AND t2.term_entity_type='item'";
            ret.0 += &(" AND t2.".to_owned() + &field + " LIKE ?");
            ret.1.push(s.to_string());
            if !langs_no.is_empty() {
                let mut tmp = Self::prep_quote(&langs_no);
                ret.0 += &(" AND t2.term_language IN (".to_owned() + &tmp.0 + ")");
                ret.1.append(&mut tmp.1);
                self.get_label_sql_helper(&mut ret, "no", "t2.term_type");
            }
            ret.0 += ")";
        });
        ret
    }

    fn parse_combination_string(s: &String) -> Combination {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"\w+(?:'\w+)?|[^\w\s]").unwrap();
        }
        match s.trim().to_lowercase().as_str() {
            "" => return Combination::None,
            "categories" | "sparql" | "manual" | "pagepile" | "wikidata" => {
                return Combination::Source(s.to_string())
            }
            _ => {}
        }
        let mut parts: Vec<String> = RE
            .captures_iter(s)
            .map(|cap| cap.get(0).unwrap().as_str().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // Problem?
        if parts.len() < 3 {
            return Combination::None;
        }

        let left = if parts.get(0).unwrap() == "(" {
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
                        comb = Combination::Union((
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
        results: &mut HashMap<String, Option<PageList>>,
        combination: &Combination,
    ) -> Option<PageList> {
        match combination {
            Combination::Source(s) => match results.get(s) {
                Some(r) => r.to_owned(),
                None => None,
            },
            Combination::Union((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, c) => self.combine_results(results, c),
                (c, Combination::None) => self.combine_results(results, c),
                (c, d) => {
                    let mut r1 = self.combine_results(results, c).unwrap();
                    let r2 = self.combine_results(results, d);
                    r1.union(r2).ok()?;
                    Some(r1)
                }
            },
            Combination::Intersection((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => None,
                (_c, Combination::None) => None,
                (c, d) => {
                    let mut r1 = self.combine_results(results, c).unwrap();
                    let r2 = self.combine_results(results, d);
                    r1.intersection(r2).ok()?;
                    Some(r1)
                }
            },
            Combination::Not((a, b)) => match (a.as_ref(), b.as_ref()) {
                (Combination::None, _c) => None,
                (c, Combination::None) => self.combine_results(results, c),
                (c, d) => {
                    let mut r1 = self.combine_results(results, c).unwrap();
                    let r2 = self.combine_results(results, d);
                    r1.difference(r2).ok()?;
                    Some(r1)
                }
            },
            Combination::None => None,
        }
    }

    pub fn result(&self) -> &Option<PageList> {
        &self.result
    }

    pub fn form_parameters(&self) -> &Arc<FormParameters> {
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

    fn run_psid(psid: usize) -> Platform {
        let state = get_state();
        let form_parameters = match state.get_query_from_psid(&format!("{}", &psid)) {
            Some(psid_query) => FormParameters::outcome_from_query(&psid_query),
            None => panic!("Can't get PSID {}", &psid),
        };
        let mut platform = Platform::new_from_parameters(&form_parameters, &state);
        platform.run();
        platform
    }

    fn check_results_for_psid(psid: usize, wiki: &str, expected: Vec<Title>) {
        let platform = run_psid(psid);
        assert!(platform.result.is_some());
        let result = platform.result.unwrap();
        assert_eq!(result.wiki, Some(wiki.to_string()));
        let entries = result
            .entries
            .iter()
            .cloned()
            .collect::<Vec<PageListEntry>>();
        assert_eq!(entries.len(), 1);
        let titles: Vec<Title> = entries.iter().map(|e| e.title()).cloned().collect();
        assert_eq!(titles, expected);
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
        check_results_for_psid(10087995, "wikidatawiki", vec![Title::new("Q13520818", 0)]);
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
        check_results_for_psid(10124667, "wikidatawiki", vec![Title::new("Q12345", 0)]); // Max 15
    }

    #[test]
    fn test_manual_list_enwiki_label_filter() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        check_results_for_psid(10125089, "wikidatawiki", vec![Title::new("Q12345", 0)]); // Label "Count%" in en
    }

    #[test]
    fn test_manual_list_enwiki_neg_cat_filter() {
        // [[Count von Count]] vs. [[Magnus Manske]]
        // Manual list on enwiki, minus [[Category:Fictional vampires]]
        check_results_for_psid(10126217, "enwiki", vec![Title::new("Magnus Manske", 0)]);
    }

}
