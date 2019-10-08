use crate::app_state::AppState;
use crate::datasource::DataSource;
use crate::datasource::SQLtuple;
use crate::pagelist::*;
use crate::platform::{Platform, PAGE_BATCH_SIZE};
use chrono::prelude::*;
use chrono::Duration;
use core::ops::Sub;
use mysql as my;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use wikibase::mediawiki::api::{Api, NamespaceID};
use wikibase::mediawiki::title::Title;

static MAX_CATEGORY_BATCH_SIZE: usize = 2500;

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabaseCatDepth {
    name: String,
    depth: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabaseParameters {
    combine: String,
    namespace_ids: Vec<usize>,
    linked_from_all: Vec<String>,
    linked_from_any: Vec<String>,
    linked_from_none: Vec<String>,
    links_to_all: Vec<String>,
    links_to_any: Vec<String>,
    links_to_none: Vec<String>,
    templates_yes: Vec<String>,
    templates_any: Vec<String>,
    templates_no: Vec<String>,
    templates_yes_talk_page: bool,
    templates_any_talk_page: bool,
    templates_no_talk_page: bool,
    page_image: String,
    ores_type: String,
    ores_prediction: String,
    ores_prob_from: Option<f32>,
    ores_prob_to: Option<f32>,
    last_edit_bot: String,
    last_edit_anon: String,
    last_edit_flagged: String,
    redirects: String,
    page_wikidata_item: String,
    larger: Option<usize>,
    smaller: Option<usize>,
    minlinks: Option<usize>,
    maxlinks: Option<usize>,
    wiki: Option<String>,
    gather_link_count: bool,
    cat_pos: Vec<String>,
    cat_neg: Vec<String>,
    depth: u16,
    max_age: Option<i64>,
    only_new_since: bool,
    before: String,
    after: String,
    use_new_category_mode: bool,
}

impl SourceDatabaseParameters {
    pub fn new() -> Self {
        Self {
            combine: "subset".to_string(),
            namespace_ids: vec![],
            linked_from_all: vec![],
            linked_from_any: vec![],
            linked_from_none: vec![],
            links_to_all: vec![],
            links_to_any: vec![],
            links_to_none: vec![],
            templates_yes: vec![],
            templates_any: vec![],
            templates_no: vec![],
            templates_yes_talk_page: false,
            templates_any_talk_page: false,
            templates_no_talk_page: false,
            page_wikidata_item: "any".to_string(),
            page_image: "any".to_string(),
            ores_prediction: "any".to_string(),
            ores_type: "".to_string(),
            ores_prob_from: None,
            ores_prob_to: None,
            last_edit_bot: "both".to_string(),
            last_edit_anon: "both".to_string(),
            last_edit_flagged: "both".to_string(),
            redirects: "".to_string(),
            larger: None,
            smaller: None,
            minlinks: None,
            maxlinks: None,
            wiki: None,
            gather_link_count: false,
            cat_pos: vec![],
            cat_neg: vec![],
            depth: 0,
            max_age: None,
            only_new_since: false,
            before: "".to_string(),
            after: "".to_string(),
            use_new_category_mode: true,
        }
    }

    pub fn db_params(platform: &Platform) -> SourceDatabaseParameters {
        let depth_signed: i32 = platform
            .get_param("depth")
            .unwrap_or("0".to_string())
            .parse::<i32>()
            .unwrap_or(0);
        let depth: u16 = if depth_signed < 0 {
            999
        } else {
            depth_signed as u16
        };
        let ret = SourceDatabaseParameters {
            combine: match platform.form_parameters().params.get("combination") {
                Some(x) => {
                    if x == "union" {
                        x.to_string()
                    } else {
                        "subset".to_string()
                    }
                }
                None => "subset".to_string(),
            },
            only_new_since: platform.has_param("only_new"),
            max_age: platform
                .get_param("max_age")
                .map(|x| x.parse::<i64>().unwrap_or(0)),
            before: platform.get_param_blank("before"),
            after: platform.get_param_blank("after"),
            templates_yes: platform.get_param_as_vec("templates_yes", "\n"),
            templates_any: platform.get_param_as_vec("templates_any", "\n"),
            templates_no: platform.get_param_as_vec("templates_no", "\n"),
            templates_yes_talk_page: platform.has_param("templates_use_talk_yes"),
            templates_any_talk_page: platform.has_param("templates_use_talk_any"),
            templates_no_talk_page: platform.has_param("templates_use_talk_no"),
            linked_from_all: platform.get_param_as_vec("outlinks_yes", "\n"),
            linked_from_any: platform.get_param_as_vec("outlinks_any", "\n"),
            linked_from_none: platform.get_param_as_vec("outlinks_no", "\n"),
            links_to_all: platform.get_param_as_vec("links_to_all", "\n"),
            links_to_any: platform.get_param_as_vec("links_to_any", "\n"),
            links_to_none: platform.get_param_as_vec("links_to_no", "\n"),
            last_edit_bot: platform.get_param_default("edits[bots]", "both"),
            last_edit_anon: platform.get_param_default("edits[anons]", "both"),
            last_edit_flagged: platform.get_param_default("edits[flagged]", "both"),
            gather_link_count: platform.has_param("minlinks") || platform.has_param("maxlinks"),
            page_image: platform.get_param_default("page_image", "any"),
            page_wikidata_item: platform.get_param_default("wikidata_item", "any"),
            ores_type: platform.get_param_blank("ores_type"),
            ores_prediction: platform.get_param_default("ores_prediction", "any"),
            depth: depth,
            cat_pos: platform.get_param_as_vec("categories", "\n"),
            cat_neg: platform.get_param_as_vec("negcats", "\n"),
            ores_prob_from: platform
                .get_param("ores_prob_from")
                .map(|x| x.parse::<f32>().unwrap_or(0.0)),
            ores_prob_to: platform
                .get_param("ores_prob_to")
                .map(|x| x.parse::<f32>().unwrap_or(1.0)),
            redirects: platform.get_param_blank("show_redirects"),
            minlinks: platform.usize_option_from_param("minlinks"),
            maxlinks: platform.usize_option_from_param("maxlinks"),
            larger: platform.usize_option_from_param("larger"),
            smaller: platform.usize_option_from_param("smaller"),
            wiki: platform.get_main_wiki(),
            namespace_ids: platform
                .form_parameters()
                .ns
                .par_iter()
                .cloned()
                .collect::<Vec<usize>>(),
            use_new_category_mode: true,
        };
        ret
    }

    pub fn set_wiki(&mut self, wiki: Option<String>) {
        self.wiki = wiki;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabase {
    cat_pos: Vec<Vec<String>>,
    cat_neg: Vec<Vec<String>>,
    has_pos_templates: bool,
    has_pos_linked_from: bool,
    params: SourceDatabaseParameters,
    talk_namespace_ids: String,
}

impl DataSource for SourceDatabase {
    fn name(&self) -> String {
        "categories".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("categories")
            || platform.has_param("templates_yes")
            || platform.has_param("templates_any")
            || platform.has_param("outlinks_yes")
            || platform.has_param("outlinks_any")
            || platform.has_param("links_to_all")
            || platform.has_param("links_to_any")
    }

    fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let ret = self.get_pages(&platform.state(), None);
        match &ret {
            Ok(pagelist) => {
                if pagelist.is_empty() {
                    platform.warn(format!("<span tt='warn_categories'></span>"));
                }
            }
            _ => {}
        }
        ret
    }
}

impl SourceDatabase {
    pub fn new(params: SourceDatabaseParameters) -> Self {
        Self {
            cat_pos: vec![],
            cat_neg: vec![],
            has_pos_templates: false,
            has_pos_linked_from: false,
            params,
            talk_namespace_ids: "".to_string(),
        }
    }

    fn parse_category_depth(
        &self,
        cats: &Vec<String>,
        default_depth: u16,
    ) -> Vec<SourceDatabaseCatDepth> {
        cats.iter()
            .filter_map(|c| {
                let mut parts = c.split("|");
                let name = match parts.next() {
                    Some(n) => n.to_string(),
                    None => return None,
                };
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
                Some(SourceDatabaseCatDepth {
                    name: name,
                    depth: depth,
                })
            })
            .collect()
    }

    fn go_depth_batch(
        &self,
        state: &Arc<AppState>,
        wiki: &String,
        categories_batch: &Vec<String>,
        categories_done: Arc<Mutex<HashSet<String>>>,
        new_categories: Arc<Mutex<Vec<String>>>,
    ) -> Result<(), String> {
        let db_user_pass = match state.get_db_mutex().lock() {
            Ok(db) => db,
            Err(e) => return Err(format!("Bad mutex: {:?}", e)),
        };
        let mut conn = state.get_wiki_db_connection(&db_user_pass, &wiki)?;
        let mut sql : SQLtuple = ("SELECT DISTINCT page_title FROM page,categorylinks WHERE cl_from=page_id AND cl_type='subcat' AND cl_to IN (".to_string(),vec![]);
        categories_batch.iter().for_each(|c| {
            // Don't par_iter, already in pool!
            (*categories_done.lock().unwrap()).insert(c.to_string());
        });
        Platform::append_sql(&mut sql, Platform::prep_quote(&categories_batch.to_vec()));
        sql.0 += ")";

        let result = match conn.prep_exec(sql.0, sql.1) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!("datasource_database::go_depth: {:?}", e));
            }
        };

        result
            .filter_map(|row_result| row_result.ok())
            .for_each(|row| {
                let page_title: String = my::from_row(row);
                if !(*categories_done.lock().unwrap()).contains(&page_title) {
                    (*new_categories.lock().unwrap()).push(page_title.to_owned());
                    (*categories_done.lock().unwrap()).insert(page_title);
                }
            });
        Ok(())
    }

    fn go_depth(
        &self,
        state: &Arc<AppState>,
        wiki: &String,
        categories_done: Arc<Mutex<HashSet<String>>>,
        categories_to_check: &Vec<String>,
        depth: u16,
    ) -> Result<(), String> {
        if depth == 0 || categories_to_check.is_empty() {
            return Ok(());
        }
        Platform::profile("DSDB::do_depth begin", Some(categories_to_check.len()));

        let error = Arc::new(Mutex::new(None));
        let new_categories: Vec<String> = vec![];
        let new_categories = Arc::new(Mutex::new(new_categories));

        let pool = match rayon::ThreadPoolBuilder::new()
            .num_threads(5) // TODO More? Less?
            .build()
        {
            Ok(pool) => pool,
            Err(e) => return Err(e.to_string()),
        };

        pool.install(|| {
            categories_to_check
                .par_iter()
                .chunks(PAGE_BATCH_SIZE)
                .for_each(|categories_batch| {
                    let categories_batch: Vec<String> =
                        categories_batch.par_iter().map(|s| s.to_string()).collect();
                    match self.go_depth_batch(
                        state,
                        wiki,
                        &categories_batch,
                        categories_done.clone(),
                        new_categories.clone(),
                    ) {
                        Ok(_) => {} // OK
                        Err(e) => *error.lock().unwrap() = Some(e),
                    }
                });
        });

        match &*error.lock().unwrap() {
            Some(e) => return Err(e.to_string()),
            None => {}
        }
        let new_categories = new_categories.lock().unwrap();

        Platform::profile("DSDB::do_depth new categories", Some(new_categories.len()));

        Platform::profile(
            "DSDB::do_depth end, categories done",
            Some(categories_done.lock().unwrap().len()),
        );

        self.go_depth(state, wiki, categories_done, &new_categories, depth - 1)?;
        Ok(())
    }

    fn get_categories_in_tree(
        &self,
        state: &Arc<AppState>,
        wiki: &String,
        title: &String,
        depth: u16,
    ) -> Result<Vec<String>, String> {
        let categories_done = Arc::new(Mutex::new(HashSet::new()));
        let title = Title::spaces_to_underscores(&Title::first_letter_uppercase(title));
        (*categories_done.lock().unwrap()).insert(title.to_owned());
        self.go_depth(state, wiki, categories_done.clone(), &vec![title], depth)?;
        let tmp = categories_done.lock().unwrap();
        Ok(tmp.par_iter().cloned().collect::<Vec<String>>())
    }

    pub fn parse_category_list(
        &self,
        state: &Arc<AppState>,
        wiki: &String,
        input: &Vec<SourceDatabaseCatDepth>,
    ) -> Result<Vec<Vec<String>>, String> {
        let error: Option<String> = None;
        let error = Arc::new(Mutex::new(error));
        let ret = input
            .par_iter()
            .filter_map(
                |i| match self.get_categories_in_tree(state, wiki, &i.name, i.depth) {
                    Ok(res) => Some(res),
                    Err(e) => {
                        *error.lock().unwrap() = Some(e.to_string());
                        None
                    }
                },
            )
            .filter(|x| !x.is_empty())
            .collect();

        let ret = match &*error.lock().unwrap() {
            Some(e) => Err(e.to_string()),
            None => Ok(ret),
        };
        ret
    }

    fn get_talk_namespace_ids(&self, conn: &mut my::Conn) -> Result<String, String> {
        Ok(conn
            .prep_exec(
                "SELECT DISTINCT page_namespace FROM page WHERE MOD(page_namespace,2)=1",
                Vec::<String>::new(),
            )
            .map_err(|e| format!("datasource_database::get_talk_namespace_ids: {:?}", e))?
            .filter_map(|row| row.ok())
            .map(|row| my::from_row::<NamespaceID>(row))
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
            .join(","))
    }

    pub fn template_subquery(
        &self,
        input: &Vec<String>,
        use_talk_page: bool,
        find_not: bool,
    ) -> SQLtuple {
        let mut sql = Platform::sql_tuple();
        if use_talk_page {
            sql.0 += if find_not {
                " AND NOT EXISTS "
            } else {
                " AND EXISTS "
            };
            //sql.0 += "(SELECT * FROM templatelinks,page pt WHERE MOD(p.page_namespace,2)=0 AND pt.page_title=p.page_title AND pt.page_namespace=p.page_namespace+1 AND tl_from=pt.page_id AND tl_namespace=10 AND tl_title";
            sql.0 += "(SELECT * FROM page pt WHERE pt.page_title=p.page_title AND pt.page_namespace-1=p.page_namespace AND pt.page_id IN (SELECT tl_from FROM templatelinks WHERE tl_namespace=10 AND tl_from_namespace IN (";
            sql.0 += &self.talk_namespace_ids;
            sql.0 += ")  AND tl_title";
        } else {
            sql.0 += if find_not {
                " AND p.page_id NOT IN "
            } else {
                " AND p.page_id IN "
            };
            sql.0 +=
                "(SELECT DISTINCT tl_from FROM templatelinks WHERE tl_namespace=10 AND tl_title";
        }

        sql.0 += " IN (";
        Platform::append_sql(&mut sql, Platform::prep_quote(input));
        sql.0 += "))";
        if use_talk_page {
            sql.0 += ")";
        }

        sql
    }

    pub fn group_link_list_by_namespace(
        &self,
        input: &Vec<String>,
        api: &Api,
    ) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        input.iter().for_each(|title| {
            let title = Title::new_from_full(title, &api);
            if !ret.contains_key(&title.namespace_id()) {
                ret.insert(title.namespace_id(), vec![]);
            }
            match ret.get_mut(&title.namespace_id()) {
                Some(x) => x.push(title.with_underscores().to_string()),
                None => {
                    println!(
                        "SourceDatabase::group_link_list_by_namespace: No namespace id in {:?}",
                        &title
                    );
                }
            }
        });
        ret
    }

    pub fn links_from_subquery(&self, input: &Vec<String>, api: &Api) -> SQLtuple {
        let mut sql: SQLtuple = ("(".to_string(), vec![]);
        let nslist = self.group_link_list_by_namespace(input, api);
        nslist.iter().for_each(|nsgroup| {
            if !sql.1.is_empty() {
                sql.0 += " ) OR ( ";
            }
            sql.0 += "( SELECT p_to.page_id FROM page p_to,page p_from,pagelinks WHERE p_from.page_namespace=";
            sql.0 += &nsgroup.0.to_string();
            sql.0 += "  AND p_from.page_id=pl_from AND pl_namespace=p_to.page_namespace AND pl_title=p_to.page_title AND p_from.page_title IN (";
            Platform::append_sql(&mut sql, Platform::prep_quote(nsgroup.1));
            sql.0 += ") )";
        });
        sql.0 += ")";
        sql
    }

    pub fn links_to_subquery(&self, input: &Vec<String>, api: &Api) -> SQLtuple {
        let mut sql: SQLtuple = ("(".to_string(), vec![]);
        let nslist = self.group_link_list_by_namespace(input, api);
        nslist.iter().for_each(|nsgroup| {
            if !sql.1.is_empty() {
                sql.0 += " ) OR ( ";
            }
            sql.0 += "( SELECT DISTINCT pl_from FROM pagelinks WHERE pl_namespace=";
            sql.0 += &nsgroup.0.to_string();
            sql.0 += " AND pl_title IN (";
            Platform::append_sql(&mut sql, Platform::prep_quote(nsgroup.1));
            sql.0 += ") )";
        });
        sql.0 += ")";
        sql
    }

    pub fn iterate_category_batches(
        &self,
        categories: &Vec<Vec<String>>,
        start: usize,
    ) -> Vec<Vec<Vec<String>>> {
        let mut ret: Vec<Vec<Vec<String>>> = vec![];
        categories[start]
            .chunks(MAX_CATEGORY_BATCH_SIZE)
            .for_each(|c| {
                if start + 1 >= categories.len() {
                    let to_add = vec![c.to_vec()];
                    ret.push(to_add);
                    return;
                }
                let tmp = self.iterate_category_batches(categories, start + 1);
                tmp.iter().for_each(|t| {
                    let mut to_add = vec![c.to_vec()];
                    to_add.append(&mut t.to_owned());
                    ret.push(to_add);
                });
            });
        ret
    }

    pub fn get_pages(
        &mut self,
        state: &Arc<AppState>,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList, String> {
        // Take wiki from given pagelist
        match primary_pagelist {
            Some(pl) => {
                if self.params.wiki.is_none() && pl.wiki().is_some() {
                    self.params.wiki = pl.wiki().to_owned();
                }
            }
            None => {}
        }

        // Paranoia
        if self.params.wiki.is_none() || self.params.wiki == Some("wiki".to_string()) {
            return Err(format!("SourceDatabase: Bad wiki '{:?}'", self.params.wiki));
        }

        let wiki = match &self.params.wiki {
            Some(wiki) => wiki.to_owned(),
            None => return Err(format!("SourceDatabase::get_pages: No wiki in params")),
        };

        // Get positive categories serial list
        self.cat_pos = self.parse_category_list(
            &state,
            &wiki,
            &self.parse_category_depth(&self.params.cat_pos, self.params.depth),
        )?;

        // Get negative categories serial list
        self.cat_neg = self.parse_category_list(
            &state,
            &wiki,
            &self.parse_category_depth(&self.params.cat_neg, self.params.depth),
        )?;

        let db_user_pass = match state.get_db_mutex().lock() {
            Ok(db) => db,
            Err(e) => return Err(format!("Bad mutex: {:?}", e)),
        };
        let mut ret = PageList::new_from_wiki(&wiki);
        let mut conn = state.get_wiki_db_connection(&db_user_pass, &wiki)?;
        self.talk_namespace_ids = self.get_talk_namespace_ids(&mut conn)?;

        self.has_pos_templates =
            !self.params.templates_yes.is_empty() || !self.params.templates_any.is_empty();
        self.has_pos_linked_from = !self.params.linked_from_all.is_empty()
            || !self.params.linked_from_any.is_empty()
            || !self.params.links_to_all.is_empty()
            || !self.params.links_to_any.is_empty();

        let primary = if !self.cat_pos.is_empty() {
            "categories"
        } else if self.has_pos_templates {
            "templates"
        } else if self.has_pos_linked_from {
            "links_from"
        } else if primary_pagelist.is_some() {
            "pagelist"
        } else if self.params.page_wikidata_item == "without" {
            "no_wikidata"
        } else {
            return Err(format!("SourceDatabase: Missing primary"));
        };

        let link_count_sql = if self.params.gather_link_count {
            ",(SELECT count(*) FROM pagelinks WHERE pl_from=p.page_id) AS link_count"
        } else {
            ",0 AS link_count" // Dummy
        };

        let mut sql_before_after = Platform::sql_tuple();
        let mut before: String = self.params.before.clone();
        let mut after: String = self.params.after.clone();
        let mut is_before_after_done: bool = false;
        match self.params.max_age {
            Some(max_age) => {
                let utc: DateTime<Utc> = Utc::now();
                let utc = utc.sub(Duration::hours(max_age));
                before = "".to_string();
                after = utc.format("%Y%m%d%H%M%S").to_string();
            }
            None => {}
        }

        if before.is_empty() && after.is_empty() {
            is_before_after_done = true;
        } else {
            sql_before_after.0 = " INNER JOIN (revision r) on r.rev_page=p.page_id".to_string();
            if self.params.only_new_since {
                sql_before_after.0 += " AND r.rev_parent_id=0";
            }
            if !before.is_empty() {
                sql_before_after.0 += " AND rev_timestamp<=?";
                sql_before_after.1.push(before);
            }
            if !after.is_empty() {
                sql_before_after.0 += " AND rev_timestamp>=?";
                sql_before_after.1.push(after);
            }
            sql_before_after.0 += " ";
        }

        let mut sql = Platform::sql_tuple();

        match primary {
            "categories" => {
                let category_batches = if self.params.use_new_category_mode {
                    self.iterate_category_batches(&self.cat_pos, 0)
                } else {
                    vec![self.cat_pos.to_owned()]
                };

                Platform::profile(
                    "DSDB::get_pages [primary:categories] BATCHES begin",
                    Some(category_batches.len()),
                );
                let error = Arc::new(Mutex::new(None));
                let ret = Arc::new(Mutex::new(ret));

                let pool = match rayon::ThreadPoolBuilder::new()
                    .num_threads(10) // TODO More? Less?
                    .build()
                {
                    Ok(pool) => pool,
                    Err(e) => return Err(e.to_string()),
                };

                pool.install(|| {
                    category_batches.par_iter().for_each(|category_batch| {
                        let mut sql = Platform::sql_tuple();
                        match self.params.combine.as_str() {
                            "subset" => {
                                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len".to_string() ;
                                sql.0 += link_count_sql;
                                sql.0 += " FROM ( SELECT * from categorylinks WHERE cl_to IN (";
                                //println!("\nSTART:\n{:?}", &sql);
                                Platform::append_sql(
                                    &mut sql,
                                    Platform::prep_quote(&category_batch[0].to_owned()),
                                );
                                sql.0 += ")) cl0";
                                //println!("\nAPPENDED:\n{:?}", &sql);
                                for a in 1..category_batch.len() {
                                    sql.0 += format!(" INNER JOIN categorylinks cl{} ON cl0.cl_from=cl{}.cl_from and cl{}.cl_to IN (",a,a,a).as_str();
                                    Platform::append_sql(
                                        &mut sql,
                                        Platform::prep_quote(&category_batch[a].to_owned()),
                                    );
                                    sql.0 += ")";
                                    //println!("\nINNER:\n{:?}", &sql);
                                }
                            }
                            "union" => {
                                let mut tmp: HashSet<String> = HashSet::new();
                                category_batch.iter().for_each(|group| {
                                    group.iter().for_each(|s| {
                                        tmp.insert(s.to_string());
                                    });
                                });
                                let tmp = tmp
                                    .par_iter()
                                    .map(|s| s.to_owned())
                                    .collect::<Vec<String>>();
                                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len".to_string() ;
                                sql.0 += link_count_sql;
                                sql.0 += " FROM ( SELECT * FROM categorylinks WHERE cl_to IN (";
                                Platform::append_sql(&mut sql, Platform::prep_quote(&tmp));
                                sql.0 += ")) cl0";
                            }
                            other => {
                                panic!("self.params.combine is '{}'", &other);
                            }
                        }
                        sql.0 += " INNER JOIN (page p";
                        sql.0 += ") ON p.page_id=cl0.cl_from";
                        let mut pl2 = PageList::new_from_wiki(&wiki.clone());
                        let api = match state.get_api_for_wiki(wiki.clone()) {
                            Ok(api) => api,
                            Err(e) => {
                                *error.lock().unwrap() = Some(e.to_string());
                                return ;
                            }
                        };
                        match self.get_pages_for_primary_new_connection(
                            state,
                            &wiki,
                            &primary.to_string(),
                            &mut sql,
                            &mut sql_before_after.clone(),
                            &mut pl2,
                            &mut is_before_after_done.clone(),
                            api,
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                *error.lock().unwrap() = Some(e.to_string());
                                return ;
                            }
                        }
                        let ret = ret.lock().unwrap();
                        if ret.is_empty() {
                            ret.swap_entries(&mut pl2);
                        } else {
                            ret.union(pl2, None).unwrap();
                        }
                        Platform::profile("DSDB::get_pages [primary:categories] BATCH COMPLETE",None);
                    });
                } ) ;

                match &*error.lock().unwrap() {
                    Some(e) => return Err(e.to_string()),
                    None => {}
                }
                let lock = Arc::try_unwrap(ret).expect("Lock still has multiple owners");
                let ret = lock.into_inner().expect("Mutex cannot be locked");
                Platform::profile(
                    "DSDB::get_pages [primary:categories] RESULTS end",
                    Some(ret.len()),
                );
                return Ok(ret);
            }
            "no_wikidata" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len".to_string() ;
                sql.0 += link_count_sql;
                sql.0 += " FROM page p";
                if !is_before_after_done {
                    is_before_after_done = true;
                    Platform::append_sql(&mut sql, sql_before_after.clone());
                }
                sql.0 += " WHERE p.page_id NOT IN (SELECT pp_page FROM page_props WHERE pp_propname='wikibase_item')" ;
            }
            "templates" | "links_from" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len ".to_string() ;
                sql.0 += link_count_sql;
                sql.0 += " FROM page p";
                if !is_before_after_done {
                    is_before_after_done = true;
                    Platform::append_sql(&mut sql, sql_before_after.clone());
                }
                sql.0 += " WHERE 1=1";
            }
            "pagelist" => {
                let primary_pagelist = primary_pagelist.ok_or(format!(
                    "SourceDatabase::get_pages: pagelist: No primary_pagelist"
                ))?;
                ret.set_wiki(primary_pagelist.wiki());
                if primary_pagelist.is_empty() {
                    // Nothing to do, but that's OK
                    return Ok(ret);
                }

                let nslist = primary_pagelist.group_by_namespace();
                let mut batches: Vec<SQLtuple> = vec![];
                nslist.iter().for_each(|nsgroup| {
                    nsgroup.1.chunks(PAGE_BATCH_SIZE*2).for_each(|titles| {
                        let titles = titles.to_vec();
                        let mut sql = Platform::sql_tuple();
                        sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len ".to_string() ;
                        sql.0 += link_count_sql;
                        sql.0 += " FROM page p";
                        if !is_before_after_done {
                            is_before_after_done = true;
                            Platform::append_sql(&mut sql, sql_before_after.clone());
                        }
                        sql.0 += " WHERE (p.page_namespace=";
                        sql.0 += &nsgroup.0.to_string();
                        sql.0 += " AND p.page_title IN (";
                        Platform::append_sql(&mut sql, Platform::prep_quote(&titles));
                        sql.0 += "))";
                        batches.push(sql);
                    });
                });

                let wiki = primary_pagelist.wiki().ok_or(format!("No wiki 12345"))?;
                let partial_ret: Vec<PageList> = batches
                    .par_iter_mut()
                    .map(|mut sql| {
                        let mut conn = state.get_wiki_db_connection(&db_user_pass, &wiki).unwrap();
                        let sql_before_after = sql_before_after.clone();
                        let mut is_before_after_done = is_before_after_done.clone();
                        let mut pl2 = PageList::new_from_wiki(&wiki.clone());
                        self.get_pages_for_primary(
                            &mut conn,
                            &primary.to_string(),
                            &mut sql,
                            sql_before_after,
                            &mut pl2,
                            &mut is_before_after_done,
                            state.get_api_for_wiki(wiki.clone()).unwrap(),
                        )
                        .unwrap();
                        pl2
                    })
                    .collect();

                for mut pl2 in partial_ret {
                    if ret.is_empty() {
                        ret.swap_entries(&mut pl2);
                    } else {
                        ret.union(pl2, None)?;
                    }
                }
                return Ok(ret);
            }
            other => {
                return Err(format!(
                    "SourceDatabase::get_pages: other primary '{}'",
                    &other
                ));
            }
        }

        let wiki = match &self.params.wiki {
            Some(wiki) => wiki,
            None => return Err(format!("No wiki 12345")),
        };
        self.get_pages_for_primary(
            &mut conn,
            &primary.to_string(),
            &mut sql,
            sql_before_after,
            &mut ret,
            &mut is_before_after_done,
            state.get_api_for_wiki(wiki.clone())?,
        )?;
        Ok(ret)
    }

    fn get_pages_for_primary_new_connection(
        &self,
        state: &Arc<AppState>,
        wiki: &String,
        primary: &String,
        sql: &mut SQLtuple,
        sql_before_after: &mut SQLtuple,
        pages_sublist: &mut PageList,
        is_before_after_done: &mut bool,
        api: Api,
    ) -> Result<(), String> {
        let db_user_pass = match state.get_db_mutex().lock() {
            Ok(db) => db,
            Err(e) => return Err(format!("Bad mutex: {:?}", e)),
        };
        let mut conn = state.get_wiki_db_connection(&db_user_pass, &wiki)?;
        self.get_pages_for_primary(
            &mut conn,
            primary,
            sql,
            sql_before_after.clone(),
            pages_sublist,
            is_before_after_done,
            api,
        )
    }

    fn get_pages_for_primary(
        &self,
        conn: &mut my::Conn,
        primary: &String,
        mut sql: &mut SQLtuple,
        sql_before_after: SQLtuple,
        pages_sublist: &mut PageList,
        is_before_after_done: &mut bool,
        api: Api,
    ) -> Result<(), String> {
        // Namespaces
        if !self.params.namespace_ids.is_empty() {
            sql.0 += " AND p.page_namespace IN(";
            sql.0 += &self
                .params
                .namespace_ids
                .par_iter()
                .map(|ns| ns.to_string())
                .collect::<Vec<String>>()
                .join(",");
            sql.0 += ")";
        }

        // Negative categories
        if !self.cat_neg.is_empty() {
            self.cat_neg.iter().for_each(|cats|{
                sql.0 += " AND p.page_id NOT IN (SELECT DISTINCT cl_from FROM categorylinks WHERE cl_to IN (" ;
                Platform::append_sql(&mut sql, Platform::prep_quote(cats));
                sql.0 += "))" ;
            });
        }

        // Templates as secondary; template namespace only!
        // TODO talk page
        if self.has_pos_templates {
            // All
            self.params.templates_yes.iter().for_each(|t| {
                let tmp = self.template_subquery(
                    &vec![t.to_string()],
                    self.params.templates_yes_talk_page,
                    false,
                );
                Platform::append_sql(&mut sql, tmp);
            });

            // Any
            if !self.params.templates_any.is_empty() {
                let tmp = self.template_subquery(
                    &self.params.templates_any,
                    self.params.templates_any_talk_page,
                    false,
                );
                Platform::append_sql(&mut sql, tmp);
            }
        }

        // Negative templates
        if !self.params.templates_no.is_empty() {
            let tmp = self.template_subquery(
                &self.params.templates_no,
                self.params.templates_no_talk_page,
                true,
            );
            Platform::append_sql(&mut sql, tmp);
        }

        // Links from all
        self.params.linked_from_all.iter().for_each(|l| {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                self.links_from_subquery(&vec![l.to_owned()], &api),
            );
        });

        // Links from any
        if !self.params.linked_from_any.is_empty() {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                self.links_from_subquery(&self.params.linked_from_any, &api),
            );
        }

        // Links from none
        if !self.params.linked_from_none.is_empty() {
            sql.0 += " AND p.page_id NOT IN ";
            Platform::append_sql(
                &mut sql,
                self.links_from_subquery(&self.params.linked_from_none, &api),
            );
        }

        // Links to all
        self.params.links_to_all.iter().for_each(|l| {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(&mut sql, self.links_to_subquery(&vec![l.to_owned()], &api));
        });

        // Links to any
        if !self.params.links_to_any.is_empty() {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                self.links_to_subquery(&self.params.links_to_any, &api),
            );
        }

        // Links to none
        if !self.params.links_to_none.is_empty() {
            sql.0 += " AND p.page_id NOT IN ";
            Platform::append_sql(
                &mut sql,
                self.links_to_subquery(&self.params.links_to_none, &api),
            );
        }

        // Lead image
        match self.params.page_image.as_str() {
            "yes" => sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))" ,
            "free" => sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image_free')" ,
            "nonfree" => sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image')" ,
            "no" => sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))" ,
            _ => {}
        }

        // ORES
        if self.params.ores_type != "any"
            && (self.params.ores_prediction != "any"
                || self.params.ores_prob_from.is_some()
                || self.params.ores_prob_to.is_some())
        {
            sql.0 += " AND EXISTS (SELECT * FROM ores_classification WHERE p.page_latest=oresc_rev AND oresc_model IN (SELECT oresm_id FROM ores_model WHERE oresm_is_current=1 AND oresm_name=?)" ;
            sql.1.push(self.params.ores_type.to_owned());
            match self.params.ores_prediction.as_str() {
                "yes" => sql.0 += " AND oresc_is_predicted=1",
                "no" => sql.0 += " AND oresc_is_predicted=0",
                _ => {}
            }
            match self.params.ores_prob_from {
                Some(x) => {
                    sql.0 += " AND oresc_probability>=";
                    sql.0 += x.to_string().as_str();
                }
                None => {}
            }
            match self.params.ores_prob_to {
                Some(x) => {
                    sql.0 += " AND oresc_probability<=";
                    sql.0 += x.to_string().as_str();
                }
                None => {}
            }
            sql.0 += ")";
        }

        // Last edit
        match self.params.last_edit_anon.as_str() {
            "yes" => sql.0 +=" AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NULL)" ,
            "no" => sql.0 +=" AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NOT NULL)" ,
            _ => {}
        }
        match self.params.last_edit_bot.as_str() {
            "yes" => sql.0 +=" AND EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')" ,
            "no" => sql.0 +=" AND NOT EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')" ,
            _ => {}
        }
        match self.params.last_edit_flagged.as_str() {
            "yes" => sql.0 +=
                " AND NOT EXISTS (SELECT * FROM flaggedpage_pending WHERE p.page_id=fpp_page_id)",
            "no" => {
                sql.0 +=
                    " AND EXISTS (SELECT * FROM flaggedpage_pending WHERE p.page_id=fpp_page_id)"
            }
            _ => {}
        }

        // Misc
        match self.params.redirects.as_str() {
            "yes" => sql.0 += " AND p.page_is_redirect=1",
            "no" => sql.0 += " AND p.page_is_redirect=0",
            _ => {}
        }
        match self.params.larger {
            Some(i) => {
                sql.0 += " AND p.page_len>=";
                sql.0 += i.to_string().as_str();
            }
            None => {}
        }
        match self.params.smaller {
            Some(i) => {
                sql.0 += " AND p.page_len<=";
                sql.0 += i.to_string().as_str();
            }
            None => {}
        }

        // Speed up "Only pages without Wikidata items"
        if primary != "no_wikidata" && self.params.page_wikidata_item == "without" {
            sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item')" ;
        }

        if !*is_before_after_done {
            Platform::append_sql(sql, sql_before_after);
            *is_before_after_done = true;
        }

        // Link count
        let mut having: Vec<SQLtuple> = vec![];
        match self.params.minlinks {
            Some(l) => having.push(("link_count>=".to_owned() + l.to_string().as_str(), vec![])),
            None => {}
        }
        match self.params.maxlinks {
            Some(l) => having.push(("link_count<=".to_owned() + l.to_string().as_str(), vec![])),
            None => {}
        }

        // HAVING
        if !having.is_empty() {
            sql.0 += " HAVING ";
            for h in having {
                Platform::append_sql(sql, h);
            }
        }

        let wiki = match &self.params.wiki {
            Some(wiki) => wiki,
            None => {
                return Err(format!(
                    "SourceDatabase::get_pages_for_primary: no wiki parameter set in self.params"
                ))
            }
        };

        let result = match conn.prep_exec(sql.0.to_owned(), sql.1.to_owned()) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!("{:?}", &e));
            }
        };

        let entries_tmp = result
            .filter_map(|row_result| row_result.ok())
            .map(|row| {
                let (page_id, page_title, page_namespace, page_timestamp, page_bytes, link_count) =
                    my::from_row::<(usize, String, NamespaceID, String, usize, usize)>(row);
                let mut entry = PageListEntry::new(Title::new(&page_title, page_namespace));
                entry.page_id = Some(page_id);
                entry.page_bytes = Some(page_bytes);
                entry.page_timestamp = Some(page_timestamp);
                if self.params.gather_link_count {
                    entry.link_count = Some(link_count);
                }
                entry
            })
            .collect();

        let pl1 = PageList::new_from_vec(wiki, entries_tmp);
        pl1.swap_entries(pages_sublist);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use serde_json::Value;
    use std::env;
    use std::fs::File;

    fn get_state() -> Arc<AppState> {
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

    #[test]
    fn test_category_subset() {
        let mut params = SourceDatabaseParameters::new();
        params.wiki = Some("enwiki".to_string());
        params.cat_pos = vec!["1974_births".to_string(), "Bioinformaticians".to_string()];
        let mut dbs = SourceDatabase::new(params);
        let state = get_state();
        let result = dbs.get_pages(&state, None).unwrap();
        //println!("{:?}", &result);
        assert_eq!(result.wiki(), Some("enwiki".to_string()));
        assert!(result.len() < 5); // This may change as more articles are written/categories added, please adjust!
        assert!(result
            .entries()
            .read()
            .unwrap()
            .iter()
            .any(|entry| entry.title().pretty() == "Magnus Manske"));
    }
}
