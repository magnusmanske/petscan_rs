use async_recursion::async_recursion;
use futures::future::join_all;
use async_trait::async_trait;
use crate::app_state::AppState;
use crate::datasource::DataSource;
use crate::datasource::SQLtuple;
use crate::pagelist::*;
use crate::platform::{Platform, PAGE_BATCH_SIZE};
use chrono::prelude::*;
use chrono::Duration;
use core::ops::Sub;
use mysql_async::prelude::Queryable;
use mysql_async::Value as MyValue;
use mysql_async::from_row;
use mysql_async as my;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use wikibase::mediawiki::api::{Api, NamespaceID};
use wikibase::mediawiki::title::Title;

static MAX_CATEGORY_BATCH_SIZE: usize = 2500;

#[derive(Debug)]
struct DsdbParams {
    link_count_sql: String,
    wiki: String,
    primary: String,
    sql_before_after: SQLtuple,
    is_before_after_done: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabaseCatDepth {
    name: String,
    depth: u16,
}

#[derive(Debug, Clone, PartialEq, Default)]
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
    category_namespace_is_case_insensitive: bool,
    template_namespace_is_case_insensitive: bool,
}

impl SourceDatabaseParameters {
    pub fn new() -> Self {
        Self {
            combine: "subset".to_string(),
            page_wikidata_item: "any".to_string(),
            page_image: "any".to_string(),
            ores_prediction: "any".to_string(),
            last_edit_bot: "both".to_string(),
            last_edit_anon: "both".to_string(),
            last_edit_flagged: "both".to_string(),
            use_new_category_mode: true,
            category_namespace_is_case_insensitive: true,
            template_namespace_is_case_insensitive: true,
            ..Default::default()
        }
    }

    pub async fn db_params(platform: &Platform) -> SourceDatabaseParameters {
        let depth_signed: i32 = platform
            .get_param("depth")
            .unwrap_or_else(|| "0".to_string())
            .parse::<i32>()
            .unwrap_or(0);
        let depth: u16 = if depth_signed < 0 {
            999
        } else {
            depth_signed as u16
        };
        let mut combine = match platform.form_parameters().params.get("combination") {
            Some(x) => {
                if x == "union" {
                    x.to_string()
                } else {
                    "subset".to_string()
                }
            }
            None => "subset".to_string(),
        };
        let cat_pos = platform.get_param_as_vec("categories", "\n");
        if cat_pos.len() == 1 && combine == "subset" {
            combine = "union".to_string(); // Easier to construct
        }
        let ns10_case_sensitive = platform.get_namespace_case_sensitivity(10).await ;
        let ns14_case_sensitive = platform.get_namespace_case_sensitivity(14).await ;
        let mut ret = SourceDatabaseParameters {
            combine,
            only_new_since: platform.has_param("only_new"),
            max_age: platform
                .get_param("max_age")
                .map(|x| x.parse::<i64>().unwrap_or(0)),
            before: platform.get_param_blank("before"),
            after: platform.get_param_blank("after"),
            templates_yes: vec![],
            templates_any: vec![],
            templates_no: vec![],
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
            depth,
            cat_pos,
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
            category_namespace_is_case_insensitive: !ns14_case_sensitive,
            template_namespace_is_case_insensitive: !ns10_case_sensitive,
        };
        ret.templates_yes = Self::vec_to_ucfirst(
            platform.get_param_as_vec("templates_yes", "\n"),
            ret.template_namespace_is_case_insensitive,
        );
        ret.templates_any = Self::vec_to_ucfirst(
            platform.get_param_as_vec("templates_any", "\n"),
            ret.template_namespace_is_case_insensitive,
        );
        ret.templates_no = Self::vec_to_ucfirst(
            platform.get_param_as_vec("templates_no", "\n"),
            ret.template_namespace_is_case_insensitive,
        );
        ret
    }

    pub fn s2u_ucfirst(s: &str, is_case_insensitive: bool) -> String {
        match is_case_insensitive {
            true => Title::spaces_to_underscores(&Title::first_letter_uppercase(s)),
            false => Title::spaces_to_underscores(s),
        }
    }

    fn vec_to_ucfirst(input: Vec<String>, is_case_insensitive: bool) -> Vec<String> {
        input
            .iter()
            .map(|s| Self::s2u_ucfirst(s, is_case_insensitive))
            .collect()
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

#[async_trait]
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

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let ret = self.get_pages(&platform.state(), None).await?;
        if ret.is_empty()? {
            platform.warn("<span tt=\'warn_categories\'></span>".to_string())?;
        }
        Ok(ret)
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
            talk_namespace_ids: String::new(),
        }
    }

    fn parse_category_depth(
        &self,
        cats: &[String],
        default_depth: u16,
    ) -> Vec<SourceDatabaseCatDepth> {
        cats.iter()
            .filter_map(|c| {
                let mut parts = c.split('|');
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
                Some(SourceDatabaseCatDepth { name , depth })
            })
            .collect()
    }

    async fn go_depth_batch(
        &self,
        state: &AppState,
        wiki: &str,
        categories_batch: Vec<String>,
        categories_done: &RwLock<HashSet<String>>,
        new_categories: &RwLock<Vec<String>>,
    ) -> Result<(), String> {
        let mut sql : SQLtuple = ("SELECT DISTINCT page_title FROM page,categorylinks WHERE cl_from=page_id AND cl_type='subcat' AND cl_to IN (".to_string(),vec![]);
        categories_batch.iter().for_each(|c| {
            // Don't par_iter, already in pool!
            if let Ok(mut cd) = categories_done.write() {
                cd.insert(c.to_string());
            }
        });
        Platform::append_sql(&mut sql, Platform::prep_quote(&categories_batch));
        sql.0 += ")";

        let mut conn = state
            .get_wiki_db_connection(&wiki)
            .await? ;
        let result = conn
            .exec_iter(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(from_row::<Vec<u8>>)
            .await
            .map_err(|e|format!("{:?}",e))?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;

        let mut err : Option<String> = None ;
        result
            .iter()
            .map(|row| String::from_utf8_lossy(&row).into_owned())
            .for_each(|page_title| {
                let do_add = match categories_done.read() {
                    Ok(cd) => !cd.contains(&page_title),
                    _ => false,
                };
                if do_add {
                    match new_categories.write() {
                        Ok(mut nc) => { nc.push(page_title.to_owned()); }
                        Err(e) => { err = Some(e.to_string()); }
                    }
                    match categories_done.write() {
                        Ok(mut cd) => { cd.insert(page_title); }
                        Err(e) => { err = Some(e.to_string()); }
                    }
                }
            });
        match err {
            Some(e) => Err(e),
            None => Ok(())
        }
    }

    #[async_recursion]
    async fn go_depth(
        &self,
        state: &AppState,
        wiki: &str,
        categories_done: &RwLock<HashSet<String>>,
        categories_to_check: &[String],
        depth: u16,
    ) -> Result<(), String> {
        if depth == 0 || categories_to_check.is_empty() {
            return Ok(());
        }
        Platform::profile("DSDB::do_depth begin", Some(categories_to_check.len()));

        let new_categories: Vec<String> = vec![];
        let new_categories = RwLock::new(new_categories);

        let category_batches = categories_to_check
            .par_iter()
            .map(|s|s.to_string())
            .chunks(PAGE_BATCH_SIZE)
            .collect::<Vec<Vec<String>>>();
        let mut futures = vec![] ;
        for categories_batch in category_batches {
            let future = self.go_depth_batch(
                &state,
                wiki,
                categories_batch,
                &categories_done,
                &new_categories,
            ) ;
            futures.push(future);
        }
        join_all(futures).await;

        let new_categories = new_categories
            .into_inner()
            .map_err(|e| format!("{:?}", e))?;

        Platform::profile("DSDB::do_depth new categories", Some(new_categories.len()));

        Platform::profile(
            "DSDB::do_depth end, categories done",
            Some(
                categories_done
                    .read()
                    .map_err(|e| format!("{:?}", e))?
                    .len(),
            ),
        );

        self.go_depth(&state, wiki, categories_done, &new_categories, depth - 1).await?;
        Ok(())
    }

    async fn get_categories_in_tree(
        &self,
        state: &AppState,
        wiki: &str,
        title: &str,
        depth: u16,
    ) -> Result<Vec<String>, String> {
        let categories_done = RwLock::new(HashSet::new());
        let title = SourceDatabaseParameters::s2u_ucfirst(
            title,
            self.params.category_namespace_is_case_insensitive,
        );
        (*categories_done.write().map_err(|e| format!("{:?}", e))?).insert(title.to_owned());
        self.go_depth(&state, wiki, &categories_done, &[title.to_string()], depth).await?;
        let mut tmp = categories_done
            .into_inner()
            .map_err(|e| format!("{:?}", e))?;
        Ok(tmp.drain().collect())
    }

    pub async fn parse_category_list(
        &self,
        state: &AppState,
        wiki: &str,
        input: &[SourceDatabaseCatDepth],
    ) -> Result<Vec<Vec<String>>, String> {
        let mut futures = vec![] ;
        for i in input {
            let future = self.get_categories_in_tree(&state, wiki, &i.name, i.depth) ;
            futures.push(future) ;
        }

        Ok(join_all(futures)
            .await
            .into_par_iter()
            .filter_map(|i|i.ok())
            .filter(|i|!i.is_empty())
            .collect())
    }

    async fn get_talk_namespace_ids(&self, conn: &mut my::Conn) -> Result<String, String> {
        let rows = conn.exec_iter("SELECT DISTINCT page_namespace FROM page WHERE MOD(page_namespace,2)=1",()).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(from_row::<NamespaceID>)
            .await
            .map_err(|e|format!("{:?}",e))?;
        Ok(rows.iter().map(|ns|ns.to_string()).collect::<Vec<String>>().join(","))
    }

    fn template_subquery(
        &self,
        input: &[String],
        use_talk_page: bool,
        find_not: bool,
    ) -> SQLtuple {
        let mut sql = Platform::sql_tuple();
        if use_talk_page {
            sql.0 += if find_not {
                " AND p.page_id NOT IN "
            } else {
                " AND p.page_id IN "
            };
            sql.0 += "(SELECT pt2.page_id FROM page pt,page pt2,templatelinks WHERE pt2.page_namespace+1=pt.page_namespace AND pt2.page_title=pt.page_title AND pt.page_id=tl_from AND tl_namespace=10 AND tl_title";
        } else {
            sql.0 += if find_not {
                " AND p.page_id NOT IN "
            } else {
                " AND p.page_id IN "
            };
            sql.0 +=
                "(SELECT DISTINCT tl_from FROM templatelinks WHERE tl_namespace=10 AND tl_title";
        }

        self.sql_in(&input, &mut sql);

        if !self.params.namespace_ids.is_empty() {
            let v : Vec<String> = self
                .params
                .namespace_ids
                .iter()
                .map(|ns| if use_talk_page { ns + 1 } else { *ns })
                .map(|s| s.to_string())
                .collect() ;
            sql.0 += " AND tl_from_namespace";
            self.sql_in(&v,&mut sql);
        }

        sql.0 += ")";

        sql
    }

    fn sql_in(&self, input: &[String], sql: &mut SQLtuple) {
        if input.len() == 1 {
            sql.0 += "=";
            Platform::append_sql(sql, Platform::prep_quote(input));
        } else {
            sql.0 += " IN (";
            Platform::append_sql(sql, Platform::prep_quote(input));
            sql.0 += ")";
        }
    }

    fn group_link_list_by_namespace(
        &self,
        input: &[String],
        api: &Api,
    ) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        input.iter().for_each(|title| {
            let title = Title::new_from_full(title, &api);
            ret.entry(title.namespace_id()).or_insert_with(Vec::new).push(title.with_underscores());
        });
        ret
    }

    fn links_from_subquery(&self, input: &[String], api: &Api) -> SQLtuple {
        let mut sql: SQLtuple = ("(".to_string(), vec![]);
        let nslist = self.group_link_list_by_namespace(input, api);
        nslist.iter().for_each(|nsgroup| {
            if !sql.1.is_empty() {
                sql.0 += " ) OR ( ";
            }
            sql.0 += "( SELECT p_to.page_id FROM page p_to,page p_from,pagelinks WHERE p_from.page_namespace=";
            sql.0 += &nsgroup.0.to_string();
            sql.0 += "  AND p_from.page_id=pl_from AND pl_namespace=p_to.page_namespace AND pl_title=p_to.page_title AND p_from.page_title" ;
            self.sql_in(nsgroup.1,&mut sql);
            sql.0 += " )";
        });
        sql.0 += ")";
        sql
    }

    fn links_to_subquery(&self, input: &[String], api: &Api) -> SQLtuple {
        let mut sql: SQLtuple = ("(".to_string(), vec![]);
        let nslist = self.group_link_list_by_namespace(input, api);
        nslist.iter().for_each(|nsgroup| {
            if !sql.1.is_empty() {
                sql.0 += " ) OR ( ";
            }
            sql.0 += "( SELECT DISTINCT pl_from FROM pagelinks WHERE pl_namespace=";
            sql.0 += &nsgroup.0.to_string();
            sql.0 += " AND pl_title";
            self.sql_in(nsgroup.1, &mut sql);
            sql.0 += " )";
        });
        sql.0 += ")";
        sql
    }

    fn iterate_category_batches(
        &self,
        categories: &[Vec<String>],
        start: usize,
    ) -> Vec<Vec<Vec<String>>> {
        let mut ret: Vec<Vec<Vec<String>>> = vec![];
        categories[start]
            .chunks(MAX_CATEGORY_BATCH_SIZE * 10)
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

    async fn get_pages_for_category_batch(
        &self,
        params: &DsdbParams,
        category_batch: &[Vec<String>],
        state: &AppState,
        ret: &PageList,
    ) -> Result<(), String> {
        let mut sql = Platform::sql_tuple();
        match self.params.combine.as_str() {
            "subset" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,p.page_len".to_string() ;
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM ( SELECT * from categorylinks WHERE cl_to IN (";
                Platform::append_sql(&mut sql, Platform::prep_quote(&category_batch[0]));
                sql.0 += ")) cl0";
                for (a, item) in category_batch.iter().enumerate().skip(1) {
                    sql.0 += format!(" INNER JOIN categorylinks cl{} ON cl0.cl_from=cl{}.cl_from and cl{}.cl_to IN (",a,a,a).as_str();
                    Platform::append_sql(&mut sql, Platform::prep_quote(&item));
                    sql.0 += ")";
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
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,p.page_len".to_string() ;
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM ( SELECT * FROM categorylinks WHERE cl_to IN (";
                Platform::append_sql(&mut sql, Platform::prep_quote(&tmp));
                sql.0 += ")) cl0";
            }
            other => {
                return Err(format!("self.params.combine is '{}'", &other));
            }
        }
        sql.0 += " INNER JOIN (page p";
        sql.0 += ") ON p.page_id=cl0.cl_from";
        let mut pl2 = PageList::new_from_wiki(&params.wiki.clone());
        let api = state.get_api_for_wiki(params.wiki.clone()).await?;
        Platform::profile(
            "DSDB::get_pages [primary:categories] START BATCH",
            Some(sql.1.len()),
        );
        self.get_pages_for_primary_new_connection(
            &state,
            &params.wiki,
            &params.primary.to_string(),
            sql,
            &mut params.sql_before_after.clone(),
            &mut pl2,
            &mut params.is_before_after_done.clone(),
            api,
        ).await?;
        Platform::profile("DSDB::get_pages [primary:categories] PROCESS BATCH", None);
        ret.union(&pl2, None).await?;
        Platform::profile("DSDB::get_pages [primary:categories] BATCH COMPLETE", None);
        Ok(())
    }

    async fn get_pages_initialize_query(
        &mut self,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<DsdbParams, String> {
        // Take wiki from given pagelist
        if let Some(pl) = primary_pagelist {
            if self.params.wiki.is_none() && pl.wiki()?.is_some() {
                self.params.wiki = pl.wiki()?;
            }
        }

        // Paranoia
        if self.params.wiki.is_none() || self.params.wiki == Some("wiki".to_string()) {
            return Err(format!("SourceDatabase: Bad wiki '{:?}'", self.params.wiki));
        }

        let wiki = match &self.params.wiki {
            Some(wiki) => wiki.to_owned(),
            None => return Err("SourceDatabase::get_pages: No wiki in params".to_string()),
        };

        // Get positive categories serial list
        self.cat_pos = self.parse_category_list(
            &state,
            &wiki,
            &self.parse_category_depth(&self.params.cat_pos, self.params.depth),
        ).await?;

        // Get negative categories serial list
        self.cat_neg = self.parse_category_list(
            &state,
            &wiki,
            &self.parse_category_depth(&self.params.cat_neg, self.params.depth),
        ).await?;

        let mut conn = state.get_wiki_db_connection(&wiki).await?;
        self.talk_namespace_ids = self.get_talk_namespace_ids(&mut conn).await?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;

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
            return Err("SourceDatabase: Missing primary".to_string());
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
        if let Some(max_age) = self.params.max_age {
            let utc: DateTime<Utc> = Utc::now();
            let utc = utc.sub(Duration::hours(max_age));
            before = String::new();
            after = utc.format("%Y%m%d%H%M%S").to_string();
        }

        if before.is_empty() && after.is_empty() {
            is_before_after_done = true;
        } else {
            sql_before_after.0 = " INNER JOIN (revision r) ON r.rev_page=p.page_id".to_string();
            if self.params.only_new_since {
                sql_before_after.0 += " AND r.rev_parent_id=0";
            } else {
                sql_before_after.0 += " AND r.rev_id=p.page_latest";
            }
            if !before.is_empty() {
                sql_before_after.0 += " AND r.rev_timestamp<=?";
                sql_before_after.1.push(MyValue::Bytes(before.into()));
            }
            if !after.is_empty() {
                sql_before_after.0 += " AND r.rev_timestamp>=?";
                sql_before_after.1.push(MyValue::Bytes(after.into()));
            }
            sql_before_after.0 += " ";
        }

        Ok(DsdbParams {
            link_count_sql: link_count_sql.to_string(),
            wiki,
            primary: primary.to_string(),
            sql_before_after,
            is_before_after_done,
        })
    }

    async fn get_pages_categories(
        &mut self,
        params: &DsdbParams,
        state: &AppState,
    ) -> Result<PageList, String> {
        let category_batches = if self.params.use_new_category_mode {
            self.iterate_category_batches(&self.cat_pos, 0)
        } else {
            vec![self.cat_pos.to_owned()]
        };

        Platform::profile(
            "DSDB::get_pages [primary:categories] BATCHES begin",
            Some(category_batches.len()),
        );
        let ret = PageList::new_from_wiki(&params.wiki);

        let futures : Vec<_> = category_batches
            .iter()
            .map(|category_batch| self.get_pages_for_category_batch(&params, category_batch, &state, &ret))
            .collect();

        let results = join_all(futures).await;

        // Check for errors
        for result in results {
            result?;
        }

        Platform::profile(
            "DSDB::get_pages [primary:categories] RESULTS end",
            Some(ret.len()?),
        );
        Ok(ret)
    }

    async fn get_pages_pagelist(
        &mut self,
        mut params: DsdbParams,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList, String> {
        let ret = PageList::new_from_wiki(&params.wiki);
        let primary_pagelist = primary_pagelist.ok_or_else(|| "SourceDatabase::get_pages: pagelist: No primary_pagelist".to_string())?;
        ret.set_wiki(primary_pagelist.wiki()?)?;
        if primary_pagelist.is_empty()? {
            // Nothing to do, but that's OK
            return Ok(ret);
        }

        let nslist = primary_pagelist.group_by_namespace()?;
        let mut batches: Vec<SQLtuple> = vec![];
        nslist.iter().for_each(|nsgroup| {
                    nsgroup.1.chunks(PAGE_BATCH_SIZE*2).for_each(|titles| {
                        let mut sql = Platform::sql_tuple();
                        sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,p.page_len ".to_string() ;
                        sql.0 += &params.link_count_sql;
                        sql.0 += " FROM page p";
                        if !params.is_before_after_done {
                            Platform::append_sql(&mut sql, params.sql_before_after.clone());
                        }
                        sql.0 += " WHERE (p.page_namespace=";
                        sql.0 += &nsgroup.0.to_string();
                        sql.0 += " AND p.page_title IN (";
                        Platform::append_sql(&mut sql, Platform::prep_quote(&titles));
                        sql.0 += "))";
                        batches.push(sql);
                    });
                });

        // Either way, it's done
        params.is_before_after_done = true;

        let wiki = primary_pagelist.wiki()?.ok_or_else(|| "No wiki 12345".to_string())?;

        let mut futures : Vec<_> = vec![] ;
        for sql in batches {
            let future = self.get_pages_pagelist_batch(wiki.clone(),sql,&state,&params) ;
            futures.push ( future ) ;
        }
        let results = join_all(futures).await;

        // TODO futures? tricky
        for pl2 in results {
            ret.union(&pl2?, None).await?;
        }
        
        Ok(ret)
    }

    async fn get_pages_pagelist_batch(&self,
        wiki:String,
        sql:SQLtuple,
        state:&AppState,
        params:&DsdbParams,
    ) -> Result<PageList,String> {
        let mut conn = state.get_wiki_db_connection(&wiki).await?;
        let sql_before_after = params.sql_before_after.clone();
        let mut is_before_after_done = params.is_before_after_done;
        let mut pl2 = PageList::new_from_wiki(&wiki.clone());
        let api = state.get_api_for_wiki(wiki.clone()).await?;
        self.get_pages_for_primary(
            &mut conn,
            &params.primary.to_string(),
            sql,
            sql_before_after,
            &mut pl2,
            &mut is_before_after_done,
            api,
        ).await?;
        drop(conn);
        Ok(pl2)
    }

    pub async fn get_pages(
        &mut self,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList, String> {
        let mut params =
            self.get_pages_initialize_query(state, primary_pagelist).await?;

        let mut sql = Platform::sql_tuple();

        match params.primary.as_str() {
            "categories" => {
                return self.get_pages_categories(&params, &state).await;
            }
            "pagelist" => {
                return self.get_pages_pagelist(params, &state, primary_pagelist).await;
            }
            "no_wikidata" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,p.page_len".to_string() ;
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p";
                if !params.is_before_after_done {
                    params.is_before_after_done = true;
                    Platform::append_sql(&mut sql, params.sql_before_after.clone());
                }
                sql.0 += " WHERE p.page_id NOT IN (SELECT pp_page FROM page_props WHERE pp_propname='wikibase_item')" ;
            }
            "templates" | "links_from" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,p.page_len ".to_string() ;
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p";
                if !params.is_before_after_done {
                    params.is_before_after_done = true;
                    Platform::append_sql(&mut sql, params.sql_before_after.clone());
                }
                sql.0 += " WHERE 1=1";
            }
            other => {
                return Err(format!(
                    "SourceDatabase::get_pages: other primary '{}'",
                    &other
                ));
            }
        }

        let mut ret = PageList::new_from_wiki(&params.wiki);
        let mut conn = state.get_wiki_db_connection(&params.wiki).await?;
        self.get_pages_for_primary(
            &mut conn,
            &params.primary.to_string(),
            sql,
            params.sql_before_after,
            &mut ret,
            &mut params.is_before_after_done,
            state.get_api_for_wiki(params.wiki.clone()).await?,
        ).await?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;
        Ok(ret)
    }

    async fn get_pages_for_primary_new_connection(
        &self,
        state: &AppState,
        wiki: &String,
        primary: &String,
        sql:SQLtuple,
        sql_before_after: &mut SQLtuple,
        pages_sublist: &mut PageList,
        is_before_after_done: &mut bool,
        api: Api,
    ) -> Result<(), String> {
        let mut conn = state.get_wiki_db_connection(&wiki).await?;
        Platform::profile(
            "DSDB::get_pages_for_primary_new_connection STARTING",
            Some(sql.1.len()),
        );
        let ret = self.get_pages_for_primary(
            &mut conn,
            primary,
            sql,
            sql_before_after.clone(),
            pages_sublist,
            is_before_after_done,
            api,
        ).await;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;
        ret
    }

    async fn get_pages_for_primary(
        &self,
        conn: &mut my::Conn,
        primary: &String,
        mut sql: SQLtuple,
        sql_before_after: SQLtuple,
        pages_sublist: &mut PageList,
        is_before_after_done: &mut bool,
        api: Api,
    ) -> Result<(), String> {
        Platform::profile("DSDB::get_pages_for_primary STARTING", Some(sql.1.len()));

        // Namespaces
        if !self.params.namespace_ids.is_empty() {
            let namespace_ids = &self
                .params
                .namespace_ids
                .iter()
                .map(|ns| ns.to_string())
                .collect::<Vec<String>>();
            sql.0 += " AND p.page_namespace";
            self.sql_in(&namespace_ids, &mut sql);
        }

        // Negative categories
        if !self.cat_neg.is_empty() {
            self.cat_neg.iter().for_each(|cats| {
                sql.0 +=
                    " AND p.page_id NOT IN (SELECT DISTINCT cl_from FROM categorylinks WHERE cl_to";
                self.sql_in(cats, &mut sql);
                sql.0 += ")";
            });
        }

        // Templates as secondary; template namespace only!
        if self.has_pos_templates {
            // All
            self.params.templates_yes.iter().for_each(|t| {
                let tmp = self.template_subquery(
                    &[t.to_string()],
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
                self.links_from_subquery(&[l.to_owned()], &api),
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
            Platform::append_sql(&mut sql, self.links_to_subquery(&[l.to_owned()], &api));
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
            sql.1.push(MyValue::Bytes(self.params.ores_type.to_owned().into()));
            match self.params.ores_prediction.as_str() {
                "yes" => sql.0 += " AND oresc_is_predicted=1",
                "no" => sql.0 += " AND oresc_is_predicted=0",
                _ => {}
            }
            if let Some(x) = self.params.ores_prob_from {
                sql.0 += " AND oresc_probability>=";
                sql.0 += x.to_string().as_str();
            }
            if let Some(x) = self.params.ores_prob_to {
                sql.0 += " AND oresc_probability<=";
                sql.0 += x.to_string().as_str();
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
        if let Some(i) = self.params.larger {
            sql.0 += " AND p.page_len>=";
            sql.0 += i.to_string().as_str();
        }
        if let Some(i) = self.params.smaller {
            sql.0 += " AND p.page_len<=";
            sql.0 += i.to_string().as_str();
        }

        // Speed up "Only pages without Wikidata items"
        if primary != "no_wikidata" && self.params.page_wikidata_item == "without" {
            sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item')" ;
        }

        // Last edit/created before/after
        if !*is_before_after_done {
            Platform::append_sql(&mut sql, sql_before_after);
            *is_before_after_done = true;
        }

        // Link count
        let mut having: Vec<SQLtuple> = vec![];
        if let Some(l) = self.params.minlinks { having.push(("link_count>=".to_owned() + l.to_string().as_str(), vec![])) }
        if let Some(l) = self.params.maxlinks { having.push(("link_count<=".to_owned() + l.to_string().as_str(), vec![])) }

        // HAVING
        if !having.is_empty() {
            sql.0 += " HAVING ";
            for h in having {
                Platform::append_sql(&mut sql, h);
            }
        }

        let wiki = match &self.params.wiki {
            Some(wiki) => wiki,
            None => {
                return Err("SourceDatabase::get_pages_for_primary: no wiki parameter set in self.params".to_string())
            }
        };

        Platform::profile(
            "DSDB::get_pages_for_primary STARTING RUN",
            Some(sql.1.len()),
        );

        let sql_1_len = sql.1.len() ;
        let rows = conn.exec_iter(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(from_row::<(u32, Vec<u8>, NamespaceID, Vec<u8>, u32, LinkCount)>)
            .await
            .map_err(|e|format!("{:?}",e))?;

        Platform::profile(
            "DSDB::get_pages_for_primary RUN FINISHED",
            Some(sql_1_len),
        );

        pages_sublist.set_wiki(Some(wiki.to_string()))?;
        pages_sublist.clear_entries()?;

        Platform::profile(
            "DSDB::get_pages_for_primary RETRIEVING RESULT",
            Some(sql_1_len),
        );

        rows
            .iter()
            .for_each(
                |(page_id, page_title, page_namespace, page_timestamp, page_bytes, link_count)| {
                    let page_title = String::from_utf8_lossy(&page_title).into_owned();
                    let page_timestamp = String::from_utf8_lossy(&page_timestamp).into_owned();
                    let mut entry = PageListEntry::new(Title::new(&page_title, *page_namespace));
                    entry.page_id = Some(*page_id);
                    entry.page_bytes = Some(*page_bytes);
                    entry.set_page_timestamp(Some(page_timestamp));
                    if self.params.gather_link_count {
                        entry.link_count = Some(*link_count);
                    }
                    if pages_sublist.add_entry(entry).is_ok() {}
                },
            );

        Platform::profile("DSDB::get_pages_for_primary COMPLETE", Some(sql_1_len));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use serde_json::Value;
    use std::env;
    use std::fs::File;
    use std::sync::Arc;

    async fn get_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let file = File::open(path).expect("Can not open config file");
        let petscan_config: Value =
            serde_json::from_reader(file).expect("Can not parse JSON from config file");
        Arc::new(AppState::new_from_config(&petscan_config).await)
    }

    async fn simulate_category_query(url_params: Vec<(&str, &str)>) -> Result<PageList, String> {
        let state = get_state().await;
        let mut fp = FormParameters::new();
        fp.params = url_params
            .iter()
            .map(|pair| (pair.0.to_string(), pair.1.to_string()))
            .collect();
        let platform = Platform::new_from_parameters(&fp, state.clone());
        let params = SourceDatabaseParameters::db_params(&platform).await;
        let mut dbs = SourceDatabase::new(params);
        dbs.get_pages(&state, None).await
    }

    #[tokio::test]
    async fn test_category_subset() {
        let params = vec![
            ("categories", "1974_births\nGerman bioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result = simulate_category_query(params).await.unwrap();
        assert_eq!(result.wiki(), Ok(Some("enwiki".to_string())));
        assert!(result.len().unwrap() < 5); // This may change as more articles are written/categories added, please adjust!
        assert!(result
            .entries()
            .read()
            .unwrap()
            .iter()
            .any(|entry| entry.title().pretty() == "Magnus Manske"));
    }

    #[tokio::test]
    async fn test_category_union() {
        let params = vec![
            ("categories", "1974_births"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result_size1 = simulate_category_query(params).await.unwrap().len();
        let params = vec![
            ("categories", "Bioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result_size2 = simulate_category_query(params).await.unwrap().len();
        let params = vec![
            ("categories", "1974_births\nBioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
            ("combination", "union"),
        ];
        let result = simulate_category_query(params).await.unwrap();
        assert!(result.len() > result_size1);
        assert!(result.len() > result_size2);
    }

    #[tokio::test]
    async fn test_category_case_sensitive() {
        let params = vec![
            ("categories", "français de France"),
            ("language", "fr"),
            ("project", "wiktionary"),
        ];
        let result = simulate_category_query(params).await.unwrap();
        assert!(result.len().unwrap() > 0);
    }

    #[tokio::test]
    async fn test_category_case_insensitive() {
        let params = vec![
            ("categories", "biology"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result = simulate_category_query(params).await.unwrap();
        assert!(result.len().unwrap() > 0);
    }
}
