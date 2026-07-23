use crate::app_state::AppState;
use crate::datasource::DataSource;
use crate::datasource::SQLtuple;
use crate::pagelist::PageList;
use crate::pagelist_entry::LinkCount;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MAX_CONCURRENT_DB_BATCHES, PAGE_BATCH_SIZE, Platform};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Duration;
use chrono::prelude::*;
use core::ops::Sub;
use futures::stream::{StreamExt, iter};
use mysql_async as my;
use mysql_async::Value as MyValue;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use rayon::prelude::*;
use std::collections::HashSet;
use tracing::debug;
use wikimisc::mediawiki::api::{Api, NamespaceID};
use wikimisc::mediawiki::title::Title;

mod helpers;
use helpers::MAX_CATEGORY_BATCH_SIZE;

const MAX_SUBCATEGORIES_IN_TREE: usize = 500000;

/// Wikidata item Q6964088 ("Category:Tracking categories"). Its sitelinks
/// give the authoritative, wiki-local name of the container category that
/// holds each wiki's tracking categories (issue #197).
const TRACKING_CATEGORIES_ITEM: u64 = 6964088;

/// Bundles the four mostly-context arguments shared by
/// `get_pages_for_primary` and `get_pages_for_primary_new_connection`.
/// `sql` and `sql_before_after` stay as separate parameters because they
/// are transformed mid-call and the two functions handle them slightly
/// differently (mut borrow + clone vs. moved owned value).
struct PrimaryQueryArgs<'a> {
    primary: &'a String,
    pages_sublist: &'a mut PageList,
    is_before_after_done: &'a mut bool,
    api: Api,
}

/// How multiple categories should be combined. Previously a free-form
/// `String` field with `"subset"`/`"union"` magic values and an explicit
/// `_ => Err(...)` fall-through; an enum makes the dispatch total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CombineMode {
    /// Intersect category sets (default).
    #[default]
    Subset,
    /// Take the union of category sets.
    Union,
}

impl CombineMode {
    /// Parse from the user-facing query-string value. Anything unrecognised
    /// (including missing) falls back to [`CombineMode::Subset`].
    pub fn from_param(s: Option<&str>) -> Self {
        match s {
            Some("union") => Self::Union,
            _ => Self::Subset,
        }
    }
}
const PAGE_SELECT_PREFIX: &str = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,p.page_len";

type PrimaryResultRow = (u32, Vec<u8>, NamespaceID, Vec<u8>, u32, LinkCount);

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
    combine: CombineMode,
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
    soft_redirects: String,
    disambiguation_pages: String,
    talk_page_exists: String,
    page_wikidata_item: String,
    larger: Option<usize>,
    smaller: Option<usize>,
    since_rev0: Option<usize>,
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
    created_by: Vec<String>,
    skip_tracking_categories: bool,
    skip_hidden_categories: bool,
}

impl SourceDatabaseParameters {
    pub fn new() -> Self {
        Self {
            combine: CombineMode::Subset,
            page_wikidata_item: "any".to_string(),
            page_image: "any".to_string(),
            ores_prediction: "any".to_string(),
            last_edit_bot: "both".to_string(),
            last_edit_anon: "both".to_string(),
            last_edit_flagged: "both".to_string(),
            talk_page_exists: "both".to_string(),
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
        let mut combine = CombineMode::from_param(
            platform
                .form_parameters()
                .params
                .get("combination")
                .map(|s| s.as_str()),
        );
        let cat_pos = platform.get_param_as_vec("categories", "\n");
        if cat_pos.len() == 1 && combine == CombineMode::Subset {
            // Single category — union is structurally equivalent and cheaper to construct.
            combine = CombineMode::Union;
        }
        let ns10_case_sensitive = platform.get_namespace_case_sensitivity(10).await;
        let ns14_case_sensitive = platform.get_namespace_case_sensitivity(14).await;
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
            soft_redirects: platform.get_param_blank("show_soft_redirects"),
            disambiguation_pages: platform.get_param_blank("show_disambiguation_pages"),
            talk_page_exists: platform.get_param_default("talk_page_exists", "both"),
            minlinks: platform.usize_option_from_param("minlinks"),
            maxlinks: platform.usize_option_from_param("maxlinks"),
            larger: platform.usize_option_from_param("larger"),
            since_rev0: platform.usize_option_from_param("since_rev0"),
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
            created_by: vec![],
            skip_tracking_categories: platform.has_param("skip_tracking_categories"),
            skip_hidden_categories: platform.has_param("skip_hidden_categories"),
        };
        ret.templates_yes = helpers::vec_to_ucfirst(
            platform.get_param_as_vec("templates_yes", "\n"),
            ret.template_namespace_is_case_insensitive,
        );
        ret.templates_any = helpers::vec_to_ucfirst(
            platform.get_param_as_vec("templates_any", "\n"),
            ret.template_namespace_is_case_insensitive,
        );
        ret.templates_no = helpers::vec_to_ucfirst(
            platform.get_param_as_vec("templates_no", "\n"),
            ret.template_namespace_is_case_insensitive,
        );
        ret.created_by = platform.get_param_as_vec("created_by", "\n");
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
    /// Wiki-local name of the tracking-categories container category
    /// (namespace stripped, underscores), resolved from Q6964088 when
    /// `skip_tracking_categories` is requested. `None` = no filtering.
    tracking_category_local: Option<String>,
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
            || platform.has_param("created_by")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let ret = self.get_pages(&platform.state(), None).await?;
        if ret.is_empty() {
            platform.warn("<span tt=\'warn_categories\'></span>".to_string())?;
        }
        Ok(ret)
    }
}

impl SourceDatabase {
    pub const fn new(params: SourceDatabaseParameters) -> Self {
        Self {
            cat_pos: vec![],
            cat_neg: vec![],
            has_pos_templates: false,
            has_pos_linked_from: false,
            params,
            talk_namespace_ids: String::new(),
            tracking_category_local: None,
        }
    }

    /// Resolve the wiki-local name of the "tracking categories" container
    /// category via its Q6964088 sitelink on the wikidatawiki replica,
    /// e.g. `enwiki` → `Tracking_categories`. Returned without namespace
    /// prefix and with underscores, ready for `lt_title` comparison.
    ///
    /// Errors if the wiki has no such sitelink: silently ignoring the
    /// user's explicit filter request would produce misleading results.
    async fn resolve_tracking_category(state: &AppState, wiki: &str) -> Result<String> {
        let rows = state
            .get_wiki_db_connection("wikidatawiki")
            .await?
            .exec_iter(
                "SELECT ips_site_page FROM wb_items_per_site WHERE ips_item_id=? AND ips_site_id=?",
                (TRACKING_CATEGORIES_ITEM, wiki),
            )
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<Vec<u8>>)
            .await
            .map_err(|e| anyhow!(e))?;
        let full_title = rows
            .first()
            .map(|row| String::from_utf8_lossy(row).into_owned())
            .ok_or_else(|| {
                anyhow!(
                    "Cannot skip tracking categories on {wiki}: no local category is linked to wikidata:Q6964088"
                )
            })?;
        let api = state.get_api_for_wiki(wiki.to_string()).await?;
        let title = Title::new_from_full(&full_title, &api);
        if title.namespace_id() != 14 {
            return Err(anyhow!(
                "Cannot skip tracking categories on {wiki}: the wikidata:Q6964088 sitelink '{full_title}' is not a category"
            ));
        }
        Ok(title.with_underscores())
    }

    async fn get_categories_in_list(
        &self,
        state: &AppState,
        wiki: &str,
        categories: &[String],
    ) -> Result<Vec<String>> {
        let sql = helpers::subcategories_query(
            categories,
            self.params.skip_hidden_categories,
            self.tracking_category_local.as_deref(),
        );
        let result = state
            .get_wiki_db_connection(wiki)
            .await?
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<Vec<u8>>)
            .await
            .map_err(|e| anyhow!(e))?;
        let result: Vec<String> = result
            .iter()
            .map(|row| String::from_utf8_lossy(row).into_owned())
            .collect();
        Ok(result)
    }

    /// Takes a root category and returns all subcategories to a specified depth.
    /// The returend list contains the root cagegory.
    /// Depth 0 returns only the root category, depth 1 adds all direct subcategories etc.
    async fn get_categories_in_tree(
        &self,
        state: &AppState,
        wiki: &str,
        title: &str,
        depth: u16,
    ) -> Result<Vec<String>> {
        let is_cs = self.params.category_namespace_is_case_insensitive;
        let mut categories_done = HashSet::new();
        let new_title = helpers::s2u_ucfirst(title, is_cs);
        categories_done.insert(new_title.to_owned());

        let mut categories_todo = vec![];
        categories_todo.push(new_title.to_owned());

        let mut remaining_depth = depth;
        while remaining_depth > 0 && !categories_todo.is_empty() {
            remaining_depth -= 1;
            let mut futures = vec![];
            for chunk in categories_todo.chunks(MAX_CATEGORY_BATCH_SIZE * 10) {
                let future = self.get_categories_in_list(state, wiki, chunk);
                futures.push(future);
            }
            let results: Vec<_> = iter(futures)
                .buffered(MAX_CONCURRENT_DB_BATCHES)
                .collect()
                .await;
            categories_todo.clear();
            categories_todo.shrink_to_fit();
            let mut categories_new = HashSet::new();
            for result in results {
                for category in result? {
                    let title2 = helpers::s2u_ucfirst(&category, is_cs);
                    if !categories_done.contains(&title2) {
                        categories_new.insert(category);
                        categories_done.insert(title2);
                    }
                }
            }
            debug!(
                remaining_depth,
                count = categories_new.len(),
                "added new sub-categories"
            );
            if categories_done.len() > MAX_SUBCATEGORIES_IN_TREE {
                return Err(anyhow!(
                    "Sub-categories for \"{new_title}\" exceed {MAX_SUBCATEGORIES_IN_TREE}, please limit that category depth, or fix the category tree"
                ));
            }
            categories_todo = categories_new.drain().collect();
        }
        Ok(categories_done.drain().collect())
    }

    pub async fn parse_category_list(
        &self,
        state: &AppState,
        wiki: &str,
        input: &[SourceDatabaseCatDepth],
    ) -> Result<Vec<Vec<String>>> {
        let mut futures = vec![];
        for i in input {
            let future = self.get_categories_in_tree(state, wiki, &i.name, i.depth);
            futures.push(future);
        }

        let mut ret = vec![];
        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;
        for result in results {
            let result = result?;
            if !result.is_empty() {
                ret.push(result);
            }
        }
        Ok(ret)
    }

    async fn get_talk_namespace_ids(&self, conn: &mut my::Conn) -> Result<String> {
        let rows = conn
            .exec_iter(
                "SELECT DISTINCT page_namespace FROM page WHERE MOD(page_namespace,2)=1",
                (),
            )
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<NamespaceID>)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(rows
            .iter()
            .map(|ns| ns.to_string())
            .collect::<Vec<String>>()
            .join(","))
    }

    fn template_subquery(&self, input: &[String], use_talk_page: bool, find_not: bool) -> SQLtuple {
        let mut sql = super::sql_tuple();
        if use_talk_page {
            sql.0 += if find_not {
                " AND p.page_id NOT IN "
            } else {
                " AND p.page_id IN "
            };
            sql.0 += "(SELECT pt2.page_id FROM page pt,page pt2,templatelinks,linktarget WHERE pt2.page_namespace+1=pt.page_namespace AND pt2.page_title=pt.page_title AND pt.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title";
        } else {
            sql.0 += if find_not {
                " AND p.page_id NOT IN "
            } else {
                " AND p.page_id IN "
            };
            sql.0 += "(SELECT DISTINCT tl_from FROM templatelinks,linktarget WHERE p.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title";
        }

        helpers::sql_in(input, &mut sql);

        if !self.params.namespace_ids.is_empty() {
            let v: Vec<String> = self
                .params
                .namespace_ids
                .iter()
                .map(|ns| if use_talk_page { ns + 1 } else { *ns })
                .map(|s| s.to_string())
                .collect();
            sql.0 += " AND tl_from_namespace";
            helpers::sql_in(&v, &mut sql);
        }

        sql.0 += ")";

        sql
    }

    /// Build the primary query for one batch of category groups. Pure SQL
    /// construction — no DB access — so it can be snapshot-tested.
    ///
    /// `Subset` intersects the groups via self-joins on `categorylinks`;
    /// `Union` merges all groups into one deduplicated `IN` list. Note the
    /// union dedup goes through a `HashSet`, so the *order* of bound titles
    /// is unspecified (only their set is).
    fn category_batch_sql(&self, link_count_sql: &str, category_batch: &[Vec<String>]) -> SQLtuple {
        let subquery = "SELECT cl_from,cl_target_id,lt_title from categorylinks,linktarget WHERE lt_id=cl_target_id AND lt_namespace=14 AND lt_title";
        let mut sql = super::sql_tuple();
        match self.params.combine {
            CombineMode::Subset => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,
                	(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,
                 p.page_len".to_string() ;
                sql.0 += link_count_sql;
                sql.0 += &format!(" FROM ( {subquery} IN (");
                super::append_sql(&mut sql, super::prep_quote(&category_batch[0]));
                sql.0 += ")) cl0";
                for (a, item) in category_batch.iter().enumerate().skip(1) {
                    sql.0 += &format!(" INNER JOIN categorylinks cl{a} ON cl0.cl_from=cl{a}.cl_from
                       INNER JOIN linktarget lt{a} ON lt{a}.lt_namespace=14 AND lt{a}.lt_id=cl{a}.cl_target_id AND lt{a}.lt_title IN (");
                    super::append_sql(&mut sql, super::prep_quote(item));
                    sql.0 += ")";
                }
            }
            CombineMode::Union => {
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
                sql.0 = PAGE_SELECT_PREFIX.to_string();
                sql.0 += link_count_sql;
                sql.0 += &format!(" FROM ( {subquery} IN (");
                super::append_sql(&mut sql, super::prep_quote(&tmp));
                sql.0 += ")) cl0";
            }
        }
        sql.0 += " INNER JOIN (page p";
        sql.0 += ") ON p.page_id=cl0.cl_from";
        sql
    }

    async fn get_pages_for_category_batch(
        &self,
        params: &DsdbParams,
        category_batch: &[Vec<String>],
        state: &AppState,
        ret: &PageList,
    ) -> Result<()> {
        let sql = self.category_batch_sql(&params.link_count_sql, category_batch);
        let mut pl2 = PageList::new_from_wiki(&params.wiki.clone());
        let api = state.get_api_for_wiki(params.wiki.clone()).await?;
        Platform::profile(
            "DSDB::get_pages [primary:categories] START BATCH",
            Some(sql.1.len()),
        );
        let primary = params.primary.to_string();
        let mut is_before_after_done = params.is_before_after_done;
        self.get_pages_for_primary_new_connection(
            state,
            &params.wiki,
            sql,
            &mut params.sql_before_after.clone(),
            PrimaryQueryArgs {
                primary: &primary,
                pages_sublist: &mut pl2,
                is_before_after_done: &mut is_before_after_done,
                api,
            },
        )
        .await?;
        Platform::profile("DSDB::get_pages [primary:categories] PROCESS BATCH", None);
        ret.union(&pl2, None).await?;
        Platform::profile("DSDB::get_pages [primary:categories] BATCH COMPLETE", None);
        Ok(())
    }

    async fn get_pages_initialize_query(
        &mut self,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<DsdbParams> {
        // Take wiki from given pagelist
        if let Some(pl) = primary_pagelist
            && self.params.wiki.is_none()
            && pl.wiki().is_some()
        {
            self.params.wiki = pl.wiki();
        }

        // Paranoia
        if self.params.wiki.is_none() || self.params.wiki == Some("wiki".to_string()) {
            return Err(anyhow!("SourceDatabase: Bad wiki '{:?}'", self.params.wiki));
        }

        let wiki = match &self.params.wiki {
            Some(wiki) => wiki.to_owned(),
            None => return Err(anyhow!("SourceDatabase::get_pages: No wiki in params")),
        };

        // Resolve the local tracking-categories category before any tree
        // traversal, so `get_categories_in_list` can filter against it.
        // Only needed when there are category trees to traverse.
        if self.params.skip_tracking_categories
            && !(self.params.cat_pos.is_empty() && self.params.cat_neg.is_empty())
        {
            self.tracking_category_local =
                Some(Self::resolve_tracking_category(state, &wiki).await?);
        }

        // Get positive categories serial list
        self.cat_pos = self
            .parse_category_list(
                state,
                &wiki,
                &helpers::parse_category_depth(&self.params.cat_pos, self.params.depth),
            )
            .await?;

        // Get negative categories serial list
        self.cat_neg = self
            .parse_category_list(
                state,
                &wiki,
                &helpers::parse_category_depth(&self.params.cat_neg, self.params.depth),
            )
            .await?;

        let mut conn = state.get_wiki_db_connection(&wiki).await?;
        self.talk_namespace_ids = self.get_talk_namespace_ids(&mut conn).await?;

        self.has_pos_templates =
            !self.params.templates_yes.is_empty() || !self.params.templates_any.is_empty();
        self.has_pos_linked_from = !self.params.linked_from_all.is_empty()
            || !self.params.linked_from_any.is_empty()
            || !self.params.links_to_all.is_empty()
            || !self.params.links_to_any.is_empty();

        let primary = self.get_primary(primary_pagelist)?;

        let link_count_sql = if self.params.gather_link_count {
            ",(SELECT count(*) FROM pagelinks WHERE pl_from=p.page_id) AS link_count"
        } else {
            ",0 AS link_count" // Dummy
        };

        let mut sql_before_after = super::sql_tuple();
        let mut before: String = self.params.before.clone();
        let mut after: String = self.params.after.clone();
        let mut is_before_after_done: bool = false;
        if let Some(max_age) = self.params.max_age {
            let utc = Utc::now().sub(Duration::try_hours(max_age).unwrap_or_default());
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

    fn get_primary(&mut self, primary_pagelist: Option<&PageList>) -> Result<String> {
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
        } else if !self.params.created_by.is_empty() {
            "created_by"
        } else {
            return Err(anyhow!("SourceDatabase: Missing primary"));
        };
        Ok(primary.to_string())
    }

    async fn get_pages_categories(
        &mut self,
        params: &DsdbParams,
        state: &AppState,
    ) -> Result<PageList> {
        let category_batches = if self.params.use_new_category_mode {
            helpers::iterate_category_batches(&self.cat_pos, 0)
        } else {
            vec![self.cat_pos.to_owned()]
        };

        Platform::profile(
            "DSDB::get_pages [primary:categories] BATCHES begin",
            Some(category_batches.len()),
        );
        let ret = PageList::new_from_wiki(&params.wiki);

        let futures: Vec<_> = category_batches
            .iter()
            .map(|category_batch| {
                self.get_pages_for_category_batch(params, category_batch, state, &ret)
            })
            .collect();

        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;

        // Check for errors
        for result in results {
            result?;
        }

        Platform::profile(
            "DSDB::get_pages [primary:categories] RESULTS end",
            Some(ret.len()),
        );
        Ok(ret)
    }

    async fn get_pages_pagelist(
        &mut self,
        mut params: DsdbParams,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList> {
        let ret = PageList::new_from_wiki(&params.wiki);
        let primary_pagelist = primary_pagelist
            .ok_or_else(|| anyhow!("SourceDatabase::get_pages: pagelist: No primary_pagelist"))?;
        ret.set_wiki(primary_pagelist.wiki());
        if primary_pagelist.is_empty() {
            // Nothing to do, but that's OK
            return Ok(ret);
        }

        let nslist = primary_pagelist.group_by_namespace();
        let mut batches: Vec<SQLtuple> = vec![];
        nslist.iter().for_each(|nsgroup| {
            nsgroup.1.chunks(PAGE_BATCH_SIZE * 2).for_each(|titles| {
                let mut sql = super::sql_tuple();
                sql.0 = PAGE_SELECT_PREFIX.to_string();
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p";
                if !params.is_before_after_done {
                    super::append_sql(&mut sql, params.sql_before_after.clone());
                }
                sql.0 += " WHERE (p.page_namespace=";
                sql.0 += &nsgroup.0.to_string();
                sql.0 += " AND p.page_title IN (";
                super::append_sql(&mut sql, super::prep_quote(titles));
                sql.0 += "))";
                batches.push(sql);
            });
        });

        // Either way, it's done
        params.is_before_after_done = true;

        let wiki = primary_pagelist
            .wiki()
            .ok_or_else(|| anyhow!("No wiki given in datasource_database::get_pages_pagelist"))?;

        let mut futures: Vec<_> = vec![];
        for sql in batches {
            let future = self.get_pages_pagelist_batch(wiki.clone(), sql, state, &params);
            futures.push(future);
        }
        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;

        for pl2 in results {
            ret.union(&pl2?, None).await?;
        }

        Ok(ret)
    }

    async fn get_pages_pagelist_batch(
        &self,
        wiki: String,
        sql: SQLtuple,
        state: &AppState,
        params: &DsdbParams,
    ) -> Result<PageList> {
        let mut conn = state.get_wiki_db_connection(&wiki).await?;
        let sql_before_after = params.sql_before_after.clone();
        let mut is_before_after_done = params.is_before_after_done;
        let mut pl2 = PageList::new_from_wiki(&wiki.clone());
        let api = state.get_api_for_wiki(wiki.clone()).await?;
        let primary = params.primary.to_string();
        self.get_pages_for_primary(
            &mut conn,
            sql,
            sql_before_after,
            PrimaryQueryArgs {
                primary: &primary,
                pages_sublist: &mut pl2,
                is_before_after_done: &mut is_before_after_done,
                api,
            },
        )
        .await?;
        drop(conn);
        Ok(pl2)
    }

    pub async fn get_pages(
        &mut self,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList> {
        let result = self.get_pages_inner(state, primary_pagelist).await?;
        // Exclude pages in the negative categories. Done here as an in-memory
        // set difference rather than an inline SQL `NOT IN (...)` so a deep
        // excluded-category tree can't blow past MySQL's 65 535 placeholder
        // limit (issue #206).
        self.subtract_negative_categories(state, &result).await?;
        Ok(result)
    }

    /// Resolve the negative ("exclude") categories to their member pages and
    /// remove those pages from `result`.
    ///
    /// The excluded title list is chunked and queried in parallel batches; each
    /// batch carries only its own placeholders, so no single statement can hit
    /// the placeholder limit regardless of how deep the category tree is.
    async fn subtract_negative_categories(
        &self,
        state: &AppState,
        result: &PageList,
    ) -> Result<()> {
        if self.cat_neg.is_empty() || result.is_empty() {
            return Ok(());
        }
        let wiki = result.wiki().ok_or_else(|| {
            anyhow!("SourceDatabase::subtract_negative_categories: result has no wiki")
        })?;

        let mut cats: Vec<String> = self.cat_neg.iter().flatten().cloned().collect();
        cats.sort_unstable();
        cats.dedup();

        let excluded = PageList::new_from_wiki(&wiki);
        let futures: Vec<_> = cats
            .chunks(MAX_CATEGORY_BATCH_SIZE * 10)
            .map(|chunk| self.fetch_category_members(state, &wiki, chunk, &excluded))
            .collect();
        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;
        for r in results {
            r?;
        }

        result.difference(&excluded, None).await?;
        Ok(())
    }

    /// Fetch all member pages of `cats` and append them (id + title + namespace)
    /// to `out`. Matches the old negative-category subquery exactly: every page
    /// directly in any of the categories, regardless of the member's namespace.
    async fn fetch_category_members(
        &self,
        state: &AppState,
        wiki: &str,
        cats: &[String],
        out: &PageList,
    ) -> Result<()> {
        let sql = helpers::category_members_query(cats);
        if sql.1.is_empty() {
            return Ok(()); // No real titles in this chunk; nothing to fetch.
        }
        let rows = state
            .get_wiki_db_connection(wiki)
            .await?
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<(u32, Vec<u8>, NamespaceID)>)
            .await
            .map_err(|e| anyhow!(e))?;
        for (page_id, page_title, page_namespace) in rows {
            let page_title = String::from_utf8_lossy(&page_title).into_owned();
            let mut entry = PageListEntry::new(Title::new(&page_title, page_namespace));
            entry.set_page_id(Some(page_id));
            out.add_entry(entry);
        }
        Ok(())
    }

    async fn get_pages_inner(
        &mut self,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList> {
        let mut params = self
            .get_pages_initialize_query(state, primary_pagelist)
            .await?;

        let mut sql = super::sql_tuple();

        match params.primary.as_str() {
            "categories" => {
                return self.get_pages_categories(&params, state).await;
            }
            "pagelist" => {
                return self
                    .get_pages_pagelist(params, state, primary_pagelist)
                    .await;
            }
            "no_wikidata" => {
                sql.0 = PAGE_SELECT_PREFIX.to_string();
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p";
                if !params.is_before_after_done {
                    params.is_before_after_done = true;
                    super::append_sql(&mut sql, params.sql_before_after.clone());
                }
                sql.0 += " WHERE p.page_id NOT IN (SELECT pp_page FROM page_props WHERE pp_propname='wikibase_item')";
            }
            "templates" | "links_from" | "created_by" => {
                sql.0 = PAGE_SELECT_PREFIX.to_string();
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p";
                if !params.is_before_after_done {
                    params.is_before_after_done = true;
                    super::append_sql(&mut sql, params.sql_before_after.clone());
                }
                sql.0 += " WHERE 1=1";
            }
            other => {
                return Err(anyhow!(
                    "SourceDatabase::get_pages: other primary '{other}'"
                ));
            }
        }

        let mut ret = PageList::new_from_wiki(&params.wiki);
        let mut conn = state.get_wiki_db_connection(&params.wiki).await?;
        let api = state.get_api_for_wiki(params.wiki.clone()).await?;
        let primary = params.primary.to_string();
        self.get_pages_for_primary(
            &mut conn,
            sql,
            params.sql_before_after,
            PrimaryQueryArgs {
                primary: &primary,
                pages_sublist: &mut ret,
                is_before_after_done: &mut params.is_before_after_done,
                api,
            },
        )
        .await?;
        Ok(ret)
    }

    async fn get_pages_for_primary_new_connection(
        &self,
        state: &AppState,
        wiki: &str,
        sql: SQLtuple,
        sql_before_after: &mut SQLtuple,
        args: PrimaryQueryArgs<'_>,
    ) -> Result<()> {
        let mut conn = state.get_wiki_db_connection(wiki).await?;
        Platform::profile(
            "DSDB::get_pages_for_primary_new_connection STARTING",
            Some(sql.1.len()),
        );
        let ret = self
            .get_pages_for_primary(&mut conn, sql, sql_before_after.clone(), args)
            .await;
        ret
    }

    async fn get_pages_for_primary(
        &self,
        conn: &mut my::Conn,
        mut sql: SQLtuple,
        sql_before_after: SQLtuple,
        args: PrimaryQueryArgs<'_>,
    ) -> Result<()> {
        let PrimaryQueryArgs {
            primary,
            pages_sublist,
            is_before_after_done,
            api,
        } = args;
        Platform::profile("DSDB::get_pages_for_primary STARTING", Some(sql.1.len()));

        self.get_pages_for_primary_namespaces(primary, &mut sql);
        // Negative categories are applied *after* the primary query as an
        // in-memory set difference (see `subtract_negative_categories`), not
        // inlined here: a deep excluded-category tree can expand to more titles
        // than MySQL's 65 535 placeholder limit allows in one statement (#206).
        self.get_pages_for_primary_templates_as_secondary(&mut sql);
        self.get_pages_for_primary_negative_templates(&mut sql);
        self.get_pages_for_primary_links_from(&mut sql, &api);
        self.get_pages_for_primary_links_to(&mut sql, api);
        self.get_pages_for_primary_lead_image(&mut sql);
        self.get_pages_for_primary_ores(&mut sql);
        self.get_pages_for_primary_last_edit(&mut sql);
        self.get_pages_for_primary_created_by(&mut sql);
        self.get_pages_for_primary_page_types(&mut sql);
        self.get_pages_for_primary_page_size(&mut sql);
        self.get_pages_for_primary_wikidata_item_speedup(primary, &mut sql);
        Self::get_pages_for_primary_last_edited(is_before_after_done, &mut sql, sql_before_after);
        self.get_pages_for_primary_having(&mut sql);

        let wiki = self
            .params
            .wiki
            .as_ref()
            .ok_or_else(|| {
                anyhow!(
                    "SourceDatabase::get_pages_for_primary: no wiki parameter set in self.params"
                )
            })?
            .to_string();

        let sql_1_len = sql.1.len();
        let rows = self.get_pages_for_primary_run_query(sql, conn).await?;
        Platform::profile("DSDB::get_pages_for_primary RUN FINISHED", Some(sql_1_len));

        pages_sublist.set_wiki(Some(wiki));
        pages_sublist.clear_entries();

        Platform::profile(
            "DSDB::get_pages_for_primary RETRIEVING RESULT",
            Some(sql_1_len),
        );

        self.get_pages_for_primary_rows_to_result(rows, pages_sublist);

        Platform::profile("DSDB::get_pages_for_primary COMPLETE", Some(sql_1_len));

        Ok(())
    }

    fn get_pages_for_primary_wikidata_item_speedup(
        &self,
        primary: &String,
        sql: &mut (String, Vec<MyValue>),
    ) {
        // Speed up "Only pages without Wikidata items"
        if primary != "no_wikidata" && self.params.page_wikidata_item == "without" {
            sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item')";
        }
    }

    fn get_pages_for_primary_rows_to_result(
        &self,
        rows: Vec<PrimaryResultRow>,
        pages_sublist: &mut PageList,
    ) {
        rows.iter().for_each(
            |(page_id, page_title, page_namespace, page_timestamp, page_bytes, link_count)| {
                let page_title = String::from_utf8_lossy(page_title).into_owned();
                let page_timestamp = String::from_utf8_lossy(page_timestamp).into_owned();
                let mut entry = PageListEntry::new(Title::new(&page_title, *page_namespace));
                entry.set_page_id(Some(*page_id));
                entry.set_page_bytes(Some(*page_bytes));
                entry.set_page_timestamp(Some(page_timestamp));
                if self.params.gather_link_count {
                    entry.set_link_count(Some(*link_count));
                }
                pages_sublist.add_entry(entry);
            },
        );
    }

    fn get_pages_for_primary_having(&self, sql: &mut (String, Vec<MyValue>)) {
        // Link count
        let mut having: Vec<String> = vec![];
        if let Some(l) = self.params.minlinks {
            having.push(format!("link_count>={l}"));
        }
        if let Some(l) = self.params.maxlinks {
            having.push(format!("link_count<={l}"));
        }

        // HAVING
        if !having.is_empty() {
            sql.0 += " HAVING ";
            sql.0 += &having.join(" AND ");
        }
    }

    fn get_pages_for_primary_page_size(&self, sql: &mut (String, Vec<MyValue>)) {
        // Size
        if let Some(i) = self.params.larger {
            sql.0 += " AND p.page_len>=";
            sql.0 += i.to_string().as_str();
        }
        if let Some(i) = self.params.smaller {
            sql.0 += &format!(" AND p.page_len<={i}");
        }
        if let Some(i) = self.params.since_rev0 {
            sql.0 += &format!(
                " AND page_len<=(SELECT rev_len FROM revision WHERE rev_page=page_id AND rev_parent_id=0 LIMIT 1)*{i}/100"
            );
        }
    }

    fn get_pages_for_primary_page_types(&self, sql: &mut (String, Vec<MyValue>)) {
        // Misc page types
        // TODO FIXME get local "Soft_redirect" page title from Wikidata Q4844001
        let soft_redirects_page = "Soft_redirect";
        if "yes" == self.params.soft_redirects.as_str() {
            sql.0 += " AND EXISTS (SELECT * FROM templatelinks,linktarget WHERE tl_from=p.page_id AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=?)";
            sql.1.push(MyValue::Bytes(soft_redirects_page.into()));
        }
        match self.params.redirects.as_str() {
            "yes" => sql.0 += " AND p.page_is_redirect=1",
            "no" => sql.0 += " AND p.page_is_redirect=0",
            _ => {}
        }
        match self.params.disambiguation_pages.as_str() {
            "yes" => {
                sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE pp_page=p.page_id AND pp_propname='disambiguation')";
            }
            "no" => {
                sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE pp_page=p.page_id AND pp_propname='disambiguation')";
            }
            _ => {}
        }
        match self.params.talk_page_exists.as_str() {
            "yes" => {
                sql.0 += " AND EXISTS (SELECT * FROM page talk_p WHERE talk_p.page_title=p.page_title AND talk_p.page_namespace=p.page_namespace+1)";
            }
            "no" => {
                sql.0 += " AND NOT EXISTS (SELECT * FROM page talk_p WHERE talk_p.page_title=p.page_title AND talk_p.page_namespace=p.page_namespace+1)";
            }
            _ => {}
        }
    }

    fn get_pages_for_primary_last_edit(&self, sql: &mut (String, Vec<MyValue>)) {
        // Last edit
        match self.params.last_edit_anon.as_str() {
            "yes" => {
                sql.0 += " AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NULL)";
            }
            "no" => {
                sql.0 += " AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NOT NULL)";
            }
            _ => {}
        }
        match self.params.last_edit_bot.as_str() {
            "yes" => {
                sql.0 += " AND EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')";
            }
            "no" => {
                sql.0 += " AND NOT EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')";
            }
            _ => {}
        }
        // `flaggedpages.fp_pending_since` stores the timestamp of the oldest
        // unreviewed edit since the last reviewed revision, or NULL if there
        // are no pending changes. "Latest edit is flagged" therefore means
        // "no pending changes" — hence the double-negative NOT EXISTS /
        // IS NOT NULL form. This intentionally treats pages that are not
        // enrolled in FlaggedRevs at all (no row in `flaggedpages`) as
        // `last_edit_flagged=yes`, matching the upstream PHP PetScan.
        match self.params.last_edit_flagged.as_str() {
            "yes" => {
                sql.0 += " AND NOT EXISTS (SELECT * FROM flaggedpages WHERE fp_pending_since IS NOT NULL AND fp_page_id=p.page_id)";
            }
            "no" => {
                sql.0 += " AND EXISTS (SELECT * FROM flaggedpages WHERE fp_pending_since IS NOT NULL AND fp_page_id=p.page_id)";
            }
            _ => {}
        }
    }

    fn get_pages_for_primary_created_by(&self, sql: &mut SQLtuple) {
        if self.params.created_by.is_empty() {
            return;
        }
        let tmp = super::prep_quote(&self.params.created_by);
        sql.0 += " AND p.page_id IN (SELECT r.rev_page FROM revision r INNER JOIN actor a ON r.rev_actor=a.actor_id WHERE r.rev_parent_id=0 AND a.actor_name IN (";
        super::append_sql(sql, tmp);
        sql.0 += "))";
    }

    fn get_pages_for_primary_ores(&self, sql: &mut (String, Vec<MyValue>)) {
        // ORES
        if self.params.ores_type != "any"
            && (self.params.ores_prediction != "any"
                || self.params.ores_prob_from.is_some()
                || self.params.ores_prob_to.is_some())
        {
            sql.0 += " AND EXISTS (SELECT * FROM ores_classification WHERE p.page_latest=oresc_rev AND oresc_model IN (SELECT oresm_id FROM ores_model WHERE oresm_is_current=1 AND oresm_name=?)";
            sql.1
                .push(MyValue::Bytes(self.params.ores_type.to_owned().into()));
            match self.params.ores_prediction.as_str() {
                "yes" => sql.0 += " AND oresc_is_predicted=1",
                "no" => sql.0 += " AND oresc_is_predicted=0",
                _ => {}
            }
            if let Some(x) = self.params.ores_prob_from {
                sql.0 += &format!(" AND oresc_probability>={x}");
            }
            if let Some(x) = self.params.ores_prob_to {
                sql.0 += &format!(" AND oresc_probability<={x}");
            }
            sql.0 += ")";
        }
    }

    fn get_pages_for_primary_lead_image(&self, sql: &mut (String, Vec<MyValue>)) {
        // Lead image
        match self.params.page_image.as_str() {
            "yes" => {
                sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))";
            }
            "free" => {
                sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image_free')";
            }
            "nonfree" => {
                sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image')";
            }
            "no" => {
                sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))";
            }
            _ => {}
        }
    }

    fn get_pages_for_primary_links_to(&self, sql: &mut (String, Vec<MyValue>), api: Api) {
        // Links to all
        self.params.links_to_all.iter().for_each(|l| {
            sql.0 += " AND p.page_id IN ";
            super::append_sql(sql, helpers::links_to_subquery(&[l.to_owned()], &api));
        });

        // Links to any
        if !self.params.links_to_any.is_empty() {
            sql.0 += " AND p.page_id IN ";
            super::append_sql(
                sql,
                helpers::links_to_subquery(&self.params.links_to_any, &api),
            );
        }

        // Links to none
        if !self.params.links_to_none.is_empty() {
            sql.0 += " AND p.page_id NOT IN ";
            super::append_sql(
                sql,
                helpers::links_to_subquery(&self.params.links_to_none, &api),
            );
        }
    }

    fn get_pages_for_primary_links_from(&self, sql: &mut (String, Vec<MyValue>), api: &Api) {
        // Links from all
        self.params.linked_from_all.iter().for_each(|l| {
            sql.0 += " AND p.page_id IN ";
            super::append_sql(sql, helpers::links_from_subquery(&[l.to_owned()], api));
        });

        // Links from any
        if !self.params.linked_from_any.is_empty() {
            sql.0 += " AND p.page_id IN ";
            super::append_sql(
                sql,
                helpers::links_from_subquery(&self.params.linked_from_any, api),
            );
        }

        // Links from none
        if !self.params.linked_from_none.is_empty() {
            sql.0 += " AND p.page_id NOT IN ";
            super::append_sql(
                sql,
                helpers::links_from_subquery(&self.params.linked_from_none, api),
            );
        }
    }

    fn get_pages_for_primary_negative_templates(&self, sql: &mut (String, Vec<MyValue>)) {
        // Negative templates
        if !self.params.templates_no.is_empty() {
            let tmp = self.template_subquery(
                &self.params.templates_no,
                self.params.templates_no_talk_page,
                true,
            );
            super::append_sql(sql, tmp);
        }
    }

    /// Templates as secondary; template namespace only!
    fn get_pages_for_primary_templates_as_secondary(&self, sql: &mut (String, Vec<MyValue>)) {
        if self.has_pos_templates {
            // All
            self.params.templates_yes.iter().for_each(|t| {
                let tmp = self.template_subquery(
                    &[t.to_string()],
                    self.params.templates_yes_talk_page,
                    false,
                );
                super::append_sql(sql, tmp);
            });

            // Any
            if !self.params.templates_any.is_empty() {
                let tmp = self.template_subquery(
                    &self.params.templates_any,
                    self.params.templates_any_talk_page,
                    false,
                );
                super::append_sql(sql, tmp);
            }
        }
    }

    fn get_pages_for_primary_namespaces(&self, primary: &String, sql: &mut (String, Vec<MyValue>)) {
        if !self.params.namespace_ids.is_empty() && primary != "pagelist" {
            let namespace_ids = &self
                .params
                .namespace_ids
                .iter()
                .map(|ns| ns.to_string())
                .collect::<Vec<String>>();
            sql.0 += " AND p.page_namespace";
            helpers::sql_in(namespace_ids, sql);
        }
    }

    fn get_pages_for_primary_last_edited(
        is_before_after_done: &mut bool,
        sql: &mut (String, Vec<MyValue>),
        sql_before_after: (String, Vec<MyValue>),
    ) {
        // Last edit/created before/after
        if !*is_before_after_done {
            super::append_sql(sql, sql_before_after);
            *is_before_after_done = true;
        }
    }

    async fn get_pages_for_primary_run_query(
        &self,
        sql: (String, Vec<MyValue>),
        conn: &mut my::Conn,
    ) -> Result<Vec<PrimaryResultRow>> {
        Platform::profile(
            "DSDB::get_pages_for_primary STARTING RUN",
            Some(sql.1.len()),
        );
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<(u32, Vec<u8>, NamespaceID, Vec<u8>, u32, LinkCount)>)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::config::Config;
    use crate::form_parameters::FormParameters;
    use std::env;
    use std::sync::Arc;

    async fn get_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let petscan_config = Config::from_file(&path).expect("config.json load failed in test");
        Arc::new(
            AppState::new_from_config(&petscan_config)
                .await
                .expect("AppState::new_from_config failed in test"),
        )
    }

    async fn simulate_category_query(url_params: Vec<(&str, &str)>) -> Result<PageList> {
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
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_category_subset() {
        let params = vec![
            ("categories", "1974_births\nGerman bioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result = simulate_category_query(params).await.unwrap();
        assert_eq!(result.wiki(), Some("enwiki".to_string()));
        assert!(result.len() < 5); // This may change as more articles are written/categories added, please adjust!
        assert!(
            result
                .as_vec()
                .iter()
                .any(|entry| entry.title().pretty() == "Magnus Manske")
        );
    }

    #[tokio::test]
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_category_union() {
        let params1 = vec![
            ("categories", "1974_births"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result_size1 = simulate_category_query(params1).await.unwrap().len();
        let params2 = vec![
            ("categories", "Bioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result_size2 = simulate_category_query(params2).await.unwrap().len();
        let params3 = vec![
            ("categories", "1974_births\nBioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
            ("combination", "union"),
        ];
        let result = simulate_category_query(params3).await.unwrap();
        assert!(result.len() > result_size1);
        assert!(result.len() > result_size2);
    }

    #[tokio::test]
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_category_case_insensitive() {
        let params = vec![
            ("categories", "biology"),
            ("language", "en"),
            ("project", "wikipedia"),
        ];
        let result = simulate_category_query(params).await.unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_resolve_tracking_category_enwiki() {
        let state = get_state().await;
        let name = SourceDatabase::resolve_tracking_category(&state, "enwiki")
            .await
            .unwrap();
        assert_eq!(name, "Tracking_categories");
    }

    #[tokio::test]
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_skip_category_filters_only_remove_pages() {
        // The filters act on the depth-1 traversal; they must not error and
        // can only ever shrink the result, never grow it or empty a normal
        // content tree.
        let base_params = |extra: Vec<(&'static str, &'static str)>| {
            let mut p = vec![
                ("categories", "Bioinformatics"),
                ("depth", "1"),
                ("language", "en"),
                ("project", "wikipedia"),
            ];
            p.extend(extra);
            p
        };
        let unfiltered = simulate_category_query(base_params(vec![])).await.unwrap();
        let filtered = simulate_category_query(base_params(vec![
            ("skip_tracking_categories", "1"),
            ("skip_hidden_categories", "1"),
        ]))
        .await
        .unwrap();
        assert!(!filtered.is_empty(), "content tree must survive filtering");
        assert!(filtered.len() <= unfiltered.len());
    }

    #[tokio::test]
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_negative_category_excludes_members() {
        // "Magnus Manske" is in both "1974 births" and "German bioinformaticians".
        // Without exclusion he appears; excluding the latter must drop him, and
        // the query must not error out (the #206 placeholder regression).
        let has_magnus = |result: &PageList| {
            result
                .as_vec()
                .iter()
                .any(|entry| entry.title().pretty() == "Magnus Manske")
        };

        let without_negcats = simulate_category_query(vec![
            ("categories", "1974_births"),
            ("language", "en"),
            ("project", "wikipedia"),
        ])
        .await
        .unwrap();
        assert!(has_magnus(&without_negcats), "baseline should include Magnus");

        let with_negcats = simulate_category_query(vec![
            ("categories", "1974_births"),
            ("negcats", "German bioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
        ])
        .await
        .unwrap();
        assert!(!has_magnus(&with_negcats), "excluded member must be removed");
        assert!(with_negcats.len() < without_negcats.len(), "exclusion must shrink the result");
    }

    // ─── SQL snapshot tests ──────────────────────────────────────────────
    //
    // These pin the exact SQL text (and bound-value count) each clause
    // builder emits — no database needed. They are the safety net for
    // refactoring the query-construction code: any change to the generated
    // SQL must show up here as a deliberate snapshot update.
    //
    // Not covered (they require a live `Api` for namespace resolution):
    // `get_pages_for_primary_links_from` / `_links_to`.

    /// Build a `SourceDatabase` from tweaked default parameters.
    fn snapshot_db(tweak: impl FnOnce(&mut SourceDatabaseParameters)) -> SourceDatabase {
        let mut params = SourceDatabaseParameters::new();
        tweak(&mut params);
        SourceDatabase::new(params)
    }

    /// Run one clause builder against an empty SQL tuple and return the
    /// generated SQL plus the number of bound values.
    fn built(apply: impl FnOnce(&mut SQLtuple)) -> (String, usize) {
        let mut sql = crate::datasource::sql_tuple();
        apply(&mut sql);
        (sql.0, sql.1.len())
    }

    /// Collapse whitespace runs so snapshots of queries with embedded
    /// newlines/indentation stay readable. Whitespace-only changes are
    /// deliberately not pinned.
    fn norm(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn sql_namespaces_multiple_uses_in_list() {
        let db = snapshot_db(|p| p.namespace_ids = vec![0, 14]);
        let (sql, n) = built(|s| db.get_pages_for_primary_namespaces(&"categories".to_string(), s));
        assert_eq!(sql, " AND p.page_namespace IN (?,?)");
        assert_eq!(n, 2);
    }

    #[test]
    fn sql_namespaces_single_uses_equality() {
        let db = snapshot_db(|p| p.namespace_ids = vec![0]);
        let (sql, n) = built(|s| db.get_pages_for_primary_namespaces(&"categories".to_string(), s));
        assert_eq!(sql, " AND p.page_namespace=?");
        assert_eq!(n, 1);
    }

    #[test]
    fn sql_namespaces_skipped_for_pagelist_primary() {
        let db = snapshot_db(|p| p.namespace_ids = vec![0]);
        let (sql, n) = built(|s| db.get_pages_for_primary_namespaces(&"pagelist".to_string(), s));
        assert_eq!(sql, "");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_templates_yes_one_subquery_per_template() {
        let mut db = snapshot_db(|p| {
            p.templates_yes = vec!["Infobox".to_string(), "Taxobox".to_string()];
        });
        db.has_pos_templates = true;
        let (sql, n) = built(|s| db.get_pages_for_primary_templates_as_secondary(s));
        let one = " AND p.page_id IN (SELECT DISTINCT tl_from FROM templatelinks,linktarget WHERE p.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=?)";
        assert_eq!(sql, format!("{one}{one}"));
        assert_eq!(n, 2);
    }

    #[test]
    fn sql_templates_any_single_subquery_with_in_list() {
        let mut db = snapshot_db(|p| {
            p.templates_any = vec!["Infobox".to_string(), "Taxobox".to_string()];
        });
        db.has_pos_templates = true;
        let (sql, n) = built(|s| db.get_pages_for_primary_templates_as_secondary(s));
        assert_eq!(
            sql,
            " AND p.page_id IN (SELECT DISTINCT tl_from FROM templatelinks,linktarget WHERE p.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title IN (?,?))"
        );
        assert_eq!(n, 2);
    }

    #[test]
    fn sql_templates_no_uses_not_in() {
        let db = snapshot_db(|p| p.templates_no = vec!["Stub".to_string()]);
        let (sql, n) = built(|s| db.get_pages_for_primary_negative_templates(s));
        assert_eq!(
            sql,
            " AND p.page_id NOT IN (SELECT DISTINCT tl_from FROM templatelinks,linktarget WHERE p.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=?)"
        );
        assert_eq!(n, 1);
    }

    #[test]
    fn sql_templates_talk_page_shifts_namespace() {
        let mut db = snapshot_db(|p| {
            p.templates_any = vec!["WikiProject_Biology".to_string()];
            p.templates_any_talk_page = true;
            p.namespace_ids = vec![0];
        });
        db.has_pos_templates = true;
        let (sql, n) = built(|s| db.get_pages_for_primary_templates_as_secondary(s));
        assert_eq!(
            sql,
            " AND p.page_id IN (SELECT pt2.page_id FROM page pt,page pt2,templatelinks,linktarget WHERE pt2.page_namespace+1=pt.page_namespace AND pt2.page_title=pt.page_title AND pt.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=? AND tl_from_namespace=?)"
        );
        // Template title + the talk-shifted namespace id (0+1=1), both bound.
        assert_eq!(n, 2);
    }

    #[test]
    fn sql_lead_image_variants() {
        let yes = snapshot_db(|p| p.page_image = "yes".to_string());
        let (sql, n) = built(|s| yes.get_pages_for_primary_lead_image(s));
        assert_eq!(
            sql,
            " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))"
        );
        assert_eq!(n, 0);

        let no = snapshot_db(|p| p.page_image = "no".to_string());
        let (sql_no, _) = built(|s| no.get_pages_for_primary_lead_image(s));
        assert_eq!(
            sql_no,
            " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))"
        );

        let any = snapshot_db(|_| {});
        let (sql_any, _) = built(|s| any.get_pages_for_primary_lead_image(s));
        assert_eq!(sql_any, "");
    }

    #[test]
    fn sql_ores_full_clause() {
        let db = snapshot_db(|p| {
            p.ores_type = "damaging".to_string();
            p.ores_prediction = "yes".to_string();
            p.ores_prob_from = Some(0.5);
            p.ores_prob_to = Some(0.75);
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_ores(s));
        assert_eq!(
            sql,
            " AND EXISTS (SELECT * FROM ores_classification WHERE p.page_latest=oresc_rev AND oresc_model IN (SELECT oresm_id FROM ores_model WHERE oresm_is_current=1 AND oresm_name=?) AND oresc_is_predicted=1 AND oresc_probability>=0.5 AND oresc_probability<=0.75)"
        );
        assert_eq!(n, 1);
    }

    #[test]
    fn sql_ores_requires_type_and_condition() {
        // Type alone (prediction "any", no probabilities) emits nothing.
        let db = snapshot_db(|p| p.ores_type = "damaging".to_string());
        let (sql, n) = built(|s| db.get_pages_for_primary_ores(s));
        assert_eq!(sql, "");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_last_edit_clauses() {
        let db = snapshot_db(|p| {
            p.last_edit_anon = "yes".to_string();
            p.last_edit_bot = "no".to_string();
            p.last_edit_flagged = "yes".to_string();
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_last_edit(s));
        assert_eq!(
            sql,
            " AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NULL) \
             AND NOT EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot') \
             AND NOT EXISTS (SELECT * FROM flaggedpages WHERE fp_pending_since IS NOT NULL AND fp_page_id=p.page_id)"
        );
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_created_by_binds_actor_names() {
        let db = snapshot_db(|p| {
            p.created_by = vec!["Alice".to_string(), "Bob".to_string()];
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_created_by(s));
        assert_eq!(
            sql,
            " AND p.page_id IN (SELECT r.rev_page FROM revision r INNER JOIN actor a ON r.rev_actor=a.actor_id WHERE r.rev_parent_id=0 AND a.actor_name IN (?,?))"
        );
        assert_eq!(n, 2);
    }

    #[test]
    fn sql_page_types_combined() {
        let db = snapshot_db(|p| {
            p.soft_redirects = "yes".to_string();
            p.redirects = "no".to_string();
            p.disambiguation_pages = "yes".to_string();
            p.talk_page_exists = "no".to_string();
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_page_types(s));
        assert_eq!(
            sql,
            " AND EXISTS (SELECT * FROM templatelinks,linktarget WHERE tl_from=p.page_id AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=?) \
             AND p.page_is_redirect=0 \
             AND EXISTS (SELECT * FROM page_props WHERE pp_page=p.page_id AND pp_propname='disambiguation') \
             AND NOT EXISTS (SELECT * FROM page talk_p WHERE talk_p.page_title=p.page_title AND talk_p.page_namespace=p.page_namespace+1)"
        );
        // The bound value is the soft-redirect template title.
        assert_eq!(n, 1);
    }

    #[test]
    fn sql_page_size_clauses() {
        let db = snapshot_db(|p| {
            p.larger = Some(1000);
            p.smaller = Some(5000);
            p.since_rev0 = Some(150);
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_page_size(s));
        assert_eq!(
            sql,
            " AND p.page_len>=1000 AND p.page_len<=5000 AND page_len<=(SELECT rev_len FROM revision WHERE rev_page=page_id AND rev_parent_id=0 LIMIT 1)*150/100"
        );
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_wikidata_item_speedup_only_outside_no_wikidata_primary() {
        let db = snapshot_db(|p| p.page_wikidata_item = "without".to_string());
        let (sql, _) =
            built(|s| db.get_pages_for_primary_wikidata_item_speedup(&"categories".to_string(), s));
        assert_eq!(
            sql,
            " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item')"
        );
        let (sql_skipped, _) = built(|s| {
            db.get_pages_for_primary_wikidata_item_speedup(&"no_wikidata".to_string(), s);
        });
        assert_eq!(sql_skipped, "");
    }

    #[test]
    fn sql_last_edited_appends_once_and_flips_flag() {
        let before_after = (
            " INNER JOIN (revision r) ON r.rev_page=p.page_id AND r.rev_id=p.page_latest AND r.rev_timestamp<=? ".to_string(),
            vec![MyValue::Bytes("20240101000000".into())],
        );
        let mut done = false;
        let mut sql = crate::datasource::sql_tuple();
        SourceDatabase::get_pages_for_primary_last_edited(&mut done, &mut sql, before_after.clone());
        assert!(done);
        assert_eq!(sql.0, before_after.0);
        assert_eq!(sql.1.len(), 1);
        // A second call must be a no-op: the flag is already set.
        SourceDatabase::get_pages_for_primary_last_edited(&mut done, &mut sql, before_after);
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn sql_having_minlinks_only() {
        let db = snapshot_db(|p| p.minlinks = Some(5));
        let (sql, n) = built(|s| db.get_pages_for_primary_having(s));
        assert_eq!(sql, " HAVING link_count>=5");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_having_maxlinks_only() {
        let db = snapshot_db(|p| p.maxlinks = Some(10));
        let (sql, n) = built(|s| db.get_pages_for_primary_having(s));
        assert_eq!(sql, " HAVING link_count<=10");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_having_min_and_max_links_are_separated() {
        let db = snapshot_db(|p| {
            p.minlinks = Some(5);
            p.maxlinks = Some(10);
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_having(s));
        assert_eq!(sql, " HAVING link_count>=5 AND link_count<=10");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_category_batch_subset_self_joins_each_group() {
        let db = snapshot_db(|p| p.combine = CombineMode::Subset);
        let batch = vec![
            vec!["Births_1974".to_string()],
            vec!["Bioinformaticians".to_string(), "Geneticists".to_string()],
        ];
        let sql = db.category_batch_sql(",0 AS link_count", &batch);
        assert_eq!(
            norm(&sql.0),
            "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace, \
             (SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched, \
             p.page_len,0 AS link_count \
             FROM ( SELECT cl_from,cl_target_id,lt_title from categorylinks,linktarget WHERE lt_id=cl_target_id AND lt_namespace=14 AND lt_title IN (?)) cl0 \
             INNER JOIN categorylinks cl1 ON cl0.cl_from=cl1.cl_from \
             INNER JOIN linktarget lt1 ON lt1.lt_namespace=14 AND lt1.lt_id=cl1.cl_target_id AND lt1.lt_title IN (?,?) \
             INNER JOIN (page p) ON p.page_id=cl0.cl_from"
        );
        assert_eq!(sql.1.len(), 3);
    }

    #[test]
    fn sql_category_batch_union_merges_and_dedups() {
        let db = snapshot_db(|p| p.combine = CombineMode::Union);
        let batch = vec![
            vec!["Chemistry".to_string(), "Biology".to_string()],
            vec!["Biology".to_string()],
        ];
        let sql = db.category_batch_sql(",0 AS link_count", &batch);
        // Dedup goes through a HashSet, so the bound-title order is
        // unspecified — pin the SQL shape and the value count only.
        assert_eq!(
            norm(&sql.0),
            "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,\
             (SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,\
             p.page_len,0 AS link_count \
             FROM ( SELECT cl_from,cl_target_id,lt_title from categorylinks,linktarget WHERE lt_id=cl_target_id AND lt_namespace=14 AND lt_title IN (?,?)) cl0 \
             INNER JOIN (page p) ON p.page_id=cl0.cl_from"
        );
        assert_eq!(sql.1.len(), 2);
    }

    /// Pin the clause order `get_pages_for_primary` applies (minus the two
    /// `Api`-dependent links clauses) for a many-filter query, so a future
    /// reordering shows up as a deliberate snapshot change.
    #[test]
    fn sql_filter_sequence_kitchen_sink() {
        let mut db = snapshot_db(|p| {
            p.namespace_ids = vec![0];
            p.templates_no = vec!["Stub".to_string()];
            p.page_image = "free".to_string();
            p.last_edit_anon = "no".to_string();
            p.redirects = "no".to_string();
            p.larger = Some(100);
            p.page_wikidata_item = "without".to_string();
            p.minlinks = Some(2);
        });
        db.has_pos_templates = false;
        let primary = "categories".to_string();
        let (sql, n) = built(|s| {
            db.get_pages_for_primary_namespaces(&primary, s);
            db.get_pages_for_primary_templates_as_secondary(s);
            db.get_pages_for_primary_negative_templates(s);
            // links_from / links_to skipped: require a live Api
            db.get_pages_for_primary_lead_image(s);
            db.get_pages_for_primary_ores(s);
            db.get_pages_for_primary_last_edit(s);
            db.get_pages_for_primary_created_by(s);
            db.get_pages_for_primary_page_types(s);
            db.get_pages_for_primary_page_size(s);
            db.get_pages_for_primary_wikidata_item_speedup(&primary, s);
            let mut done = true;
            SourceDatabase::get_pages_for_primary_last_edited(
                &mut done,
                s,
                crate::datasource::sql_tuple(),
            );
            db.get_pages_for_primary_having(s);
        });
        assert_eq!(
            sql,
            " AND p.page_namespace=? \
             AND p.page_id NOT IN (SELECT DISTINCT tl_from FROM templatelinks,linktarget WHERE p.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=? AND tl_from_namespace=?) \
             AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image_free') \
             AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NOT NULL) \
             AND p.page_is_redirect=0 \
             AND p.page_len>=100 \
             AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item') \
             HAVING link_count>=2"
        );
        assert_eq!(n, 3);
    }
}
