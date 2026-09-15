use crate::app_state::AppState;
use crate::database_manager::DbCluster;
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
use std::collections::{HashMap, HashSet};
use tracing::debug;
use wikimisc::mediawiki::api::{Api, NamespaceID};
use wikimisc::mediawiki::title::Title;

mod helpers;
use helpers::MAX_CATEGORY_BATCH_SIZE;

/// An SQL fragment with no bound values.
fn sql_text(sql: &str) -> SQLtuple {
    SQLtuple(sql.to_string(), vec![])
}

const MAX_SUBCATEGORIES_IN_TREE: usize = 500000;

/// The most pages a [`Filter::seed`] may name and still restrict the base
/// query.
///
/// A seeded base query costs one statement per [`PAGE_BATCH_SIZE`] seed
/// pages (per category batch), each a random-access probe of the links
/// tables — on Commons roughly 1.5 s per statement. An unseeded one costs
/// its full result set, then that many rows again in the deferred pass. The
/// cap keeps a seed from being applied where it would be the larger side:
/// Commons creates about 55 000 pages a day, so this admits a "created in
/// the last few days" window and rejects a "last edited this month" one.
const MAX_SEED_PAGES: usize = 150_000;

/// The tables each family of filter clauses reads besides `page`. Named so a
/// clause and the cluster it can run on cannot drift apart.
const TEMPLATELINKS: &[&str] = &["templatelinks", "linktarget"];
const PAGELINKS: &[&str] = &["pagelinks", "linktarget"];
const PAGE_PROPS: &[&str] = &["page_props"];
const REVISION: &[&str] = &["revision"];
const REVISION_ACTOR: &[&str] = &["revision", "actor"];
const REVISION_USER_GROUPS: &[&str] = &["revision", "actor", "user_groups"];
const FLAGGEDPAGES: &[&str] = &["flaggedpages"];
const ORES: &[&str] = &["ores_classification", "ores_model"];

/// Wikidata item Q6964088 ("Category:Tracking categories"). Its sitelinks
/// give the authoritative, wiki-local name of the container category that
/// holds each wiki's tracking categories (issue #197).
const TRACKING_CATEGORIES_ITEM: u64 = 6964088;

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
/// Which positive source drives the primary page query. Previously a
/// free-form `String` ("categories", "templates", …) with a runtime
/// `other =>` error branch in `get_pages_inner`; an enum makes the
/// dispatch total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Primary {
    Categories,
    Templates,
    LinksFrom,
    Pagelist,
    NoWikidata,
    CreatedBy,
}

/// `page_touched` is the timestamp of a page's latest revision, which only
/// `revision` knows. Where that table is on another host than the rest of the
/// base query — Commons' links cluster — the column comes back empty and
/// [`SourceDatabase::apply_deferred_filters`] fills it in from the core
/// cluster instead.
const PAGE_TOUCHED_FROM_REVISION: &str =
    "(SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched";

/// The base query's page columns, as the cluster it runs on can provide them.
fn page_select_prefix(cluster: DbCluster) -> String {
    let page_touched = match cluster {
        DbCluster::Core => PAGE_TOUCHED_FROM_REVISION,
        _ => "'' AS page_touched",
    };
    format!("SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,{page_touched},p.page_len")
}

type PrimaryResultRow = (u32, Vec<u8>, NamespaceID, Vec<u8>, u32, LinkCount);

#[derive(Debug)]
struct DsdbParams {
    link_count_sql: String,
    wiki: String,
    primary: Primary,
    /// The cluster the base query runs on; see [`Filters::base_cluster`].
    base_cluster: DbCluster,
    /// Every filter clause the query asks for, collected once and split per
    /// batch. The clauses depend only on the query parameters, so building
    /// them here also settles `base_cluster`.
    filters: Filters,
    /// Page IDs every result is among, when a deferred filter could name
    /// them cheaply up front; see [`Filter::seed`].
    seed: Option<Vec<u32>>,
}

impl DsdbParams {
    /// The page-ID restrictions to run the base query under: one per chunk
    /// of the seed, or a single unrestricted run without one.
    fn seed_chunks(&self) -> Vec<Option<&[u32]>> {
        match &self.seed {
            Some(ids) => ids.chunks(PAGE_BATCH_SIZE).map(Some).collect(),
            None => vec![None],
        }
    }
}

/// Page IDs as an SQL `IN` list body. They come from the database, so
/// interpolating them costs no placeholders and risks no injection.
fn id_list(ids: &[u32]) -> String {
    ids.iter().map(u32::to_string).collect::<Vec<_>>().join(",")
}

/// One filter clause of the primary query, plus the tables it reads besides
/// `page`.
///
/// The SQL is the same wherever it runs; only *where* it can run depends on
/// the wiki, so clauses are collected first and routed to clusters when the
/// query is assembled.
#[derive(Debug, Clone)]
struct Filter {
    tables: &'static [&'static str],
    sql: SQLtuple,
    /// A query selecting a superset of the page IDs this clause matches,
    /// driven by an index of its own so it needs no page set to start from.
    ///
    /// Only clauses that typically match few pages — a window on
    /// `revision`'s timestamp, a creator — offer one. When such a clause is
    /// deferred, the base query would otherwise produce every page of its
    /// source (a large category tree, say) only for the deferred pass to
    /// drop nearly all of them; the seed lets the base query start from the
    /// few instead. The clause itself is still applied afterwards, so the
    /// seed only has to be a superset and correctness never depends on it.
    seed: Option<SQLtuple>,
}

/// The filter clauses of one primary query.
///
/// Commons keeps its links tables (`categorylinks`, `pagelinks`,
/// `templatelinks`, `langlinks`, …) on a different host than `revision`,
/// `page_props`, `actor` and friends, so a single statement cannot filter on
/// both. [`Self::split`] puts everything the base query's cluster can serve
/// into that query and groups the rest by the cluster that can, to be applied
/// as `page_id IN (…)` passes afterwards. Off Commons there is only ever one
/// cluster and nothing is deferred.
///
/// Every clause must be a self-contained ` AND …` over the `page p` alias, so
/// that it reads the same in the base query as in a deferred
/// `SELECT p.page_id FROM page p WHERE …` pass.
#[derive(Debug, Clone, Default)]
struct Filters(Vec<Filter>);

impl Filters {
    /// Records a clause. Empty SQL is dropped, so a builder can push
    /// unconditionally.
    fn push(&mut self, tables: &'static [&'static str], sql: SQLtuple) {
        self.push_seeded(tables, sql, None);
    }

    /// Records a clause with its [`Filter::seed`].
    fn push_seeded(
        &mut self,
        tables: &'static [&'static str],
        sql: SQLtuple,
        seed: Option<SQLtuple>,
    ) {
        if !sql.0.is_empty() {
            self.0.push(Filter { tables, sql, seed });
        }
    }

    /// Records a clause that reads no table but `page`.
    fn push_page_only(&mut self, sql: SQLtuple) {
        self.push(&[], sql);
    }

    /// The cluster to run the base query on, given the tables it joins itself.
    ///
    /// The links cluster wins as soon as anything in the query needs it.
    /// Which cluster carries the selective predicates decides the cost: on
    /// Commons the links tables hold what a query actually selects on — a
    /// category, a template, an incoming link — while the core-only filters
    /// (`revision` timestamps, `page_props` flags) only narrow an
    /// already-small set. Deferring a links predicate instead would leave the
    /// base query scanning all of `page`.
    fn base_cluster(&self, state: &AppState, wiki: &str, base_tables: &[&str]) -> DbCluster {
        let needs_links = base_tables
            .iter()
            .copied()
            .chain(self.0.iter().flat_map(|f| f.tables.iter().copied()))
            .any(|table| !state.cluster_hosts_tables(wiki, DbCluster::Core, &[table]));
        if needs_links && state.wiki_has_cluster(wiki, DbCluster::Links) {
            DbCluster::Links
        } else {
            DbCluster::Core
        }
    }

    /// The seed of the first clause a base query on `base` has to defer, and
    /// the cluster to run it on. Inline clauses need none: there the
    /// optimizer picks the selective side itself.
    fn seed(
        &self,
        state: &AppState,
        wiki: &str,
        base: DbCluster,
    ) -> Result<Option<(DbCluster, SQLtuple)>> {
        self.0
            .iter()
            .filter(|filter| !state.cluster_hosts_tables(wiki, base, filter.tables))
            .find_map(|filter| filter.seed.clone().map(|seed| (filter.tables, seed)))
            .map(|(tables, seed)| Ok((state.cluster_for_tables(wiki, tables)?, seed)))
            .transpose()
    }

    /// Splits the clauses into the ones a base query on `base` can carry and
    /// the ones that have to run on another cluster, grouped by cluster.
    fn split(
        self,
        state: &AppState,
        wiki: &str,
        base: DbCluster,
    ) -> Result<(SQLtuple, HashMap<DbCluster, SQLtuple>)> {
        let mut inline = super::sql_tuple();
        let mut deferred: HashMap<DbCluster, SQLtuple> = HashMap::new();
        for Filter { tables, sql, .. } in self.0 {
            if state.cluster_hosts_tables(wiki, base, tables) {
                super::append_sql(&mut inline, sql);
            } else {
                let cluster = state.cluster_for_tables(wiki, &[&["page"], tables].concat())?;
                super::append_sql(deferred.entry(cluster).or_default(), sql);
            }
        }
        Ok((inline, deferred))
    }
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
        // `page_props` holds the hidden-category flag but lives on the core
        // cluster, while `categorylinks` may not (Commons); where the two
        // cannot be joined, the filter becomes a second query below.
        let hiddencat_tables = &[helpers::SUBCATEGORIES_TABLES, &["page_props"]].concat();
        let filter_hidden_inline = self.params.skip_hidden_categories
            && state.cluster_for_tables(wiki, hiddencat_tables).is_ok();

        let sql = helpers::subcategories_query(
            categories,
            filter_hidden_inline,
            self.tracking_category_local.as_deref(),
        );
        let rows = state
            .get_wiki_db_connection_for_tables(wiki, helpers::SUBCATEGORIES_TABLES)
            .await?
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<(u32, Vec<u8>)>)
            .await
            .map_err(|e| anyhow!(e))?;
        let subcategories: Vec<(u32, String)> = rows
            .into_iter()
            .map(|(page_id, title)| (page_id, String::from_utf8_lossy(&title).into_owned()))
            .collect();

        if self.params.skip_hidden_categories && !filter_hidden_inline {
            return Self::remove_hidden_categories(state, wiki, subcategories).await;
        }
        Ok(subcategories.into_iter().map(|(_, title)| title).collect())
    }

    /// Drop the `__HIDDENCAT__` subcategories from `subcategories`.
    ///
    /// The equivalent of `subcategories_query`'s inline `page_props` anti-join,
    /// for wikis where `page_props` and `categorylinks` are on different hosts
    /// and so cannot be joined in one statement.
    async fn remove_hidden_categories(
        state: &AppState,
        wiki: &str,
        subcategories: Vec<(u32, String)>,
    ) -> Result<Vec<String>> {
        let page_ids: Vec<u32> = subcategories.iter().map(|(page_id, _)| *page_id).collect();
        if page_ids.is_empty() {
            return Ok(vec![]);
        }
        let sql = helpers::hidden_categories_query(&page_ids);
        let hidden: HashSet<u32> = state
            .get_wiki_db_connection_for_tables(wiki, &["page_props"])
            .await?
            .exec_iter(sql.as_str(), ())
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<u32>)
            .await
            .map_err(|e| anyhow!(e))?
            .into_iter()
            .collect();
        Ok(subcategories
            .into_iter()
            .filter(|(page_id, _)| !hidden.contains(page_id))
            .map(|(_, title)| title)
            .collect())
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
    ///
    /// A `seed` restricts the members considered to those page IDs, inside
    /// the `categorylinks` sub-select: that is where it makes the server
    /// probe the few seed pages' categories instead of every member of the
    /// batch's categories.
    fn category_batch_sql(
        &self,
        base_cluster: DbCluster,
        link_count_sql: &str,
        category_batch: &[Vec<String>],
        seed: Option<&[u32]>,
    ) -> SQLtuple {
        let seed = seed.map_or(String::new(), |ids| {
            format!("cl_from IN ({}) AND ", id_list(ids))
        });
        let subquery = format!(
            "SELECT cl_from,cl_target_id,lt_title from categorylinks,linktarget WHERE {seed}lt_id=cl_target_id AND lt_namespace=14 AND lt_title"
        );
        let mut sql = super::sql_tuple();
        match self.params.combine {
            CombineMode::Subset => {
                sql.0 = page_select_prefix(base_cluster);
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
                sql.0 = page_select_prefix(base_cluster);
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

    /// Runs each base query through [`Self::get_pages_for_primary`], a few
    /// at a time, and unions the results.
    async fn run_base_queries(
        &self,
        state: &AppState,
        params: &DsdbParams,
        queries: Vec<SQLtuple>,
    ) -> Result<PageList> {
        let ret = PageList::new_from_wiki(&params.wiki);
        let futures: Vec<_> = queries
            .into_iter()
            .map(|sql| self.get_pages_for_primary(state, params, sql))
            .collect();
        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;
        for pages in results {
            ret.union(&pages?, None).await?;
        }
        Ok(ret)
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
        drop(conn);

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

        let api = state.get_api_for_wiki(wiki.clone()).await?;
        let filters = self.collect_filters(primary, api);
        let base_cluster = filters.base_cluster(state, &wiki, &self.base_query_tables(primary));
        // A pagelist is already narrowed to its titles; a seed would only add
        // a query.
        let seed = match primary {
            Primary::Pagelist => None,
            _ => Self::fetch_seed(state, &wiki, &filters, base_cluster).await?,
        };

        Ok(DsdbParams {
            link_count_sql: link_count_sql.to_string(),
            wiki,
            primary,
            base_cluster,
            filters,
            seed,
        })
    }

    /// Fetches the page IDs the base query is restricted to, if a deferred
    /// filter offers a [`Filter::seed`] and it names at most
    /// [`MAX_SEED_PAGES`] of them. Beyond that the seed is the larger side
    /// and the query runs unseeded.
    async fn fetch_seed(
        state: &AppState,
        wiki: &str,
        filters: &Filters,
        base_cluster: DbCluster,
    ) -> Result<Option<Vec<u32>>> {
        let Some((cluster, mut sql)) = filters.seed(state, wiki, base_cluster)? else {
            return Ok(None);
        };
        sql.0 += &format!(" LIMIT {}", MAX_SEED_PAGES + 1);
        let mut conn = state
            .get_wiki_db_connection_for_cluster(wiki, cluster)
            .await?;
        let ids = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<u32>)
            .await
            .map_err(|e| anyhow!(e))?;
        Platform::profile("DSDB::fetch_seed", Some(ids.len()));
        Ok((ids.len() <= MAX_SEED_PAGES).then_some(ids))
    }

    /// The tables the base query joins itself, besides `page`: the primary
    /// source's, plus `pagelinks` when the link count is being gathered as a
    /// `SELECT` column.
    fn base_query_tables(&self, primary: Primary) -> Vec<&'static str> {
        let mut tables = match primary {
            // The only primary whose source is joined in the base query; the
            // others are `FROM page p` plus filter clauses.
            Primary::Categories => vec!["categorylinks", "linktarget"],
            Primary::Templates
            | Primary::LinksFrom
            | Primary::Pagelist
            | Primary::NoWikidata
            | Primary::CreatedBy => vec![],
        };
        if self.params.gather_link_count {
            tables.push("pagelinks");
        }
        tables
    }

    fn get_primary(&mut self, primary_pagelist: Option<&PageList>) -> Result<Primary> {
        let primary = if !self.cat_pos.is_empty() {
            Primary::Categories
        } else if self.has_pos_templates {
            Primary::Templates
        } else if self.has_pos_linked_from {
            Primary::LinksFrom
        } else if primary_pagelist.is_some() {
            Primary::Pagelist
        } else if self.params.page_wikidata_item == "without" {
            Primary::NoWikidata
        } else if !self.params.created_by.is_empty() {
            Primary::CreatedBy
        } else {
            return Err(anyhow!("SourceDatabase: Missing primary"));
        };
        Ok(primary)
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
        let queries: Vec<SQLtuple> = category_batches
            .iter()
            .flat_map(|category_batch| {
                params.seed_chunks().into_iter().map(|seed| {
                    self.category_batch_sql(
                        params.base_cluster,
                        &params.link_count_sql,
                        category_batch,
                        seed,
                    )
                })
            })
            .collect();
        let ret = self.run_base_queries(state, params, queries).await?;
        Platform::profile(
            "DSDB::get_pages [primary:categories] RESULTS end",
            Some(ret.len()),
        );
        Ok(ret)
    }

    async fn get_pages_pagelist(
        &mut self,
        params: DsdbParams,
        state: &AppState,
        primary_pagelist: Option<&PageList>,
    ) -> Result<PageList> {
        let primary_pagelist = primary_pagelist
            .ok_or_else(|| anyhow!("SourceDatabase::get_pages: pagelist: No primary_pagelist"))?;

        let nslist = primary_pagelist.group_by_namespace();
        let mut batches: Vec<SQLtuple> = vec![];
        nslist.iter().for_each(|nsgroup| {
            nsgroup.1.chunks(PAGE_BATCH_SIZE * 2).for_each(|titles| {
                let mut sql = super::sql_tuple();
                sql.0 = page_select_prefix(params.base_cluster);
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p";
                sql.0 += " WHERE (p.page_namespace=";
                sql.0 += &nsgroup.0.to_string();
                sql.0 += " AND p.page_title IN (";
                super::append_sql(&mut sql, super::prep_quote(titles));
                sql.0 += "))";
                batches.push(sql);
            });
        });

        let ret = self.run_base_queries(state, &params, batches).await?;
        ret.set_wiki(primary_pagelist.wiki());
        Ok(ret)
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
            .get_wiki_db_connection_for_tables(wiki, helpers::CATEGORY_MEMBERS_TABLES)
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
        let params = self
            .get_pages_initialize_query(state, primary_pagelist)
            .await?;

        match params.primary {
            Primary::Categories => {
                return self.get_pages_categories(&params, state).await;
            }
            Primary::Pagelist => {
                return self
                    .get_pages_pagelist(params, state, primary_pagelist)
                    .await;
            }
            // Every other primary selects from `page` alone; what narrows it
            // down is a filter clause. `NoWikidata` in particular is just the
            // "without a Wikidata item" filter, added by
            // `get_pages_for_primary_wikidata_item`.
            Primary::Templates | Primary::LinksFrom | Primary::CreatedBy | Primary::NoWikidata => {}
        }

        let queries: Vec<SQLtuple> = params
            .seed_chunks()
            .into_iter()
            .map(|seed| {
                let mut sql = super::sql_tuple();
                sql.0 = page_select_prefix(params.base_cluster);
                sql.0 += &params.link_count_sql;
                sql.0 += " FROM page p WHERE 1=1";
                if let Some(ids) = seed {
                    sql.0 += &format!(" AND p.page_id IN ({})", id_list(ids));
                }
                sql
            })
            .collect();
        self.run_base_queries(state, &params, queries).await
    }

    /// Collects every filter clause the query's parameters ask for.
    ///
    /// Negative categories are *not* here: they are applied after the primary
    /// query as an in-memory set difference (see
    /// `subtract_negative_categories`), because a deep excluded-category tree
    /// can expand to more titles than MySQL's 65 535 placeholder limit allows
    /// in one statement (#206).
    fn collect_filters(&self, primary: Primary, api: Api) -> Filters {
        let mut filters = Filters::default();
        self.get_pages_for_primary_namespaces(primary, &mut filters);
        self.get_pages_for_primary_templates_as_secondary(&mut filters);
        self.get_pages_for_primary_negative_templates(&mut filters);
        self.get_pages_for_primary_links_from(&mut filters, &api);
        self.get_pages_for_primary_links_to(&mut filters, api);
        self.get_pages_for_primary_lead_image(&mut filters);
        self.get_pages_for_primary_ores(&mut filters);
        self.get_pages_for_primary_last_edit(&mut filters);
        self.get_pages_for_primary_created_by(&mut filters);
        self.get_pages_for_primary_page_types(&mut filters);
        self.get_pages_for_primary_page_size(&mut filters);
        self.get_pages_for_primary_wikidata_item(&mut filters);
        self.get_pages_for_primary_last_edited(&mut filters);
        filters
    }

    /// Runs one base query on its cluster, then the deferred filters, and
    /// returns the surviving pages.
    async fn get_pages_for_primary(
        &self,
        state: &AppState,
        params: &DsdbParams,
        mut sql: SQLtuple,
    ) -> Result<PageList> {
        let base_cluster = params.base_cluster;
        Platform::profile("DSDB::get_pages_for_primary STARTING", Some(sql.1.len()));

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

        let (inline, deferred) = params.filters.clone().split(state, &wiki, base_cluster)?;
        super::append_sql(&mut sql, inline);
        self.get_pages_for_primary_having(&mut sql);

        let sql_1_len = sql.1.len();
        let mut conn = state
            .get_wiki_db_connection_for_cluster(&wiki, base_cluster)
            .await?;
        let rows = self.get_pages_for_primary_run_query(sql, &mut conn).await?;
        drop(conn);
        Platform::profile("DSDB::get_pages_for_primary RUN FINISHED", Some(sql_1_len));

        let rows = Self::apply_deferred_filters(state, &wiki, base_cluster, deferred, rows).await?;

        Platform::profile(
            "DSDB::get_pages_for_primary RETRIEVING RESULT",
            Some(sql_1_len),
        );
        let mut pages = PageList::new_from_wiki(&wiki);
        self.get_pages_for_primary_rows_to_result(rows, &mut pages);
        Platform::profile("DSDB::get_pages_for_primary COMPLETE", Some(sql_1_len));
        Ok(pages)
    }

    /// Applies the filter clauses the base query's cluster could not serve,
    /// and fills in `page_touched` where the base query could not select it.
    ///
    /// One extra query per cluster involved — none at all off Commons —
    /// selecting from `page` the IDs among `rows` that satisfy that cluster's
    /// clauses; rows whose ID is missing from the answer are dropped. The
    /// core-cluster pass doubles as the `page_touched` lookup, since that
    /// value comes from `revision`, which only the core cluster has.
    async fn apply_deferred_filters(
        state: &AppState,
        wiki: &str,
        base_cluster: DbCluster,
        mut deferred: HashMap<DbCluster, SQLtuple>,
        mut rows: Vec<PrimaryResultRow>,
    ) -> Result<Vec<PrimaryResultRow>> {
        // `page_touched` comes from `revision`; a base query that did not run
        // on the core cluster left the column empty, so it needs a pass there
        // even with no clauses to apply.
        let fetch_page_touched = base_cluster != DbCluster::Core;
        if fetch_page_touched {
            deferred.entry(DbCluster::Core).or_default();
        }
        if deferred.is_empty() || rows.is_empty() {
            return Ok(rows);
        }

        for (cluster, clauses) in deferred {
            let want_page_touched = fetch_page_touched && cluster == DbCluster::Core;
            let page_ids: Vec<u32> = rows.iter().map(|row| row.0).collect();
            let batches: Vec<SQLtuple> = page_ids
                .chunks(PAGE_BATCH_SIZE)
                .map(|chunk| {
                    let mut sql = sql_text(&format!(
                        "SELECT p.page_id,{page_touched} FROM page p WHERE p.page_id IN ({ids})",
                        page_touched = if want_page_touched {
                            PAGE_TOUCHED_FROM_REVISION
                        } else {
                            "'' AS page_touched"
                        },
                        ids = id_list(chunk),
                    ));
                    super::append_sql(&mut sql, clauses.clone());
                    sql
                })
                .collect();

            let futures: Vec<_> = batches
                .into_iter()
                .map(|sql| Self::run_deferred_filter_batch(state, wiki, cluster, sql))
                .collect();
            let results: Vec<_> = iter(futures)
                .buffered(MAX_CONCURRENT_DB_BATCHES)
                .collect()
                .await;

            let mut kept: HashMap<u32, Vec<u8>> = HashMap::new();
            for result in results {
                kept.extend(result?);
            }
            rows.retain(|row| kept.contains_key(&row.0));
            if want_page_touched {
                for row in &mut rows {
                    if let Some(page_touched) = kept.get(&row.0) {
                        row.3 = page_touched.clone();
                    }
                }
            }
            if rows.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    /// Runs one batch of [`Self::apply_deferred_filters`], returning the
    /// surviving page IDs and their `page_touched`.
    async fn run_deferred_filter_batch(
        state: &AppState,
        wiki: &str,
        cluster: DbCluster,
        sql: SQLtuple,
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        debug_assert!(
            sql.placeholders_balanced(),
            "unbalanced placeholders: {}",
            sql.0
        );
        let mut conn = state
            .get_wiki_db_connection_for_cluster(wiki, cluster)
            .await?;
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<(u32, Vec<u8>)>)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(rows)
    }

    /// "Only pages without Wikidata items". Also carries
    /// [`Primary::NoWikidata`], whose only condition this is: as a filter
    /// clause it can be deferred to the core cluster where `page_props` is
    /// not joinable with the rest of the query.
    fn get_pages_for_primary_wikidata_item(&self, filters: &mut Filters) {
        if self.params.page_wikidata_item == "without" {
            filters.push(PAGE_PROPS, sql_text(" AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item')"));
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

    fn get_pages_for_primary_having(&self, sql: &mut SQLtuple) {
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

    fn get_pages_for_primary_page_size(&self, filters: &mut Filters) {
        // Size
        if let Some(i) = self.params.larger {
            filters.push_page_only(sql_text(&format!(" AND p.page_len>={i}")));
        }
        if let Some(i) = self.params.smaller {
            filters.push_page_only(sql_text(&format!(" AND p.page_len<={i}")));
        }
        if let Some(i) = self.params.since_rev0 {
            filters.push(
                REVISION,
                sql_text(&format!(
                    " AND p.page_len<=(SELECT rev_len FROM revision WHERE rev_page=p.page_id AND rev_parent_id=0 LIMIT 1)*{i}/100"
                )),
            );
        }
    }

    fn get_pages_for_primary_page_types(&self, filters: &mut Filters) {
        // Misc page types
        // TODO FIXME get local "Soft_redirect" page title from Wikidata Q4844001
        let soft_redirects_page = "Soft_redirect";
        if "yes" == self.params.soft_redirects.as_str() {
            filters.push(
                TEMPLATELINKS,
                SQLtuple(
                    " AND EXISTS (SELECT * FROM templatelinks,linktarget WHERE tl_from=p.page_id AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=?)".to_string(),
                    vec![MyValue::Bytes(soft_redirects_page.into())],
                ),
            );
        }
        match self.params.redirects.as_str() {
            "yes" => filters.push_page_only(sql_text(" AND p.page_is_redirect=1")),
            "no" => filters.push_page_only(sql_text(" AND p.page_is_redirect=0")),
            _ => {}
        }
        match self.params.disambiguation_pages.as_str() {
            "yes" => filters.push(PAGE_PROPS, sql_text(" AND EXISTS (SELECT * FROM page_props WHERE pp_page=p.page_id AND pp_propname='disambiguation')")),
            "no" => filters.push(PAGE_PROPS, sql_text(" AND NOT EXISTS (SELECT * FROM page_props WHERE pp_page=p.page_id AND pp_propname='disambiguation')")),
            _ => {}
        }
        match self.params.talk_page_exists.as_str() {
            "yes" => filters.push_page_only(sql_text(" AND EXISTS (SELECT * FROM page talk_p WHERE talk_p.page_title=p.page_title AND talk_p.page_namespace=p.page_namespace+1)")),
            "no" => filters.push_page_only(sql_text(" AND NOT EXISTS (SELECT * FROM page talk_p WHERE talk_p.page_title=p.page_title AND talk_p.page_namespace=p.page_namespace+1)")),
            _ => {}
        }
    }

    fn get_pages_for_primary_last_edit(&self, filters: &mut Filters) {
        // Last edit
        match self.params.last_edit_anon.as_str() {
            "yes" => filters.push(REVISION_ACTOR, sql_text(" AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user IS NULL)")),
            "no" => filters.push(REVISION_ACTOR, sql_text(" AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user IS NOT NULL)")),
            _ => {}
        }
        match self.params.last_edit_bot.as_str() {
            "yes" => filters.push(REVISION_USER_GROUPS, sql_text(" AND EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')")),
            "no" => filters.push(REVISION_USER_GROUPS, sql_text(" AND NOT EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')")),
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
            "yes" => filters.push(FLAGGEDPAGES, sql_text(" AND NOT EXISTS (SELECT * FROM flaggedpages WHERE fp_pending_since IS NOT NULL AND fp_page_id=p.page_id)")),
            "no" => filters.push(FLAGGEDPAGES, sql_text(" AND EXISTS (SELECT * FROM flaggedpages WHERE fp_pending_since IS NOT NULL AND fp_page_id=p.page_id)")),
            _ => {}
        }
    }

    fn get_pages_for_primary_created_by(&self, filters: &mut Filters) {
        if self.params.created_by.is_empty() {
            return;
        }
        // The pages a user created are few and indexed by actor, so the
        // sub-select doubles as the clause's seed.
        let mut creations = sql_text(
            "SELECT r.rev_page FROM revision r INNER JOIN actor a ON r.rev_actor=a.actor_id WHERE r.rev_parent_id=0 AND a.actor_name IN (",
        );
        super::append_sql(&mut creations, super::prep_quote(&self.params.created_by));
        creations.0 += ")";
        let mut sql = sql_text(" AND p.page_id IN (");
        super::append_sql(&mut sql, creations.clone());
        sql.0 += ")";
        filters.push_seeded(REVISION_ACTOR, sql, Some(creations));
    }

    fn get_pages_for_primary_ores(&self, filters: &mut Filters) {
        // ORES
        if self.params.ores_type == "any"
            || (self.params.ores_prediction == "any"
                && self.params.ores_prob_from.is_none()
                && self.params.ores_prob_to.is_none())
        {
            return;
        }
        let mut sql = SQLtuple(
            " AND EXISTS (SELECT * FROM ores_classification WHERE p.page_latest=oresc_rev AND oresc_model IN (SELECT oresm_id FROM ores_model WHERE oresm_is_current=1 AND oresm_name=?)".to_string(),
            vec![MyValue::Bytes(self.params.ores_type.to_owned().into())],
        );
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
        filters.push(ORES, sql);
    }

    fn get_pages_for_primary_lead_image(&self, filters: &mut Filters) {
        // Lead image
        let clause = match self.params.page_image.as_str() {
            "yes" => {
                " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))"
            }
            "free" => {
                " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image_free')"
            }
            "nonfree" => {
                " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image')"
            }
            "no" => {
                " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))"
            }
            _ => return,
        };
        filters.push(PAGE_PROPS, sql_text(clause));
    }

    fn get_pages_for_primary_links_to(&self, filters: &mut Filters, api: Api) {
        let mut push = |prefix: &str, titles: &[String]| {
            let mut sql = sql_text(prefix);
            super::append_sql(&mut sql, helpers::links_to_subquery(titles, &api));
            filters.push(PAGELINKS, sql);
        };

        // Links to all
        for l in &self.params.links_to_all {
            push(" AND p.page_id IN ", &[l.to_owned()]);
        }

        // Links to any
        if !self.params.links_to_any.is_empty() {
            push(" AND p.page_id IN ", &self.params.links_to_any);
        }

        // Links to none
        if !self.params.links_to_none.is_empty() {
            push(" AND p.page_id NOT IN ", &self.params.links_to_none);
        }
    }

    fn get_pages_for_primary_links_from(&self, filters: &mut Filters, api: &Api) {
        let mut push = |prefix: &str, titles: &[String]| {
            let mut sql = sql_text(prefix);
            super::append_sql(&mut sql, helpers::links_from_subquery(titles, api));
            filters.push(PAGELINKS, sql);
        };

        // Links from all
        for l in &self.params.linked_from_all {
            push(" AND p.page_id IN ", &[l.to_owned()]);
        }

        // Links from any
        if !self.params.linked_from_any.is_empty() {
            push(" AND p.page_id IN ", &self.params.linked_from_any);
        }

        // Links from none
        if !self.params.linked_from_none.is_empty() {
            push(" AND p.page_id NOT IN ", &self.params.linked_from_none);
        }
    }

    fn get_pages_for_primary_negative_templates(&self, filters: &mut Filters) {
        // Negative templates
        if !self.params.templates_no.is_empty() {
            filters.push(
                TEMPLATELINKS,
                self.template_subquery(
                    &self.params.templates_no,
                    self.params.templates_no_talk_page,
                    true,
                ),
            );
        }
    }

    /// Templates as secondary; template namespace only!
    fn get_pages_for_primary_templates_as_secondary(&self, filters: &mut Filters) {
        if !self.has_pos_templates {
            return;
        }
        // All
        for t in &self.params.templates_yes {
            filters.push(
                TEMPLATELINKS,
                self.template_subquery(
                    &[t.to_string()],
                    self.params.templates_yes_talk_page,
                    false,
                ),
            );
        }

        // Any
        if !self.params.templates_any.is_empty() {
            filters.push(
                TEMPLATELINKS,
                self.template_subquery(
                    &self.params.templates_any,
                    self.params.templates_any_talk_page,
                    false,
                ),
            );
        }
    }

    fn get_pages_for_primary_namespaces(&self, primary: Primary, filters: &mut Filters) {
        if self.params.namespace_ids.is_empty() || primary == Primary::Pagelist {
            return;
        }
        let namespace_ids = &self
            .params
            .namespace_ids
            .iter()
            .map(|ns| ns.to_string())
            .collect::<Vec<String>>();
        let mut sql = sql_text(" AND p.page_namespace");
        helpers::sql_in(namespace_ids, &mut sql);
        filters.push_page_only(sql);
    }

    /// "Last edited (or created) before/after", as an `EXISTS` over
    /// `revision`.
    ///
    /// Was an `INNER JOIN (revision r)` fragment threaded through the query
    /// builders with an "already appended?" flag, because the category
    /// primary's SQL has no `WHERE` to hang a condition off. `EXISTS` is
    /// equivalent under the query's `SELECT DISTINCT`, needs no such
    /// bookkeeping, and — being an ordinary clause — can be deferred to the
    /// cluster that has `revision`.
    fn get_pages_for_primary_last_edited(&self, filters: &mut Filters) {
        // `max_age` is a relative form of `after`, and overrides both.
        let (before, after) = match self.params.max_age {
            Some(max_age) => {
                let utc = Utc::now().sub(Duration::try_hours(max_age).unwrap_or_default());
                (String::new(), utc.format("%Y%m%d%H%M%S").to_string())
            }
            None => (self.params.before.clone(), self.params.after.clone()),
        };
        if before.is_empty() && after.is_empty() {
            return;
        }

        let mut sql = sql_text(" AND EXISTS (SELECT 1 FROM revision r WHERE r.rev_page=p.page_id");
        if self.params.only_new_since {
            sql.0 += " AND r.rev_parent_id=0";
        } else {
            sql.0 += " AND r.rev_id=p.page_latest";
        }
        if !before.is_empty() {
            sql.0 += " AND r.rev_timestamp<=?";
            sql.1.push(MyValue::Bytes(before.clone().into()));
        }
        if !after.is_empty() {
            sql.0 += " AND r.rev_timestamp>=?";
            sql.1.push(MyValue::Bytes(after.clone().into()));
        }
        sql.0 += ")";

        // A lower bound on the timestamp is selective enough to drive a
        // query off `revision`'s timestamp index; an upper bound alone
        // matches nearly everything. Pages with any revision in the window
        // are a superset of the clause's matches, which is all a seed needs.
        let seed = (!after.is_empty()).then(|| {
            let mut seed =
                sql_text("SELECT DISTINCT rev_page FROM revision WHERE rev_timestamp>=?");
            seed.1.push(MyValue::Bytes(after.into()));
            if !before.is_empty() {
                seed.0 += " AND rev_timestamp<=?";
                seed.1.push(MyValue::Bytes(before.into()));
            }
            if self.params.only_new_since {
                seed.0 += " AND rev_parent_id=0";
            }
            seed
        });
        filters.push_seeded(REVISION, sql, seed);
    }

    async fn get_pages_for_primary_run_query(
        &self,
        sql: SQLtuple,
        conn: &mut my::Conn,
    ) -> Result<Vec<PrimaryResultRow>> {
        debug_assert!(
            sql.placeholders_balanced(),
            "unbalanced placeholders: {}",
            sql.0
        );
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

    /// The Commons links split in one query: `categorylinks` on the links
    /// cluster, `revision` (the `before` filter *and* `page_touched`) on the
    /// core one. Nothing but the split machinery can make this return rows.
    #[tokio::test]
    #[ignore = "requires live MySQL replica + config.json; run with --ignored"]
    async fn test_commons_category_query_spans_clusters() {
        let commons_category = vec![
            ("categories", "Cambridge"),
            ("language", "commons"),
            ("project", "wikimedia"),
            ("ns[14]", "1"),
        ];
        let unfiltered = simulate_category_query(commons_category.clone())
            .await
            .unwrap();
        assert_eq!(unfiltered.wiki(), Some("commonswiki".to_string()));
        assert!(!unfiltered.is_empty());
        // `page_touched` comes from `revision`, which the links cluster does
        // not have, so a populated timestamp proves the deferred core pass ran.
        assert!(
            unfiltered
                .as_vec()
                .iter()
                .all(|entry| entry.get_page_timestamp().is_some_and(|ts| ts.len() == 14)),
            "every row needs a page_touched from the core cluster"
        );

        // Add a filter that can only run on the core cluster.
        let mut filtered_params = commons_category;
        filtered_params.push(("before", "20100101000000"));
        let filtered = simulate_category_query(filtered_params).await.unwrap();
        assert!(filtered.len() < unfiltered.len(), "the filter must bite");
        let unfiltered_entries = unfiltered.as_vec();
        let unfiltered_titles: HashSet<&str> = unfiltered_entries
            .iter()
            .map(|entry| entry.title().pretty())
            .collect();
        for entry in filtered.as_vec() {
            assert!(
                unfiltered_titles.contains(entry.title().pretty()),
                "filtering must only remove rows, not invent them: {:?}",
                entry.title()
            );
            let timestamp = entry.get_page_timestamp().unwrap_or_default();
            assert!(timestamp.as_str() <= "20100101000000", "got {timestamp}");
        }
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
    async fn test_skip_filters_remove_tracking_subcategories() {
        // `Category:Automatic category TOC tracking categories` on enwiki
        // directly contains subcategories that are themselves members of
        // `Category:Tracking categories` (e.g. "…generates no TOC") and are
        // flagged __HIDDENCAT__. A depth-1 traversal picks them up; each
        // filter must drop them, shrinking the tree below the unfiltered size.
        let state = get_state().await;
        let tracking = SourceDatabase::resolve_tracking_category(&state, "enwiki")
            .await
            .unwrap();
        let tree = |skip_tracking: bool, skip_hidden: bool| {
            let state = state.clone();
            let tracking = tracking.clone();
            async move {
                let mut params = SourceDatabaseParameters::new();
                params.set_wiki(Some("enwiki".to_string()));
                params.skip_tracking_categories = skip_tracking;
                params.skip_hidden_categories = skip_hidden;
                let mut db = SourceDatabase::new(params);
                if skip_tracking {
                    db.tracking_category_local = Some(tracking);
                }
                db.get_categories_in_tree(
                    &state,
                    "enwiki",
                    "Automatic_category_TOC_tracking_categories",
                    1,
                )
                .await
                .unwrap()
                .len()
            }
        };

        let unfiltered = tree(false, false).await;
        let skip_tracking = tree(true, false).await;
        let skip_hidden = tree(false, true).await;

        // The root is always retained, so an effective filter still leaves >= 1.
        assert!(
            unfiltered > skip_tracking,
            "tracking filter removed nothing: {unfiltered} vs {skip_tracking}"
        );
        assert!(
            unfiltered > skip_hidden,
            "hidden filter removed nothing: {unfiltered} vs {skip_hidden}"
        );
        assert!(skip_tracking >= 1, "root category must survive filtering");
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
        assert!(
            has_magnus(&without_negcats),
            "baseline should include Magnus"
        );

        let with_negcats = simulate_category_query(vec![
            ("categories", "1974_births"),
            ("negcats", "German bioinformaticians"),
            ("language", "en"),
            ("project", "wikipedia"),
        ])
        .await
        .unwrap();
        assert!(
            !has_magnus(&with_negcats),
            "excluded member must be removed"
        );
        assert!(
            with_negcats.len() < without_negcats.len(),
            "exclusion must shrink the result"
        );
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

    /// Run one clause builder and return the concatenated SQL it produced
    /// plus the number of bound values — i.e. what the base query gets when
    /// nothing needs deferring. Every complete fragment must have one bound
    /// value per `?` placeholder.
    fn built(apply: impl FnOnce(&mut Filters)) -> (String, usize) {
        let mut filters = Filters::default();
        apply(&mut filters);
        let mut sql = crate::datasource::sql_tuple();
        for filter in filters.0 {
            crate::datasource::append_sql(&mut sql, filter.sql);
        }
        assert!(
            sql.placeholders_balanced(),
            "unbalanced placeholders: {}",
            sql.0
        );
        (sql.0, sql.1.len())
    }

    /// Like [`built`], for the builders that still append straight to the
    /// query text rather than emitting a routable clause (`HAVING`).
    fn built_sql(apply: impl FnOnce(&mut SQLtuple)) -> (String, usize) {
        let mut sql = crate::datasource::sql_tuple();
        apply(&mut sql);
        assert!(
            sql.placeholders_balanced(),
            "unbalanced placeholders: {}",
            sql.0
        );
        (sql.0, sql.1.len())
    }

    /// The tables the clauses of a builder read, in the order pushed.
    fn built_tables(apply: impl FnOnce(&mut Filters)) -> Vec<Vec<&'static str>> {
        let mut filters = Filters::default();
        apply(&mut filters);
        filters.0.into_iter().map(|f| f.tables.to_vec()).collect()
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
        let (sql, n) = built(|s| db.get_pages_for_primary_namespaces(Primary::Categories, s));
        assert_eq!(sql, " AND p.page_namespace IN (?,?)");
        assert_eq!(n, 2);
    }

    #[test]
    fn sql_namespaces_single_uses_equality() {
        let db = snapshot_db(|p| p.namespace_ids = vec![0]);
        let (sql, n) = built(|s| db.get_pages_for_primary_namespaces(Primary::Categories, s));
        assert_eq!(sql, " AND p.page_namespace=?");
        assert_eq!(n, 1);
    }

    #[test]
    fn sql_namespaces_skipped_for_pagelist_primary() {
        let db = snapshot_db(|p| p.namespace_ids = vec![0]);
        let (sql, n) = built(|s| db.get_pages_for_primary_namespaces(Primary::Pagelist, s));
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
            " AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user IS NULL) \
             AND NOT EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot') \
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
            " AND p.page_len>=1000 AND p.page_len<=5000 AND p.page_len<=(SELECT rev_len FROM revision WHERE rev_page=p.page_id AND rev_parent_id=0 LIMIT 1)*150/100"
        );
        assert_eq!(n, 0);
        // Only the `since_rev0` clause reads `revision`, so only it has to
        // move to another cluster where `page` and `revision` are apart.
        assert_eq!(
            built_tables(|s| db.get_pages_for_primary_page_size(s)),
            vec![vec![], vec![], vec!["revision"]]
        );
    }

    #[test]
    fn sql_wikidata_item_clause_for_every_primary() {
        let db = snapshot_db(|p| p.page_wikidata_item = "without".to_string());
        let (sql, _) = built(|s| db.get_pages_for_primary_wikidata_item(s));
        assert_eq!(
            sql,
            " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item')"
        );
        // Also the sole condition of the `NoWikidata` primary, whose base
        // query is now `FROM page p` alone.
        assert_eq!(
            built_tables(|s| db.get_pages_for_primary_wikidata_item(s)),
            vec![vec!["page_props"]]
        );

        let db_off = snapshot_db(|p| p.page_wikidata_item = "any".to_string());
        let (sql_skipped, _) = built(|s| db_off.get_pages_for_primary_wikidata_item(s));
        assert_eq!(sql_skipped, "");
    }

    #[test]
    fn sql_last_edited_before_and_after() {
        let db = snapshot_db(|p| {
            p.before = "20240101000000".to_string();
            p.after = "20230101000000".to_string();
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_last_edited(s));
        assert_eq!(
            sql,
            " AND EXISTS (SELECT 1 FROM revision r WHERE r.rev_page=p.page_id AND r.rev_id=p.page_latest AND r.rev_timestamp<=? AND r.rev_timestamp>=?)"
        );
        assert_eq!(n, 2);
        assert_eq!(
            built_tables(|s| db.get_pages_for_primary_last_edited(s)),
            vec![vec!["revision"]]
        );
    }

    #[test]
    fn sql_last_edited_only_new_since_matches_creation() {
        let db = snapshot_db(|p| {
            p.after = "20230101000000".to_string();
            p.only_new_since = true;
        });
        let (sql, n) = built(|s| db.get_pages_for_primary_last_edited(s));
        assert_eq!(
            sql,
            " AND EXISTS (SELECT 1 FROM revision r WHERE r.rev_page=p.page_id AND r.rev_parent_id=0 AND r.rev_timestamp>=?)"
        );
        assert_eq!(n, 1);
    }

    /// The seed a builder attached to its clause, as SQL text and value
    /// count.
    fn built_seed(apply: impl FnOnce(&mut Filters)) -> Option<(String, usize)> {
        let mut filters = Filters::default();
        apply(&mut filters);
        assert_eq!(filters.0.len(), 1);
        let seed = filters.0.remove(0).seed?;
        assert!(seed.placeholders_balanced(), "unbalanced: {}", seed.0);
        Some((seed.0, seed.1.len()))
    }

    #[test]
    fn seed_last_edited_needs_a_lower_bound() {
        // "Created since" seeds off the timestamp index, creations only ...
        let db = snapshot_db(|p| {
            p.after = "20230101000000".to_string();
            p.only_new_since = true;
        });
        assert_eq!(
            built_seed(|s| db.get_pages_for_primary_last_edited(s)),
            Some((
                "SELECT DISTINCT rev_page FROM revision WHERE rev_timestamp>=? AND rev_parent_id=0"
                    .to_string(),
                1
            ))
        );
        // ... "edited within" takes any revision in the window, a superset
        // of the pages whose *latest* revision is in it ...
        let edited_within = snapshot_db(|p| {
            p.before = "20240101000000".to_string();
            p.after = "20230101000000".to_string();
        });
        assert_eq!(
            built_seed(|s| edited_within.get_pages_for_primary_last_edited(s)),
            Some((
                "SELECT DISTINCT rev_page FROM revision WHERE rev_timestamp>=? AND rev_timestamp<=?"
                    .to_string(),
                2
            ))
        );
        // ... and an upper bound alone matches nearly every page, so it is
        // no seed at all.
        let before_only = snapshot_db(|p| p.before = "20240101000000".to_string());
        assert_eq!(
            built_seed(|s| before_only.get_pages_for_primary_last_edited(s)),
            None
        );
    }

    #[test]
    fn seed_created_by_is_the_clause_sub_select() {
        let db = snapshot_db(|p| p.created_by = vec!["Alice".to_string()]);
        assert_eq!(
            built_seed(|s| db.get_pages_for_primary_created_by(s)),
            Some((
                "SELECT r.rev_page FROM revision r INNER JOIN actor a ON r.rev_actor=a.actor_id WHERE r.rev_parent_id=0 AND a.actor_name IN (?)"
                    .to_string(),
                1
            ))
        );
    }

    #[test]
    fn sql_last_edited_absent_without_before_or_after() {
        let db = snapshot_db(|_| {});
        let (sql, n) = built(|s| db.get_pages_for_primary_last_edited(s));
        assert_eq!(sql, "");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_having_minlinks_only() {
        let db = snapshot_db(|p| p.minlinks = Some(5));
        let (sql, n) = built_sql(|s| db.get_pages_for_primary_having(s));
        assert_eq!(sql, " HAVING link_count>=5");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_having_maxlinks_only() {
        let db = snapshot_db(|p| p.maxlinks = Some(10));
        let (sql, n) = built_sql(|s| db.get_pages_for_primary_having(s));
        assert_eq!(sql, " HAVING link_count<=10");
        assert_eq!(n, 0);
    }

    #[test]
    fn sql_having_min_and_max_links_are_separated() {
        let db = snapshot_db(|p| {
            p.minlinks = Some(5);
            p.maxlinks = Some(10);
        });
        let (sql, n) = built_sql(|s| db.get_pages_for_primary_having(s));
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
        let sql = db.category_batch_sql(DbCluster::Core, ",0 AS link_count", &batch, None);
        assert_eq!(
            norm(&sql.0),
            "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,\
             (SELECT rev_timestamp FROM revision WHERE rev_id=p.page_latest LIMIT 1) AS page_touched,\
             p.page_len,0 AS link_count \
             FROM ( SELECT cl_from,cl_target_id,lt_title from categorylinks,linktarget WHERE lt_id=cl_target_id AND lt_namespace=14 AND lt_title IN (?)) cl0 \
             INNER JOIN categorylinks cl1 ON cl0.cl_from=cl1.cl_from \
             INNER JOIN linktarget lt1 ON lt1.lt_namespace=14 AND lt1.lt_id=cl1.cl_target_id AND lt1.lt_title IN (?,?) \
             INNER JOIN (page p) ON p.page_id=cl0.cl_from"
        );
        assert_eq!(sql.1.len(), 3);
    }

    #[test]
    fn sql_category_batch_seed_restricts_the_members_sub_select() {
        let db = snapshot_db(|p| p.combine = CombineMode::Subset);
        let batch = vec![vec!["Births_1974".to_string()]];
        let sql = db.category_batch_sql(DbCluster::Links, "", &batch, Some(&[7, 42]));
        // Inside the sub-select, ahead of the title list: the server then
        // probes the seed pages' categories rather than every member of the
        // batch's categories.
        assert!(
            sql.0.contains(
                "from categorylinks,linktarget WHERE cl_from IN (7,42) AND lt_id=cl_target_id"
            ),
            "got: {}",
            sql.0
        );
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn sql_category_batch_union_merges_and_dedups() {
        let db = snapshot_db(|p| p.combine = CombineMode::Union);
        let batch = vec![
            vec!["Chemistry".to_string(), "Biology".to_string()],
            vec!["Biology".to_string()],
        ];
        let sql = db.category_batch_sql(DbCluster::Core, ",0 AS link_count", &batch, None);
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

    /// A many-filter query, built through every clause builder
    /// `collect_filters` calls except the two `Api`-dependent links ones.
    fn kitchen_sink_filters() -> Filters {
        let mut db = snapshot_db(|p| {
            p.namespace_ids = vec![0];
            p.templates_no = vec!["Stub".to_string()];
            p.page_image = "free".to_string();
            p.last_edit_anon = "no".to_string();
            p.redirects = "no".to_string();
            p.larger = Some(100);
            p.page_wikidata_item = "without".to_string();
            p.before = "20240101000000".to_string();
        });
        db.has_pos_templates = false;
        let mut filters = Filters::default();
        db.get_pages_for_primary_namespaces(Primary::Categories, &mut filters);
        db.get_pages_for_primary_templates_as_secondary(&mut filters);
        db.get_pages_for_primary_negative_templates(&mut filters);
        // links_from / links_to skipped: require a live Api
        db.get_pages_for_primary_lead_image(&mut filters);
        db.get_pages_for_primary_ores(&mut filters);
        db.get_pages_for_primary_last_edit(&mut filters);
        db.get_pages_for_primary_created_by(&mut filters);
        db.get_pages_for_primary_page_types(&mut filters);
        db.get_pages_for_primary_page_size(&mut filters);
        db.get_pages_for_primary_wikidata_item(&mut filters);
        db.get_pages_for_primary_last_edited(&mut filters);
        filters
    }

    /// Pin the clause order `collect_filters` produces, so a future
    /// reordering shows up as a deliberate snapshot change.
    #[test]
    fn sql_filter_sequence_kitchen_sink() {
        let (sql, n) = built(|f| *f = kitchen_sink_filters());
        assert_eq!(
            sql,
            " AND p.page_namespace=? \
             AND p.page_id NOT IN (SELECT DISTINCT tl_from FROM templatelinks,linktarget WHERE p.page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=? AND tl_from_namespace=?) \
             AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image_free') \
             AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=p.page_latest AND rev_page=p.page_id AND rev_actor=actor_id AND actor_user IS NOT NULL) \
             AND p.page_is_redirect=0 \
             AND p.page_len>=100 \
             AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='wikibase_item') \
             AND EXISTS (SELECT 1 FROM revision r WHERE r.rev_page=p.page_id AND r.rev_id=p.page_latest AND r.rev_timestamp<=?)"
        );
        assert_eq!(n, 4);
    }

    // ─── Cluster routing of the filter clauses ──────────────────────────────

    #[test]
    fn filters_all_inline_off_commons() {
        let state = AppState::default();
        let filters = kitchen_sink_filters();
        let expected = built(|f| *f = kitchen_sink_filters());
        // Off Commons every table is on the core cluster, so nothing moves:
        // one query, byte-for-byte the pre-split one.
        assert_eq!(
            filters.base_cluster(&state, "enwiki", &["categorylinks", "linktarget"]),
            DbCluster::Core
        );
        let (inline, deferred) = kitchen_sink_filters()
            .split(&state, "enwiki", DbCluster::Core)
            .unwrap();
        assert!(deferred.is_empty());
        assert_eq!((inline.0, inline.1.len()), expected);
    }

    #[test]
    fn filters_split_across_commons_clusters() {
        let state = AppState::default();
        // A category query reads `categorylinks`, so the base query belongs
        // on the links cluster ...
        let base = kitchen_sink_filters().base_cluster(
            &state,
            "commonswiki",
            &["categorylinks", "linktarget"],
        );
        assert_eq!(base, DbCluster::Links);

        let (inline, deferred) = kitchen_sink_filters()
            .split(&state, "commonswiki", base)
            .unwrap();
        // ... where the page-only and templatelinks clauses can run too ...
        assert!(inline.0.contains("p.page_namespace=?"));
        assert!(inline.0.contains("FROM templatelinks,linktarget"));
        assert!(inline.0.contains("p.page_is_redirect=0"));
        assert!(inline.0.contains("p.page_len>=100"));
        // ... while everything reading page_props / revision / actor moves to
        // the core cluster.
        assert_eq!(deferred.len(), 1);
        let core = &deferred[&DbCluster::Core];
        assert!(core.0.contains("pp_propname='page_image_free'"));
        assert!(core.0.contains("pp_propname='wikibase_item'"));
        assert!(core.0.contains("FROM revision,actor"));
        assert!(core.0.contains("r.rev_timestamp<=?"));
        assert!(!core.0.contains("templatelinks"));
        // No clause is lost or duplicated, and each query's placeholders
        // still match its bound values.
        assert!(inline.placeholders_balanced());
        assert!(core.placeholders_balanced());
        assert_eq!(inline.1.len() + core.1.len(), 4);
    }

    #[test]
    fn filters_stay_on_core_when_no_links_table_is_read() {
        // "Pages without a Wikidata item" reads only `page` and `page_props`,
        // so even on Commons it is one core-cluster query.
        let state = AppState::default();
        let db = snapshot_db(|p| p.page_wikidata_item = "without".to_string());
        let mut filters = Filters::default();
        db.get_pages_for_primary_wikidata_item(&mut filters);
        assert_eq!(
            filters.base_cluster(&state, "commonswiki", &[]),
            DbCluster::Core
        );
        let (inline, deferred) = filters
            .split(&state, "commonswiki", DbCluster::Core)
            .unwrap();
        assert!(deferred.is_empty());
        assert!(inline.0.contains("pp_propname='wikibase_item'"));
    }

    #[test]
    fn filters_base_cluster_follows_the_link_count_column() {
        // Gathering the link count puts a `pagelinks` sub-select in the base
        // query's SELECT list, which on Commons decides the cluster.
        let state = AppState::default();
        let db = snapshot_db(|p| {
            p.gather_link_count = true;
            p.page_wikidata_item = "without".to_string();
        });
        let mut filters = Filters::default();
        db.get_pages_for_primary_wikidata_item(&mut filters);
        let base_tables = db.base_query_tables(Primary::NoWikidata);
        assert_eq!(base_tables, vec!["pagelinks"]);
        assert_eq!(
            filters.base_cluster(&state, "commonswiki", &base_tables),
            DbCluster::Links
        );
    }

    #[test]
    fn filters_seed_only_from_a_deferred_clause() {
        let state = AppState::default();
        let db = snapshot_db(|p| {
            p.after = "20230101000000".to_string();
            p.only_new_since = true;
        });
        let mut filters = Filters::default();
        db.get_pages_for_primary_namespaces(Primary::Categories, &mut filters);
        db.get_pages_for_primary_last_edited(&mut filters);

        // On Commons a category query runs on the links cluster and has to
        // defer the revision clause, whose seed then comes from the core.
        let seed = filters
            .seed(&state, "commonswiki", DbCluster::Links)
            .unwrap()
            .expect("deferred revision clause offers a seed");
        assert_eq!(seed.0, DbCluster::Core);
        assert!(
            seed.1
                .0
                .starts_with("SELECT DISTINCT rev_page FROM revision")
        );

        // Off Commons the clause is inline and the optimizer's business.
        assert!(
            filters
                .seed(&state, "enwiki", DbCluster::Core)
                .unwrap()
                .is_none()
        );

        // A deferred clause without a seed offers none.
        let no_item = snapshot_db(|p| p.page_wikidata_item = "without".to_string());
        let mut unseeded = Filters::default();
        no_item.get_pages_for_primary_wikidata_item(&mut unseeded);
        assert!(
            unseeded
                .seed(&state, "commonswiki", DbCluster::Links)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn seed_chunks_split_like_the_deferred_passes() {
        let params = |seed| DsdbParams {
            link_count_sql: String::new(),
            wiki: "commonswiki".to_string(),
            primary: Primary::Categories,
            base_cluster: DbCluster::Links,
            filters: Filters::default(),
            seed,
        };
        // No seed: one unrestricted run.
        assert_eq!(params(None).seed_chunks(), vec![None]);
        // An empty seed: nothing can match, so nothing runs.
        assert!(params(Some(vec![])).seed_chunks().is_empty());
        // One chunk per PAGE_BATCH_SIZE IDs.
        let ids: Vec<u32> = (0..=PAGE_BATCH_SIZE as u32).collect();
        let p = params(Some(ids.clone()));
        let chunks = p.seed_chunks();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], Some(&ids[..PAGE_BATCH_SIZE]));
        assert_eq!(chunks[1], Some(&ids[PAGE_BATCH_SIZE..]));
    }

    #[test]
    fn id_list_joins_without_placeholders() {
        assert_eq!(id_list(&[]), "");
        assert_eq!(id_list(&[1, 20, 300]), "1,20,300");
    }

    #[test]
    fn page_select_prefix_only_reads_revision_on_core() {
        // `page_touched` comes from `revision`; off the core cluster the
        // column is a placeholder the deferred pass fills in.
        assert!(page_select_prefix(DbCluster::Core).contains("rev_timestamp"));
        assert!(!page_select_prefix(DbCluster::Links).contains("rev_timestamp"));
        assert!(page_select_prefix(DbCluster::Links).contains("'' AS page_touched"));
        // Same column count and order either way, so one row type fits both.
        assert_eq!(
            page_select_prefix(DbCluster::Core).matches(',').count(),
            page_select_prefix(DbCluster::Links).matches(',').count()
        );
    }
}
