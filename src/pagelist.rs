use crate::app_state::AppState;
use crate::datasource::SQLtuple;
use crate::pagelist_entry::{PageListEntry, PageListSort, sort_or_shuffle};
use crate::platform::{MAX_CONCURRENT_DB_BATCHES, PAGE_BATCH_SIZE, Platform};
use crate::query_context::QueryContext;
use anyhow::{Result, anyhow};
use futures::stream::{StreamExt, iter};
use mysql_async as my;
use mysql_async::Value as MyValue;
use mysql_async::prelude::Queryable;
use rayon::prelude::*;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use wikimisc::mediawiki::api::{Api, NamespaceID};
use wikimisc::mediawiki::title::Title;

/// Acquire a read lock, transparently recovering from a poisoned lock.
///
/// The original code in this module called `.read().unwrap()` (and the
/// matching `.write().unwrap()`) at ~33 sites with the rationale "die
/// immediately on PoisonError". Combined with `panic = "abort"` that
/// meant a stray panic anywhere in the request pipeline would crash the
/// whole multi-tenant server. The audit (P1 #10) flagged this.
///
/// The locks here only protect a `HashSet<PageListEntry>` (owned data,
/// no internal pointers) and a couple of `Option<String>`/`bool` cells.
/// A previous holder that panicked mid-mutation can leave the contents
/// in an unusual state, but never in an unsafe one. We trade "crash on
/// poison" for "continue with whatever data is there", which is the
/// right call for a long-running multi-tenant service.
fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|p| p.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|p| p.into_inner())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatabaseCluster {
    Default,
    X3,
}

/// Distinguishes Wikidata item entities (Q-prefixed, namespace 0) from
/// property entities (P-prefixed, namespace 120). The two flavours map to
/// different `wbt_*` term-store tables and column names; keeping the
/// per-variant lookup colocated makes `add_wikidata_labels_for_namespace`
/// total over its inputs (no more `_ => return None` filter arms).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WikidataEntityType {
    Item,
    Property,
}

impl WikidataEntityType {
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Item => "Q",
            Self::Property => "P",
        }
    }
    pub const fn terms_table(self) -> &'static str {
        match self {
            Self::Item => "wbt_item_terms",
            Self::Property => "wbt_property_terms",
        }
    }
    pub const fn id_field(self) -> &'static str {
        match self {
            Self::Item => "wbit_item_id",
            Self::Property => "wbpt_property_id",
        }
    }
    pub const fn term_in_lang_field(self) -> &'static str {
        match self {
            Self::Item => "wbit_term_in_lang_id",
            Self::Property => "wbpt_term_in_lang_id",
        }
    }
}

#[derive(Debug)]
pub struct PageList {
    wiki: RwLock<Option<String>>,
    entries: RwLock<HashSet<PageListEntry>>,
    has_sitelink_counts: RwLock<bool>,
}

impl PageList {
    pub fn new_from_wiki(wiki: &str) -> Self {
        Self {
            wiki: RwLock::new(Some(wiki.to_string())),
            entries: RwLock::new(HashSet::new()),
            has_sitelink_counts: RwLock::new(false),
        }
    }

    pub fn new_from_wiki_with_capacity(wiki: &str, capacity: usize) -> Self {
        Self {
            wiki: RwLock::new(Some(wiki.to_string())),
            entries: RwLock::new(HashSet::with_capacity(capacity)),
            has_sitelink_counts: RwLock::new(false),
        }
    }

    pub fn set_from(&self, other: Self) {
        *write_lock(&self.wiki) = read_lock(&other.wiki).clone();
        *write_lock(&self.entries) = read_lock(&other.entries).clone();
        self.set_has_sitelink_counts(other.has_sitelink_counts());
    }

    pub fn set_has_sitelink_counts(&self, new_state: bool) {
        *write_lock(&self.has_sitelink_counts) = new_state;
    }

    pub fn has_sitelink_counts(&self) -> bool {
        *read_lock(&self.has_sitelink_counts)
    }

    pub fn set_entries(&self, entries: HashSet<PageListEntry>) {
        *write_lock(&self.entries) = entries;
    }

    pub fn retain_entries(&self, f: &dyn Fn(&PageListEntry) -> bool) {
        write_lock(&self.entries).retain(f);
    }

    pub fn get_entry(&self, entry: &PageListEntry) -> Option<PageListEntry> {
        self.entries.read().ok()?.get(entry).cloned()
    }

    pub fn to_titles_namespaces(&self) -> Vec<(String, NamespaceID)> {
        read_lock(&self.entries)
            .par_iter()
            .map(|entry| {
                (
                    entry.title().with_underscores(),
                    entry.title().namespace_id(),
                )
            })
            .collect()
    }

    pub fn to_full_pretty_titles(&self, api: &Api) -> Vec<String> {
        read_lock(&self.entries)
            .par_iter()
            .filter_map(|entry| entry.title().full_pretty(api))
            .collect()
    }

    pub fn change_namespaces(&self, to_talk: bool) {
        let add = to_talk as i64;
        let tmp = read_lock(&self.entries)
            .par_iter()
            .map(|entry| {
                let mut nsid = entry.title().namespace_id();
                nsid = nsid - (nsid & 1) + add; // Change "talk" bit
                let t = entry.title().pretty();
                let new_title = Title::new(t, nsid);
                PageListEntry::new(new_title)
            })
            .collect();
        *(write_lock(&self.entries)) = tmp;
    }

    pub fn as_vec(&self) -> Vec<PageListEntry> {
        read_lock(&self.entries).iter().cloned().collect()
    }

    pub fn set_wiki(&self, wiki: Option<String>) {
        *write_lock(&self.wiki) = wiki;
    }

    pub fn wiki(&self) -> Option<String> {
        read_lock(&self.wiki).clone()
    }

    pub fn drain_into_sorted_vec(&self, sorter: PageListSort) -> Vec<PageListEntry> {
        let mut ret: Vec<PageListEntry> = write_lock(&self.entries).drain().collect();
        sort_or_shuffle(&mut ret, &sorter, self.is_wikidata());
        ret
    }

    /// Drains all entries into an unsorted `Vec`, leaving the set empty.
    /// Prefer this over `drain_into_sorted_vec` in async contexts so the
    /// sort can be offloaded to a blocking thread via `spawn_blocking`.
    pub fn drain_into_vec(&self) -> Vec<PageListEntry> {
        write_lock(&self.entries).drain().collect()
    }

    pub fn group_by_namespace(&self) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        read_lock(&self.entries).iter().for_each(|entry| {
            ret.entry(entry.title().namespace_id())
                .or_default()
                .push(entry.title().with_underscores());
        });
        ret
    }

    pub fn is_empty(&self) -> bool {
        read_lock(&self.entries).is_empty()
    }

    pub fn len(&self) -> usize {
        read_lock(&self.entries).len()
    }

    pub fn add_entry(&self, entry: PageListEntry) {
        write_lock(&self.entries).replace(entry);
    }

    async fn check_before_merging(
        &self,
        pagelist: &PageList,
        platform: Option<&dyn QueryContext>,
    ) -> Result<()> {
        let self_wiki = self
            .wiki()
            .ok_or_else(|| anyhow!("PageList::check_before_merging No wiki set (self)"))?;
        let pagelist_wiki = pagelist
            .wiki()
            .ok_or_else(|| anyhow!("PageList::check_before_merging No wiki set (pagelist)"))?;
        if self_wiki != pagelist_wiki {
            let platform = platform
                .ok_or_else(|| anyhow!("PageList::check_before_merging platform in None"))?;
            Platform::profile(
                format!(
                    "PageList::check_before_merging Converting {} entries from {pagelist_wiki} to {self_wiki}",
                    pagelist.len(),
                )
                .as_str(),
                None,
            );
            pagelist.convert_to_wiki(&self_wiki, platform).await?;
        }
        Ok(())
    }

    pub async fn union(
        &self,
        pagelist: &PageList,
        platform: Option<&dyn QueryContext>,
    ) -> Result<()> {
        self.check_before_merging(pagelist, platform).await?;
        Platform::profile("PageList::union START UNION/1", None);
        // Clone the other list's entries so they can be moved into the blocking task.
        let other: HashSet<PageListEntry> = read_lock(&pagelist.entries).clone();
        // Fast path: if self is empty just replace with the clone we already have.
        let is_self_empty = read_lock(&self.entries).is_empty();
        let merged = if is_self_empty {
            other
        } else {
            Platform::profile("PageList::union START UNION/2", None);
            let self_set: HashSet<PageListEntry> = write_lock(&self.entries).drain().collect();
            tokio::task::spawn_blocking(move || {
                let mut result = self_set;
                result.extend(other);
                result
            })
            .await
            .map_err(|e| anyhow!("{e}"))?
        };
        Platform::profile("PageList::union UNION DONE", None);
        *write_lock(&self.entries) = merged;
        Ok(())
    }

    pub async fn intersection(
        &self,
        pagelist: &PageList,
        platform: Option<&dyn QueryContext>,
    ) -> Result<()> {
        self.check_before_merging(pagelist, platform).await?;
        // Clone the other list's entries so they can be moved into the blocking task.
        let other: HashSet<PageListEntry> = read_lock(&pagelist.entries).clone();
        let self_set: HashSet<PageListEntry> = write_lock(&self.entries).drain().collect();
        let filtered = tokio::task::spawn_blocking(move || {
            self_set
                .into_iter()
                .filter(|e| other.contains(e))
                .collect::<HashSet<_>>()
        })
        .await
        .map_err(|e| anyhow!("{e}"))?;
        *write_lock(&self.entries) = filtered;
        Ok(())
    }

    pub async fn difference(
        &self,
        pagelist: &PageList,
        platform: Option<&dyn QueryContext>,
    ) -> Result<()> {
        self.check_before_merging(pagelist, platform).await?;
        // Clone the other list's entries so they can be moved into the blocking task.
        let other: HashSet<PageListEntry> = read_lock(&pagelist.entries).clone();
        let self_set: HashSet<PageListEntry> = write_lock(&self.entries).drain().collect();
        let filtered = tokio::task::spawn_blocking(move || {
            self_set
                .into_iter()
                .filter(|e| !other.contains(e))
                .collect::<HashSet<_>>()
        })
        .await
        .map_err(|e| anyhow!("{e}"))?;
        *write_lock(&self.entries) = filtered;
        Ok(())
    }

    /// Builds SQL WHERE-clause batches for .
    /// Each chunk of  titles becomes one .
    fn sql_batches_for_ns(
        chunk_size: usize,
        nsid: NamespaceID,
        titles: &[String],
    ) -> Vec<SQLtuple> {
        titles
            .chunks(chunk_size)
            .map(|chunk| {
                let mut sql = crate::datasource::prep_quote(chunk);
                sql.0 = format!("(page_namespace={nsid} AND page_title IN({}))", sql.0);
                sql
            })
            .collect()
    }

    pub fn to_sql_batches(&self, chunk_size: usize) -> Vec<SQLtuple> {
        if self.is_empty() {
            return vec![];
        }
        self.group_by_namespace()
            .into_iter()
            .flat_map(|(nsid, titles)| Self::sql_batches_for_ns(chunk_size, nsid, &titles))
            .collect()
    }

    pub fn to_sql_batches_namespace(
        &self,
        chunk_size: usize,
        namespace_id: NamespaceID,
    ) -> Vec<SQLtuple> {
        if self.is_empty() {
            return vec![];
        }
        self.group_by_namespace()
            .into_iter()
            .filter(|(nsid, _)| *nsid == namespace_id)
            .flat_map(|(nsid, titles)| Self::sql_batches_for_ns(chunk_size, nsid, &titles))
            .collect()
    }

    pub fn clear_entries(&self) {
        write_lock(&self.entries).clear();
    }

    pub fn replace_entries(&self, other: &PageList) {
        let other_entries = read_lock(&other.entries);
        if let Ok(mut entries) = self.entries.write() {
            for entry in other_entries.iter() {
                entries.replace(entry.to_owned());
            }
        }
    }

    async fn run_batch_query(
        &self,
        state: &AppState,
        sql: SQLtuple,
        wiki: &str,
        cluster: DatabaseCluster,
    ) -> Result<Vec<my::Row>> {
        let mut conn = match cluster {
            DatabaseCluster::Default => state.get_wiki_db_connection(wiki).await,
            DatabaseCluster::X3 => state.get_x3_db_connection().await,
        }
        .map_err(|e| anyhow!(e))?;
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await?
            .collect_and_drop()
            .await?;
        // `conn` is leased from a `mysql_async::Pool`; dropping returns it
        // to the pool. Explicit `disconnect()` would terminate the
        // connection instead of recycling it.
        drop(conn);

        Ok(rows)
    }

    /// Runs batched queries for `process_batch_results` and `annotate_batch_results`
    pub async fn run_batch_queries(
        &self,
        state: &AppState,
        batches: Vec<SQLtuple>,
    ) -> Result<Vec<my::Row>> {
        self.run_batch_queries_with_cluster(state, batches, DatabaseCluster::Default)
            .await
    }

    /// Runs batched queries for `process_batch_results` and `annotate_batch_results`
    pub async fn run_batch_queries_with_cluster(
        &self,
        state: &AppState,
        batches: Vec<SQLtuple>,
        cluster: DatabaseCluster,
    ) -> Result<Vec<my::Row>> {
        let wiki = self
            .wiki()
            .ok_or_else(|| anyhow!("PageList::run_batch_queries: No wiki"))?;

        self.run_batch_queries_mutex(state, batches, wiki, cluster)
            .await
    }

    /// Runs batched queries for `process_batch_results` and `annotate_batch_results`
    /// Uses Mutex.
    async fn run_batch_queries_mutex(
        &self,
        state: &AppState,
        batches: Vec<SQLtuple>,
        wiki: String,
        cluster: DatabaseCluster,
    ) -> Result<Vec<my::Row>> {
        let mut futures = vec![];
        for sql in batches {
            futures.push(self.run_batch_query(state, sql, &wiki, cluster));
        }
        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;
        let mut ret = vec![];
        for x in results {
            ret.append(&mut x?);
        }
        Ok(ret)
    }

    pub fn string_from_row(row: &my::Row, col_num: usize) -> Option<String> {
        match row.get(col_num)? {
            my::Value::Bytes(uv) => String::from_utf8(uv).ok(),
            _ => None,
        }
    }

    pub fn entry_from_row(
        &self,
        row: &my::Row,
        col_title: usize,
        col_ns: usize,
    ) -> Option<PageListEntry> {
        let page_title = Self::string_from_row(row, col_title)?;
        let namespace_id = match row.get(col_ns)? {
            my::Value::Int(i) => i,
            _ => return None,
        };
        Some(PageListEntry::new(Title::new(&page_title, namespace_id)))
    }

    async fn load_missing_page_metadata(&self, platform: &dyn QueryContext) -> Result<()> {
        if read_lock(&self.entries).par_iter().any(|entry| {
            entry.page_id().is_none()
                || entry.page_bytes().is_none()
                || entry.get_page_timestamp().is_none()
        }) {
            let batches: Vec<SQLtuple> = self
                .to_sql_batches(PAGE_BATCH_SIZE)
                .par_iter_mut()
                .map(|sql_batch| {
                    sql_batch.0 =
                        "SELECT page_title,page_namespace,page_id,page_len,(SELECT rev_timestamp FROM revision WHERE rev_id=page_latest LIMIT 1) AS page_last_rev_timestamp FROM page WHERE"
                            .to_string() + &sql_batch.0;
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

            let the_f = |row: my::Row, entry: &mut PageListEntry| match my::from_row_opt::<(
                Vec<u8>,
                NamespaceID,
                u32,
                u32,
                Vec<u8>,
            )>(row)
            {
                Ok((_page_title, _page_namespace, page_id, page_len, page_last_rev_timestamp)) => {
                    let page_last_rev_timestamp =
                        String::from_utf8_lossy(&page_last_rev_timestamp).into_owned();
                    entry.set_page_id(Some(page_id));
                    entry.set_page_bytes(Some(page_len));
                    entry.set_page_timestamp(Some(page_last_rev_timestamp));
                }
                Err(_e) => {}
            };
            let col_title = 0;
            let col_ns = 1;
            let rows = self.run_batch_queries(&platform.state(), batches).await?;
            rows.iter()
                .filter_map(|row| {
                    self.entry_from_row(row, col_title, col_ns)
                        .map(|entry| (row, entry))
                })
                .filter_map(|(row, entry)| match self.entries.read() {
                    Ok(entries) => entries.get(&entry).map(|e| (row, e.clone())),
                    _ => None,
                })
                .for_each(|(row, mut entry)| {
                    the_f(row.clone(), &mut entry);
                    self.add_entry(entry);
                });
        }
        Ok(())
    }

    pub async fn load_missing_metadata(
        &self,
        wikidata_language: Option<String>,
        platform: &dyn QueryContext,
    ) -> Result<()> {
        Platform::profile("begin load_missing_metadata", None);
        Platform::profile("before load_missing_page_metadata", None);
        self.load_missing_page_metadata(platform).await?;
        Platform::profile("after load_missing_page_metadata", None);

        // All done
        if !self.is_wikidata() || wikidata_language.is_none() {
            return Ok(());
        }

        // No need to load labels for WDFIST mode
        if !platform.has_param("rxp_filter") && platform.has_param("wdf_main") {
            return Ok(());
        }

        if let Some(wikidata_language) = wikidata_language {
            self.add_wikidata_labels_for_namespace(
                0,
                WikidataEntityType::Item,
                &wikidata_language,
                platform,
            )
            .await?;
            self.add_wikidata_labels_for_namespace(
                120,
                WikidataEntityType::Property,
                &wikidata_language,
                platform,
            )
            .await?;
        }
        Platform::profile("end load_missing_metadata", None);
        Ok(())
    }

    async fn add_wikidata_labels_for_namespace(
        &self,
        namespace_id: NamespaceID,
        entity_type: WikidataEntityType,
        wikidata_language: &str,
        platform: &dyn QueryContext,
    ) -> Result<()> {
        // wbt_ done
        let prefix = entity_type.prefix();
        let table = entity_type.terms_table();
        let field_name = entity_type.id_field();
        let term_in_lang_id = entity_type.term_in_lang_field();
        let batches: Vec<SQLtuple> = self
            .to_sql_batches_namespace(PAGE_BATCH_SIZE, namespace_id)
            .iter_mut()
            .filter_map(|sql_batch| {
                let id_params: Vec<MyValue> = sql_batch
                    .1
                    .iter()
                    .filter_map(|s2| match s2 {
                        MyValue::Bytes(b) => {
                            let s = String::from_utf8_lossy(b);
                            s.get(1..)?.parse::<u64>().ok().map(MyValue::UInt)
                        }
                        _ => None,
                    })
                    .collect();
                if id_params.is_empty() {
                    return None;
                }
                let id_placeholders = vec!["?"; id_params.len()].join(",");
                sql_batch.1 = std::iter::once(MyValue::Bytes(wikidata_language.to_owned().into()))
                    .chain(id_params)
                    .collect();
                sql_batch.0 = format!(
                    "SELECT concat('{prefix}',{field_name}) AS term_full_entity_id,
                	{namespace_id} AS dummy_namespace,
	                 wbx_text as term_text,
					 (case when wbtl_type_id = 1 then 'label'
             			when wbtl_type_id = 2 then 'description'
                		when wbtl_type_id = 3 then 'alias'
                		end) AS term_type
					 FROM {table}
					 INNER JOIN wbt_term_in_lang ON {term_in_lang_id} = wbtl_id
					 INNER JOIN wbt_type ON wbtl_type_id = wby_id
					 INNER JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id
					 INNER JOIN wbt_text ON wbxl_text_id = wbx_id AND wbxl_language=?
					 WHERE {field_name} IN ({id_placeholders})"
                );
                Some(sql_batch.to_owned())
            })
            .collect::<Vec<SQLtuple>>();

        let the_f = |row: my::Row, entry: &mut PageListEntry| {
            if let Ok((_page_title, _page_namespace, term_text, term_type)) =
                my::from_row_opt::<(Vec<u8>, NamespaceID, Vec<u8>, Vec<u8>)>(row)
            {
                let term_text = String::from_utf8_lossy(&term_text).into_owned();
                match String::from_utf8_lossy(&term_type).into_owned().as_str() {
                    "label" => entry.set_wikidata_label(Some(term_text)),
                    "description" => entry.set_wikidata_description(Some(term_text)),
                    _ => {}
                }
            }
        };
        let col_title = 0;
        let col_ns = 1;
        self.run_batch_queries_with_cluster(&platform.state(), batches, DatabaseCluster::X3)
            .await?
            .iter()
            .filter_map(|row| {
                self.entry_from_row(row, col_title, col_ns)
                    .map(|entry| (row, entry))
            })
            .filter_map(|(row, entry)| {
                match self.entries.read() {
                    Ok(entries) => entries.get(&entry).map(|e| (row, e.clone())),
                    _ => None, // TODO error?
                }
            })
            .for_each(|(row, mut entry)| {
                the_f(row.clone(), &mut entry);
                self.add_entry(entry);
            });
        Ok(())
    }

    pub async fn convert_to_wiki(&self, wiki: &str, platform: &dyn QueryContext) -> Result<()> {
        // Already that wiki?
        if self.wiki().is_none() || self.wiki() == Some(wiki.to_string()) {
            return Ok(());
        }
        self.convert_to_wikidata(platform).await?;
        if wiki != "wikidatawiki" {
            self.convert_from_wikidata(wiki, platform).await?;
        }
        Ok(())
    }

    async fn convert_to_wikidata(&self, platform: &dyn QueryContext) -> Result<()> {
        if self.wiki().is_none() || self.is_wikidata() {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = self.to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql|{
                sql.0 = "SELECT pp_value FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ".to_owned()+&sql.0;
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();
        self.clear_entries();
        let state = platform.state();
        let the_f = |row: my::Row| match my::from_row_opt::<Vec<u8>>(row) {
            Ok(pp_value) => {
                let pp_value = String::from_utf8_lossy(&pp_value).into_owned();
                Some(PageListEntry::new(Title::new(&pp_value, 0)))
            }
            Err(_e) => None,
        };

        let results = self.run_batch_queries(&state, batches);
        let results = results.await?;
        results
            .iter()
            .filter_map(|row| the_f(row.to_owned()))
            .for_each(|entry| self.add_entry(entry));

        self.set_wiki(Some("wikidatawiki".to_string()));
        Ok(())
    }

    async fn convert_from_wikidata(&self, wiki: &str, platform: &dyn QueryContext) -> Result<()> {
        if !self.is_wikidata() {
            return Ok(());
        }
        Platform::profile("PageList::convert_from_wikidata START", None);
        let batches = self.to_sql_batches(PAGE_BATCH_SIZE*2)
            .par_iter_mut()
            .map(|sql|{
                sql.0 = "SELECT ips_site_page FROM wb_items_per_site,page WHERE ips_item_id=substr(page_title,2)*1 AND ".to_owned()+&sql.0+" AND ips_site_id=?";
                sql.1.push(MyValue::Bytes(wiki.into()));
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        Platform::profile(
            "PageList::convert_from_wikidata BATCHES CREATED",
            Some(batches.len()),
        );

        self.clear_entries();
        let api = platform
            .state()
            .get_api_for_wiki(wiki.to_string())
            .await
            .map_err(|e| anyhow!(e))?;
        Platform::profile("PageList::convert_from_wikidata STARTING BATCHES", None);

        let batches = batches.chunks(5).collect::<Vec<_>>();
        let state = platform.state();
        let mut futures = vec![];
        for batch_chunk in batches {
            let future = self.run_batch_queries(&state, batch_chunk.to_vec());
            futures.push(future);
        }

        let the_fn = |row: my::Row| {
            let ips_site_page = my::from_row_opt::<Vec<u8>>(row).ok()?;
            let ips_site_page = String::from_utf8_lossy(&ips_site_page).into_owned();
            Some(PageListEntry::new(Title::new_from_full(
                &ips_site_page,
                &api,
            )))
        };

        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;
        results
            .into_par_iter()
            .filter_map(|r| r.ok())
            .flatten()
            .filter_map(the_fn)
            .for_each(|entry| self.add_entry(entry));

        Platform::profile("PageList::convert_from_wikidata ALL BATCHES COMPLETE", None);
        self.set_wiki(Some(wiki.to_string()));
        Platform::profile("PageList::convert_from_wikidata END", None);
        Ok(())
    }

    pub fn regexp_filter(&self, regexp: &str) {
        let regexp_all = "^".to_string() + regexp + "$";
        let is_wikidata = self.is_wikidata();
        if let Ok(re) = Regex::new(&regexp_all) {
            self.retain_entries(&|entry: &PageListEntry| match is_wikidata {
                true => match &entry.get_wikidata_label() {
                    Some(s) => re.is_match(s.as_str()),
                    None => false,
                },
                false => re.is_match(entry.title().pretty()),
            });
        }
    }

    async fn search_entry(&self, api: &Api, search: &str, page_id: u32) -> Result<bool> {
        let params = [
            ("action".to_string(), "query".to_string()),
            ("list".to_string(), "search".to_string()),
            ("srnamespace".to_string(), "*".to_string()),
            ("srlimit".to_string(), "1".to_string()),
            ("srsearch".to_string(), format!("pageid:{page_id} {search}")),
        ]
        .iter()
        .cloned()
        .collect();
        let result = match api.get_query_api_json(&params).await {
            Ok(result) => result,
            Err(e) => return Err(anyhow!("{e}")),
        };
        let titles = Api::result_array_to_titles(&result);
        Ok(!titles.is_empty())
    }

    pub async fn search_filter(&self, platform: &dyn QueryContext, search: &str) -> Result<()> {
        let max_page_number: usize = 10000;
        if self.len() > max_page_number {
            return Err(anyhow!(
                "Too many pages ({}), maximum is {max_page_number}",
                self.len()
            ));
        }
        let wiki = match self.wiki() {
            Some(wiki) => wiki,
            None => return Ok(()),
        };
        let page_ids: Vec<u32> = read_lock(&self.entries)
            .iter()
            .filter_map(|entry| entry.page_id())
            .collect();
        let api = platform
            .state()
            .get_api_for_wiki(wiki)
            .await
            .map_err(|e| anyhow!(e))?;
        let mut futures = vec![];
        page_ids.iter().for_each(|page_id| {
            let fut = self.search_entry(&api, search, page_id.to_owned());
            futures.push(fut);
        });
        // This fans over per-page API calls rather than DB batches, but the
        // same upper bound applies — unbounded parallelism here would hammer
        // the upstream search API.
        let results: Vec<_> = iter(futures)
            .buffered(MAX_CONCURRENT_DB_BATCHES)
            .collect()
            .await;

        let mut searches_failed = false;
        let retain_page_ids: Vec<u32> = page_ids
            .iter()
            .zip(results.iter())
            .filter_map(|(page_id, result)| match result {
                Ok(true) => Some(page_id.to_owned()),
                Err(_) => {
                    searches_failed = true;
                    None
                }
                _ => None,
            })
            .collect();
        if searches_failed {
            return Err(anyhow!("Filter searches have failed"));
        }

        self.retain_entries(&|entry: &PageListEntry| match entry.page_id() {
            Some(page_id) => retain_page_ids.contains(&page_id),
            None => false,
        });
        Ok(())
    }

    pub fn is_wikidata(&self) -> bool {
        self.wiki() == Some("wikidatawiki".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn make_entry(title: &str, ns: i64) -> PageListEntry {
        PageListEntry::new(Title::new(title, ns))
    }

    #[test]
    fn page_list_sort() {
        assert_eq!(
            PageListSort::new_from_params("incoming_links", true),
            PageListSort::IncomingLinks(true)
        );
        assert_eq!(
            PageListSort::new_from_params("ns_title", false),
            PageListSort::NsTitle(false)
        );
        assert_eq!(
            PageListSort::new_from_params("this is not a sort parameter", true),
            PageListSort::Default(true)
        );
    }

    #[test]
    fn test_new_from_wiki() {
        let pl = PageList::new_from_wiki("enwiki");
        assert_eq!(pl.wiki(), Some("enwiki".to_string()));
        assert!(pl.is_empty());
        assert_eq!(pl.len(), 0);
    }

    #[test]
    fn test_set_wiki() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.set_wiki(Some("dewiki".to_string()));
        assert_eq!(pl.wiki(), Some("dewiki".to_string()));
        pl.set_wiki(None);
        assert_eq!(pl.wiki(), None);
    }

    #[test]
    fn test_is_wikidata() {
        let pl = PageList::new_from_wiki("wikidatawiki");
        assert!(pl.is_wikidata());
        let pl2 = PageList::new_from_wiki("enwiki");
        assert!(!pl2.is_wikidata());
    }

    #[test]
    fn test_sitelink_counts() {
        let pl = PageList::new_from_wiki("enwiki");
        assert!(!pl.has_sitelink_counts());
        pl.set_has_sitelink_counts(true);
        assert!(pl.has_sitelink_counts());
        pl.set_has_sitelink_counts(false);
        assert!(!pl.has_sitelink_counts());
    }

    #[test]
    fn test_add_entry_len_is_empty() {
        let pl = PageList::new_from_wiki("enwiki");
        assert!(pl.is_empty());
        pl.add_entry(make_entry("Foo", 0));
        assert!(!pl.is_empty());
        assert_eq!(pl.len(), 1);
        pl.add_entry(make_entry("Bar", 0));
        assert_eq!(pl.len(), 2);
    }

    #[test]
    fn test_set_entries() {
        let pl = PageList::new_from_wiki("enwiki");
        let mut entries = HashSet::new();
        entries.insert(make_entry("Foo", 0));
        entries.insert(make_entry("Bar", 0));
        pl.set_entries(entries);
        assert_eq!(pl.len(), 2);
    }

    #[test]
    fn test_as_vec() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Foo", 0));
        pl.add_entry(make_entry("Bar", 4));
        let v = pl.as_vec();
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn test_clear_entries() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Foo", 0));
        pl.add_entry(make_entry("Bar", 0));
        assert_eq!(pl.len(), 2);
        pl.clear_entries();
        assert!(pl.is_empty());
    }

    #[test]
    fn test_retain_entries() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Foo", 0));
        pl.add_entry(make_entry("Bar", 4));
        pl.add_entry(make_entry("Baz", 0));
        pl.retain_entries(&|entry| entry.title().namespace_id() == 0);
        assert_eq!(pl.len(), 2);
    }

    #[test]
    fn test_group_by_namespace() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Foo", 0));
        pl.add_entry(make_entry("Bar", 0));
        pl.add_entry(make_entry("Talk:Foo", 1));
        let groups = pl.group_by_namespace();
        assert_eq!(groups.get(&0).map(|v| v.len()), Some(2));
        assert_eq!(groups.get(&1).map(|v| v.len()), Some(1));
    }

    #[tokio::test]
    async fn test_union_same_wiki() {
        let pl1 = PageList::new_from_wiki("enwiki");
        pl1.add_entry(make_entry("Foo", 0));
        pl1.add_entry(make_entry("Bar", 0));

        let pl2 = PageList::new_from_wiki("enwiki");
        pl2.add_entry(make_entry("Bar", 0));
        pl2.add_entry(make_entry("Baz", 0));

        pl1.union(&pl2, None).await.unwrap();
        assert_eq!(pl1.len(), 3); // Foo, Bar, Baz
    }

    /// Same-wiki merges must not consult the [`QueryContext`] — the cross-wiki
    /// conversion in `check_before_merging` is the only consumer, and it must
    /// short-circuit before touching state when the wikis match. Proves the
    /// dependency-inversion seam works (a stub flows through where a real
    /// `Platform` used to be required).
    #[tokio::test]
    async fn test_union_same_wiki_does_not_consult_query_context() {
        use crate::app_state::AppState;
        use crate::query_context::QueryContext;
        use std::sync::Arc;

        struct PanickingStub;
        impl QueryContext for PanickingStub {
            fn state(&self) -> Arc<AppState> {
                panic!("state() must not be called on the same-wiki union path");
            }
            fn has_param(&self, _: &str) -> bool {
                panic!("has_param() must not be called on the same-wiki union path");
            }
        }

        let pl1 = PageList::new_from_wiki("enwiki");
        pl1.add_entry(make_entry("Foo", 0));
        let pl2 = PageList::new_from_wiki("enwiki");
        pl2.add_entry(make_entry("Bar", 0));

        pl1.union(&pl2, Some(&PanickingStub)).await.unwrap();
        assert_eq!(pl1.len(), 2);
    }

    #[tokio::test]
    async fn test_intersection_same_wiki() {
        let pl1 = PageList::new_from_wiki("enwiki");
        pl1.add_entry(make_entry("Foo", 0));
        pl1.add_entry(make_entry("Bar", 0));

        let pl2 = PageList::new_from_wiki("enwiki");
        pl2.add_entry(make_entry("Bar", 0));
        pl2.add_entry(make_entry("Baz", 0));

        pl1.intersection(&pl2, None).await.unwrap();
        assert_eq!(pl1.len(), 1); // Only Bar
        let entries = pl1.as_vec();
        assert_eq!(entries[0].title().pretty(), "Bar");
    }

    #[tokio::test]
    async fn test_difference_same_wiki() {
        let pl1 = PageList::new_from_wiki("enwiki");
        pl1.add_entry(make_entry("Foo", 0));
        pl1.add_entry(make_entry("Bar", 0));

        let pl2 = PageList::new_from_wiki("enwiki");
        pl2.add_entry(make_entry("Bar", 0));

        pl1.difference(&pl2, None).await.unwrap();
        assert_eq!(pl1.len(), 1); // Only Foo
        let entries = pl1.as_vec();
        assert_eq!(entries[0].title().pretty(), "Foo");
    }

    #[test]
    fn test_set_from() {
        let pl1 = PageList::new_from_wiki("enwiki");
        pl1.add_entry(make_entry("Foo", 0));
        pl1.set_has_sitelink_counts(true);

        let pl2 = PageList::new_from_wiki("dewiki");
        pl2.set_from(pl1);

        assert_eq!(pl2.wiki(), Some("enwiki".to_string()));
        assert_eq!(pl2.len(), 1);
        assert!(pl2.has_sitelink_counts());
    }

    #[test]
    fn test_replace_entries_unions_disjoint_sets() {
        // replace_entries inserts each entry from `other` into `self`,
        // overwriting any existing entry with the same title.
        let pl1 = PageList::new_from_wiki("enwiki");
        pl1.add_entry(make_entry("Foo", 0));

        let pl2 = PageList::new_from_wiki("enwiki");
        pl2.add_entry(make_entry("Bar", 0));
        pl2.add_entry(make_entry("Baz", 0));

        pl1.replace_entries(&pl2);

        let titles: std::collections::BTreeSet<String> = pl1
            .as_vec()
            .iter()
            .map(|e| e.title().pretty().to_string())
            .collect();
        assert_eq!(titles.len(), 3);
        assert!(titles.contains("Foo"));
        assert!(titles.contains("Bar"));
        assert!(titles.contains("Baz"));
    }

    #[test]
    fn test_replace_entries_overwrites_same_title() {
        // PageListEntry equality is by title only, so replace_entries on
        // a same-title entry must overwrite the existing one rather than
        // duplicate it. Pin that semantic: inject distinct field values
        // and check which one survives.
        let pl1 = PageList::new_from_wiki("enwiki");
        let mut original = make_entry("Foo", 0);
        original.set_page_bytes(Some(100));
        pl1.add_entry(original);

        let pl2 = PageList::new_from_wiki("enwiki");
        let mut replacement = make_entry("Foo", 0);
        replacement.set_page_bytes(Some(999));
        pl2.add_entry(replacement);

        pl1.replace_entries(&pl2);

        assert_eq!(pl1.len(), 1);
        let entry = pl1.as_vec().into_iter().next().unwrap();
        assert_eq!(entry.title().pretty(), "Foo");
        assert_eq!(entry.page_bytes(), Some(999));
    }

    #[test]
    fn test_drain_into_sorted_vec() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Zebra", 0));
        pl.add_entry(make_entry("Apple", 0));
        pl.add_entry(make_entry("Mango", 0));

        let sorted = pl.drain_into_sorted_vec(PageListSort::Title(false));
        assert!(pl.is_empty()); // drained
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].title().pretty(), "Apple");
        assert_eq!(sorted[1].title().pretty(), "Mango");
        assert_eq!(sorted[2].title().pretty(), "Zebra");
    }

    #[test]
    fn test_drain_into_sorted_vec_descending() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Apple", 0));
        pl.add_entry(make_entry("Zebra", 0));

        let sorted = pl.drain_into_sorted_vec(PageListSort::Title(true));
        assert_eq!(sorted[0].title().pretty(), "Zebra");
        assert_eq!(sorted[1].title().pretty(), "Apple");
    }

    #[test]
    fn test_new_from_wiki_with_capacity() {
        let pl = PageList::new_from_wiki_with_capacity("enwiki", 100);
        assert_eq!(pl.wiki(), Some("enwiki".to_string()));
        assert!(pl.is_empty());
    }

    #[test]
    fn test_regexp_filter() {
        let pl = PageList::new_from_wiki("enwiki");
        pl.add_entry(make_entry("Magnus_Manske", 0));
        pl.add_entry(make_entry("Count_von_Count", 0));
        pl.add_entry(make_entry("Magnus_Something", 0));

        pl.regexp_filter("Magnus.*");
        assert_eq!(pl.len(), 2);
    }
}
