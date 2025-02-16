use crate::app_state::AppState;
use crate::datasource::SQLtuple;
use crate::pagelist_entry::{PageListEntry, PageListSort};
use crate::platform::{Platform, PAGE_BATCH_SIZE};
use futures::future::join_all;
use mysql_async as my;
use mysql_async::prelude::Queryable;
use mysql_async::Value as MyValue;
use rayon::prelude::*;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use wikimisc::mediawiki::api::{Api, NamespaceID};
use wikimisc::mediawiki::title::Title;

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

    pub fn clear(&mut self) -> Result<(), String> {
        *self.wiki.write().map_err(|e| format!("{:?}", e))? = None;
        self.entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .clear();
        Ok(())
    }

    pub fn set_from(&self, other: Self) -> Result<(), String> {
        *self.wiki.write().map_err(|e| format!("{:?}", e))? =
            other.wiki.read().map_err(|e| format!("{:?}", e))?.clone();
        *self.entries.write().map_err(|e| format!("{:?}", e))? = other
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .clone();
        self.set_has_sitelink_counts(other.has_sitelink_counts()?)?;
        Ok(())
    }

    pub fn set_has_sitelink_counts(&self, new_state: bool) -> Result<(), String> {
        *self
            .has_sitelink_counts
            .write()
            .map_err(|e| format!("{:?}", e))? = new_state;
        Ok(())
    }

    pub fn has_sitelink_counts(&self) -> Result<bool, String> {
        let ret: bool = *self
            .has_sitelink_counts
            .read()
            .map_err(|e| format!("{:?}", e))?;
        Ok(ret)
    }

    pub fn set_entries(&self, entries: HashSet<PageListEntry>) -> Result<(), String> {
        *self.entries.write().map_err(|e| format!("{:?}", e))? = entries;
        Ok(())
    }

    pub fn retain_entries(&self, f: &dyn Fn(&PageListEntry) -> bool) -> Result<(), String> {
        self.entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .retain(f);
        Ok(())
    }

    pub fn get_entry(&self, entry: &PageListEntry) -> Option<PageListEntry> {
        self.entries.read().ok()?.get(entry).map(|e| e.clone())
    }

    pub fn to_titles_namepsaces(&self) -> Result<Vec<(String, NamespaceID)>, String> {
        let title_ns = self
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .par_iter()
            .map(|entry| {
                (
                    entry.title().with_underscores(),
                    entry.title().namespace_id(),
                )
            })
            .collect();
        Ok(title_ns)
    }

    pub fn to_full_pretty_titles(&self, api: &Api) -> Result<Vec<String>, String> {
        let ret = self
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .par_iter()
            .filter_map(|entry| entry.title().full_pretty(api))
            .collect();
        Ok(ret)
    }

    pub fn change_namespaces(&self, to_talk: bool) -> Result<(), String> {
        let add = to_talk as i64;
        let tmp = self
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .par_iter()
            .map(|entry| {
                let mut nsid = entry.title().namespace_id();
                nsid = nsid - (nsid & 1) + add; // Change "talk" bit
                let t = entry.title().pretty();
                let new_title = Title::new(t, nsid);
                PageListEntry::new(new_title)
            })
            .collect();
        *(self.entries.write().map_err(|e| format!("{:?}", e))?) = tmp;
        Ok(())
    }

    pub fn as_vec(&self) -> Result<Vec<PageListEntry>, String> {
        Ok(self.entries.read().unwrap().iter().cloned().collect())
    }

    pub fn set_wiki(&self, wiki: Option<String>) -> Result<(), String> {
        *self.wiki.write().map_err(|e| format!("{:?}", e))? = wiki;
        Ok(())
    }

    pub fn wiki(&self) -> Result<Option<String>, String> {
        Ok(self.wiki.read().map_err(|e| format!("{:?}", e))?.clone())
    }

    pub fn drain_into_sorted_vec(
        &self,
        sorter: PageListSort,
    ) -> Result<Vec<PageListEntry>, String> {
        let mut ret: Vec<PageListEntry> = self
            .entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .drain()
            .collect();
        ret.par_sort_by(|a, b| a.compare(b, &sorter, self.is_wikidata()));
        Ok(ret)
    }

    pub fn group_by_namespace(&self) -> Result<HashMap<NamespaceID, Vec<String>>, String> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        self.entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .iter()
            .for_each(|entry| {
                ret.entry(entry.title().namespace_id())
                    .or_default()
                    .push(entry.title().with_underscores());
            });
        Ok(ret)
    }

    pub fn is_empty(&self) -> Result<bool, String> {
        Ok(self
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .is_empty())
    }

    pub fn len(&self) -> Result<usize, String> {
        Ok(self.entries.read().map_err(|e| format!("{:?}", e))?.len())
    }

    pub fn add_entry(&self, entry: PageListEntry) -> Result<(), String> {
        self.entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .replace(entry);
        Ok(())
    }

    async fn check_before_merging(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        let my_wiki = match self.wiki()? {
            Some(wiki) => wiki,
            None => return Err("PageList::check_before_merging self.wiki is not set".to_string()),
        };
        if pagelist.wiki()?.is_none() {
            return Err("PageList::check_before_merging pagelist.wiki is not set".to_string());
        }
        if self.wiki()? != pagelist.wiki()? {
            match platform {
                Some(platform) => {
                    Platform::profile(
                        format!(
                            "PageList::check_before_merging Converting {} entries from {} to {}",
                            pagelist.len()?,
                            pagelist
                                .wiki()?
                                .unwrap_or_else(|| "NO WIKI SET".to_string()),
                            &my_wiki
                        )
                        .as_str(),
                        None,
                    );
                    pagelist.convert_to_wiki(&my_wiki, platform).await?;
                }
                None => {
                    return Err(format!(
                        "PageList::check_before_merging wikis are not identical: {}/{}",
                        self.wiki()?
                            .unwrap_or_else(|| "PageList::check_before_merging:1".to_string()),
                        pagelist
                            .wiki()?
                            .unwrap_or_else(|| "PageList::check_before_merging:2".to_string())
                    ))
                }
            }
        }
        Ok(())
    }

    pub async fn union(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        self.check_before_merging(pagelist, platform).await?;
        Platform::profile("PageList::union START UNION/1", None);
        let mut me = self.entries.write().map_err(|e| format!("{:?}", e))?;
        if me.is_empty() {
            *me = pagelist
                .entries
                .read()
                .map_err(|e| format!("{:?}", e))?
                .clone();
            return Ok(());
        }
        Platform::profile("PageList::union START UNION/2", None);
        pagelist
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .iter()
            .for_each(|x| {
                me.insert(x.to_owned());
            });
        Platform::profile("PageList::union UNION DONE", None);
        Ok(())
    }

    pub async fn intersection(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        self.check_before_merging(pagelist, platform).await?;
        let other_entries = pagelist.entries.read().map_err(|e| format!("{:?}", e))?;
        self.entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .retain(|page_list_entry| other_entries.contains(page_list_entry));
        Ok(())
    }

    pub async fn difference(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        self.check_before_merging(pagelist, platform).await?;
        let other_entries = pagelist.entries.read().map_err(|e| format!("{:?}", e))?;
        self.entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .retain(|page_list_entry| !other_entries.contains(page_list_entry));
        Ok(())
    }

    pub fn to_sql_batches(&self, chunk_size: usize) -> Result<Vec<SQLtuple>, String> {
        let mut ret: Vec<SQLtuple> = vec![];
        if self.is_empty()? {
            return Ok(ret);
        }
        let by_ns = self.group_by_namespace()?;
        for (nsid, titles) in by_ns {
            titles.chunks(chunk_size).for_each(|chunk| {
                let mut sql = Platform::prep_quote(chunk);
                sql.0 = format!("(page_namespace={} AND page_title IN({}))", nsid, &sql.0);
                ret.push(sql);
            });
        }
        Ok(ret)
    }

    pub fn to_sql_batches_namespace(
        &self,
        chunk_size: usize,
        namespace_id: NamespaceID,
    ) -> Result<Vec<SQLtuple>, String> {
        let mut ret: Vec<SQLtuple> = vec![];
        if self.is_empty()? {
            return Ok(ret);
        }
        let by_ns = self.group_by_namespace()?;
        for (nsid, titles) in by_ns {
            if nsid == namespace_id {
                titles.chunks(chunk_size).for_each(|chunk| {
                    let mut sql = Platform::prep_quote(chunk);
                    sql.0 = format!("(page_namespace={} AND page_title IN({}))", nsid, &sql.0);
                    ret.push(sql);
                });
            }
        }
        Ok(ret)
    }

    pub fn clear_entries(&self) -> Result<(), String> {
        self.entries
            .write()
            .map_err(|e| format!("{:?}", e))?
            .clear();
        Ok(())
    }

    pub fn replace_entries(&self, other: &PageList) -> Result<(), String> {
        other
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .iter()
            .for_each(|entry| {
                if let Ok(mut entries) = self.entries.write() {
                    entries.replace(entry.to_owned());
                }
            });
        Ok(())
    }

    async fn run_batch_query(
        &self,
        state: &AppState,
        sql: SQLtuple,
        wiki: &str,
    ) -> Result<Vec<my::Row>, String> {
        let mut conn = state
            .get_wiki_db_connection(wiki)
            .await
            .map_err(|e| format!("PageList::run_batch_query: get_wiki_db_connection: {:?}", e))?;
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await // TODO fix to_owned
            .map_err(|e| format!("PageList::run_batch_query: SQL query error[1]: {:?}", e))?
            .collect_and_drop()
            .await
            .map_err(|e| format!("PageList::run_batch_query: SQL query error[2]: {:?}", e))?;
        conn.disconnect().await.map_err(|e| format!("{:?}", e))?;

        Ok(rows)
    }

    /// Runs batched queries for process_batch_results and annotate_batch_results
    pub async fn run_batch_queries(
        &self,
        state: &AppState,
        batches: Vec<SQLtuple>,
    ) -> Result<Vec<my::Row>, String> {
        let wiki = self
            .wiki()?
            .ok_or_else(|| "PageList::run_batch_queries: No wiki".to_string())?;

        if true {
            self.run_batch_queries_mutex(state, batches, wiki).await
        } else {
            self.run_batch_queries_serial(state, batches, wiki).await
        }
    }

    /// Runs batched queries for process_batch_results and annotate_batch_results
    /// Uses serial processing (not Mutex)
    async fn run_batch_queries_serial(
        &self,
        state: &AppState,
        batches: Vec<SQLtuple>,
        wiki: String,
    ) -> Result<Vec<my::Row>, String> {
        // TODO?: "SET STATEMENT max_statement_time = 300 FOR SELECT..."
        let mut rows: Vec<my::Row> = vec![];
        for sql in batches {
            let mut data = self.run_batch_query(state, sql, &wiki).await?;
            rows.append(&mut data);
        }
        Ok(rows)
    }

    /// Runs batched queries for process_batch_results and annotate_batch_results
    /// Uses Mutex.
    async fn run_batch_queries_mutex(
        &self,
        state: &AppState,
        batches: Vec<SQLtuple>,
        wiki: String,
    ) -> Result<Vec<my::Row>, String> {
        // TODO?: "SET STATEMENT max_statement_time = 300 FOR SELECT..."

        // TODO parallel
        let mut futures = vec![];
        for sql in batches {
            futures.push(self.run_batch_query(state, sql, &wiki));
        }
        let results = join_all(futures).await;
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

    async fn load_missing_page_metadata(&self, platform: &Platform) -> Result<(), String> {
        if self
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .par_iter()
            .any(|entry| {
                entry.page_id().is_none()
                    || entry.page_bytes().is_none()
                    || entry.get_page_timestamp().is_none()
            })
        {
            let batches: Vec<SQLtuple> = self
                .to_sql_batches(PAGE_BATCH_SIZE)?
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
            self.run_batch_queries(&platform.state(), batches)
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
                    self.add_entry(entry).unwrap_or(());
                });
        }
        Ok(())
    }

    pub async fn load_missing_metadata(
        &self,
        wikidata_language: Option<String>,
        platform: &Platform,
    ) -> Result<(), String> {
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
            self.add_wikidata_labels_for_namespace(0, "item", &wikidata_language, platform)
                .await?;
            self.add_wikidata_labels_for_namespace(120, "property", &wikidata_language, platform)
                .await?;
        }
        Platform::profile("end load_missing_metadata", None);
        Ok(())
    }

    async fn add_wikidata_labels_for_namespace(
        &self,
        namespace_id: NamespaceID,
        entity_type: &str,
        wikidata_language: &str,
        platform: &Platform,
    ) -> Result<(), String> {
        let batches: Vec<SQLtuple> = self
            .to_sql_batches_namespace(PAGE_BATCH_SIZE,namespace_id)?
            .iter_mut()
            .filter_map(|sql_batch| {
                // entity_type and namespace_id are "database safe"
                let prefix = match entity_type {
                    "item" => "Q",
                    "property" => "P",
                    _ => return None
                };
                let table = match entity_type {
                    "item" => "wbt_item_terms",
                    "property" => "wbt_property_terms",
                    _ => return None
                } ;
                let field_name = match entity_type {
                    "item" => "wbit_item_id",
                    "property" => "wbpt_property_id",
                    _ => return None
                } ;
                let term_in_lang_id = match entity_type {
                    "item" => "wbit_term_in_lang_id",
                    "property" => "wbpt_term_in_lang_id",
                    _ => return None
                } ;
                let item_ids = sql_batch.1.iter().map(|s|{
                    match s {
                        MyValue::Bytes(s) => String::from_utf8_lossy(s)[1..].to_string(),
                        _ => String::new()
                    }
                }).collect::<Vec<String>>().join(",");
                sql_batch.1 = vec![MyValue::Bytes(wikidata_language.to_owned().into())];
                sql_batch.0 = format!("SELECT concat('{}',{}) AS term_full_entity_id,{} AS dummy_namespace,wbx_text as term_text,wby_name as term_type
FROM {}
INNER JOIN wbt_term_in_lang ON {} = wbtl_id
INNER JOIN wbt_type ON wbtl_type_id = wby_id
INNER JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id
INNER JOIN wbt_text ON wbxl_text_id = wbx_id AND wbxl_language=?
WHERE {} IN ({})",prefix,&field_name,namespace_id,table,term_in_lang_id,&field_name,item_ids);
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
        self.run_batch_queries(&platform.state(), batches)
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
                self.add_entry(entry).unwrap_or(());
            });
        Ok(())
    }

    pub async fn convert_to_wiki(&self, wiki: &str, platform: &Platform) -> Result<(), String> {
        // Already that wiki?
        if self.wiki()?.is_none() || self.wiki()? == Some(wiki.to_string()) {
            return Ok(());
        }
        self.convert_to_wikidata(platform).await?;
        if wiki != "wikidatawiki" {
            self.convert_from_wikidata(wiki, platform).await?;
        }
        Ok(())
    }

    async fn convert_to_wikidata(&self, platform: &Platform) -> Result<(), String> {
        if self.wiki()?.is_none() || self.is_wikidata() {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = self.to_sql_batches(PAGE_BATCH_SIZE)?
            .par_iter_mut()
            .map(|sql|{
                sql.0 = "SELECT pp_value FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ".to_owned()+&sql.0;
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();
        self.clear_entries()?;
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
            .for_each(|entry| self.add_entry(entry).unwrap_or(()));

        self.set_wiki(Some("wikidatawiki".to_string()))?;
        Ok(())
    }

    async fn convert_from_wikidata(&self, wiki: &str, platform: &Platform) -> Result<(), String> {
        if !self.is_wikidata() {
            return Ok(());
        }
        Platform::profile("PageList::convert_from_wikidata START", None);
        let batches = self.to_sql_batches(PAGE_BATCH_SIZE*2)?
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

        self.clear_entries()?;
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
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

        join_all(futures)
            .await
            .into_par_iter()
            .filter_map(|r| r.ok())
            .flatten()
            .filter_map(the_fn)
            .for_each(|entry| self.add_entry(entry).unwrap_or(()));

        Platform::profile("PageList::convert_from_wikidata ALL BATCHES COMPLETE", None);
        self.set_wiki(Some(wiki.to_string()))?;
        Platform::profile("PageList::convert_from_wikidata END", None);
        Ok(())
    }

    pub fn regexp_filter(&self, regexp: &str) -> Result<(), String> {
        let regexp_all = "^".to_string() + regexp + "$";
        let is_wikidata = self.is_wikidata();
        if let Ok(re) = Regex::new(&regexp_all) {
            self.retain_entries(&|entry: &PageListEntry| match is_wikidata {
                true => match &entry.get_wikidata_label() {
                    Some(s) => re.is_match(s.as_str()),
                    None => false,
                },
                false => re.is_match(entry.title().pretty()),
            })?
        }
        Ok(())
    }

    async fn search_entry(&self, api: &Api, search: &str, page_id: u32) -> Result<bool, String> {
        let params = [
            ("action".to_string(), "query".to_string()),
            ("list".to_string(), "search".to_string()),
            ("srnamespace".to_string(), "*".to_string()),
            ("srlimit".to_string(), "1".to_string()),
            (
                "srsearch".to_string(),
                format!("pageid:{} {}", page_id, search),
            ),
        ]
        .iter()
        .cloned()
        .collect();
        let result = match api.get_query_api_json(&params).await {
            Ok(result) => result,
            Err(e) => return Err(format!("{:?}", e)),
        };
        let titles = Api::result_array_to_titles(&result);
        Ok(!titles.is_empty())
    }

    pub async fn search_filter(&self, platform: &Platform, search: &str) -> Result<(), String> {
        let max_page_number: usize = 10000;
        if self.len()? > max_page_number {
            return Err(format!(
                "Too many pages ({}), maximum is {}",
                self.len()?,
                &max_page_number
            ));
        }
        let wiki = match self.wiki()? {
            Some(wiki) => wiki,
            None => return Ok(()),
        };
        let page_ids: Vec<u32> = self
            .entries
            .read()
            .map_err(|e| format!("{:?}", e))?
            .iter()
            .filter_map(|entry| entry.page_id())
            .collect();
        let api = platform.state().get_api_for_wiki(wiki).await?;
        let mut futures = vec![];
        page_ids.iter().for_each(|page_id| {
            let fut = self.search_entry(&api, search, page_id.to_owned());
            futures.push(fut);
        });
        let results = join_all(futures).await;

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
            return Err("Filter searches have failed".to_string());
        }

        self.retain_entries(&|entry: &PageListEntry| match entry.page_id() {
            Some(page_id) => retain_page_ids.contains(&page_id),
            None => false,
        })?;
        Ok(())
    }

    pub fn is_wikidata(&self) -> bool {
        self.wiki().unwrap_or(None) == Some("wikidatawiki".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
