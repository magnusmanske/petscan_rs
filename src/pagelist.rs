use crate::app_state::AppState;
use crate::datasource::SQLtuple;
use crate::platform::{Platform, PAGE_BATCH_SIZE};
use mediawiki::api::NamespaceID;
use mediawiki::title::Title;
use mysql as my;
use rayon::prelude::*;
use regex::Regex;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub enum PageListSort {
    Default(bool),
    Title(bool),
    NsTitle(bool),
    Size(bool),
    Date(bool),
    RedlinksCount(bool),
    IncomingLinks(bool),
    FileSize(bool),
    UploadDate(bool),
    Random(bool),
}

impl PageListSort {
    pub fn new_from_params(s: &String, descending: bool) -> Self {
        match s.as_str() {
            "title" => Self::Title(descending),
            "ns_title" => Self::NsTitle(descending),
            "size" => Self::Size(descending),
            "date" => Self::Date(descending),
            "redlinks" => Self::RedlinksCount(descending),
            "incoming_links" => Self::IncomingLinks(descending),
            "filesize" => Self::FileSize(descending),
            "uploaddate" => Self::UploadDate(descending),
            "random" => Self::Random(descending),
            _ => Self::Default(descending),
        }
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct FileUsage {
    title: Title,
    wiki: String,
    namespace_name: String,
}

impl FileUsage {
    pub fn new_from_part(part: &String) -> Option<Self> {
        let mut parts: Vec<&str> = part.split(":").collect();
        if parts.len() < 4 {
            return None;
        }
        let wiki = parts.remove(0);
        let namespace_id = parts.remove(0).parse::<NamespaceID>().ok()?;
        let namespace_name = parts.remove(0);
        let page = parts.join(":");
        Some(Self {
            title: Title::new(&page, namespace_id),
            namespace_name: namespace_name.to_string(),
            wiki: wiki.to_string(),
        })
    }

    pub fn wiki(&self) -> &String {
        &self.wiki
    }

    pub fn title(&self) -> &Title {
        &self.title
    }

    pub fn namespace_name(&self) -> &String {
        &self.namespace_name
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct FileInfo {
    pub file_usage: Vec<FileUsage>,
    pub img_size: Option<usize>,
    pub img_width: Option<usize>,
    pub img_height: Option<usize>,
    pub img_media_type: Option<String>,
    pub img_major_mime: Option<String>,
    pub img_minor_mime: Option<String>,
    pub img_user_text: Option<String>,
    pub img_timestamp: Option<String>,
    pub img_sha1: Option<String>,
}

impl FileInfo {
    pub fn new_from_gil_group(gil_group: &String) -> Self {
        let mut ret = FileInfo::new();
        ret.file_usage = gil_group
            .split("|")
            .filter_map(|part| FileUsage::new_from_part(&part.to_string()))
            .collect();
        ret
    }

    pub fn new() -> Self {
        Self {
            file_usage: vec![],
            img_size: None,
            img_width: None,
            img_height: None,
            img_media_type: None,
            img_major_mime: None,
            img_minor_mime: None,
            img_user_text: None,
            img_timestamp: None,
            img_sha1: None,
        }
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct PageCoordinates {
    pub lat: f64,
    pub lon: f64,
}

impl PageCoordinates {
    pub fn new_from_lat_lon(s: &String) -> Option<Self> {
        let parts: Vec<&str> = s.split(',').collect();
        let lat = parts.get(0)?.parse::<f64>().ok()?;
        let lon = parts.get(1)?.parse::<f64>().ok()?;
        Some(Self { lat: lat, lon: lon })
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone)]
pub struct PageListEntry {
    title: Title,
    pub page_id: Option<usize>,
    pub page_bytes: Option<usize>,
    pub page_timestamp: Option<String>,
    pub page_image: Option<String>,
    pub defaultsort: Option<String>,
    pub disambiguation: Option<bool>,
    pub incoming_links: Option<usize>,
    pub coordinates: Option<PageCoordinates>,
    pub link_count: Option<usize>,
    pub file_info: Option<FileInfo>,
    pub wikidata_item: Option<String>,
    pub wikidata_label: Option<String>,
    pub wikidata_description: Option<String>,
    pub redlink_count: Option<usize>,
}

impl Hash for PageListEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.title.namespace_id().hash(state);
        self.title.pretty().hash(state);
    }
}

impl PartialEq for PageListEntry {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title
    }
}

impl Eq for PageListEntry {}

impl PageListEntry {
    pub fn new(title: Title) -> Self {
        Self {
            title: title,
            wikidata_item: None,
            page_id: None,
            page_bytes: None,
            page_timestamp: None,
            defaultsort: None,
            disambiguation: None,
            incoming_links: None,
            page_image: None,
            coordinates: None,
            link_count: None,
            file_info: None,
            wikidata_label: None,
            wikidata_description: None,
            redlink_count: None,
        }
    }

    pub fn title(&self) -> &Title {
        &self.title
    }

    pub fn compare(&self, other: &Self, sorter: &PageListSort) -> Ordering {
        match sorter {
            PageListSort::Default(d) => self.compare_by_page_id(other, *d),
            PageListSort::Title(d) => self.compare_by_title(other, *d),
            PageListSort::NsTitle(d) => self.compare_by_ns_title(other, *d),
            PageListSort::Size(d) => self.compare_by_size(other, *d),
            PageListSort::IncomingLinks(d) => self.compare_by_incoming(other, *d),
            PageListSort::Date(d) => self.compare_by_date(other, *d),
            PageListSort::UploadDate(d) => self.compare_by_upload_date(other, *d),
            PageListSort::FileSize(d) => self.compare_by_file_size(other, *d),
            PageListSort::RedlinksCount(d) => self.compare_by_redlinks(other, *d),
            PageListSort::Random(d) => self.compare_by_random(other, *d),
        }
    }

    fn compare_by_page_id(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.page_id, &other.page_id, descending)
    }

    fn compare_by_redlinks(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.redlink_count, &other.redlink_count, descending)
    }

    fn compare_by_random(
        self: &PageListEntry,
        _other: &PageListEntry,
        _descending: bool,
    ) -> Ordering {
        if rand::random() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }

    fn compare_by_size(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        self.compare_by_opt(&self.page_bytes, &other.page_bytes, descending)
    }

    fn compare_by_incoming(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.incoming_links, &other.incoming_links, descending)
    }

    fn compare_by_date(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        self.compare_by_opt(&self.page_timestamp, &other.page_timestamp, descending)
    }

    fn compare_by_file_size(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        match (&self.file_info, &other.file_info) {
            (Some(f1), Some(f2)) => self.compare_by_opt(&f1.img_size, &f2.img_size, descending),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    fn compare_by_upload_date(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        match (&self.file_info, &other.file_info) {
            (Some(f1), Some(f2)) => {
                self.compare_by_opt(&f1.img_timestamp, &f2.img_timestamp, descending)
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    fn compare_by_opt<T: PartialOrd>(
        &self,
        mine: &Option<T>,
        other: &Option<T>,
        descending: bool,
    ) -> Ordering {
        self.compare_order(
            match (mine, other) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Less),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            descending,
        )
    }

    fn compare_by_ns_title(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        if self.title.namespace_id() == other.title.namespace_id() {
            self.compare_by_title(other, descending)
        } else {
            self.compare_order(
                self.title
                    .namespace_id()
                    .partial_cmp(&other.title.namespace_id())
                    .unwrap_or(Ordering::Less),
                descending,
            )
        }
    }

    fn compare_by_title(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        self.compare_order(
            self.title
                .pretty()
                .partial_cmp(other.title.pretty())
                .unwrap_or(Ordering::Less),
            descending,
        )
    }

    fn compare_order(&self, ret: Ordering, descending: bool) -> Ordering {
        if descending {
            ret.reverse()
        } else {
            ret
        }
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct PageList {
    wiki: Option<String>,
    pub entries: HashSet<PageListEntry>,
}

impl PageList {
    pub fn new_from_wiki(wiki: &str) -> Self {
        Self {
            wiki: Some(wiki.to_string()),
            entries: HashSet::new(),
        }
    }

    pub fn new_from_vec(wiki: &str, entries: Vec<PageListEntry>) -> Self {
        let mut entries_hashset: HashSet<PageListEntry> = HashSet::new();
        entries.iter().for_each(|e| {
            entries_hashset.insert(e.to_owned());
        });
        Self {
            wiki: Some(wiki.to_string()),
            entries: entries_hashset,
        }
    }

    pub fn set_wiki(&mut self, wiki: &Option<String>) {
        println!("NEW WIKI: {:?}", wiki);
        self.wiki = wiki.to_owned();
    }

    pub fn wiki(&self) -> &Option<String> {
        &self.wiki
    }

    pub fn get_sorted_vec(&self, sorter: PageListSort) -> Vec<PageListEntry> {
        let mut ret: Vec<PageListEntry> = self.entries.iter().cloned().collect();
        ret.par_sort_by(|a, b| a.compare(b, &sorter));
        ret
    }

    pub fn group_by_namespace(&self) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        self.entries.iter().for_each(|entry| {
            if !ret.contains_key(&entry.title.namespace_id()) {
                ret.insert(entry.title.namespace_id(), vec![]);
            }
            match ret.get_mut(&entry.title.namespace_id()) {
                Some(x) => {
                    x.push(entry.title.with_underscores().to_string());
                }
                None => {}
            }
        });
        ret
    }

    pub fn swap_entries(&mut self, other: &mut PageList) {
        std::mem::swap(&mut self.entries, &mut other.entries);
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn add_entry(&mut self, entry: PageListEntry) {
        self.entries.replace(entry);
    }

    pub fn set_entries_from_vec(&mut self, entries: Vec<PageListEntry>) {
        entries.iter().for_each(|e| {
            self.entries.insert(e.to_owned());
        });
    }

    fn check_before_merging(
        &self,
        pagelist: Option<PageList>,
        platform: Option<&Platform>,
    ) -> Result<HashSet<PageListEntry>, String> {
        match pagelist {
            Some(mut pagelist) => {
                let my_wiki = match &self.wiki {
                    Some(wiki) => wiki,
                    None => {
                        return Err(
                            "PageList::check_before_merging self.wiki is not set".to_string()
                        )
                    }
                };
                if pagelist.wiki.is_none() {
                    return Err(
                        "PageList::check_before_merging pagelist.wiki is not set".to_string()
                    );
                }
                if self.wiki != pagelist.wiki {
                    match platform {
                        Some(platform) => {
                            pagelist.convert_to_wiki(&my_wiki, platform)?;
                        }
                        None => {
                            return Err(format!(
                                "PageList::check_before_merging wikis are not identical: {}/{}",
                                &self
                                    .wiki
                                    .as_ref()
                                    .unwrap_or(&"PageList::check_before_merging:1".to_string()),
                                &pagelist
                                    .wiki
                                    .unwrap_or("PageList::check_before_merging:2".to_string())
                            ))
                        }
                    }
                }
                Ok(pagelist.entries)
            }
            None => Err("PageList::check_before_merging pagelist is None".to_string()),
        }
    }

    pub fn union(
        &mut self,
        pagelist: Option<PageList>,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        let other_entries = self.check_before_merging(pagelist, platform)?;
        self.entries = self.entries.union(&other_entries).cloned().collect();
        Ok(())
    }

    pub fn intersection(
        &mut self,
        pagelist: Option<PageList>,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        let other_entries = self.check_before_merging(pagelist, platform)?;
        self.entries = self.entries.intersection(&other_entries).cloned().collect();
        Ok(())
    }

    pub fn difference(
        &mut self,
        pagelist: Option<PageList>,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        let other_entries = self.check_before_merging(pagelist, platform)?;
        self.entries = self.entries.difference(&other_entries).cloned().collect();
        Ok(())
    }

    pub fn to_sql_batches(&self, chunk_size: usize) -> Vec<SQLtuple> {
        let mut ret: Vec<SQLtuple> = vec![];
        if self.is_empty() {
            return ret;
        }
        let by_ns = self.group_by_namespace();
        for (nsid, titles) in by_ns {
            titles.chunks(chunk_size).for_each(|chunk| {
                let mut sql = Platform::prep_quote(&chunk.to_vec());
                sql.0 = format!("(page_namespace={} AND page_title IN({}))", nsid, &sql.0);
                ret.push(sql);
            });
        }
        ret
    }

    pub fn to_sql_batches_namespace(
        &self,
        chunk_size: usize,
        namespace_id: NamespaceID,
    ) -> Vec<SQLtuple> {
        let mut ret: Vec<SQLtuple> = vec![];
        if self.is_empty() {
            return ret;
        }
        let by_ns = self.group_by_namespace();
        for (nsid, titles) in by_ns {
            if nsid == namespace_id {
                titles.chunks(chunk_size).for_each(|chunk| {
                    let mut sql = Platform::prep_quote(&chunk.to_vec());
                    sql.0 = format!("(page_namespace={} AND page_title IN({}))", nsid, &sql.0);
                    ret.push(sql);
                });
            }
        }
        ret
    }

    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }

    pub fn replace_entries(&mut self, other: &PageList) {
        other.entries.iter().for_each(|entry| {
            self.entries.replace(entry.to_owned());
        });
    }

    /// Runs batched queries for process_batch_results and annotate_batch_results
    pub fn run_batch_queries(
        &self,
        state: Arc<AppState>,
        batches: Vec<SQLtuple>,
    ) -> Result<Mutex<Vec<my::Row>>, String> {
        // TODO?: "SET STATEMENT max_statement_time = 300 FOR SELECT..."
        let rows: Mutex<Vec<my::Row>> = Mutex::new(vec![]);
        let error: Mutex<Option<String>> = Mutex::new(None);

        batches.par_iter().for_each(|sql| {
            // Get DB connection
            let db_user_pass = match state.get_db_mutex().lock() {
                Ok(db) => db,
                Err(e) => {
                    *error.lock().unwrap() =
                        Some(format!("PageList::run_batch_queries: Bad mutex: {:?}", e));
                    return;
                }
            };
            let wiki = match self.wiki.as_ref() {
                Some(wiki) => wiki,
                None => {
                    *error.lock().unwrap() = Some(format!("PageList::run_batch_queries: No wiki"));
                    return;
                }
            };
            let mut conn = match state.get_wiki_db_connection(&db_user_pass, &wiki) {
                Ok(conn) => conn,
                Err(e) => {
                    *error.lock().unwrap() = Some(format!(
                        "PageList::run_batch_queries: Can't get wiki db connection: {:?}",
                        e
                    ));
                    return;
                }
            };

            // Run query
            let result = match conn.prep_exec(&sql.0, &sql.1) {
                Ok(r) => r,
                Err(e) => {
                    *error.lock().unwrap() = Some(format!("ERROR: {:?}", e));
                    return;
                }
            };

            // Add to row list
            let mut rows_lock = rows.lock().unwrap();
            result
                .filter_map(|row| row.ok())
                .for_each(|row| rows_lock.push(row.clone()));
        });

        // Check error
        match error.lock() {
            Ok(error) => match &*error {
                Some(e) => return Err(e.to_string()),
                None => {}
            },
            Err(e) => return Err(e.to_string()),
        }

        Ok(rows)
    }

    /// Adds/replaces entries based on SQL query batch results.
    pub fn process_batch_results(
        &mut self,
        state: Arc<AppState>,
        batches: Vec<SQLtuple>,
        f: &dyn Fn(my::Row) -> Option<PageListEntry>,
    ) -> Result<(), String> {
        let rows = self.run_batch_queries(state, batches)?;

        // Rows to entries
        match rows.lock() {
            Ok(rows) => {
                rows.iter().for_each(|row| match f(row.to_owned()) {
                    Some(entry) => self.add_entry(entry),
                    None => {}
                });
            }
            Err(e) => return Err(e.to_string()),
        }
        Ok(())
    }

    /// Similar to `process_batch_results` but to modify existing entrties. Does not add new entries.
    pub fn annotate_batch_results(
        &mut self,
        state: Arc<AppState>,
        batches: Vec<SQLtuple>,
        col_title: usize,
        col_ns: usize,
        f: &dyn Fn(my::Row, &mut PageListEntry),
    ) -> Result<(), String> {
        let rows = self.run_batch_queries(state, batches)?;

        // Rows to entries
        match rows.lock() {
            Ok(rows) => {
                rows.iter().for_each(|row| {
                    let page_title = match row.get(col_title) {
                        Some(title) => match title {
                            my::Value::Bytes(uv) => match String::from_utf8(uv) {
                                Ok(s) => s,
                                Err(_) => return,
                            },
                            _ => return,
                        },
                        None => return,
                    };
                    let namespace_id = match row.get(col_ns) {
                        Some(title) => match title {
                            my::Value::Int(i) => i,
                            _ => return,
                        },
                        None => return,
                    };

                    let tmp_entry = PageListEntry::new(Title::new(&page_title, namespace_id));
                    let mut entry = match self.entries.get(&tmp_entry) {
                        Some(e) => (*e).clone(),
                        None => return,
                    };

                    f(row.clone(), &mut entry);
                    self.add_entry(entry);
                });
            }
            Err(e) => return Err(e.to_string()),
        };
        Ok(())
    }

    pub fn load_missing_metadata(
        &mut self,
        wikidata_language: Option<String>,
        platform: &Platform,
    ) -> Result<(), String> {
        let batches: Vec<SQLtuple> = self
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|mut sql_batch| {
                sql_batch.0 =
                    "SELECT page_title,page_namespace,page_id,page_len,page_touched FROM page WHERE"
                        .to_string() + &sql_batch.0;
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        self.annotate_batch_results(
            platform.state(),
            batches,
            0,
            1,
            &|row: my::Row, entry: &mut PageListEntry| {
                let (_page_title, _page_namespace, page_id, page_len, page_touched) =
                    my::from_row::<(String, NamespaceID, usize, usize, String)>(row);
                entry.page_id = Some(page_id);
                entry.page_bytes = Some(page_len);
                entry.page_timestamp = Some(page_touched);
            },
        )?;

        // All done
        if self.wiki != Some("wikidatawiki".to_string()) || wikidata_language.is_none() {
            return Ok(());
        }

        // No need to load labels for WDFIST mode
        if !platform.has_param("regexp_filter") && platform.has_param("wdf_main") {
            return Ok(());
        }

        match wikidata_language {
            Some(wikidata_language) => {
                self.add_wikidata_labels_for_namespace(0, "item", &wikidata_language, platform)?;
                self.add_wikidata_labels_for_namespace(
                    120,
                    "property",
                    &wikidata_language,
                    platform,
                )?;
            }
            None => {}
        }
        Ok(())
    }

    fn add_wikidata_labels_for_namespace(
        &mut self,
        namespace_id: NamespaceID,
        entity_type: &str,
        wikidata_language: &String,
        platform: &Platform,
    ) -> Result<(), String> {
        let batches: Vec<SQLtuple> = self
            .to_sql_batches_namespace(PAGE_BATCH_SIZE,namespace_id)
            .par_iter_mut()
            .map(|mut sql_batch| {
                // entity_type and namespace_id are "database safe"
                sql_batch.0 = format!("SELECT term_full_entity_id,{} AS dummy_namespace,term_text,term_type FROM wb_terms WHERE term_entity_type='{}' AND term_language=? AND term_full_entity_id IN (",namespace_id,&entity_type);
                sql_batch.0 += &sql_batch.1.iter().map(|_|"?").collect::<Vec<&str>>().join(",") ;
                sql_batch.0 += ")" ;
                sql_batch.1.insert(0,wikidata_language.to_string());
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        self.annotate_batch_results(
            platform.state(),
            batches,
            0,
            1,
            &|row: my::Row, entry: &mut PageListEntry| {
                let (_page_title, _page_namespace, term_text, term_type) =
                    my::from_row::<(String, NamespaceID, String, String)>(row);
                match term_type.as_str() {
                    "label" => entry.wikidata_label = Some(term_text),
                    "description" => entry.wikidata_description = Some(term_text),
                    _ => {}
                }
            },
        )
    }

    pub fn convert_to_wiki(&mut self, wiki: &str, platform: &Platform) -> Result<(), String> {
        // Already that wiki?
        if self.wiki == None || self.wiki == Some(wiki.to_string()) {
            return Ok(());
        }
        self.convert_to_wikidata(platform)?;
        if wiki != "wikidatawiki" {
            self.convert_from_wikidata(wiki, platform)?;
        }
        Ok(())
    }

    fn convert_to_wikidata(&mut self, platform: &Platform) -> Result<(), String> {
        if self.wiki == None || self.wiki == Some("wikidatawiki".to_string()) {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = self.to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql|{
                sql.0 = "SELECT pp_value FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ".to_owned()+&sql.0;
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();
        self.entries.clear();
        self.process_batch_results(platform.state(), batches, &|row: my::Row| {
            let pp_value: String = my::from_row(row);
            Some(PageListEntry::new(Title::new(&pp_value, 0)))
        })?;
        self.wiki = Some("wikidatawiki".to_string());
        Ok(())
    }

    fn convert_from_wikidata(&mut self, wiki: &str, platform: &Platform) -> Result<(), String> {
        if self.wiki == None || self.wiki != Some("wikidatawiki".to_string()) {
            return Ok(());
        }
        let batches = self.to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql|{
                sql.0 = "SELECT ips_site_page FROM wb_items_per_site,page WHERE ips_item_id=substr(page_title,2)*1 AND ".to_owned()+&sql.0+" AND ips_site_id=?";
                sql.1.push(wiki.to_string());
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        self.entries.clear();
        let api = platform.state().get_api_for_wiki(wiki.to_string())?;
        self.process_batch_results(platform.state(), batches, &|row: my::Row| {
            let ips_site_page: String = my::from_row(row);
            Some(PageListEntry::new(Title::new_from_full(
                &ips_site_page,
                &api,
            )))
        })?;
        self.wiki = Some(wiki.to_string());
        Ok(())
    }

    pub fn regexp_filter(&mut self, regexp: &String) {
        let regexp_all = "^".to_string() + regexp + "$";
        let is_wikidata = self.is_wikidata();
        match Regex::new(&regexp_all) {
            Ok(re) => self.entries.retain(|entry| match is_wikidata {
                true => match &entry.wikidata_label {
                    Some(s) => re.is_match(s.as_str()),
                    None => false,
                },
                false => re.is_match(entry.title().pretty().as_str()),
            }),
            _ => {}
        }
    }

    pub fn is_wikidata(&self) -> bool {
        self.wiki == Some("wikidatawiki".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_list_sort() {
        assert_eq!(
            PageListSort::new_from_params(&"incoming_links".to_string(), true),
            PageListSort::IncomingLinks(true)
        );
        assert_eq!(
            PageListSort::new_from_params(&"ns_title".to_string(), false),
            PageListSort::NsTitle(false)
        );
        assert_eq!(
            PageListSort::new_from_params(&"this is not a sort parameter".to_string(), true),
            PageListSort::Default(true)
        );
    }

    #[test]
    fn file_usage() {
        // 3 instead of 4 parts
        assert_eq!(
            FileUsage::new_from_part(&"the_wiki:7:the_namespace_name".to_string()),
            None
        );
        // String instead of namespace ID
        assert_eq!(
            FileUsage::new_from_part(
                &"the_wiki:the_namespace_id:the_namespace_name:The:page".to_string()
            ),
            None
        );
        // This should work
        let fu = FileUsage::new_from_part(&"the_wiki:7:the_namespace_name:The:page".to_string())
            .unwrap();
        assert_eq!(fu.wiki(), "the_wiki");
        assert_eq!(fu.namespace_name(), "the_namespace_name");
        assert_eq!(*fu.title(), Title::new("The:page", 7));
    }

    #[test]
    fn file_info() {
        let fu = FileUsage::new_from_part(&"the_wiki:7:the_namespace_name:The:page".to_string())
            .unwrap();
        let fi = FileInfo::new_from_gil_group(
            &"|somesuch|the_wiki:7:the_namespace_name:The:page|the_wiki:7:the_namespace_name"
                .to_string(),
        );
        assert_eq!(fi.file_usage, vec![fu]);
    }

    #[test]
    fn lat_lon() {
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"something".to_string()),
            None
        );
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"-0.1234".to_string()),
            None
        );
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"-0.1234,A".to_string()),
            None
        );
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"-0.1234,2.345".to_string()),
            Some(PageCoordinates {
                lat: -0.1234,
                lon: 2.345
            })
        );
    }
}
