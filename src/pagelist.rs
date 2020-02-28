use crate::app_state::AppState;
use crate::datasource::SQLtuple;
use crate::platform::{Platform, PAGE_BATCH_SIZE};
use mysql as my;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};
use wikibase::mediawiki::api::NamespaceID;
use wikibase::mediawiki::title::Title;

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
        let mut parts = part.split(":");
        let wiki = parts.next()?;
        let namespace_id = parts.next()?.parse::<NamespaceID>().ok()?;
        let namespace_name = parts.next()?;
        let page = parts.collect::<Vec<&str>>().join(":");
        if page.is_empty() {
            return None;
        }
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
        let mut ret = Self::new();
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

pub type LinkCount = u32;

#[derive(Debug, Clone, PartialEq)]
pub enum TriState {
    Yes,
    No,
    Unknown,
}

impl TriState {
    pub fn as_json(&self) -> Value {
        match self {
            Self::Yes => json!(true),
            Self::No => json!(false),
            Self::Unknown => Value::Null,
        }
    }

    pub fn as_option_bool(&self) -> Option<bool> {
        match self {
            Self::Yes => Some(true),
            Self::No => Some(false),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageListEntry {
    title: Title,
    pub disambiguation: TriState,
    pub page_id: Option<u32>,
    pub page_bytes: Option<u32>,
    pub incoming_links: Option<LinkCount>,
    pub link_count: Option<LinkCount>,
    pub redlink_count: Option<LinkCount>,
    page_timestamp: Option<Box<String>>,
    page_image: Option<Box<String>>,
    wikidata_item: Option<Box<String>>,
    wikidata_label: Option<Box<String>>,
    wikidata_description: Option<Box<String>>,
    defaultsort: Option<Box<String>>,
    coordinates: Option<Box<PageCoordinates>>,
    file_info: Option<Box<FileInfo>>,
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
            disambiguation: TriState::Unknown,
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

    pub fn get_file_info(&self) -> Option<FileInfo> {
        match &self.file_info {
            Some(file_info) => Some(*(file_info.clone())),
            None => None,
        }
    }

    pub fn set_file_info(&mut self, file_info_option: Option<FileInfo>) {
        self.file_info = match file_info_option {
            Some(file_info) => Some(Box::new(file_info)),
            None => None,
        }
    }

    pub fn get_coordinates(&self) -> Option<PageCoordinates> {
        match &self.coordinates {
            Some(coordinates) => Some(*(coordinates.clone())),
            None => None,
        }
    }

    pub fn set_coordinates(&mut self, coordinates_option: Option<PageCoordinates>) {
        self.coordinates = match coordinates_option {
            Some(coordinates) => Some(Box::new(coordinates)),
            None => None,
        }
    }

    pub fn get_defaultsort(&self) -> Option<String> {
        match &self.defaultsort {
            Some(defaultsort) => Some(*(defaultsort.clone())),
            None => None,
        }
    }

    pub fn set_defaultsort(&mut self, defaultsort_option: Option<String>) {
        self.defaultsort = match defaultsort_option {
            Some(defaultsort) => Some(Box::new(defaultsort)),
            None => None,
        }
    }

    pub fn get_wikidata_description(&self) -> Option<String> {
        match &self.wikidata_description {
            Some(wikidata_description) => Some(*(wikidata_description.clone())),
            None => None,
        }
    }

    pub fn set_wikidata_description(&mut self, wikidata_description_option: Option<String>) {
        self.wikidata_description = match wikidata_description_option {
            Some(wikidata_description) => Some(Box::new(wikidata_description)),
            None => None,
        }
    }

    pub fn get_wikidata_label(&self) -> Option<String> {
        match &self.wikidata_label {
            Some(wikidata_label) => Some(*(wikidata_label.clone())),
            None => None,
        }
    }

    pub fn set_wikidata_label(&mut self, wikidata_label_option: Option<String>) {
        self.wikidata_label = match wikidata_label_option {
            Some(wikidata_label) => Some(Box::new(wikidata_label)),
            None => None,
        }
    }

    pub fn get_wikidata_item(&self) -> Option<String> {
        match &self.wikidata_item {
            Some(wikidata_item) => Some(*(wikidata_item.clone())),
            None => None,
        }
    }

    pub fn set_wikidata_item(&mut self, wikidata_item_option: Option<String>) {
        self.wikidata_item = match wikidata_item_option {
            Some(wikidata_item) => Some(Box::new(wikidata_item)),
            None => None,
        }
    }

    pub fn get_page_image(&self) -> Option<String> {
        match &self.page_image {
            Some(page_image) => Some(*(page_image.clone())),
            None => None,
        }
    }

    pub fn set_page_image(&mut self, page_image_option: Option<String>) {
        self.page_image = match page_image_option {
            Some(page_image) => Some(Box::new(page_image)),
            None => None,
        }
    }

    pub fn get_page_timestamp(&self) -> Option<String> {
        match &self.page_timestamp {
            Some(page_timestamp) => Some(*(page_timestamp.clone())),
            None => None,
        }
    }

    pub fn set_page_timestamp(&mut self, page_timestamp_option: Option<String>) {
        self.page_timestamp = match page_timestamp_option {
            Some(page_timestamp) => Some(Box::new(page_timestamp)),
            None => None,
        }
    }

    pub fn title(&self) -> &Title {
        &self.title
    }

    pub fn compare(&self, other: &Self, sorter: &PageListSort, is_wikidata: bool) -> Ordering {
        match sorter {
            PageListSort::Default(d) => self.compare_by_page_id(other, *d),
            PageListSort::Title(d) => {
                if is_wikidata {
                    self.compare_by_label(other, *d)
                } else {
                    self.compare_by_title(other, *d)
                }
            }
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
        self.compare_by_opt(
            &self.get_page_timestamp(),
            &other.get_page_timestamp(),
            descending,
        )
    }

    fn compare_by_file_size(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        match (&self.get_file_info(), &other.get_file_info()) {
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
        match (&self.get_file_info(), &other.get_file_info()) {
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

    fn compare_by_label(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        let l1 = self
            .get_wikidata_label()
            .or_else(|| Some(self.title.pretty().to_owned()))
            .unwrap()
            .to_lowercase();
        let l2 = other
            .get_wikidata_label()
            .or_else(|| Some(self.title.pretty().to_owned()))
            .unwrap()
            .to_lowercase();
        self.compare_order(l1.partial_cmp(&l2).unwrap_or(Ordering::Less), descending)
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

#[derive(Debug, Clone)]
pub struct PageList {
    wiki: Arc<RwLock<Option<String>>>,
    entries: Arc<RwLock<HashSet<PageListEntry>>>,
}

impl PartialEq for PageList {
    fn eq(&self, other: &Self) -> bool {
        self.wiki() == other.wiki()
            && *self.entries.read().unwrap() == *other.entries.read().unwrap()
    }
}

impl PageList {
    pub fn new_from_wiki(wiki: &str) -> Self {
        Self {
            wiki: Arc::new(RwLock::new(Some(wiki.to_string()))),
            entries: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn new_from_wiki_with_capacity(wiki: &str, capacity: usize) -> Self {
        Self {
            wiki: Arc::new(RwLock::new(Some(wiki.to_string()))),
            entries: Arc::new(RwLock::new(HashSet::with_capacity(capacity))),
        }
    }

    pub fn clear(&mut self) {
        *self.wiki.write().unwrap() = None;
        self.entries.write().unwrap().clear();
    }

    pub fn set_from(&self, other: Self) {
        *self.wiki.write().unwrap() = other.wiki.read().unwrap().clone();
        *self.entries.write().unwrap() = other.entries.read().unwrap().clone();
    }

    pub fn entries(&self) -> Arc<RwLock<HashSet<PageListEntry>>> {
        self.entries.clone()
    }

    pub fn set_entries(&self, entries: HashSet<PageListEntry>) {
        *self.entries.write().unwrap() = entries
    }

    pub fn retain_entries(&self, f: &dyn Fn(&PageListEntry) -> bool) {
        self.entries.write().unwrap().retain(f);
    }

    pub fn set_wiki(&self, wiki: Option<String>) {
        *self.wiki.write().unwrap() = wiki;
    }

    pub fn wiki(&self) -> Option<String> {
        self.wiki.read().unwrap().clone()
    }

    pub fn drain_into_sorted_vec(&self, sorter: PageListSort) -> Vec<PageListEntry> {
        let mut ret: Vec<PageListEntry> = self.entries.write().unwrap().drain().collect();
        ret.par_sort_by(|a, b| a.compare(b, &sorter, self.is_wikidata()));
        ret
    }

    pub fn group_by_namespace(&self) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        self.entries.read().unwrap().iter().for_each(|entry| {
            ret.entry(entry.title.namespace_id())
                .or_insert(vec![])
                .push(entry.title.with_underscores().to_string());
        });
        ret
        /*
        // THIS IS A PARALLEL VERSION BUT REQUIRES MUTEX. FASTER?
        let ret = Mutex::new(HashMap::new());
        self.entries.read().unwrap().par_iter().for_each(|entry| {
            ret.lock()
                .unwrap()
                .entry(entry.title.namespace_id())
                .or_insert(vec![])
                .push(entry.title.with_underscores().to_string());
        });
        ret.into_inner().unwrap()
        */
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    pub fn add_entry(&self, entry: PageListEntry) {
        self.entries.write().unwrap().replace(entry);
    }

    fn check_before_merging(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<Arc<RwLock<HashSet<PageListEntry>>>, String> {
        let my_wiki = match self.wiki() {
            Some(wiki) => wiki,
            None => return Err("PageList::check_before_merging self.wiki is not set".to_string()),
        };
        if pagelist.wiki().is_none() {
            return Err("PageList::check_before_merging pagelist.wiki is not set".to_string());
        }
        if self.wiki() != pagelist.wiki() {
            match platform {
                Some(platform) => {
                    pagelist.convert_to_wiki(&my_wiki, platform)?;
                }
                None => {
                    return Err(format!(
                        "PageList::check_before_merging wikis are not identical: {}/{}",
                        self.wiki()
                            .unwrap_or("PageList::check_before_merging:1".to_string()),
                        pagelist
                            .wiki()
                            .unwrap_or("PageList::check_before_merging:2".to_string())
                    ))
                }
            }
        }
        Ok(pagelist.entries())
    }

    pub fn union(&self, pagelist: &PageList, platform: Option<&Platform>) -> Result<(), String> {
        let other_entries = self.check_before_merging(&pagelist, platform)?;
        let me = self.entries.read().unwrap();
        let mut tmp_vec: Vec<PageListEntry> = other_entries
            .read()
            .unwrap()
            .par_iter()
            .filter(|x| !me.contains(&x))
            .cloned()
            .collect();
        let mut me = self.entries.write().unwrap();
        tmp_vec.drain(..).for_each(|x| {
            me.replace(x);
        });
        Ok(())
    }

    pub fn intersection(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        let other_entries = self.check_before_merging(&pagelist, platform)?;
        let other_entries = other_entries.read().unwrap();
        self.entries
            .write()
            .unwrap()
            .retain(|x| other_entries.contains(&x));
        Ok(())
    }

    pub fn difference(
        &self,
        pagelist: &PageList,
        platform: Option<&Platform>,
    ) -> Result<(), String> {
        let other_entries = self.check_before_merging(&pagelist, platform)?;
        let other_entries = other_entries.read().unwrap();
        self.entries
            .write()
            .unwrap()
            .retain(|x| !other_entries.contains(&x));
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
                let mut sql = Platform::prep_quote(&chunk);
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
                    let mut sql = Platform::prep_quote(&chunk);
                    sql.0 = format!("(page_namespace={} AND page_title IN({}))", nsid, &sql.0);
                    ret.push(sql);
                });
            }
        }
        ret
    }

    pub fn clear_entries(&self) {
        self.entries.write().unwrap().clear();
    }

    pub fn replace_entries(&self, other: &PageList) {
        other.entries.read().unwrap().iter().for_each(|entry| {
            self.entries.write().unwrap().replace(entry.to_owned());
        });
    }

    fn run_batch_query(
        &self,
        state: Arc<AppState>,
        sql: &SQLtuple,
        wiki: &String,
    ) -> Result<Vec<my::Row>, String> {
        let db_user_pass = state
            .get_db_mutex()
            .lock()
            .map_err(|e| format!("PageList::run_batch_query: Bad mutex: {:?}", e))?;
        let mut conn = state
            .get_wiki_db_connection(&db_user_pass, &wiki)
            .map_err(|e| format!("PageList::run_batch_query: get_wiki_db_connection: {:?}", e))?;
        let result = conn
            .prep_exec(&sql.0, &sql.1)
            .map_err(|e| format!("PageList::run_batch_queries: SQL query error: {:?}", e))?;
        Ok(result.filter_map(|row| row.ok()).collect())
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
        let wiki = self
            .wiki()
            .ok_or(format!("PageList::run_batch_queries: No wiki"))?;

        batches.par_iter().for_each(|sql| {
            match self.run_batch_query(state.clone(), sql, &wiki) {
                Ok(mut data) => rows.lock().unwrap().append(&mut data),
                Err(e) => *error.lock().unwrap() = Some(e),
            };
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
        &self,
        state: Arc<AppState>,
        batches: Vec<SQLtuple>,
        f: &dyn Fn(my::Row) -> Option<PageListEntry>,
    ) -> Result<(), String> {
        self.run_batch_queries(state, batches)?
            .lock()
            .map_err(|e| e.to_string())?
            .iter()
            .filter_map(|row| f(row.to_owned()))
            .for_each(|entry| self.add_entry(entry));
        Ok(())
    }

    pub fn string_from_row(row: &my::Row, col_num: usize) -> Option<String> {
        match row.get(col_num)? {
            my::Value::Bytes(uv) => String::from_utf8(uv).ok(),
            _ => return None,
        }
    }

    fn entry_from_row(
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

    /// Similar to `process_batch_results` but to modify existing entrties. Does not add new entries.
    pub fn annotate_batch_results(
        &self,
        state: Arc<AppState>,
        batches: Vec<SQLtuple>,
        col_title: usize,
        col_ns: usize,
        f: &dyn Fn(my::Row, &mut PageListEntry),
    ) -> Result<(), String> {
        self.run_batch_queries(state, batches)?
            .lock()
            .map_err(|e| e.to_string())?
            .iter()
            .filter_map(|row| {
                self.entry_from_row(row, col_title, col_ns)
                    .map(|entry| (row, entry))
            })
            .filter_map(|(row, entry)| {
                self.entries
                    .read()
                    .unwrap()
                    .get(&entry)
                    .map(|e| (row, e.clone()))
            })
            .for_each(|(row, mut entry)| {
                f(row.clone(), &mut entry);
                self.add_entry(entry);
            });
        Ok(())
    }

    fn load_missing_page_metadata(&self, platform: &Platform) -> Result<(), String> {
        if self.entries.read().unwrap().par_iter().any(|entry| {
            entry.page_id.is_none()
                || entry.page_bytes.is_none()
                || entry.get_page_timestamp().is_none()
        }) {
            let batches: Vec<SQLtuple> = self
                .to_sql_batches(PAGE_BATCH_SIZE)
                .par_iter_mut()
                .map(|mut sql_batch| {
                    sql_batch.0 =
                        "SELECT page_title,page_namespace,page_id,page_len,(SELECT rev_timestamp FROM revision WHERE rev_id=page_latest LIMIT 1) AS page_touched FROM page WHERE"
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
                        my::from_row::<(String, NamespaceID, u32, u32, String)>(row);
                    entry.page_id = Some(page_id);
                    entry.page_bytes = Some(page_len);
                    entry.set_page_timestamp(Some(page_touched));
                },
            )?;
        }
        Ok(())
    }

    pub fn load_missing_metadata(
        &self,
        wikidata_language: Option<String>,
        platform: &Platform,
    ) -> Result<(), String> {
        self.load_missing_page_metadata(platform)?;

        // All done
        if !self.is_wikidata() || wikidata_language.is_none() {
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
        &self,
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
                sql_batch.0 += &sql_batch.1.par_iter().map(|_|"?").collect::<Vec<&str>>().join(",") ;
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
                    "label" => entry.set_wikidata_label(Some(term_text)),
                    "description" => entry.set_wikidata_description(Some(term_text)),
                    _ => {}
                }
            },
        )
    }

    pub fn convert_to_wiki(&self, wiki: &str, platform: &Platform) -> Result<(), String> {
        // Already that wiki?
        if self.wiki() == None || self.wiki() == Some(wiki.to_string()) {
            return Ok(());
        }
        self.convert_to_wikidata(platform)?;
        if wiki != "wikidatawiki" {
            self.convert_from_wikidata(wiki, platform)?;
        }
        Ok(())
    }

    fn convert_to_wikidata(&self, platform: &Platform) -> Result<(), String> {
        if self.wiki() == None || self.is_wikidata() {
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
        self.process_batch_results(platform.state(), batches, &|row: my::Row| {
            let pp_value: String = my::from_row(row);
            Some(PageListEntry::new(Title::new(&pp_value, 0)))
        })?;
        self.set_wiki(Some("wikidatawiki".to_string()));
        Ok(())
    }

    fn convert_from_wikidata(&self, wiki: &str, platform: &Platform) -> Result<(), String> {
        if !self.is_wikidata() {
            return Ok(());
        }
        let batches = self.to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql|{
                sql.0 = "SELECT ips_site_page FROM wb_items_per_site,page WHERE ips_item_id=substr(page_title,2)*1 AND ".to_owned()+&sql.0+" AND ips_site_id=?";
                sql.1.push(wiki.to_string());
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        self.clear_entries();
        let api = platform.state().get_api_for_wiki(wiki.to_string())?;
        self.process_batch_results(platform.state(), batches, &|row: my::Row| {
            let ips_site_page: String = my::from_row(row);
            Some(PageListEntry::new(Title::new_from_full(
                &ips_site_page,
                &api,
            )))
        })?;
        self.set_wiki(Some(wiki.to_string()));
        Ok(())
    }

    pub fn regexp_filter(&self, regexp: &String) {
        let regexp_all = "^".to_string() + regexp + "$";
        let is_wikidata = self.is_wikidata();
        match Regex::new(&regexp_all) {
            Ok(re) => self.retain_entries(&|entry: &PageListEntry| match is_wikidata {
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
        self.wiki() == Some("wikidatawiki".to_string())
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
