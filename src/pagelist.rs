use crate::datasource::SQLtuple;
use crate::platform::{Platform, PAGE_BATCH_SIZE};
use mediawiki::api::NamespaceID;
use mediawiki::title::Title;
use mysql as my;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
//use rayon::prelude::*;

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone)]
pub struct FileUsage {
    title: Title,
    wiki: String,
    namespace_name: String,
}

impl FileUsage {
    pub fn new_from_part(part: &String) -> Self {
        let mut parts: Vec<&str> = part.split(":").collect();
        let wiki = parts.remove(0);
        let namespace_id = parts.remove(0).parse::<NamespaceID>().unwrap();
        let namespace_name = parts.remove(0);
        let page = parts.join(":");
        Self {
            title: Title::new(&page, namespace_id),
            namespace_name: namespace_name.to_string(),
            wiki: wiki.to_string(),
        }
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone)]
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
            .map(|part| FileUsage::new_from_part(&part.to_string()))
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

#[derive(Debug, Clone)]
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
}

impl Hash for PageListEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.title.namespace_id().hash(state);
        self.title.pretty().hash(state);
    }
}

impl PartialEq for PageListEntry {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title // && self.namespace_id == other.namespace_id
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
        }
    }

    pub fn title(&self) -> &Title {
        &self.title
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct PageList {
    pub wiki: Option<String>,
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

    pub fn group_by_namespace(&self) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        self.entries.iter().for_each(|entry| {
            if !ret.contains_key(&entry.title.namespace_id()) {
                ret.insert(entry.title.namespace_id(), vec![]);
            }
            ret.get_mut(&entry.title.namespace_id())
                .unwrap()
                .push(entry.title.with_underscores().to_string());
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
    ) -> Result<HashSet<PageListEntry>, String> {
        match pagelist {
            Some(pagelist) => {
                if self.wiki.is_none() {
                    return Err("PageList::check_before_merging self.wiki is not set".to_string());
                }
                if pagelist.wiki.is_none() {
                    return Err(
                        "PageList::check_before_merging pagelist.wiki is not set".to_string()
                    );
                }
                if self.wiki != pagelist.wiki {
                    return Err(format!(
                        "PageList::check_before_merging wikis are not identical: {}/{}",
                        &self.wiki.as_ref().unwrap(),
                        &pagelist.wiki.unwrap()
                    ));
                }
                Ok(pagelist.entries)
            }
            None => Err("PageList::check_before_merging pagelist is None".to_string()),
        }
    }

    pub fn union(&mut self, pagelist: Option<PageList>) -> Result<(), String> {
        let other_entries = self.check_before_merging(pagelist)?;
        self.entries = self.entries.union(&other_entries).cloned().collect();
        Ok(())
    }

    pub fn intersection(&mut self, pagelist: Option<PageList>) -> Result<(), String> {
        let other_entries = self.check_before_merging(pagelist)?;
        self.entries = self.entries.intersection(&other_entries).cloned().collect();
        Ok(())
    }

    pub fn difference(&mut self, pagelist: Option<PageList>) -> Result<(), String> {
        let other_entries = self.check_before_merging(pagelist)?;
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

    pub fn convert_to_wiki(&mut self, wiki: &str, platform: &Platform) {
        // Already that wiki?
        if self.wiki == None || self.wiki == Some(wiki.to_string()) {
            return;
        }
        self.convert_to_wikidata(platform);
        if wiki != "wikidatawiki" {
            self.convert_from_wikidata(wiki, platform);
        }
    }

    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }

    pub fn replace_entries(&mut self, other: &PageList) {
        other.entries.iter().for_each(|entry| {
            self.entries.replace(entry.to_owned());
        });
    }

    /// Adds/replaces entries based on SQL query batch results.
    pub fn process_batch_results(
        &mut self,
        platform: &Platform,
        batches: Vec<SQLtuple>,
        f: &dyn Fn(my::Row) -> Option<PageListEntry>,
    ) {
        let db_user_pass = platform.state.get_db_mutex().lock().unwrap(); // Force DB connection placeholder
        let mut conn = platform
            .state
            .get_wiki_db_connection(&db_user_pass, &self.wiki.as_ref().unwrap())
            .unwrap();

        batches.iter().for_each(|sql| {
            let result = match conn.prep_exec(&sql.0, &sql.1) {
                Ok(r) => r,
                Err(e) => {
                    println!("ERROR: {:?}", e);
                    return;
                }
            };
            for row in result {
                match row {
                    Ok(row) => match f(row) {
                        Some(entry) => self.add_entry(entry),
                        None => {}
                    },
                    _ => {} // Ignore error
                }
            }
        });
    }

    /// Similar to `process_batch_results` but to modify existing entrties. Does not add new entries.
    pub fn annotate_batch_results(
        &mut self,
        platform: &Platform,
        batches: Vec<SQLtuple>,
        col_title: usize,
        col_ns: usize,
        f: &dyn Fn(my::Row, &mut PageListEntry),
    ) {
        let db_user_pass = platform.state.get_db_mutex().lock().unwrap(); // Force DB connection placeholder
        let mut conn = platform
            .state
            .get_wiki_db_connection(&db_user_pass, &self.wiki.as_ref().unwrap())
            .unwrap();

        batches.iter().for_each(|sql| {
            let result = match conn.prep_exec(&sql.0, &sql.1) {
                Ok(r) => r,
                Err(e) => {
                    println!("ERROR: {:?}", e);
                    return;
                }
            };
            for row in result {
                match row {
                    Ok(row) => {
                        let page_title = match row.get(col_title) {
                            Some(title) => match title {
                                my::Value::Bytes(uv) => String::from_utf8(uv).unwrap(),
                                _ => continue,
                            },
                            None => continue,
                        };
                        let namespace_id = match row.get(col_ns) {
                            Some(title) => match title {
                                my::Value::Int(i) => i,
                                _ => continue,
                            },
                            None => continue,
                        };

                        let tmp_entry = PageListEntry::new(Title::new(&page_title, namespace_id));
                        let mut entry = match self.entries.get(&tmp_entry) {
                            Some(e) => (*e).clone(),
                            None => continue,
                        };

                        f(row, &mut entry);
                        self.add_entry(entry);
                    }
                    _ => {} // Ignore error
                }
            }
        });
    }

    pub fn load_missing_metadata(
        &mut self,
        wikidata_language: Option<String>,
        platform: &Platform,
    ) {
        let batches: Vec<SQLtuple> = self
            .to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|mut sql_batch| {
                sql_batch.0 =
                    "SELECT page_title,page_namespace,page_id,page_len,page_touched FROM page WHERE"
                        .to_string() + &sql_batch.0;
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        self.annotate_batch_results(
            platform,
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
        );

        // All done
        if self.wiki != Some("wikidatawiki".to_string()) || wikidata_language.is_none() {
            return;
        }

        // No need to load labels for WDFIST mode
        if !platform.has_param("regexp_filter") && platform.has_param("wdf_main") {
            return;
        }

        match wikidata_language {
            Some(wikidata_language) => {
                self.add_wikidata_labels_for_namespace(0, "item", &wikidata_language, platform);
                self.add_wikidata_labels_for_namespace(
                    120,
                    "property",
                    &wikidata_language,
                    platform,
                );
            }
            None => {}
        }
    }

    fn add_wikidata_labels_for_namespace(
        &mut self,
        namespace_id: NamespaceID,
        entity_type: &str,
        wikidata_language: &String,
        platform: &Platform,
    ) {
        let batches: Vec<SQLtuple> = self
            .to_sql_batches_namespace(PAGE_BATCH_SIZE,namespace_id)
            .iter_mut()
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
            platform,
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
        );
    }

    fn convert_to_wikidata(&mut self, platform: &Platform) {
        if self.wiki == None || self.wiki == Some("wikidatatwiki".to_string()) {
            return;
        }

        let batches: Vec<SQLtuple> = self.to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql|{
                sql.0 = "SELECT pp_value FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ".to_owned()+&sql.0;
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();
        self.entries.clear();
        self.process_batch_results(platform, batches, &|row: my::Row| {
            let pp_value: String = my::from_row(row);
            Some(PageListEntry::new(Title::new(&pp_value, 0)))
        });
        self.wiki = Some("wikidatawiki".to_string());
    }

    fn convert_from_wikidata(&mut self, wiki: &str, platform: &Platform) {
        if self.wiki == None || self.wiki != Some("wikidatatwiki".to_string()) {
            return;
        }

        let batches = self.to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql|{
                sql.0 = "SELECT ips_site_page FROM wb_items_per_site,page WHERE ips_item_id=substr(page_title,2)*1 AND ".to_owned()+&sql.0+" AND ips_site_id='?'";
                sql.1.push(wiki.to_string());
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        self.entries.clear();
        let api = platform.state.get_api_for_wiki(wiki.to_string()).unwrap();
        self.process_batch_results(platform, batches, &|row: my::Row| {
            let ips_site_page: String = my::from_row(row);
            Some(PageListEntry::new(Title::new_from_full(
                &ips_site_page,
                &api,
            )))
        });
        self.wiki = Some(wiki.to_string());
    }
}
