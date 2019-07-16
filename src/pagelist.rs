use crate::datasource::SQLtuple;
use crate::platform::Platform;
use mediawiki::api::NamespaceID;
use mediawiki::title::Title;
use mysql as my;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
//use rayon::prelude::*;

//type NamespaceID = mediawiki::api::NamespaceID;

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone)]
pub struct PageListEntry {
    title: Title,
    //pub does_exist: Option<bool>,
    //pub is_redirect: Option<bool>,
    pub wikidata_item: Option<String>,
    pub page_id: Option<usize>,
    pub page_bytes: Option<usize>,
    pub page_timestamp: Option<String>,
    pub link_count: Option<usize>,
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
            //does_exist: None,
            //is_redirect: None,
            wikidata_item: None,
            page_id: None,
            page_bytes: None,
            page_timestamp: None,
            link_count: None,
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
        self.entries.insert(entry);
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

    fn process_batch_results(
        &mut self,
        conn: &mut my::Conn,
        batches: Vec<SQLtuple>,
        pre: &str,
        post: &str,
        f: &dyn Fn(my::Row) -> Option<PageListEntry>,
    ) {
        batches.iter().for_each(|sql| {
            let sql = (pre.to_string() + &sql.0 + &post.to_string(), sql.1.clone());
            println!("EXECUTING:\n{:?}", &sql);
            let result = match conn.prep_exec(sql.0, sql.1) {
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

    fn convert_to_wikidata(&mut self, platform: &Platform) {
        if self.wiki == None || self.wiki == Some("wikidatatwiki".to_string()) {
            return;
        }

        let db_user_pass = platform.state.get_db_mutex().lock().unwrap(); // Force DB connection placeholder
        println!("Connecting to {}", self.wiki.as_ref().unwrap());
        let mut conn = platform
            .state
            .get_wiki_db_connection(&db_user_pass, &self.wiki.as_ref().unwrap())
            .unwrap();

        let batches = self.to_sql_batches(20000);
        self.wiki = Some("wikidata".to_string());
        self.entries.clear();
        self.process_batch_results(
            &mut conn,
            batches,
            "SELECT pp_value FROM page_props,page WHERE page_id=pp_page AND pp_propname='wikibase_item' AND ",
            "",
            &|row: my::Row| {
                let pp_value: String = my::from_row(row);
                Some(PageListEntry::new(Title::new(&pp_value, 0)))
            },
        );
    }

    fn convert_from_wikidata(&mut self, _wiki: &str, _platform: &Platform) {
        if self.wiki == None || self.wiki != Some("wikidatatwiki".to_string()) {
            return;
        }
    }
}
