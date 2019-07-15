use mediawiki::api::NamespaceID;
use mediawiki::title::Title;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
//use rayon::prelude::*;

//type NamespaceID = mediawiki::api::NamespaceID;

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone)]
pub struct PageListEntry {
    title: Title,
    pub does_exist: Option<bool>,
    pub is_redirect: Option<bool>,
    pub wikidata_item: Option<String>,
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
            does_exist: None,
            is_redirect: None,
            wikidata_item: None,
        }
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
                .push(entry.title.pretty().to_string());
        });
        ret
    }

    pub fn swap_entries(&mut self, other: &mut PageList) {
        std::mem::swap(&mut self.entries, &mut other.entries);
        //let tmp = self.entries;
        //self.entries = other.entries;
        //other.entries = tmp;
        //(self.entries, other.entries) = (other.entries, self.entries);
    }

    /*
    pub fn entries_as_vec(&self) -> Vec<PageListEntry> {
        self.entries.iter().cloned().collect::<Vec<PageListEntry>>()
    }
    */

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
}
