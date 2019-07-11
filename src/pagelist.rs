use mediawiki::title::Title;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

type NamespaceID = mediawiki::api::NamespaceID;

#[derive(Debug, Clone)]
pub struct PageListEntry {
    title: Title,
}

impl Hash for PageListEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.title.namespace_id().hash(state);
        self.title.with_underscores().hash(state);
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
        Self { title: title }
    }

    pub fn new_from_title_ns(title: String, namespace_id: NamespaceID) -> Self {
        Self {
            title: Title::new(&title, namespace_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PageList {
    pub wiki: Option<String>,
    pub entries: HashSet<PageListEntry>,
}

impl PageList {
    pub fn new() -> Self {
        Self {
            wiki: None,
            entries: HashSet::new(),
        }
    }

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
