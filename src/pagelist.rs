//use mediawiki::title::Title;

type NamespaceID = mediawiki::api::NamespaceID;

#[derive(Debug, Clone, PartialEq)]
pub struct PageListEntry {
    title: String,
    namespace_id: NamespaceID,
}

impl PageListEntry {
    pub fn new(title: String, namespace_id: NamespaceID) -> Self {
        Self {
            title,
            namespace_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PageList {
    pub wiki: Option<String>,
    pub entries: Vec<PageListEntry>,
}

impl PageList {
    pub fn new() -> Self {
        Self {
            wiki: None,
            entries: vec![],
        }
    }

    pub fn new_from_wiki(wiki: &str) -> Self {
        Self {
            wiki: Some(wiki.to_string()),
            entries: vec![],
        }
    }

    pub fn add_entry(&mut self, entry: PageListEntry) {
        // TODO unique
        self.entries.push(entry);
    }

    pub fn union(&mut self, pagelist: Option<PageList>) -> Result<(), &str> {
        if pagelist.is_none() {
            return Err("PageList::union pagelist is None");
        }
        let pagelist = pagelist.as_ref().unwrap();
        if self.wiki.is_none() {
            return Err("PageList::union self.wiki is not set");
        }
        if pagelist.wiki.is_none() {
            return Err("PageList::union pagelist.wiki is not set");
        }
        // TODO unique
        pagelist
            .entries
            .iter()
            .for_each(|e| self.entries.push(e.clone()));
        Ok(())
    }
}
