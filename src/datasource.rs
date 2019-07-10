use crate::pagelist::*;
use crate::platform::Platform;
use mediawiki::api::Api;

pub trait DataSource {
    fn can_run(&self, platform: &Platform) -> bool;
    fn run(&self, platform: &Platform) -> Option<PageList>;
    fn name(&self) -> String;
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabase {}

impl DataSource for SourceDatabase {
    fn name(&self) -> String {
        "SourceDatabase".to_string()
    }

    fn can_run(&self, _platform: &Platform) -> bool {
        false
    }

    fn run(&self, _platform: &Platform) -> Option<PageList> {
        None // TODO
    }
}

impl SourceDatabase {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceSparql {}

impl DataSource for SourceSparql {
    fn name(&self) -> String {
        "SourceSparql".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        match &platform.form_parameters().sparql {
            Some(sparql) => !sparql.is_empty(),
            None => false,
        }
    }

    fn run(&self, platform: &Platform) -> Option<PageList> {
        let sparql = platform.form_parameters().sparql.as_ref()?;
        let api = Api::new("https://www.wikidata.org/w/api.php").ok()?;
        let result = api.sparql_query(sparql.as_str()).ok()?;
        let first_var = result["head"]["vars"][0].as_str()?;
        let entities = api.entities_from_sparql_result(&result, first_var);
        let mut ret = PageList::new_from_wiki("wikidatawiki");
        // TODO letters/namespaces are hardcoded?
        // TODO M for commons?
        entities.iter().for_each(|e| match e.chars().next() {
            Some('Q') => ret.add_entry(PageListEntry::new(e.to_string(), 0)),
            Some('P') => ret.add_entry(PageListEntry::new(e.to_string(), 120)),
            Some('L') => ret.add_entry(PageListEntry::new(e.to_string(), 146)),
            _ => {}
        });
        Some(ret)
    }
}

impl SourceSparql {
    pub fn new() -> Self {
        Self {}
    }
}
