use crate::datasource::DataSource;
use crate::pagelist::*;
use crate::platform::Platform;
/*
use mediawiki::api::Api;
use mediawiki::title::Title;
use mysql as my;
use rayon::prelude::*;
use serde_json::value::Value;
*/

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabase {}

impl DataSource for SourceDatabase {
    fn name(&self) -> String {
        "categories".to_string()
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
