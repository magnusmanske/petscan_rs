use crate::pagelist::*;
use crate::platform::Platform;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use mysql_async::Value as MyValue;
use rayon::prelude::*;
use serde_json::value::Value;
use std::collections::HashMap;
use std::time;
use wikibase::mediawiki::api::Api;
use wikibase::mediawiki::title::Title;
use async_trait::async_trait;

pub type SQLtuple = (String, Vec<MyValue>);

#[async_trait]
pub trait DataSource {
    fn can_run(&self, platform: &Platform) -> bool;
    async fn run(&mut self, platform: &Platform) -> Result<PageList, String>;
    fn name(&self) -> String;
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceLabels {}

#[async_trait]
impl DataSource for SourceLabels {
    fn name(&self) -> String {
        "labels".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("labels_yes") || platform.has_param("labels_any")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let sql = platform.get_label_sql();
        let rows = platform
            .state()
            .get_wiki_db_connection( &"wikidatawiki".to_string())
            .await?
            .exec_iter(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(|row| from_row::<(Vec<u8>,)>(row))
            .await
            .map_err(|e|format!("{:?}",e))?;
        let ret = PageList::new_from_wiki_with_capacity("wikidatawiki",rows.len());
        rows
            .iter()
            .map(|row|String::from_utf8_lossy(&row.0))
            .filter_map(|item|Platform::entry_from_entity(&item))
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()) );
        Ok(ret)
    }
}

impl SourceLabels {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceWikidata {}

#[async_trait]
impl DataSource for SourceWikidata {
    fn name(&self) -> String {
        "wikidata".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("wpiu_no_statements") && platform.has_param("wikidata_source_sites")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let no_statements = platform.has_param("wpiu_no_statements");
        let sites = platform
            .get_param("wikidata_source_sites")
            .ok_or(format!("Missing parameter 'wikidata_source_sites'"))?;
        let sites: Vec<String> = sites.split(",").map(|s| s.to_string()).collect();
        if sites.is_empty() {
            return Err(format!("SourceWikidata: No wikidata source sites given"));
        }

        let sites = Platform::prep_quote(&sites);

        let mut sql = "SELECT ips_item_id FROM wb_items_per_site".to_string();
        if no_statements {
            sql += ",page_props,page";
        }
        sql += " WHERE ips_site_id IN (";
        sql += &sites.0;
        sql += ")";
        if no_statements {
            sql += " AND page_namespace=0 AND ips_item_id=substr(page_title,2)*1 AND page_id=pp_page AND pp_propname='wb-claims' AND pp_sortkey=0" ;
        }

        // Perform DB query
        let rows = platform
            .state()
            .get_wiki_db_connection(&"wikidatawiki".to_string())
            .await?
            .exec_iter(sql.as_str(),()).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(|row| from_row::<usize>(row))
            .await
            .map_err(|e|format!("{:?}",e))?;

        let ret = PageList::new_from_wiki(&"wikidatawiki".to_string());
        for ips_item_id in rows {
            let term_full_entity_id = format!("Q{}", ips_item_id);
            match Platform::entry_from_entity(&term_full_entity_id) {
                Some(entry) => {ret.add_entry(entry).unwrap_or(());}
                None => {}
            }
        }
        Ok(ret)
    }
}

impl SourceWikidata {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourcePagePile {}

#[async_trait]
impl DataSource for SourcePagePile {
    fn name(&self) -> String {
        "pagepile".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("pagepile")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let pagepile = platform
            .get_param("pagepile")
            .ok_or(format!("Missing parameter 'pagepile'"))?;
        let timeout = time::Duration::from_secs(240);
        let builder = reqwest::ClientBuilder::new().timeout(timeout);
        let api = Api::new_from_builder("https://www.wikidata.org/w/api.php", builder).await
            .map_err(|e| e.to_string())?;
        let params = api.params_into(&[
            ("id", &pagepile.to_string()),
            ("action", "get_data"),
            ("format", "json"),
            ("doit", "1"),
        ]);
        let text = api
            .query_raw("https://tools.wmflabs.org/pagepile/api.php", &params, "GET").await
            .map_err(|e| format!("PagePile: {:?}", e))?;
        let v: Value =
            serde_json::from_str(&text).map_err(|e| format!("PagePile JSON: {:?}", e))?;
        let wiki = v["wiki"]
            .as_str()
            .ok_or(format!("PagePile {} does not specify a wiki", &pagepile))?;
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?; // Just because we need query_raw
        let ret = PageList::new_from_wiki(wiki);
        v["pages"]
            .as_array()
            .ok_or(format!(
                "PagePile {} does not have a 'pages' array",
                &pagepile
            ))?
            .iter()
            .filter_map(|title| title.as_str())
            .map(|title| PageListEntry::new(Title::new_from_full(&title.to_string(), &api)))
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
        if ret.is_empty()? {
            platform.warn(format!("<span tt='warn_pagepile'></span>"))?;
        }
        Ok(ret)
    }
}

impl SourcePagePile {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceSearch {}

#[async_trait]
impl DataSource for SourceSearch {
    fn name(&self) -> String {
        "search".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("search_query")
            && platform.has_param("search_wiki")
            && platform.has_param("search_max_results")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let wiki = platform
            .get_param("search_wiki")
            .ok_or(format!("Missing parameter 'search_wiki'"))?;
        let query = platform
            .get_param("search_query")
            .ok_or(format!("Missing parameter 'search_query'"))?;
        let max = match platform
            .get_param("search_max_results")
            .ok_or(format!("Missing parameter 'search_max_results'"))?
            .parse::<usize>()
        {
            Ok(max) => max,
            Err(e) => return Err(format!("{:?}", e)),
        };
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let srlimit = if max > 500 { 500 } else { max };
        let srlimit = format!("{}", srlimit);
        let namespace_ids = platform
            .form_parameters()
            .ns
            .par_iter()
            .cloned()
            .collect::<Vec<usize>>();
        let namespace_ids = if namespace_ids.is_empty() {
            "*".to_string()
        } else {
            namespace_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<String>>()
                .join(",")
        };
        let params = api.params_into(&vec![
            ("action", "query"),
            ("list", "search"),
            ("srlimit", srlimit.as_str()),
            ("srsearch", query.as_str()),
            ("srnamespace", namespace_ids.as_str()),
        ]);
        let result = match api.get_query_api_json_limit(&params, Some(max)).await {
            Ok(result) => result,
            Err(e) => return Err(format!("{:?}", e)),
        };
        let titles = Api::result_array_to_titles(&result);
        let ret = PageList::new_from_wiki(&wiki);
        titles
            .iter()
            .map(|title| PageListEntry::new(title.to_owned()))
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
        if ret.is_empty()? {
            platform.warn(format!("<span tt='warn_search'></span>"))?;
        }
        Ok(ret)
    }
}

impl SourceSearch {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceManual {}

#[async_trait]
impl DataSource for SourceManual {
    fn name(&self) -> String {
        "manual".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("manual_list") && platform.has_param("manual_list_wiki")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let wiki = platform
            .get_param("manual_list_wiki")
            .ok_or(format!("Missing parameter 'manual_list_wiki'"))?;
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let ret = PageList::new_from_wiki(&wiki);
        platform
            .get_param("manual_list")
            .ok_or(format!("Missing parameter 'manual_list'"))?
            .split("\n")
            .filter_map(|line| {
                let line = line.trim().to_string();
                if !line.is_empty() {
                    let title = Title::new_from_full(&line, &api);
                    let entry = PageListEntry::new(title);
                    Some(entry)
                } else {
                    None
                }
            })
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
        Ok(ret)
    }
}

impl SourceManual {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceSparql {}

#[async_trait]
impl DataSource for SourceSparql {
    fn name(&self) -> String {
        "sparql".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("sparql")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let sparql = platform
            .get_param("sparql")
            .ok_or(format!("Missing parameter 'sparql'"))?;

        let timeout = time::Duration::from_secs(120);
        let builder = reqwest::ClientBuilder::new().timeout(timeout);
        let api = Api::new_from_builder("https://www.wikidata.org/w/api.php", builder).await
            .map_err(|e| format!("SourceSparql::run:1 {:?}", e))?;

        let sparql_url = api.get_site_info_string("general", "wikibase-sparql")?;
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("query".to_string(), sparql.to_string());
        params.insert("format".to_string(), "json".to_string());

        let response = match api
            .client()
            .post(sparql_url)
            .header(reqwest::header::USER_AGENT, "PetScan")
            .form(&params)
            .send().await
        {
            Ok(resp) => resp,
            Err(e) => return Err(format!("SPARL: {:?}", e)),
        };

        let ret = PageList::new_from_wiki("wikidatawiki");
        let response = response.text().await.unwrap();
        //let reader = BufReader::new(response); // TODO read line by line from stream, somehow
        let mut mode: u8 = 0;
        let mut header = String::new();
        let mut binding = String::new();
        let mut first_var = String::new();
        for line in response.split("\n") { // reader.lines() {
            match line {
                "{" => continue,
                "}" => continue,
                "  \"results\" : {" => {}
                "    \"bindings\" : [ {" => {
                    mode = mode + 1;
                    header = "{".to_string() + &header + "\"dummy\": {}}";
                    let j: Value = serde_json::from_str(&header).unwrap_or(json!({}));
                    first_var = j["head"]["vars"][0]
                        .as_str()
                        .ok_or(format!("No variables found in SPARQL result"))?
                        .to_string();
                }
                "    }, {" | "    } ]" => match mode {
                    0 => header += &line,
                    1 => {
                        binding = "{".to_string() + &binding + "}";
                        let j: Value = serde_json::from_str(&binding).unwrap_or(json!({}));
                        binding.clear();
                        match j[&first_var]["value"].as_str() {
                            Some(entity_url) => match api.extract_entity_from_uri(entity_url) {
                                Ok(entity) => match Platform::entry_from_entity(&entity) {
                                    Some(entry) => ret.add_entry(entry).unwrap_or(()),
                                    None => {}
                                },
                                _ => {}
                            },
                            None => {}
                        }
                    }
                    _ => {}
                },
                other => match mode {
                    0 => header += other,
                    1 => binding += other,
                    _ => {}
                },
            }
        }

        Ok(ret)
    }

    /*
    // using serde, obsolete because of high memory usage
    fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let sparql = platform
            .get_param("sparql")
            .ok_or(format!("Missing parameter 'sparql'"))?;

        let timeout = Some(time::Duration::from_secs(120));
        let builder = reqwest::blocking::ClientBuilder::new().timeout(timeout);
        let api = Api::new_from_builder("https://www.wikidata.org/w/api.php", builder)
            .map_err(|e| format!("SourceSparql::run:1 {:?}", e))?;
        let result = api
            .sparql_query(sparql.as_str())
            .map_err(|e| format!("SourceSparql::run:2 {:?}", e))?;
        let first_var = result["head"]["vars"][0]
            .as_str()
            .ok_or(format!("No variables found in SPARQL result"))?;
        let ret = PageList::new_from_wiki("wikidatawiki");
        api.entities_from_sparql_result(&result, first_var)
            .par_iter()
            .filter_map(|e| Platform::entry_from_entity(e))
            .for_each(|entry| ret.add_entry(entry));
        if ret.is_empty() {
            platform.warn(format!("<span tt='warn_sparql'></span>"));
        }
        Ok(ret)
    }
    */
}

impl SourceSparql {
    pub fn new() -> Self {
        Self {}
    }
}
