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

#[derive(Debug, Clone, PartialEq, Default)]
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
        let mut conn = platform
            .state()
            .get_wiki_db_connection( &"wikidatawiki".to_string())
            .await? ;
        let rows = conn
            .exec_iter(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(from_row::<(Vec<u8>,)>)
            .await
            .map_err(|e|format!("{:?}",e))?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;
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

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceSitelinks {
    main_wiki: String
}

#[async_trait]
impl DataSource for SourceSitelinks {
    fn name(&self) -> String {
        "sitelinks".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("sitelinks_yes") || platform.has_param("sitelinks_any")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let sitelinks_yes = platform.get_param_as_vec("sitelinks_yes", "\n");
        let sitelinks_any = platform.get_param_as_vec("sitelinks_any", "\n");
        let sitelinks_no = platform.get_param_as_vec("sitelinks_no", "\n");
        let sitelinks_min = platform.get_param_blank("min_sitelink_count");
        let sitelinks_max = platform.get_param_blank("max_sitelink_count");

        let use_min_max = !sitelinks_min.is_empty() || !sitelinks_max.is_empty();

        let mut yes_any = vec![] ;
        yes_any.extend(&sitelinks_yes);
        yes_any.extend(&sitelinks_any);
        self.main_wiki = match yes_any.get(0) {
            Some(wiki) => wiki.to_string(),
            None => return Err("No yes/any sitelink found in SourceSitelinks::run".to_string())
        };

        let sitelinks_any : Vec<String> = sitelinks_any.iter().filter_map(|site|self.site2lang(site)).collect();
        let sitelinks_no : Vec<String> = sitelinks_no.iter().filter_map(|site|self.site2lang(site)).collect();

        let mut sql: SQLtuple = (String::new(), vec![]);
        sql.0 += "SELECT ";
        if use_min_max {
            sql.0 += "page_title,(SELECT count(*) FROM langlinks WHERE ll_from=page_id) AS sitelink_count" ;
        } else {
            sql.0 += "DISTINCT page_title,0";
        }
        sql.0 += " FROM page WHERE page_namespace=0";

        sitelinks_yes.iter().filter_map(|site|self.site2lang(site)).for_each(|lang|{
            sql.0 += " AND page_id IN (SELECT ll_from FROM langlinks WHERE ll_lang=?)" ;
            sql.1.push(lang.into());
        });
        if !sitelinks_any.is_empty() {
            sql.0 += " AND page_id IN (SELECT ll_from FROM langlinks WHERE ll_lang IN (" ;
            let tmp = Platform::prep_quote(&sitelinks_any);
            Platform::append_sql(&mut sql, tmp);
            sql.0 += "))";
        }
        if !sitelinks_no.is_empty() {
            sql.0 += " AND page_id NOT IN (SELECT ll_from FROM langlinks WHERE ll_lang IN (" ;
            let tmp = Platform::prep_quote(&sitelinks_no);
            Platform::append_sql(&mut sql, tmp);
            sql.0 += "))";
        }

        let mut having: Vec<String> = vec![];
        if let Ok(s) = sitelinks_min.parse::<usize>() { having.push(format!("sitelink_count>={}", s)) }
        if let Ok(s) = sitelinks_max.parse::<usize>() { having.push(format!("sitelink_count<={}", s)) }

        if use_min_max {
            sql.0 += " GROUP BY page_title";
        }
        if !having.is_empty() {
            sql.0 += " HAVING ";
            sql.0 += &having.join(" AND ");
        }

        let mut conn = platform
            .state()
            .get_wiki_db_connection( &self.main_wiki)
            .await? ;
        let rows = conn
            .exec_iter(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(from_row::<(Vec<u8>,u32)>)
            .await
            .map_err(|e|format!("{:?}",e))?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;

        let ret = PageList::new_from_wiki_with_capacity(&self.main_wiki,rows.len());
        if use_min_max {
            ret.set_has_sitelink_counts(true)? ;
        }
        rows
            .iter()
            .map(|row|(String::from_utf8_lossy(&row.0),row.1))
            .map(|(page,sitelinks)| {
                let mut ret = PageListEntry::new(Title::new(&page, 0)) ;
                if use_min_max {
                    ret.sitelink_count = Some(sitelinks);
                }
                ret
            })
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()) );
        Ok(ret)
    }
}

impl SourceSitelinks {
    pub fn new() -> Self {
        Self { ..Default::default() }
    }

    fn site2lang(&self,site:&str) -> Option<String> {
        if *site == self.main_wiki {
            return None;
        }
        let ret = if site.ends_with("wiki") {
            site.split_at(site.len()-4).0.to_owned()
        } else {
            site.to_owned()
        };
        Some(ret)
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq, Default)]
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
            .ok_or_else(|| "Missing parameter \'wikidata_source_sites\'".to_string())?;
        let sites: Vec<String> = sites.split(',').map(|s| s.to_string()).collect();
        if sites.is_empty() {
            return Err("SourceWikidata: No wikidata source sites given".to_string());
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
        let mut conn = platform
            .state()
            .get_wiki_db_connection(&"wikidatawiki".to_string())
            .await? ;
        let rows = conn
            .exec_iter(sql.as_str(),()).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(from_row::<usize>)
            .await
            .map_err(|e|format!("{:?}",e))?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;
        let ret = PageList::new_from_wiki(&"wikidatawiki".to_string());
        for ips_item_id in rows {
            let term_full_entity_id = format!("Q{}", ips_item_id);
            if let Some(entry) = Platform::entry_from_entity(&term_full_entity_id) {ret.add_entry(entry).unwrap_or(());}
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

#[derive(Debug, Clone, PartialEq, Default)]
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
            .ok_or_else(|| "Missing parameter \'pagepile\'".to_string())?;
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
            platform.warn("<span tt=\'warn_pagepile\'></span>".to_string())?;
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

#[derive(Debug, Clone, PartialEq, Default)]
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
            && !platform.is_param_blank("search_query")
            && !platform.is_param_blank("search_wiki")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let wiki = platform
            .get_param("search_wiki")
            .ok_or_else(|| "Missing parameter \'search_wiki\'".to_string())?;
        let query = platform
            .get_param("search_query")
            .ok_or_else(|| "Missing parameter \'search_query\'".to_string())?;
        let max = match platform
            .get_param("search_max_results")
            .ok_or_else(|| "Missing parameter \'search_max_results\'".to_string())?
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
        let params = api.params_into(&[("action", "query"),
            ("list", "search"),
            ("srlimit", srlimit.as_str()),
            ("srsearch", query.as_str()),
            ("srnamespace", namespace_ids.as_str())]);
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
            platform.warn("<span tt=\'warn_search\'></span>".to_string())?;
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

#[derive(Debug, Clone, PartialEq, Default)]
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
            .ok_or_else(|| "Missing parameter \'manual_list_wiki\'".to_string())?;
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let ret = PageList::new_from_wiki(&wiki);
        platform
            .get_param("manual_list")
            .ok_or_else(|| "Missing parameter \'manual_list\'".to_string())?
            .split('\n')
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

#[derive(Debug, Clone, PartialEq, Default)]
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
            .ok_or_else(|| "Missing parameter \'sparql\'".to_string())?;

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
        let response = response.text().await.map_err(|e|format!("{:?}",e))?;
        let mut mode: u8 = 0;
        let mut header = String::new();
        let mut binding = String::new();
        let mut first_var = String::new();
        for line in response.split('\n') {
            match line {
                "{" => continue,
                "}" => continue,
                "  \"results\" : {" => {}
                "    \"bindings\" : [ {" => {
                    mode += 1;
                    header = "{".to_string() + &header + "\"dummy\": {}}";
                    let j: Value = serde_json::from_str(&header).unwrap_or_else(|_| json!({}));
                    first_var = j["head"]["vars"][0]
                        .as_str()
                        .ok_or_else(|| "No variables found in SPARQL result".to_string())?
                        .to_string();
                }
                "    }, {" | "    } ]" => match mode {
                    0 => header += &line,
                    1 => {
                        binding = "{".to_string() + &binding + "}";
                        let j: Value = serde_json::from_str(&binding).unwrap_or_else(|_| json!({}));
                        binding.clear();
                        if let Some(entity_url) = j[&first_var]["value"].as_str() {
                            if let Ok(entity) = api.extract_entity_from_uri(entity_url) {
                                if let Some(entry) = Platform::entry_from_entity(&entity) { ret.add_entry(entry).unwrap_or(()) }
                            }
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
