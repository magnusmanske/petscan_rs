use crate::datasource::DataSource;
use crate::pagelist_disk::PageListDisk;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::value::Value;
use std::collections::HashMap;
use std::time;
use wikimisc::mediawiki::api::Api;

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

    async fn run(&mut self, platform: &Platform) -> Result<PageListDisk> {
        let sparql = platform
            .get_param("sparql")
            .ok_or_else(|| anyhow!("Missing parameter \'sparql\'"))?;

        let timeout = time::Duration::from_secs(120);
        let builder = reqwest::ClientBuilder::new().timeout(timeout);
        let api = Api::new_from_builder("https://www.wikidata.org/w/api.php", builder).await?;
        let sparql_url = api.get_site_info_string("general", "wikibase-sparql")?;
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("query".to_string(), sparql.to_string());
        params.insert("format".to_string(), "json".to_string());

        let response = api
            .client()
            .post(sparql_url)
            .header(reqwest::header::USER_AGENT, "PetScan")
            .form(&params)
            .send()
            .await?;

        let ret = PageListDisk::new_from_wiki("wikidatawiki");
        let response = response.text().await?;
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
                        .ok_or_else(|| anyhow!("No variables found in SPARQL result"))?
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
                                if let Some(entry) = Platform::entry_from_entity(&entity) {
                                    ret.add_entry(entry).unwrap_or(())
                                }
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
