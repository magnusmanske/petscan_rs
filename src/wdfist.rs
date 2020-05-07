use crate::app_state::AppState;
use crate::datasource::SQLtuple;
use crate::form_parameters::FormParameters;
use crate::pagelist::PageList;
use crate::platform::*;
use mysql_async as my;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use mysql_async::Value as MyValue;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use wikibase::mediawiki::api::Api;

pub static MIN_IGNORE_DB_FILE_COUNT: usize = 3;
pub static MAX_FILE_COUNT_IN_RESULT_SET: usize = 5;
pub static NEARBY_FILES_RADIUS_IN_METERS: usize = 100;
pub static MAX_WIKI_API_THREADS: usize = 10;

pub struct WDfist {
    item2files: HashMap<String, HashMap<String, usize>>,
    items: Vec<String>,
    files2ignore: HashSet<String>, // Requires normailzed, valid filenames
    form_parameters: FormParameters,
    state: Arc<AppState>,
    wdf_allow_svg: bool,
    wdf_only_jpeg: bool,
}

impl WDfist {
    pub fn new(platform: &Platform, result: &Option<PageList>) -> Option<Self> {
        let mut items: Vec<String> = match result {
            Some(pagelist) => {
                if !pagelist.is_wikidata() {
                    return None;
                }
                pagelist
                    .entries()
                    .read()
                    .unwrap()
                    .par_iter()
                    .filter(|e| e.title().namespace_id() == 0)
                    .map(|e| e.title().pretty().to_owned())
                    .collect()
            }
            None => vec![],
        };
        items.par_sort();
        items.dedup();
        Some(Self {
            item2files: HashMap::new(),
            items: items,
            files2ignore: HashSet::new(),
            form_parameters: platform.form_parameters().clone(),
            state: platform.state(),
            wdf_allow_svg: false,
            wdf_only_jpeg: false,
        })
    }

    pub async fn run(&mut self) -> Result<Value, String> {
        let mut j = json!({"status":"OK","data":{}});
        self.wdf_allow_svg = self.bool_param("wdf_allow_svg");
        self.wdf_only_jpeg = self.bool_param("wdf_only_jpeg");
        if self.items.is_empty() {
            j["status"] = json!("No items from original query");
            return Ok(j);
        }

        self.seed_ignore_files().await?;
        self.filter_items().await?;
        if self.items.is_empty() {
            j["status"] = json!("No items qualify");
            return Ok(j);
        }

        // Main process
        if self.bool_param("wdf_langlinks") {
            self.follow_language_links().await?;
        }
        if self.bool_param("wdf_coords") {
            self.follow_coords().await?;
        }
        if self.bool_param("wdf_search_commons") {
            self.follow_search_commons().await?;
        }
        if self.bool_param("wdf_commons_cats") {
            self.follow_commons_cats()?;
        }

        self.filter_files().await?;

        j["data"] = json!(&self.item2files);
        Ok(j)
    }

    async fn get_language_links(&self) -> Result<HashMap<String, Vec<(String, String)>>, String> {
        // Prepare batches to get item/wiki/title triples
        let mut batches: Vec<SQLtuple> = vec![];
        self.items.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::prep_quote(&chunk);
            sql.0 = format!("SELECT ips_item_id,ips_site_id,ips_site_page FROM wb_items_per_site WHERE ips_item_id IN ({})",&sql.0) ;
            sql.1 = sql.1.par_iter().filter_map(|q|{
                match q {
                    MyValue::Bytes(q) => Some(MyValue::Bytes(q[1..].into())),
                    _ => None
                }
            }).collect();
            batches.push(sql);
        });

        // Run batches
        let pagelist = PageList::new_from_wiki("wikidatawiki");
        let rows = pagelist.run_batch_queries(&self.state, batches).await?;

        // Collect pages and items, per wiki
        let mut wiki2title_q: HashMap<String, Vec<(String, String)>> = HashMap::new();
        rows.iter()
            .map(|row| my::from_row::<(u64, String, String)>(row.to_owned()))
            .for_each(|(item_id, wiki, page)| {
                if wiki == "wikidatawiki" {
                    return;
                }
                let q = format!("Q{}", item_id);
                let page = page.replace(" ", "_");
                if !wiki2title_q.contains_key(&wiki) {
                    wiki2title_q.insert(wiki.to_owned(), vec![]);
                }
                match wiki2title_q.get_mut(&wiki) {
                    Some(ref mut title_q) => {
                        title_q.push((page, q));
                    }
                    None => {}
                }
            });
        Ok(wiki2title_q)
    }

    async fn filter_page_images(
        &self,
        wiki: &String,
        page_file: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>, String> {
        if !self.bool_param("wdf_only_page_images") {
            return Ok(page_file);
        }

        // Prepare batches
        let mut batches: Vec<SQLtuple> = vec![];
        let mut titles: Vec<String> = page_file
            .par_iter()
            .map(|(page, _file)| page.to_string())
            .collect();
        titles.par_sort();
        titles.dedup();
        titles.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::prep_quote(&chunk);
            sql.0 = format!("SELECT page_title,pp_value FROM page,page_props WHERE page_id=pp_page AND page_namespace=0 AND pp_propname='page_image_free' AND page_title IN ({})",&sql.0) ;
            batches.push(sql);
        });

        // Run batches
        let pagelist = PageList::new_from_wiki(wiki);
        let rows = pagelist.run_batch_queries(&self.state, batches).await?;
        let ret: Vec<(String, String)> = rows
            .par_iter()
            .map(|row| my::from_row::<(String, String)>(row.to_owned()))
            .filter(|(page, image)| page_file.contains(&(page.to_owned(), image.to_owned())))
            .collect();

        Ok(ret)
    }

    async fn follow_language_links(&mut self) -> Result<(), String> {
        let add_item_file: Mutex<Vec<(String, String)>> = Mutex::new(vec![]);
        let wiki2title_q = self.get_language_links().await?;
        for (wiki, title_q) in wiki2title_q {
            // Prepare batches
            let page2q: HashMap<String, String> = title_q
                .par_iter()
                .map(|(title, q)| (title.to_string(), q.to_string()))
                .collect();
            let titles: Vec<String> = page2q.par_iter().map(|(title, _q)| title.to_string()).collect();
            let mut batches: Vec<SQLtuple> = vec![];
            titles.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
                let mut sql = Platform::prep_quote(&chunk);
                sql.0 = format!("SELECT DISTINCT gil_page_title AS page,gil_to AS image FROM page,globalimagelinks WHERE gil_wiki='{}' AND gil_page_title IN ({})",wiki,&sql.0) ;
                sql.0 += " AND gil_page_namespace_id=0 AND page_namespace=6 and page_title=gil_to AND page_is_redirect=0" ;
                sql.0 += " AND NOT EXISTS (SELECT * FROM categorylinks where page_id=cl_from and cl_to='Crop_for_Wikidata')" ; // To-be-cropped
                batches.push(sql);
            });

            // Run batches
            let rows = PageList::new_from_wiki("commonswiki").run_batch_queries(&self.state, batches).await.map_err(|e|format!("{:?}",e))?;

            // Collect pages and items, per wiki
            let page_file: Vec<(String, String)> = rows
                    .par_iter()
                    .map(|row| my::from_row::<(String, String)>(row.to_owned()))
                    .collect();
            let mut page_file = self.filter_page_images(&wiki, page_file).await.map_err(|e|format!("{:?}",e))?
                .par_iter()
                .filter_map(|(page, file)| match page2q.get(page) {
                    Some(q) => {
                        Some((q.to_string(),file.to_string()))
                    }
                    None => None
                }).collect();
            add_item_file.lock().unwrap().append(&mut page_file);
        }

        // Add files
        add_item_file
            .lock()
            .unwrap()
            .iter()
            .for_each(|(q, file)| self.add_file_to_item(q, file));

        Ok(())
    }

    async fn follow_coords(&mut self) -> Result<(), String> {
        // Prepare batches
        let mut batches: Vec<SQLtuple> = vec![];
        self.items.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::prep_quote(&chunk);
            sql.0 = format!("SELECT page_title,gt_lat,gt_lon FROM geo_tags,page WHERE page_namespace=0 AND page_id=gt_page_id AND gt_globe='earth' AND gt_primary=1 AND page_title IN ({})",&sql.0) ;
            batches.push(sql);
        });

        // Run batches
        let pagelist = PageList::new_from_wiki("wikidatawiki");
        let rows = pagelist.run_batch_queries(&self.state, batches).await?;

        // Process results
        let page_coords: Vec<(String, f64, f64)> = rows
            .par_iter()
            .map(|row| my::from_row::<(String, f64, f64)>(row.to_owned()))
            .collect();

        // Get nearby files
        let api = Api::new("https://commons.wikimedia.org/w/api.php").await
            .map_err(|e| format!("{:?}", e))?;
        //let add_item_file: Mutex<Vec<(String, String)>> = Mutex::new(vec![]);

        let params : Vec<_> = page_coords
            .iter()
            .map(|(_q, lat, lon)| {
                api.params_into(&vec![
                    ("action", "query"),
                    ("list", "geosearch"),
                    ("gscoord", format!("{}|{}", lat, lon).as_str()),
                    (
                        "gsradius",
                        format!("{}", NEARBY_FILES_RADIUS_IN_METERS).as_str(),
                    ),
                    ("gslimit", "50"),
                    ("gsnamespace", "6"),
                ])
            })
            .collect();

        /*
        let futures : Vec<_> = params
            .iter()
            .map(|params|api.get_query_api_json(&params))
            .collect();

        let results = join_all(futures).await;
        */

        let mut results : Vec<_> = vec![] ;
        for param in params {
            match api.get_query_api_json(&param).await {
                Ok(x) => { results.push ( x ) }
                _ => { results.push(json!({})) } // Ignore
            }
        }

        let add_item_file : Vec<(String, String)> = results.iter()
            .zip(page_coords)
            .filter_map(|(result,(q,_lat,_lon))|{
                let images = result["query"]["geosearch"].as_array()?;
                let item_file: Vec<(String, String)> = images
                    .par_iter()
                    .filter_map(|j| match j["title"].as_str() {
                        Some(filename) => {
                            let filename = filename[5..].to_string(); // Remove leading "File:"
                            let filename = self.normalize_filename(&filename);
                            Some((q.to_string(), filename))
                        }
                        None => None,
                    })
                    .collect();
                Some(item_file)
            })
            .flatten()
            .collect();

        // Add files
        add_item_file
            .iter()
            .for_each(|(q, file)| self.add_file_to_item(q, file));

        Ok(())
    }

    async fn follow_search_commons(&mut self) -> Result<(), String> {
        // Prepare batches
        let mut batches: Vec<SQLtuple> = vec![];
        self.items.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::full_entity_id_to_number(&chunk);
            sql.0 = format!("SELECT concat('Q',wbit_item_id) AS term_full_entity_id, wbx_text as term_text FROM wbt_item_terms INNER JOIN wbt_term_in_lang ON wbit_term_in_lang_id = wbtl_id INNER JOIN wbt_type ON wbtl_type_id = wby_id AND wby_name='label' INNER JOIN wbt_text_in_lang ON wbtl_text_in_lang_id = wbxl_id INNER JOIN wbt_text ON wbxl_text_id = wbx_id AND wbxl_language='en' WHERE wbit_item_id IN ({})",&sql.0) ;
            batches.push(sql);
        });

        // Run batches
        let pagelist = PageList::new_from_wiki("wikidatawiki");
        let rows = pagelist.run_batch_queries(&self.state, batches).await?;

        // Process results
        let item2label: Vec<(String, String)> = rows
            .par_iter()
            .map(|row| my::from_row::<(String, String)>(row.to_owned()))
            .collect();

        // Get search results
        let api = Api::new("https://commons.wikimedia.org/w/api.php").await.map_err(|e| format!("{:?}", e))?;

        let params : Vec<_> = item2label
            .iter()
            .map(|(_q, label)|
                api.params_into(&vec![
                    ("action", "query"),
                    ("list", "search"),
                    ("srnamespace", "6"),
                    ("srsearch", label.as_str()),
                ]))
            .collect();

        /*
        let futures : Vec<_> = params
            .iter()
            .map(|params|api.get_query_api_json(&params))
            .collect();

        let results = join_all(futures).await;
        */

        let mut results : Vec<_> = vec![] ;
        for param in params {
            match api.get_query_api_json(&param).await {
                Ok(x) => { results.push ( x ) }
                _ => { results.push(json!({})) } // Ignore
            }
        }


        let add_item_file : Vec<(String, String)> = results.iter()
            .zip(item2label)
            .filter_map(|(result,(q,_label)):(&Value,(String,String))|{
                let images = match result["query"]["search"].as_array() {
                    Some(a) => a,
                    None => {
                        return None;
                    }
                };
                let item_file: Vec<(String, String)> = images
                    .par_iter()
                    .filter_map(|j| match j["title"].as_str() {
                        Some(filename) => {
                            let filename = filename[5..].to_string(); // Remove leading "File:"
                            let filename = self.normalize_filename(&filename);
                            Some((q.to_string(), filename))
                        }
                        None => None,
                    })
                    .collect();
                Some(item_file)
            })
            .flatten()
            .collect();

        // Add files
        add_item_file
            .iter()
            .for_each(|(q, file)| self.add_file_to_item(q, file));

        Ok(())
    }

    fn follow_commons_cats(&mut self) -> Result<(), String> {
        // TODO
        Ok(())
    }

    fn bool_param(&self, key: &str) -> bool {
        match self.form_parameters.params.get(key) {
            Some(v) => !v.trim().is_empty(),
            None => false,
        }
    }

    async fn seed_ignore_files(&mut self) -> Result<(), String> {
        self.seed_ignore_files_from_wiki_page().await?;
        self.seed_ignore_files_from_ignore_database().await?;
        Ok(())
    }

    async fn seed_ignore_files_from_wiki_page(&mut self) -> Result<(), String> {
        let url_with_ignore_list =
            "http://www.wikidata.org/w/index.php?title=User:Magnus_Manske/FIST_icons&action=raw";
        let api = match Api::new("https://www.wikidata.org/w/api.php").await {
            Ok(api) => api,
            Err(_e) => return Err(format!("Can't open Wikidata API")),
        };
        let wikitext = match api.query_raw(url_with_ignore_list, &HashMap::new(), "GET").await {
            Ok(t) => t,
            Err(e) => {
                return Err(format!(
                    "Can't load ignore list from {} : {}",
                    &url_with_ignore_list, e
                ))
            }
        };
        // TODO only rows starting with '*'?
        wikitext.split("\n").for_each(|filename| {
            let filename = filename.trim_start_matches(|c| c == ' ' || c == '*');
            let filename = self.normalize_filename(&filename.to_string());
            if self.is_valid_filename(&filename) {
                self.files2ignore.insert(filename);
            }
        });
        Ok(())
    }

    async fn seed_ignore_files_from_ignore_database(&mut self) -> Result<(), String> {
        let state = self.state.clone();
        let tool_db_user_pass = state
            .get_tool_db_user_pass()
            .lock().await;
        let mut conn = state.get_tool_db_connection(tool_db_user_pass.clone()).await?;

        let sql = format!("SELECT CONVERT(`file` USING utf8) FROM s51218__wdfist_p.ignore_files GROUP BY file HAVING count(*)>={}",MIN_IGNORE_DB_FILE_COUNT);

        let rows = conn.exec_iter(sql.as_str(),()).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(|row| from_row::<Vec<u8>>(row))
            .await
            .map_err(|e|format!("{:?}",e))?;

        for filename in rows {
            let filename = String::from_utf8_lossy(&filename);
            let filename = self.normalize_filename(&filename.to_string());
            if self.is_valid_filename(&filename) {
                self.files2ignore.insert(filename);
            }
        }

        Ok(())
    }

    async fn filter_items(&mut self) -> Result<(), String> {
        // To batches (all items are ns=0)
        let wdf_only_items_without_p18 = self.bool_param("wdf_only_items_without_p18");
        let mut batches: Vec<SQLtuple> = vec![];
        self.items.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::prep_quote(&chunk);
            sql.0 = format!("SELECT page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND page_title IN ({})",&sql.0) ;
            if  wdf_only_items_without_p18 {sql.0 += " AND NOT EXISTS (SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=120 AND pl_title='P18')" ;}
            sql.0 += " AND NOT EXISTS (SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=0 AND pl_title IN ('Q13406463','Q4167410'))" ; // No list/disambig
            batches.push(sql);
        });

        // Run batches
        let pagelist = PageList::new_from_wiki("wikidatawiki");
        let rows = pagelist.run_batch_queries(&self.state, batches).await?;

        self.items = rows
            .par_iter()
            .map(|row| my::from_row::<String>(row.to_owned()))
            .collect();
        Ok(())
    }

    async fn filter_files(&mut self) -> Result<(), String> {
        self.filter_files_from_ignore_database().await?;
        self.filter_files_five_or_is_used().await?;
        self.remove_items_with_no_file_candidates()?;
        Ok(())
    }

    async fn filter_files_from_ignore_database(&mut self) -> Result<(), String> {
        if self.items.is_empty() {
            return Ok(());
        }

        // Prepare batches
        let mut batches: Vec<SQLtuple> = vec![];
        let items: Vec<String> = self
            .item2files
            .par_iter()
            .map(|(q, _files)| q[1..].to_string())
            .collect();
        items.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::prep_quote(&chunk);
            sql.0 = format!(
                "SELECT concat('Q',q),CONVERT(`file` USING utf8) FROM s51218__wdfist_p.ignore_files WHERE q IN ({})",
                &sql.0
            );
            batches.push(sql);
        });

        // Prepare
        let state = self.state.clone();
        let tool_db_user_pass = state
            .get_tool_db_user_pass()
            .lock()
            .await;
        let mut conn = state.get_tool_db_connection(tool_db_user_pass.clone()).await?;

        // Run batches sequentially
        for sql in batches {
            let rows = conn.exec_iter(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await
                .map_err(|e|format!("{:?}",e))?
                .map_and_drop(|row| from_row::<(String, String)>(row))
                .await
                .map_err(|e|format!("{:?}",e))?;

            for (item, filename) in rows {
                let filename = self.normalize_filename(&filename.to_string());
                match self.item2files.get_mut(&item) {
                    Some(ref mut files) => {
                        files.remove(&filename);
                        if files.is_empty() {
                            self.item2files.remove(&item);
                        }
                    }
                    None => {}, // Odd
                }
            }
        }
        Ok(())
    }

    async fn filter_files_five_or_is_used(&mut self) -> Result<(), String> {
        if self.item2files.is_empty() {
            return Ok(());
        }

        // Collect all filenames, and how often they are used in this result set
        let mut file2count: HashMap<String, usize> = HashMap::new();
        self.item2files.iter().for_each(|(_item, files)| {
            files
                .iter()
                .for_each(|fc| *file2count.entry(fc.0.to_string()).or_insert(0) += 1);
        });
        if file2count.is_empty() {
            return Ok(());
        }

        let mut files_to_remove: Vec<String> = vec![];

        if self.bool_param("wdf_only_files_not_on_wd") {
            // Get distinct filenames to check
            let filenames: Vec<String> = file2count
                .par_iter()
                .map(|(file, _count)| file.to_owned())
                .collect();

            // Create batches
            let mut batches: Vec<SQLtuple> = vec![];
            filenames.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
                let mut sql = Platform::prep_quote(&chunk);
                sql.0 = format!(
                    "SELECT DISTINCT il_to FROM imagelinks WHERE il_from_namespace=0 AND il_to IN ({})",
                    &sql.0
                );
                batches.push(sql);
            });

            // Run batches, and get a list of files to remove
            let pagelist = PageList::new_from_wiki("wikidatawiki");
            let rows = pagelist.run_batch_queries(&self.state, batches).await?;
            files_to_remove = rows
                .par_iter()
                .map(|row| my::from_row::<String>(row.to_owned()))
                .collect();
        }

        // Remove max files returned
        if self.bool_param("wdf_max_five_results") {
            files_to_remove.extend(
                file2count
                    .iter()
                    .filter(|(_file, count)| **count >= MAX_FILE_COUNT_IN_RESULT_SET)
                    .map(|(file, _count)| file.to_owned()),
            );
            files_to_remove.par_sort();
            files_to_remove.dedup();
        }

        // Remove the files
        self.item2files.iter_mut().for_each(|(_item, files)| {
            files_to_remove.iter().for_each(|filename| {
                files.remove(filename);
            });
        });

        // Remove empty item results
        self.item2files.retain(|_item, files| !files.is_empty());

        Ok(())
    }

    fn remove_items_with_no_file_candidates(&mut self) -> Result<(), String> {
        self.item2files.retain(|_item, files| !files.is_empty());
        Ok(())
    }

    fn normalize_filename(&self, filename: &String) -> String {
        filename.trim().replace(" ", "_")
    }

    // Requires normalized filename
    fn is_valid_filename(&self, filename: &String) -> bool {
        lazy_static! {
            static ref RE_FILETYPE: Regex = Regex::new(r#"^(.+)\.([^.]+)$"#)
                .expect("WDfist::is_valid_filename RE_FILETYPE is invalid");
            static ref RE_KEY_PHRASES: Regex =
                Regex::new(r#"(Flag_of_|Crystal_Clear_|Nuvola_|Kit_)"#)
                    .expect("WDfist::is_valid_filename RE_KEY_PHRASES is invalid");
            static ref RE_KEY_PHRASES_PNG: Regex = Regex::new(r#"(600px_)"#)
                .expect("WDfist::is_valid_filename RE_KEY_PHRASES_PNG is invalid");
        }

        if filename.is_empty() {
            return false;
        }
        if self.files2ignore.contains(filename) {
            return false;
        }

        // Only one result, but...
        for cap in RE_FILETYPE.captures_iter(filename) {
            let filetype = cap[2].to_lowercase();
            if self.wdf_only_jpeg && filetype != "jpg" && filetype != "jpeg" {
                return false;
            }
            match filetype.as_str() {
                "svg" => return self.wdf_allow_svg,
                "pdf" | "gif" => return false,
                _ => {}
            };
            if RE_KEY_PHRASES.is_match(filename) {
                return false;
            }
            if filetype == "png" && RE_KEY_PHRASES_PNG.is_match(filename) {
                return false;
            }
            return true;
        }
        false
    }

    fn add_file_to_item(&mut self, item: &String, filename: &String) {
        if !self.is_valid_filename(filename) {
            return;
        }
        match self.item2files.get_mut(item) {
            Some(ref mut files) => match files.get_mut(filename) {
                Some(ref mut file2count) => {
                    **file2count += 1;
                }
                None => {
                    files.insert(filename.to_string(), 1);
                }
            },
            None => {
                let mut file_entry: HashMap<String, usize> = HashMap::new();
                file_entry.insert(filename.to_string(), 1);
                self.item2files.insert(item.to_string(), file_entry);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use serde_json::Value;
    use std::env;
    use std::fs::File;

    async fn get_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let file = File::open(path).expect("Can not open config file");
        let petscan_config: Value =
            serde_json::from_reader(file).expect("Can not parse JSON from config file");
        Arc::new(AppState::new_from_config(&petscan_config).await)
    }

    async fn get_wdfist(params: Vec<(&str, &str)>, items: Vec<&str>) -> WDfist {
        let form_parameters = FormParameters {
            params: params
                .par_iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect(),
            ns: HashSet::new(),
        };
        WDfist {
            item2files: HashMap::new(),
            items: items.par_iter().map(|s| s.to_string()).collect(),
            files2ignore: HashSet::new(),
            form_parameters: form_parameters,
            state: get_state().await,
            wdf_allow_svg: false,
            wdf_only_jpeg: false,
        }
    }

    fn set_item2files(wdfist: &mut WDfist, q: &str, files: Vec<(&str, usize)>) {
        wdfist.item2files.insert(
            q.to_string(),
            files
                .par_iter()
                .map(|x| (x.0.to_string(), x.1 as usize))
                .collect(),
        );
    }

    #[tokio::test]
    async fn test_wdfist_filter_items() {
        let params: Vec<(&str, &str)> = vec![("wdf_only_items_without_p18", "1")];
        let items: Vec<&str> = vec![
            "Q63810120", // Some scientific paper, unlikely to ever get an image, designated survivor of this test
            "Q13520818", // Magnus Manske, has image
            "Q1782953",  // List item
            "Q21002367", // Disambig item
            "Q10000067", // Redirect
        ];
        let mut wdfist = get_wdfist(params, items).await;
        let _j = wdfist.run().await.unwrap();
        assert_eq!(wdfist.items, vec!["Q63810120".to_string()]);
    }

    #[tokio::test]
    async fn test_filter_files_five_or_is_used() {
        let params: Vec<(&str, &str)> = vec![
            ("wdf_max_five_results", "1"),
            ("wdf_only_files_not_on_wd", "1"),
        ];
        let mut wdfist = get_wdfist(params, vec![]).await;
        set_item2files(&mut wdfist, "Q1", vec![("More_than_5.jpg", 0)]);
        set_item2files(&mut wdfist, "Q2", vec![("More_than_5.jpg", 0)]);
        set_item2files(
            &mut wdfist,
            "Q3",
            vec![
                ("More_than_5.jpg", 0),
                ("Douglas_adams_portrait_cropped.jpg", 0),
            ],
        );
        set_item2files(&mut wdfist, "Q4", vec![("More_than_5.jpg", 0)]);
        set_item2files(
            &mut wdfist,
            "Q5",
            vec![
                ("More_than_5.jpg", 0),
                ("This_is_a_test_no_such_file_exists.jpg", 0),
            ],
        );
        set_item2files(&mut wdfist, "Q6", vec![("More_than_5.jpg", 0)]);
        wdfist.filter_files_five_or_is_used().await.unwrap();
        assert_eq!(
            json!(wdfist.item2files),
            json!({"Q5":{"This_is_a_test_no_such_file_exists.jpg":0}})
        );
    }

    #[tokio::test]
    async fn test_is_valid_filename() {
        let params: Vec<(&str, &str)> = vec![];
        let mut wdfist = get_wdfist(params, vec![]).await;
        assert!(wdfist.is_valid_filename(&"foobar.jpg".to_string()));
        assert!(!wdfist.is_valid_filename(&"foobar.GIF".to_string()));
        assert!(!wdfist.is_valid_filename(&"foobar.pdf".to_string()));
        assert!(wdfist.is_valid_filename(&"some_600px_bs.jpg".to_string()));
        assert!(!wdfist.is_valid_filename(&"some_600px_bs.png".to_string()));
        assert!(!wdfist.is_valid_filename(&"Flag_of_foobar.jpg".to_string()));
        assert!(!wdfist.is_valid_filename(&"fooCrystal_Clear_bar.jpg".to_string()));
        assert!(!wdfist.is_valid_filename(&"fooNuvola_bar.jpg".to_string()));
        assert!(!wdfist.is_valid_filename(&"fooKit_bar.jpg".to_string()));
        wdfist.wdf_allow_svg = true;
        assert!(wdfist.is_valid_filename(&"foobar.svg".to_string()));
        wdfist.wdf_allow_svg = false;
        assert!(!wdfist.is_valid_filename(&"foobar.svg".to_string()));
    }

    #[tokio::test]
    async fn test_follow_language_links() {
        let params: Vec<(&str, &str)> = vec![];
        let mut wdfist = get_wdfist(params, vec!["Q1481"]).await;

        // All files
        wdfist.wdf_allow_svg = true;
        wdfist.follow_language_links().await.unwrap();
        assert!(wdfist.item2files.contains_key(&"Q1481".to_string()));
        assert!(wdfist.item2files.get(&"Q1481".to_string()).unwrap().len() > 90);

        // No SVG
        wdfist.item2files.clear();
        wdfist.wdf_allow_svg = false;
        wdfist.follow_language_links().await.unwrap();
        assert!(wdfist.item2files.contains_key(&"Q1481".to_string()));
        assert!(wdfist.item2files.get(&"Q1481".to_string()).unwrap().len() < 50);
        assert!(wdfist
            .item2files
            .get(&"Q1481".to_string())
            .unwrap()
            .contains_key(&"Felsburg.jpg".to_string()));

        // Page images
        let params: Vec<(&str, &str)> = vec![("wdf_only_page_images", "1")];
        let mut wdfist = get_wdfist(params, vec!["Q1481"]).await;
        wdfist.follow_language_links().await.unwrap();
        assert!(wdfist.item2files.contains_key(&"Q1481".to_string()));
        let x = wdfist.item2files.get(&"Q1481".to_string()).unwrap() ;
        assert!(x.len() < 50);
        assert!(x.contains_key(&"Felsberg_(Hessen).jpg".to_string()));
    }

    #[tokio::test]
    async fn test_follow_coords() {
        let params: Vec<(&str, &str)> = vec![];
        let mut wdfist = get_wdfist(params, vec!["Q350"]).await;
        wdfist.follow_coords().await.unwrap();
        assert!(wdfist.item2files.get(&"Q350".to_string()).unwrap().len() > 40);
        assert!(wdfist
            .item2files
            .get(&"Q350".to_string())
            .unwrap()
            .contains_key(&"Cambridge_Wikidata_dinner.jpg".to_string()));
    }

    #[tokio::test]
    async fn test_follow_search_commons() {
        let params: Vec<(&str, &str)> = vec![];
        let mut wdfist = get_wdfist(params, vec!["Q66711783"]).await;
        wdfist.follow_search_commons().await.unwrap();
        println!(
            "{} results",
            wdfist
                .item2files
                .get(&"Q66711783".to_string())
                .unwrap()
                .len()
        );
        assert!(
            wdfist
                .item2files
                .get(&"Q66711783".to_string())
                .unwrap()
                .len()
                > 5
        );
        assert!(wdfist
            .item2files
            .get(&"Q66711783".to_string())
            .unwrap()
            .contains_key(&"Walter_Rueth.jpg".to_string()));
    }
}
