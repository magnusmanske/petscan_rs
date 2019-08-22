use crate::app_state::AppState;
use crate::datasource::SQLtuple;
use crate::form_parameters::FormParameters;
use crate::pagelist::PageList;
use crate::platform::*;
use mediawiki::api::Api;
use mysql as my;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub static MIN_IGNORE_DB_FILE_COUNT: usize = 3;

/*

class TWDFIST {
public:
    typedef map <string,int32_t> string2int32 ;
    TWDFIST ( TPageList *pagelist , TPlatform *platform ) : pagelist(pagelist) , platform(platform) {} ;

protected :

    bool wdf_langlinks , wdf_coords , wdf_search_commons , wdf_commons_cats ;
    bool wdf_only_items_without_p18 , wdf_only_files_not_on_wd , wdf_only_jpeg , wdf_max_five_results , wdf_only_page_images , wdf_allow_svg ;
    map <string,uint8_t> files2ignore ;
    map <string,string2int32 > q2image ;
} ;
*/

pub struct WDfist {
    item2files: HashMap<String, HashMap<String, usize>>,
    items: Vec<String>,
    files2ignore: HashSet<String>, // Requires normailzed, valid filenames
    form_parameters: Arc<FormParameters>,
    state: Arc<AppState>,
}

impl WDfist {
    pub fn new(platform: &Platform, result: &Option<PageList>) -> Option<Self> {
        let items: Vec<String> = match result {
            Some(pagelist) => {
                let mut pagelist = pagelist.clone(); // TODO remove clone()
                pagelist.convert_to_wiki("wikidatawiki", platform).ok()?; // TODO do this upstream and just check here,
                pagelist
                    .entries
                    .iter()
                    .filter(|e| e.title().namespace_id() == 0)
                    .map(|e| e.title().pretty().to_owned())
                    .collect()
            }
            None => vec![],
        };
        Some(Self {
            item2files: HashMap::new(),
            items: items,
            files2ignore: HashSet::new(),
            form_parameters: platform.form_parameters().clone(),
            state: platform.state(),
        })
    }

    pub fn run(&mut self) -> Result<Value, String> {
        let mut j = json!({"status":"OK","data":{}});
        if self.items.is_empty() {
            j["status"] = json!("No items from original query");
            return Ok(j);
        }

        self.seed_ignore_files()?;
        self.filter_items()?;
        if self.items.is_empty() {
            j["status"] = json!("No items qualify");
            return Ok(j);
        }

        // Main process
        if self.bool_param("wdf_langlinks") {
            self.follow_language_links()?;
        }
        if self.bool_param("wdf_coords") {
            self.follow_coords()?;
        }
        if self.bool_param("wdf_search_commons") {
            self.follow_search_commons()?;
        }
        if self.bool_param("wdf_commons_cats") {
            self.follow_commons_cats()?;
        }

        self.filter_files()?;

        j["data"] = json!(&self.item2files);
        Ok(j)
    }

    fn follow_language_links(&mut self) -> Result<(), String> {
        // TODO
        Ok(())
    }

    fn follow_coords(&mut self) -> Result<(), String> {
        // TODO
        Ok(())
    }

    fn follow_search_commons(&mut self) -> Result<(), String> {
        // TODO
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

    fn seed_ignore_files(&mut self) -> Result<(), String> {
        self.seed_ignore_files_from_wiki_page()?;
        self.seed_ignore_files_from_ignore_database()?;
        Ok(())
    }

    fn seed_ignore_files_from_wiki_page(&mut self) -> Result<(), String> {
        let url_with_ignore_list =
            "http://www.wikidata.org/w/index.php?title=User:Magnus_Manske/FIST_icons&action=raw";
        let api = match Api::new("https://www.wikidata.org/w/api.php") {
            Ok(api) => api,
            Err(_e) => return Err(format!("Can't open Wikidata API")),
        };
        let wikitext = match api.query_raw(url_with_ignore_list, &HashMap::new(), "GET") {
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

    fn seed_ignore_files_from_ignore_database(&mut self) -> Result<(), String> {
        let state = self.state.clone();
        let tool_db_user_pass = match state.get_tool_db_user_pass().lock() {
            Ok(x) => x,
            Err(e) => return Err(format!("Bad mutex: {:?}", e)),
        };
        let mut conn = state.get_tool_db_connection(tool_db_user_pass.clone())?;

        let sql = format!("SELECT CONVERT(`file` USING utf8) FROM s51218__wdfist_p.ignore_files GROUP BY file HAVING count(*)>={}",MIN_IGNORE_DB_FILE_COUNT);
        let result = match conn.prep_exec(sql, ()) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!(
                    "wdfist::seed_ignore_files_from_ignore_database: {:?}",
                    e
                ))
            }
        };

        result
            .filter_map(|row_result| row_result.ok())
            .map(|row| my::from_row::<String>(row))
            .for_each(|filename| {
                let filename = self.normalize_filename(&filename.to_string());
                if self.is_valid_filename(&filename) {
                    self.files2ignore.insert(filename);
                }
            });

        Ok(())
    }

    fn filter_items(&mut self) -> Result<(), String> {
        // To batches (all items are ns=0)
        let wdf_only_items_without_p18 = self.bool_param("wdf_only_items_without_p18");
        let mut batches: Vec<SQLtuple> = vec![];
        self.items.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let mut sql = Platform::prep_quote(&chunk.to_vec());
            sql.0 = format!("SELECT page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND page_title IN ({})",&sql.0) ;
            if  wdf_only_items_without_p18 {sql.0 += " AND NOT EXISTS (SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=120 AND pl_title='P18')" ;}
            sql.0 += " AND NOT EXISTS (SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=0 AND pl_title IN ('Q13406463','Q4167410'))" ; // No list/disambig
            batches.push(sql);
        });

        // Run batches
        let pagelist = PageList::new_from_wiki("wikidatawiki");
        let rows = pagelist.run_batch_queries(self.state.clone(), batches)?;

        match rows.lock() {
            Ok(rows) => {
                self.items = rows
                    .iter()
                    .map(|row| my::from_row::<String>(row.to_owned()))
                    .collect();
            }
            Err(e) => return Err(e.to_string()),
        }

        Ok(())
    }

    // Requires normalized filename
    fn is_valid_filename(&self, _filename: &String) -> bool {
        true // TODO
    }

    fn filter_files(&mut self) -> Result<(), String> {
        self.filter_files_from_ignore_database()?;
        self.filter_files_five_or_is_used()?;
        self.remove_items_with_no_file_candidates()?;
        Ok(())
    }

    fn filter_files_from_ignore_database(&mut self) -> Result<(), String> {
        // TODO
        Ok(())
    }

    fn filter_files_five_or_is_used(&mut self) -> Result<(), String> {
        // TODO
        Ok(())
    }

    fn remove_items_with_no_file_candidates(&mut self) -> Result<(), String> {
        self.item2files.retain(|_item, files| !files.is_empty());
        Ok(())
    }

    fn normalize_filename(&self, filename: &String) -> String {
        filename.trim().replace(" ", "_")
    }
}

/*

typedef pair <string,string> t_title_q ;


void TWDFIST::filterFilesFiveOrIsUsed () {
    map <string,uint8_t> remove_files ;

    // Get distinct files and their usage count
    map <string,uint64_t> file2count ;
    for ( auto& qi:q2image ) {
        for ( auto& fc:qi.second ) {
            if ( file2count.find(fc.first) == file2count.end() ) file2count[fc.first] = 1 ;
            else file2count[fc.first]++ ;
        }
    }

    if ( file2count.size() == 0 ) return ; // No files, no problem

    // Remove files that are already used on Wikidata
    if ( wdf_only_files_not_on_wd ) {
        TWikidataDB wd_db ( "wikidatawiki" , platform );
        vector <string> parts ;
        parts.push_back ( "" ) ;
        for ( auto& fc:file2count ) {
            if ( parts[parts.size()-1].size() > 500000 ) parts.push_back ( "" ) ; // String length limit
            if ( !parts[parts.size()-1].empty() ) parts[parts.size()-1] += "," ;
            parts[parts.size()-1] += "\"" + wd_db.escape(fc.first) + "\"" ;
        }
        for ( auto& part:parts ) {
            if ( part.empty() ) continue ;
            string sql = "SELECT il_to FROM imagelinks WHERE il_from_namespace=0 AND il_to IN (" + part + ")" ;
            MYSQL_RES *result = wd_db.getQueryResults ( sql ) ;
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(result))) {
                remove_files[row[0]] = 1 ;
            }
            mysql_free_result(result);
        }
    }

    // Remove files that are in at least five items
    if ( wdf_max_five_results ) {
        for ( auto& fc:file2count ) {
            if ( fc.second < 5 ) continue ;
            remove_files[fc.first] = 1 ;
        }
    }


    if ( remove_files.size() == 0 ) return ; // Nothing to remove

    // Remove files
    for ( auto& qi:q2image ) {
        string q = qi.first ;
        string2int32 new_files ;
        for ( auto& fc:qi.second ) {
            if ( remove_files.find(fc.first) != remove_files.end() ) continue ;
            new_files[fc.first] = fc.second ;
        }
        qi.second.swap ( new_files ) ;
    }
}

void TWDFIST::filterFilesFromIgnoreDatabase () {
    // Chunk item list
    vector <string> item_batches ;
    uint64_t cnt = 0 ;
    for ( auto& qi:q2image ) {
        if ( cnt % PAGE_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
        if ( !item_batches[item_batches.size()-1].empty() ) item_batches[item_batches.size()-1] += "," ;
        item_batches[item_batches.size()-1] += qi.first.substr(1) ;
        cnt++ ;
    }

    // Get files to avoid, per item, from the database
    TWikidataDB wdfist_db ;
    wdfist_db.setHostDB ( "tools.labsdb" , "s51218__wdfist_p" , true ) ; // HARDCODED publicly readable
    wdfist_db.doConnect ( true ) ;
    for ( auto &s:item_batches ) {
        string sql = "SELECT q,file FROM ignore_files WHERE q IN (" + s + ")" ;
        MYSQL_RES *result = wdfist_db.getQueryResults ( sql ) ;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            string q = "Q" + string(row[0]) ;
            if ( q2image.find(q) == q2image.end() ) continue ; // Paranoia
            string file = normalizeFilename ( row[1] ) ;
            if ( !isValidFile(file) ) continue ;
            auto f = q2image[q].find(file) ;
            if ( f == q2image[q].end() ) continue ;
            q2image[q].erase ( f ) ;
        }
        mysql_free_result(result);
    }
}


bool TWDFIST::isValidFile ( string file ) { // Requires normalized filename
    if ( file.empty() ) return false ;

    // Check files2ignore
    if ( files2ignore.find(file) != files2ignore.end() ) return false ;

    // Check type
    size_t dot = file.find_last_of ( "." ) ;
    string type = file.substr ( dot+1 ) ;
    std::transform(type.begin(), type.end(), type.begin(), ::tolower);
    if ( wdf_only_jpeg && type!="jpg" && type!="jpeg" ) return false ;
    if ( type == "svg" && !wdf_allow_svg ) return false ;
    if ( type == "pdf" || type == "gif" ) return false ;

    // Check key phrases
    if ( file.find("Flag_of_") == 0 ) return false ;
    if ( file.find("Crystal_Clear_") == 0 ) return false ;
    if ( file.find("Nuvola_") == 0 ) return false ;
    if ( file.find("Kit_") == 0 ) return false ;

//  if ( preg_match ( '/\bribbon.jpe{0,1}g/i' , $i ) ) // TODO
    if ( file.find("600px_") == 0 && type == "png" ) return false ;

    return true ;
}

void TWDFIST::addFileToQ ( string q , string file ) {
    if ( !isValidFile ( file ) ) return ;
    if ( q2image.find(q) == q2image.end() ) q2image[q] = string2int32 () ;
    if ( q2image[q].find(file) == q2image[q].end() ) q2image[q][file] = 1 ;
    else q2image[q][file]++ ;
}



void TWDFIST::followLanguageLinks () {
    // Chunk item list
    vector <string> item_batches ;
    for ( uint64_t cnt = 0 ; cnt < items.size() ; cnt++ ) {
        if ( cnt % PAGE_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
        if ( !item_batches[item_batches.size()-1].empty() ) item_batches[item_batches.size()-1] += "," ;
        item_batches[item_batches.size()-1] += "\"" + items[cnt] + "\"" ;
    }

    // Get sitelinks
    map <string,vector <t_title_q> > titles_by_wiki ;
    TWikidataDB wd_db ( "wikidatawiki" , platform );
    for ( uint64_t x = 0 ; x < item_batches.size() ; x++ ) {
        string item_ids ;
        item_ids.reserve ( item_batches[x].size() ) ;
        for ( uint64_t p = 0 ; p < item_batches[x].size() ; p++ ) {
            if ( item_batches[x][p] != 'Q' ) item_ids += item_batches[x][p] ;
        }
        string sql = "SELECT ips_item_id,ips_site_id,ips_site_page FROM wb_items_per_site WHERE ips_item_id IN (" + item_ids + ")" ;
        MYSQL_RES *result = wd_db.getQueryResults ( sql ) ;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            string wiki = row[1] ;
            if ( wiki == "wikidatawiki" ) continue ; // Not relevant
            string q = "Q" + string(row[0]) ;
            string title = row[2] ;
            replace ( title.begin(), title.end(), ' ', '_' ) ;
            titles_by_wiki[wiki].push_back ( t_title_q ( title , q ) ) ;
        }
        mysql_free_result(result);
    }

    // Get images for sitelinks from globalimagelinks
    TWikidataDB commons_db ( "commonswiki" , platform );
    for ( auto& wp:titles_by_wiki ) {
        string wiki = wp.first ;
        vector <string> parts ;
        map <string,string> title2q ;
        parts.push_back ( "" ) ;
        for ( auto& tq:wp.second ) {
            if ( parts[parts.size()-1].size() > 500000 ) parts.push_back ( "" ) ; // String length limit
            if ( !parts[parts.size()-1].empty() ) parts[parts.size()-1] += "," ;
            parts[parts.size()-1] += "\"" + commons_db.escape(tq.first) + "\"" ;
            title2q[tq.first] = tq.second ;
        }

        for ( auto& part:parts ) {
            if ( part.empty() ) continue ;

            // Page images
            map <string,string> title2file ;
            if ( 1 ) {
                string sql = "SELECT DISTINCT gil_page_title AS page,gil_to AS image FROM page,globalimagelinks WHERE gil_wiki='" + wiki + "' AND gil_page_namespace_id=0" ;
                sql += " AND gil_page_title IN (" + part + ") AND page_namespace=6 and page_title=gil_to AND page_is_redirect=0" ;
                sql += " AND NOT EXISTS (SELECT * FROM categorylinks where page_id=cl_from and cl_to='Crop_for_Wikidata')" ; // To-be-cropped
                MYSQL_RES *result = commons_db.getQueryResults ( sql ) ;
                MYSQL_ROW row;
                while ((row = mysql_fetch_row(result))) {
                    string title = row[0] ;
                    string file = normalizeFilename ( row[1] ) ;
                    if ( wdf_only_page_images ) {
                        if ( title2file.find(title) == title2file.end() ) continue ; // Page has no page image
                        if ( title2file[title] != file ) continue ;
                    }
                    if ( title2q.find(title) == title2q.end() ) {
                        cout << "Not found : " << title << endl ;
                        continue ;
                    }
                    string q = title2q[title] ;
                    addFileToQ ( q , file ) ;
                }
                mysql_free_result(result);
            }
        }

    }

}

void TWDFIST::followCoordinates () {
    // Chunk item list
    vector <string> item_batches ;
    for ( uint64_t cnt = 0 ; cnt < items.size() ; cnt++ ) {
        if ( cnt % PAGE_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
        if ( !item_batches[item_batches.size()-1].empty() ) item_batches[item_batches.size()-1] += "," ;
        item_batches[item_batches.size()-1] += "\"" + items[cnt] + "\"" ;
    }

    // Get coordinates
    map <string,pair <string,string>> q2coord ;
    TWikidataDB wd_db ( "wikidatawiki" , platform );
    for ( auto& batch:item_batches ) {
        string sql = "SELECT page_title,gt_lat,gt_lon FROM geo_tags,page WHERE page_namespace=0 AND page_id=gt_page_id AND gt_globe='earth' AND gt_primary=1 AND page_title IN (" + batch + ")" ;
        MYSQL_RES *result = wd_db.getQueryResults ( sql ) ;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            q2coord[row[0]] = pair <string,string> ( row[1] , row[2] ) ;
        }
        mysql_free_result(result);
    }

    // Run queries
    string radius = "100" ; // meters
    for ( auto& qc:q2coord ) {
        string q = qc.first ;
        string lat = qc.second.first ;
        string lon = qc.second.second ;
        string url = "https://commons.wikimedia.org/w/api.php?action=query&list=geosearch&gscoord="+lat+"|"+lon+"&gsradius="+radius+"&gslimit=50&gsnamespace=6&format=json" ;
        json j ;
        loadJSONfromURL ( url , j ) ;
        for ( uint32_t i = 0 ; i < j["query"]["geosearch"].size() ; i++ ) {
            string file = j["query"]["geosearch"][i]["title"] ;
            file = file.substr ( 5 ) ;
            file = normalizeFilename ( file ) ;
            addFileToQ ( q , file ) ;
        }
    }
}

void TWDFIST::followSearchCommons () {
    // Chunk item list
    vector <string> item_batches ;
    for ( uint64_t cnt = 0 ; cnt < items.size() ; cnt++ ) {
        if ( cnt % PAGE_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
        if ( !item_batches[item_batches.size()-1].empty() ) item_batches[item_batches.size()-1] += "," ;
        item_batches[item_batches.size()-1] += "\"" + items[cnt] + "\"" ;
    }

    // Get strings
    map <string,string> q2label ;
    TWikidataDB wd_db ( "wikidatawiki" , platform );
    for ( auto& batch:item_batches ) {
        string sql = "SELECT term_entity_id,term_text FROM wb_terms WHERE term_entity_type='item' AND term_language='en' AND term_type='label' AND term_full_entity_id IN (" + batch + ")" ;
        MYSQL_RES *result = wd_db.getQueryResults ( sql ) ;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            q2label[row[0]] = row[1] ;
        }
        mysql_free_result(result);
    }

    // Run search
    for ( auto& ql:q2label ) {
        string q = ql.first ;
        string label = ql.second ;
        string url = "https://commons.wikimedia.org/w/api.php?action=query&list=search&srnamespace=6&format=json&srsearch=" + urlencode(label) ;
        json j ;
        loadJSONfromURL ( url , j ) ;
        for ( uint32_t i = 0 ; i < j["query"]["search"].size() ; i++ ) {
            string file = j["query"]["search"][i]["title"] ;
            file = file.substr ( 5 ) ;
            file = normalizeFilename ( file ) ;
            addFileToQ ( q , file ) ;
        }
    }
}




*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use serde_json::Value;
    use std::env;
    use std::fs::File;

    fn get_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let file = File::open(path).expect("Can not open config file");
        let petscan_config: Value =
            serde_json::from_reader(file).expect("Can not parse JSON from config file");
        Arc::new(AppState::new_from_config(&petscan_config))
    }

    fn get_wdfist(params: Vec<(&str, &str)>, items: Vec<&str>) -> WDfist {
        let form_parameters = FormParameters {
            params: params
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect(),
            ns: HashSet::new(),
        };
        WDfist {
            item2files: HashMap::new(),
            items: items.iter().map(|s| s.to_string()).collect(),
            files2ignore: HashSet::new(),
            form_parameters: Arc::new(form_parameters),
            state: get_state(),
        }
    }

    #[test]
    fn test_wdfist_filter_items() {
        let params: Vec<(&str, &str)> = vec![("wdf_only_items_without_p18", "1")];
        let items: Vec<&str> = vec![
            "Q63810120", // Some scientific paper, unlikely to ever get an image, designated survivor of this test
            "Q13520818", // Magnus Manske, has image
            "Q37651",    // List item
            "Q21002367", // Disambig item
            "Q10000067", // Redirect
        ];
        let mut wdfist = get_wdfist(params, items);
        let _j = wdfist.run().unwrap();
        assert_eq!(wdfist.items, vec!["Q63810120".to_string()]);
    }
}
