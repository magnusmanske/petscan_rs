use crate::pagelist::PageList;
use crate::platform::*;

pub struct WDfist {}

impl WDfist {
    pub fn new(_platform: &Platform) -> Self {
        Self {}
    }

    pub fn run(&mut self) -> Option<PageList> {
        None
    }
}

/*

#define ITEM_BATCH_SIZE 10000

typedef pair <string,string> t_title_q ;

void TWDFIST::filterItems () {
    // Chunk item list
    vector <string> item_batches ;
    for ( uint64_t cnt = 0 ; cnt < items.size() ; cnt++ ) {
        if ( cnt % ITEM_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
        if ( !item_batches[item_batches.size()-1].empty() ) item_batches[item_batches.size()-1] += "," ;
        item_batches[item_batches.size()-1] += "\"" + items[cnt] + "\"" ;
    }

    // Check items
    vector <string> new_items ;
    new_items.reserve ( items.size() ) ;
    TWikidataDB wd_db ( "wikidatawiki" , platform );
    for ( size_t chunk = 0 ; chunk < item_batches.size() ; chunk++ ) {
        string sql = "SELECT page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0" ;
        sql += " AND page_title IN (" + item_batches[chunk] + ")" ;
        if ( wdf_only_items_without_p18 ) sql += " AND NOT EXISTS (SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=120 AND pl_title='P18')" ;
        sql += " AND NOT EXISTS (SELECT * FROM pagelinks WHERE pl_from=page_id AND pl_namespace=0 AND pl_title IN ('Q13406463','Q4167410'))" ; // No list/disambig
        MYSQL_RES *result = wd_db.getQueryResults ( sql ) ;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            new_items.push_back ( row[0] ) ;
        }
        mysql_free_result(result);
    }
    items.swap ( new_items ) ;
}

void TWDFIST::filterFiles () {
    filterFilesFromIgnoreDatabase () ;
    filterFilesFiveOrIsUsed () ;
    removeItemsWithNoFileCandidates () ;
}

void TWDFIST::removeItemsWithNoFileCandidates () {
    // Remove items with no files
    vector <string> remove_q ;
    for ( auto qi:q2image ) {
        if ( qi.second.empty() ) remove_q.push_back ( qi.first ) ;
    }
    for ( auto& q:remove_q ) {
        q2image.erase ( q2image.find(q) ) ;
    }
}

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
        if ( cnt % ITEM_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
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

string TWDFIST::normalizeFilename ( string filename ) {
    string ret = trim ( filename ) ;
    replace ( ret.begin(), ret.end(), ' ', '_' ) ;
    json o = ret ;
    try { // HACK
        string dummy = o.dump() ;
    } catch ( ... ) {
        ret = "" ;
    }
    return ret ;
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

// 	if ( preg_match ( '/\bribbon.jpe{0,1}g/i' , $i ) ) // TODO
    if ( file.find("600px_") == 0 && type == "png" ) return false ;

    return true ;
}

void TWDFIST::seedIgnoreFilesFromWikiPage () {
    // Load wiki list
    string wikitext = loadTextfromURL ( "http://www.wikidata.org/w/index.php?title=User:Magnus_Manske/FIST_icons&action=raw" ) ;
    vector <string> rows ;
    split ( wikitext , rows , '\n' ) ;
    for ( size_t row = 0 ; row < rows.size() ; row++ ) {
        string file = rows[row] ;
        while ( !file.empty() && (file[0]=='*'||file[0]==' ') ) file.erase ( file.begin() , file.begin()+1 ) ;
        file = normalizeFilename ( trim ( file ) ) ;
        if ( !isValidFile(file) ) continue ;
        files2ignore[file] = 1 ;
    }
}

void TWDFIST::seedIgnoreFilesFromIgnoreDatabase () {
    // Load files that were ignored at least three times
    TWikidataDB wdfist_db ;
    wdfist_db.setHostDB ( "tools.labsdb" , "s51218__wdfist_p" , true ) ; // HARDCODED publicly readable
    wdfist_db.doConnect ( true ) ;
    string sql = "SELECT CONVERT(`file` USING utf8) FROM ignore_files GROUP BY file HAVING count(*)>=3" ;
    MYSQL_RES *result = wdfist_db.getQueryResults ( sql ) ;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        string file = normalizeFilename ( row[0] ) ;
        if ( !isValidFile(file) ) continue ;
        files2ignore[file] = 1 ;
    }
    mysql_free_result(result);
}

void TWDFIST::seedIgnoreFiles () {
    seedIgnoreFilesFromWikiPage() ;
    seedIgnoreFilesFromIgnoreDatabase() ;
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
        if ( cnt % ITEM_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
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
        if ( cnt % ITEM_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
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
        if ( cnt % ITEM_BATCH_SIZE == 0 ) item_batches.push_back ( "" ) ;
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

void TWDFIST::followCommonsCats () {} // TODO

string TWDFIST::run () {
    // Init JSON output
    json j ;
    j["status"] = "OK" ;
    j["data"] = json::object() ;

    platform->content_type = "application/json; charset=utf-8" ; // Output is always JSON
    pagelist->convertToWiki ( "wikidatawiki" ) ; // Making sure

    // Convert pagelist into item list, then save space by clearing pagelist
    items.reserve ( pagelist->size() ) ;
    for ( auto& i:pagelist->pages ) {
        if ( i.meta.ns != 0 ) continue ;
        items.push_back ( i.getNameWithoutNamespace() ) ;
    }
    pagelist->clear() ;
    if ( items.empty() ) { // No items
        j["status"] = "No items from original query" ;
        return j.dump() ;
    }

    // Follow
    wdf_langlinks = !(platform->getParam("wdf_langlinks","").empty()) ;
    wdf_coords = !(platform->getParam("wdf_coords","").empty()) ;
    wdf_search_commons = !(platform->getParam("wdf_search_commons","").empty()) ;
    wdf_commons_cats = !(platform->getParam("wdf_commons_cats","").empty()) ;

    // Options
    wdf_only_items_without_p18 = !(platform->getParam("wdf_only_items_without_p18","").empty()) ;
    wdf_only_files_not_on_wd = !(platform->getParam("wdf_only_files_not_on_wd","").empty()) ;
    wdf_only_jpeg = !(platform->getParam("wdf_only_jpeg","").empty()) ;
    wdf_max_five_results = !(platform->getParam("wdf_max_five_results","").empty()) ;
    wdf_only_page_images = !(platform->getParam("wdf_only_page_images","").empty()) ;
    wdf_allow_svg = !(platform->getParam("wdf_allow_svg","").empty()) ;

    // Prepare
    seedIgnoreFiles() ;
    filterItems() ;
    if ( items.size() == 0 ) {
        j["status"] = "No items from original query" ;
        return j.dump() ;
    }

    // Run followers
    if ( wdf_langlinks ) followLanguageLinks() ;
    if ( wdf_coords ) followCoordinates() ;
    if ( wdf_search_commons ) followSearchCommons() ;
    if ( wdf_commons_cats ) followCommonsCats() ;

    filterFiles() ;

    j["data"] = q2image ;
    return j.dump() ;
}
*/
