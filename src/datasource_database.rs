use crate::datasource::DataSource;
use crate::datasource::SQLtuple;
use crate::pagelist::*;
use crate::platform::Platform;
use mediawiki::title::Title;
use mysql as my;
use rayon::prelude::*;
use std::collections::HashSet;
/*
use mediawiki::api::Api;
use serde_json::value::Value;
*/

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabaseCatDepth {
    pub name: String,
    pub depth: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabase {}

impl DataSource for SourceDatabase {
    fn name(&self) -> String {
        "categories".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        // TODO more
        platform.has_param("categories")
    }

    fn run(&self, _platform: &Platform) -> Option<PageList> {
        None // TODO
    }
}

impl SourceDatabase {
    pub fn new() -> Self {
        Self {}
    }

    fn go_depth(
        &self,
        conn: &mut my::Conn,
        tmp: &mut HashSet<String>,
        cats: &Vec<String>,
        depth: u16,
    ) {
        if depth == 0 || cats.is_empty() {
            return;
        }
        let mut sql : SQLtuple = ("SELECT DISTINCT page_title FROM page,categorylinks WHERE cl_from=page_id AND cl_type='subcat' AND cl_to IN (".to_string(),vec![]);
        cats.iter().for_each(|c| {
            tmp.insert(c.to_string());
        });
        Platform::append_sql(&mut sql, &mut Platform::prep_quote(cats));
        sql.0 += ")";
        println!("{:?}", sql);

        let mut new_cats: Vec<String> = vec![];
        let result = match conn.prep_exec(sql.0, sql.1) {
            Ok(r) => r,
            Err(e) => {
                println!("ERROR: {:?}", e);
                return;
            }
        };
        for row in result {
            let page_title: String = my::from_row(row.unwrap());
            if !tmp.contains(&page_title) {
                new_cats.push(page_title.to_owned());
                tmp.insert(page_title);
            }
        }

        self.go_depth(conn, tmp, &new_cats, depth - 1);
    }

    fn get_categories_in_tree(
        &self,
        conn: &mut my::Conn,
        title: &String,
        depth: u16,
    ) -> Vec<String> {
        let mut tmp: HashSet<String> = HashSet::new();
        let title = Title::spaces_to_underscores(&Title::first_letter_uppercase(title));
        tmp.insert(title.to_owned());
        self.go_depth(conn, &mut tmp, &vec![title], depth);
        tmp.par_iter().cloned().collect::<Vec<String>>()
    }

    pub fn parse_category_list(
        &self,
        conn: &mut my::Conn,
        input: &Vec<SourceDatabaseCatDepth>,
    ) -> Vec<Vec<String>> {
        input
            .iter()
            .map(|i| self.get_categories_in_tree(conn, &i.name, i.depth))
            .filter(|x| !x.is_empty())
            .collect()
    }

    pub fn template_subquery(
        &self,
        input: &Vec<String>,
        use_talk_page: bool,
        find_not: bool,
    ) -> SQLtuple {
        let mut sql = Platform::sql_tuple();
        if use_talk_page {
            sql.0 += if find_not {
                " AND NOT EXISTS "
            } else {
                " AND EXISTS "
            };
            sql.0 += "(SELECT * FROM templatelinks,page pt WHERE MOD(p.page_namespace,2)=0 AND pt.page_title=p.page_title AND pt.page_namespace=p.page_namespace+1 AND tl_from=pt.page_id AND tl_namespace=10 AND tl_title";
        } else {
            sql.0 += if find_not {
                " AND p.page_id NOT IN "
            } else {
                " AND p.page_id IN "
            };
            sql.0 +=
                "(SELECT DISTINCT tl_from FROM templatelinks WHERE tl_namespace=10 AND tl_title";
        }

        sql.0 += " IN (";
        Platform::append_sql(&mut sql, &mut Platform::prep_quote(input));
        sql.0 += ")";

        sql
    }

    /*
    void TSourceDatabase::groupLinkListByNamespace ( vector <string> &input , map <int32_t,vector <string> > &nslist ) {
    string TSourceDatabase::linksFromSubquery ( TWikidataDB &db , vector <string> input ) { // TODO speed up (e.g. IN ()); pages from all namespaces?
    string TSourceDatabase::linksToSubquery ( TWikidataDB &db , vector <string> input ) { // TODO speed up (e.g. IN ()); pages from all namespaces?
    void TSourceDatabase::iterateCategoryBatches ( vector <vvs> &ret , vvs &categories , uint32_t start ) {
    bool TSourceDatabase::getPages () {
    bool TSourceDatabase::getPagesforPrimary ( TWikidataDB &db , string primary , string sql , string sql_before_after , vector <TPage> &pages_sublist , bool is_before_after_done ) {

    */
}
