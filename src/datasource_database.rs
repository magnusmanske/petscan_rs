use crate::datasource::DataSource;
use crate::datasource::SQLtuple;
use crate::pagelist::*;
use crate::platform::Platform;
use mediawiki::api::{Api, NamespaceID};
use mediawiki::title::Title;
use mysql as my;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
/*
use serde_json::value::Value;
*/

static MAX_CATEGORY_BATCH_SIZE: usize = 5000;
//static USE_NEW_CATEGORY_MODE: bool = true;

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

    pub fn group_link_list_by_namespace(
        &self,
        input: &Vec<String>,
        api: &Api,
    ) -> HashMap<NamespaceID, Vec<String>> {
        let mut ret: HashMap<NamespaceID, Vec<String>> = HashMap::new();
        input.iter().for_each(|title| {
            let title = Title::new_from_full(title, &api);
            if !ret.contains_key(&title.namespace_id()) {
                ret.insert(title.namespace_id(), vec![]);
            }
            ret.get_mut(&title.namespace_id())
                .unwrap()
                .push(title.pretty().to_string());
        });
        ret
    }

    pub fn links_from_subquery(&self, input: &Vec<String>, api: &Api) -> SQLtuple {
        let mut sql: SQLtuple = ("(".to_string(), vec![]);
        let nslist = self.group_link_list_by_namespace(input, api);
        nslist.iter().for_each(|nsgroup| {
            if !sql.1.is_empty() {
                sql.0 += " ) OR ( ";
            }
            sql.0 += "( SELECT p_to.page_id FROM page p_to,page p_from,pagelinks WHERE p_from.page_namespace=";
            sql.0 += &nsgroup.0.to_string();
            sql.0 += "  AND p_from.page_id=pl_from AND pl_namespace=p_to.page_namespace AND pl_title=p_to.page_title AND p_from.page_title IN (";
            Platform::append_sql(&mut sql, &mut Platform::prep_quote(nsgroup.1));
            sql.0 += ") )";
        });
        sql.0 += ")";
        sql
    }

    pub fn links_to_subquery(&self, input: &Vec<String>, api: &Api) -> SQLtuple {
        let mut sql: SQLtuple = ("(".to_string(), vec![]);
        let nslist = self.group_link_list_by_namespace(input, api);
        nslist.iter().for_each(|nsgroup| {
            if !sql.1.is_empty() {
                sql.0 += " ) OR ( ";
            }
            sql.0 += "( SELECT DISTINCT pl_from FROM pagelinks WHERE pl_namespace=";
            sql.0 += &nsgroup.0.to_string();
            sql.0 += " AND pl_title IN (";
            Platform::append_sql(&mut sql, &mut Platform::prep_quote(nsgroup.1));
            sql.0 += ") )";
        });
        sql.0 += ")";
        sql
    }

    pub fn iterate_category_batches(
        &self,
        categories: &Vec<Vec<String>>,
        start: usize,
    ) -> Vec<Vec<Vec<String>>> {
        let mut ret: Vec<Vec<Vec<String>>> = vec![];
        categories[start]
            .chunks(MAX_CATEGORY_BATCH_SIZE)
            .for_each(|c| {
                if start + 1 >= categories.len() {
                    let to_add = vec![c.to_vec()];
                    ret.push(to_add);
                    return;
                }
                let tmp = self.iterate_category_batches(categories, start + 1);
                tmp.iter().for_each(|t| {
                    let mut to_add = vec![c.to_vec()];
                    to_add.append(&mut t.to_owned());
                    ret.push(to_add);
                });
            });
        ret
    }

    /*
    bool TSourceDatabase::getPages () {
    bool TSourceDatabase::getPagesforPrimary ( TWikidataDB &db , string primary , string sql , string sql_before_after , vector <TPage> &pages_sublist , bool is_before_after_done ) {

    */
}
