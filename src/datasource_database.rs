use crate::datasource::DataSource;
use crate::datasource::SQLtuple;
use crate::pagelist::*;
use crate::platform::Platform;
use chrono::prelude::*;
use chrono::Duration;
use core::ops::Sub;
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
static USE_NEW_CATEGORY_MODE: bool = false;

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabaseCatDepth {
    pub name: String,
    pub depth: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabaseParameters {
    pub combine: String,
    pub namespace_ids: Vec<usize>,
    pub linked_from_all: Vec<String>,
    pub linked_from_any: Vec<String>,
    pub linked_from_none: Vec<String>,
    pub links_to_all: Vec<String>,
    pub links_to_any: Vec<String>,
    pub links_to_none: Vec<String>,
    pub templates_yes: Vec<String>,
    pub templates_any: Vec<String>,
    pub templates_no: Vec<String>,
    pub templates_yes_talk_page: bool,
    pub templates_any_talk_page: bool,
    pub templates_no_talk_page: bool,
    pub page_image: String,
    pub ores_type: String,
    pub ores_prediction: String,
    pub ores_prob_from: f32,
    pub ores_prob_to: f32,
    pub last_edit_bot: String,
    pub last_edit_anon: String,
    pub last_edit_flagged: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabase {
    cat_pos: Vec<Vec<String>>,
    cat_neg: Vec<Vec<String>>,
    has_pos_cats: bool,
    has_neg_cats: bool,
    has_pos_templates: bool,
    has_pos_linked_from: bool,
    params: SourceDatabaseParameters,
}

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
    pub fn new(params: SourceDatabaseParameters) -> Self {
        Self {
            cat_pos: vec![],
            cat_neg: vec![],
            has_pos_cats: false,
            has_neg_cats: false,
            has_pos_templates: false,
            has_pos_linked_from: false,
            params,
        }
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

    fn parse_category_depth(
        &self,
        cats: &Vec<String>,
        default_depth: u16,
    ) -> Vec<SourceDatabaseCatDepth> {
        cats.iter()
            .filter_map(|c| {
                let mut parts = c.split("|");
                let name = match parts.next() {
                    Some(n) => n.to_string(),
                    None => return None,
                };
                let depth = match parts.next() {
                    Some(depth) => depth.parse::<u16>().ok()?,
                    None => default_depth,
                };
                Some(SourceDatabaseCatDepth {
                    name: name,
                    depth: depth,
                })
            })
            .collect()
    }

    pub fn get_pages(
        &mut self,
        platform: &Platform,
        primary_pagelist: Option<&PageList>,
    ) -> Option<PageList> {
        let wiki = match primary_pagelist {
            Some(pl) => pl.wiki.clone(),
            None => platform.get_param("wiki"),
        };

        // Paranoia
        if wiki.is_none() || wiki == Some("wiki".to_string()) {
            return None;
        }
        let wiki = wiki.unwrap();

        let db_user_pass = platform.state.get_db_mutex().lock().unwrap(); // Force DB connection placeholder
        let mut ret = PageList::new_from_wiki(&wiki);
        let mut conn = platform
            .state
            .get_wiki_db_connection(&db_user_pass, &wiki)?;

        let depth = platform
            .get_param("depth")
            .unwrap_or("0".to_string())
            .parse::<u16>()
            .ok()?;
        let cat_pos =
            self.parse_category_depth(&platform.get_param_as_vec("categories", "\n"), depth);
        let cat_neg = self.parse_category_depth(&platform.get_param_as_vec("negcats", "\n"), depth);
        self.cat_pos = self.parse_category_list(&mut conn, &cat_pos);
        self.cat_neg = self.parse_category_list(&mut conn, &cat_neg);

        self.has_pos_templates =
            !self.params.templates_yes.is_empty() || !self.params.templates_any.is_empty();
        self.has_pos_linked_from = !self.params.linked_from_all.is_empty()
            || !self.params.linked_from_any.is_empty()
            || !self.params.links_to_all.is_empty()
            || !self.params.links_to_any.is_empty();

        let primary = if !self.cat_pos.is_empty() {
            "categories"
        } else if self.has_pos_templates {
            "templates"
        } else if self.has_pos_linked_from {
            "links_from"
        } else if primary_pagelist.is_some() {
            "pagelist"
        } else if platform.get_param("page_wikidata_item") == Some("without".to_string()) {
            "no_wikidata"
        } else {
            return None;
        };
        println!("PRIMARY: {}", &primary);

        let lc = if platform.has_param("minlinks") || platform.has_param("maxlinks") {
            ",(SELECT count(*) FROM pagelinks WHERE pl_from=p.page_id) AS link_count"
        } else {
            ""
        };

        let mut sql_before_after = Platform::sql_tuple();
        let mut before: String = "".to_string();
        let mut after: String = "".to_string();
        let mut is_before_after_done: bool = false;
        match platform.get_param("max_age") {
            Some(max_age) => {
                let utc: DateTime<Utc> = Utc::now();
                let utc = utc.sub(Duration::hours(max_age.parse::<i64>().unwrap_or(0)));
                before = "".to_string();
                after = utc.format("%Y%m%d%H%M%S").to_string();
            }
            None => {}
        }

        if !before.is_empty() || after.is_empty() {
            is_before_after_done = true;
        } else {
            sql_before_after.0 = " INNER JOIN (revision r) on r.rev_page=p.page_id".to_string();
            if platform.has_param("only_new_since") {
                sql_before_after.0 += " AND r.rev_parent_id=0";
            }
            if !before.is_empty() {
                sql_before_after.0 += " AND rev_timestamp<=?";
                sql_before_after.1.push(before);
            }
            if !after.is_empty() {
                sql_before_after.0 += " AND rev_timestamp>=?";
                sql_before_after.1.push(after);
            }
            sql_before_after.0 += " ";
        }

        let mut sql = Platform::sql_tuple();

        match primary {
            "categories" => {
                let category_batches = if USE_NEW_CATEGORY_MODE {
                    self.iterate_category_batches(&self.cat_pos, 0)
                } else {
                    vec![self.cat_pos.to_owned()]
                };

                for category_batch in category_batches {
                    match self.params.combine.as_str() {
                        "subset" => {
                            sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len".to_string() ;
                            sql.0 += lc;
                            sql.0 += " FROM ( SELECT * from categorylinks where cl_to IN (";
                            Platform::append_sql(
                                &mut sql,
                                &mut Platform::prep_quote(&category_batch[0].to_owned()),
                            );
                            sql.0 += ")) cl0";
                            for a in 1..category_batch.len() - 1 {
                                sql.0 += format!(" INNER JOIN categorylinks cl{} on cl0.cl_from=cl{}.cl_from and cl{}.cl_to IN (",a,a,a).as_str();
                                Platform::append_sql(
                                    &mut sql,
                                    &mut Platform::prep_quote(&category_batch[a].to_owned()),
                                );
                                sql.0 += ")";
                            }
                        }
                        "union" => {
                            let mut tmp: HashSet<String> = HashSet::new();
                            category_batch.iter().for_each(|group| {
                                group.iter().for_each(|s| {
                                    tmp.insert(s.to_string());
                                });
                            });
                            let tmp = tmp.iter().map(|s| s.to_owned()).collect::<Vec<String>>();
                            sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len".to_string() ;
                            sql.0 += lc;
                            sql.0 += " FROM ( SELECT * FROM categorylinks WHERE cl_to IN (";
                            Platform::append_sql(&mut sql, &mut Platform::prep_quote(&tmp));
                            sql.0 += ")) cl0";
                        }
                        other => {
                            panic!("self.params.combine is '{}'", &other);
                        }
                    }
                    sql.0 += " INNER JOIN (page p";
                    sql.0 += ") ON p.page_id=cl0.cl_from";
                    let mut pl2 = PageList::new_from_wiki(&wiki.clone());
                    if self.get_pages_for_primary(
                        &mut conn,
                        &primary.to_string(),
                        &mut sql,
                        &sql_before_after,
                        &pl2,
                        is_before_after_done,
                        platform.state.get_api_for_wiki(wiki.clone())?,
                    ) {
                        if ret.is_empty() {
                            ret.swap_entries(&mut pl2);
                        } else {
                            ret.union(Some(pl2)).unwrap();
                        }
                    } else {
                        return None;
                    }
                }

                //data_loaded = true ;
                return Some(ret);
            }
            "no_wikidata" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len".to_string() ;
                sql.0 += lc;
                sql.0 += " FROM page p";
                if !is_before_after_done {
                    is_before_after_done = true;
                    Platform::append_sql(&mut sql, &mut sql_before_after);
                }
                sql.0 += " WHERE p.page_id NOT IN (SELECT pp_page FROM page_props WHERE pp_propname='wikibase_item')" ;
            }
            "templates" | "links_from" => {
                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len FROM page p".to_string() ;
                if !is_before_after_done {
                    is_before_after_done = true;
                    Platform::append_sql(&mut sql, &mut sql_before_after);
                }
                sql.0 += " WHERE 1=1";
            }
            "pagelist" => {
                let primary_pagelist = primary_pagelist.unwrap();
                ret.wiki = primary_pagelist.wiki.to_owned();
                if primary_pagelist.is_empty() {
                    // Nothing to do, but that's OK
                    return Some(ret);
                }

                sql.0 = "SELECT DISTINCT p.page_id,p.page_title,p.page_namespace,p.page_touched,p.page_len FROM page p".to_string() ;
                if !is_before_after_done {
                    is_before_after_done = true;
                    Platform::append_sql(&mut sql, &mut sql_before_after);
                }
                sql.0 += " WHERE (0=1)";

                let nslist = primary_pagelist.group_by_namespace();
                nslist.iter().for_each(|nsgroup| {
                    sql.0 += " OR (p.page_namespace=";
                    sql.0 += &nsgroup.0.to_string();
                    sql.0 += " AND p.page_title IN (";
                    Platform::append_sql(&mut sql, &mut Platform::prep_quote(nsgroup.1));
                    sql.0 += "))";
                });
            }
            other => {
                println!("SourceDatabase::get_pages: other primary '{}'", &other);
                return None;
            }
        }

        //if ( !getPagesforPrimary ( db , primary , sql , sql_before_after , pages , is_before_after_done ) ) return false ;

        if self.get_pages_for_primary(
            &mut conn,
            &primary.to_string(),
            &mut sql,
            &sql_before_after,
            &ret,
            is_before_after_done,
            platform.state.get_api_for_wiki(wiki.clone())?,
        ) {
            //data_loaded = true ;
            Some(ret)
        } else {
            None
        }
    }

    fn get_pages_for_primary(
        &self,
        _conn: &mut my::Conn,
        _primary: &String,
        mut sql: &mut SQLtuple,
        _sql_before_after: &SQLtuple,
        _pages_sublist: &PageList,
        _is_before_after_done: bool,
        api: Api,
    ) -> bool {
        // Namespaces
        if !self.params.namespace_ids.is_empty() {
            sql.0 += " AND p.page_namespace IN(";
            sql.0 += &self
                .params
                .namespace_ids
                .iter()
                .map(|ns| ns.to_string())
                .collect::<Vec<String>>()
                .join(",");
            sql.0 += ")";
        }

        // Negative categories
        if !self.cat_neg.is_empty() {
            self.cat_neg.iter().for_each(|cats|{
                sql.0 += " AND p.page_id NOT IN (SELECT DISTINCT cl_from FROM categorylinks WHERE cl_to IN (" ;
                Platform::append_sql(&mut sql, &mut Platform::prep_quote(cats));
                sql.0 += "))" ;
            });
        }

        // Templates as secondary; template namespace only!
        // TODO talk page
        if self.has_pos_templates {
            // All
            self.params.templates_yes.iter().for_each(|t| {
                let mut tmp = self.template_subquery(
                    &vec![t.to_string()],
                    self.params.templates_yes_talk_page,
                    false,
                );
                Platform::append_sql(&mut sql, &mut tmp);
            });

            // Any
            if !self.params.templates_any.is_empty() {
                let mut tmp = self.template_subquery(
                    &self.params.templates_any,
                    self.params.templates_any_talk_page,
                    false,
                );
                Platform::append_sql(&mut sql, &mut tmp);
            }
        }

        // Negative templates
        if !self.params.templates_no.is_empty() {
            let mut tmp = self.template_subquery(
                &self.params.templates_no,
                self.params.templates_no_talk_page,
                true,
            );
            Platform::append_sql(&mut sql, &mut tmp);
        }

        // Links from all
        self.params.linked_from_all.iter().for_each(|l| {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                &mut self.links_from_subquery(&vec![l.to_owned()], &api),
            );
        });

        // Links from any
        if !self.params.linked_from_any.is_empty() {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                &mut self.links_from_subquery(&self.params.linked_from_any, &api),
            );
        }

        // Links from none
        if !self.params.linked_from_none.is_empty() {
            sql.0 += " AND p.page_id NOT IN ";
            Platform::append_sql(
                &mut sql,
                &mut self.links_from_subquery(&self.params.linked_from_none, &api),
            );
        }

        // Links to all
        self.params.links_to_all.iter().for_each(|l| {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                &mut self.links_to_subquery(&vec![l.to_owned()], &api),
            );
        });

        // Links to any
        if !self.params.links_to_any.is_empty() {
            sql.0 += " AND p.page_id IN ";
            Platform::append_sql(
                &mut sql,
                &mut self.links_to_subquery(&self.params.links_to_any, &api),
            );
        }

        // Links to none
        if !self.params.links_to_none.is_empty() {
            sql.0 += " AND p.page_id NOT IN ";
            Platform::append_sql(
                &mut sql,
                &mut self.links_to_subquery(&self.params.links_to_none, &api),
            );
        }

        // Lead image
        match self.params.page_image.as_str() {
            "yes" => sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))" ,
            "free" => sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image_free')" ,
            "nonfree" => sql.0 += " AND EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname='page_image')" ,
            "no" => sql.0 += " AND NOT EXISTS (SELECT * FROM page_props WHERE p.page_id=pp_page AND pp_propname IN ('page_image','page_image_free'))" ,
            _ => {}
        }

        // ORES
        // TODO Option<> instead of 0.0?
        if self.params.ores_type != "any"
            && (self.params.ores_prediction != "any"
                || self.params.ores_prob_from != 0.0
                || self.params.ores_prob_to != 1.0)
        {
            sql.0 += " AND EXISTS (SELECT * FROM ores_classification WHERE p.page_latest=oresc_rev AND oresc_model IN (SELECT oresm_id FROM ores_model WHERE oresm_is_current=1 AND oresm_name=?)" ;
            sql.1.push(self.params.ores_type.to_owned());
            match self.params.ores_prediction.as_str() {
                "yes" => sql.0 += " AND oresc_is_predicted=1",
                "no" => sql.0 += " AND oresc_is_predicted=0",
                _ => {}
            }
            if self.params.ores_prob_from != 0.0 {
                sql.0 += " AND oresc_probability>=";
                sql.0 += self.params.ores_prob_from.to_string().as_str();
            }
            if self.params.ores_prob_to != 1.0 {
                sql.0 += " AND oresc_probability<=";
                sql.0 += self.params.ores_prob_to.to_string().as_str();
            }
            sql.0 += ")";
        }

        // Last edit
        match self.params.last_edit_anon.as_str() {
            "yes" => sql.0 +=" AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NULL)" ,
            "no" => sql.0 +=" AND EXISTS (SELECT * FROM revision,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user IS NOT NULL)" ,
            _ => {}
        }
        match self.params.last_edit_bot.as_str() {
            "yes" => sql.0 +=" AND EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')" ,
            "no" => sql.0 +=" AND NOT EXISTS (SELECT * FROM revision,user_groups,actor WHERE rev_id=page_latest AND rev_page=page_id AND rev_actor=actor_id AND actor_user=ug_user AND ug_group='bot')" ,
            _ => {}
        }
        match self.params.last_edit_flagged.as_str() {
            "yes" => sql.0 +=
                " AND NOT EXISTS (SELECT * FROM flaggedpage_pending WHERE p.page_id=fpp_page_id)",
            "no" => {
                sql.0 +=
                    " AND EXISTS (SELECT * FROM flaggedpage_pending WHERE p.page_id=fpp_page_id)"
            }
            _ => {}
        }

        // TODO remove when done
        false
    }
}
