use crate::datasource::SQLtuple;
use crate::datasource::database::{SourceDatabase, SourceDatabaseParameters};
use crate::pagelist::{DatabaseCluster, PageList};
use crate::pagelist_entry::{FileInfo, LinkCount, PageListEntry, TriState};
use crate::platform::{PAGE_BATCH_SIZE, Platform};
use anyhow::{Result, anyhow};
use my::Value::Bytes;
use mysql_async as my;
use mysql_async::Value as MyValue;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use rayon::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::Mutex as TokioMutex;
use wikimisc::mediawiki::api::NamespaceID;
use wikimisc::mediawiki::title::Title;

// ─── Page-fields flags ───────────────────────────────────────────────────────

/// Captures which optional page fields were requested for `process_pages`.
struct PageFields {
    add_image: bool,
    add_coordinates: bool,
    add_defaultsort: bool,
    add_disambiguation: bool,
    add_incoming_links: bool,
    add_sitelinks: bool,
    is_wikidata: bool,
}

impl PageFields {
    const fn any(&self) -> bool {
        self.add_image
            || self.add_coordinates
            || self.add_defaultsort
            || self.add_disambiguation
            || self.add_incoming_links
            || self.add_sitelinks
    }

    /// Builds the SQL SELECT column list for the requested fields.
    fn build_select_columns(&self) -> String {
        let mut sql = "SELECT page_title,page_namespace".to_string();
        if self.add_image {
            sql += ",(SELECT pp_value FROM page_props WHERE pp_page=page_id AND pp_propname IN ('page_image','page_image_free') LIMIT 1) AS image";
        }
        if self.add_coordinates {
            sql += ",(SELECT concat(gt_lat,',',gt_lon) FROM geo_tags WHERE gt_primary=1 AND gt_globe='earth' AND gt_page_id=page_id LIMIT 1) AS coord";
        }
        if self.add_defaultsort {
            sql += ",(SELECT pp_value FROM page_props WHERE pp_page=page_id AND pp_propname='defaultsort' LIMIT 1) AS defaultsort";
        }
        if self.add_disambiguation {
            sql += ",(SELECT pp_value FROM page_props WHERE pp_page=page_id AND pp_propname='disambiguation' LIMIT 1) AS disambiguation";
        }
        if self.add_incoming_links {
            sql += ",(SELECT count(*) FROM pagelinks,linktarget WHERE pl_target_id=lt_id AND lt_namespace=page_namespace AND lt_title=page_title AND pl_from_namespace=0) AS incoming_links";
        }
        if self.add_sitelinks {
            if self.is_wikidata {
                sql += ",(SELECT count(*) FROM wb_items_per_site WHERE page_namespace IN (0,120) AND ips_item_id=substr(page_title,2)) AS sitelinks";
            } else {
                sql += ",(SELECT count(*) FROM langlinks WHERE ll_from=page_id) AS sitelinks";
            }
        }
        sql += " FROM page WHERE ";
        sql
    }

    /// Applies the result row values to an entry (after stripping `page_title` and `page_namespace`).
    fn apply_row_to_entry(&self, parts: &mut Vec<MyValue>, entry: &mut PageListEntry) {
        if self.add_image {
            entry.set_page_image(match parts.remove(0) {
                Bytes(s) => String::from_utf8(s).ok(),
                _ => None,
            });
        }
        if self.add_coordinates {
            let coordinates = match parts.remove(0) {
                Bytes(s) => match String::from_utf8(s) {
                    Ok(lat_lon) => wikimisc::lat_lon::LatLon::from_str(&lat_lon).ok(),
                    _ => None,
                },
                _ => None,
            };
            entry.set_coordinates(coordinates);
        }
        if self.add_defaultsort {
            entry.set_defaultsort(match parts.remove(0) {
                Bytes(s) => String::from_utf8(s).ok(),
                _ => None,
            });
        }
        if self.add_disambiguation {
            let dis = match parts.remove(0) {
                my::Value::NULL => TriState::No,
                _ => TriState::Yes,
            };
            entry.set_disambiguation(dis);
        }
        if self.add_incoming_links {
            let il = match parts.remove(0) {
                my::Value::Int(i) => Some(i as LinkCount),
                _ => None,
            };
            entry.set_incoming_links(il);
        }
        if self.add_sitelinks {
            let sc = match parts.remove(0) {
                my::Value::Int(i) => Some(i as LinkCount),
                _ => None,
            };
            entry.set_sitelink_count(sc);
        }
    }
}

impl Platform {
    // ─── Entry helpers ───────────────────────────────────────────────────────

    /// Converts a Wikidata entity ID string (e.g. "Q123", "P456") into a `PageListEntry`
    pub fn entry_from_entity(entity: &str) -> Option<PageListEntry> {
        match entity.chars().next() {
            Some('Q') => Some(PageListEntry::new(Title::new(entity, 0))),
            Some('P') => Some(PageListEntry::new(Title::new(entity, 120))),
            Some('L') => Some(PageListEntry::new(Title::new(entity, 146))),
            _ => None,
        }
    }

    // ─── Label SQL (new wbt_ schema) ─────────────────────────────────────────

    fn get_label_sql_helper_new(&self, ret: &mut SQLtuple, part1: &str) {
        let mut wbt_type: Vec<String> = vec![];
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_l")) {
            wbt_type.push("1".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_a")) {
            wbt_type.push("3".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_d")) {
            wbt_type.push("2".to_string());
        }
        if !wbt_type.is_empty() {
            if wbt_type.len() == 1 {
                ret.0 += &format!(" AND wbtl_type_id={}", wbt_type.join(","));
            } else {
                ret.0 += &format!(" AND wbtl_type_id IN ({})", wbt_type.join(","));
            }
        }
    }

    fn get_label_sql_subquery_new(
        &self,
        ret: &mut SQLtuple,
        key: &str,
        languages: &[String],
        s: &str,
    ) {
        let has_pattern = !s.is_empty() && s != "%";
        let has_languages = !languages.is_empty();
        ret.0 += "SELECT * FROM wbt_term_in_lang,wbt_item_terms t2";
        if has_languages || has_pattern {
            ret.0 += ",wbt_text_in_lang";
        }
        if has_pattern {
            ret.0 += ",wbt_text";
        }
        ret.0 += " WHERE t2.wbit_item_id=t1.wbit_item_id AND wbtl_id=t2.wbit_term_in_lang_id";
        self.get_label_sql_helper_new(ret, key);
        if has_languages || has_pattern {
            let mut tmp = crate::datasource::prep_quote(languages);
            ret.0 += " AND wbtl_text_in_lang_id=wbxl_id";
            if !tmp.1.is_empty() {
                if tmp.1.len() == 1 {
                    ret.0 += &(" AND wbxl_language=".to_owned() + &tmp.0);
                } else {
                    ret.0 += &(" AND wbxl_language IN (".to_owned() + &tmp.0 + ")");
                }
                ret.1.append(&mut tmp.1);
            }
            if has_pattern {
                ret.0 += " AND wbxl_text_id=wbx_id AND wbx_text LIKE ?";
                ret.1.push(MyValue::Bytes(s.to_owned().into()));
            }
        }
    }

    fn get_label_sql_new(&self, namespace_id: &NamespaceID) -> Option<SQLtuple> {
        let mut ret: SQLtuple = (String::new(), vec![]);
        let yes = self.get_param_as_vec("labels_yes", "\n");
        let any = self.get_param_as_vec("labels_any", "\n");
        let no = self.get_param_as_vec("labels_no", "\n");
        if yes.len() + any.len() + no.len() == 0 {
            return None;
        }

        let langs_yes = self.get_param_as_vec("langs_labels_yes", ",");
        let langs_any = self.get_param_as_vec("langs_labels_any", ",");
        let langs_no = self.get_param_as_vec("langs_labels_no", ",");

        if *namespace_id == 0 {
            ret.0 =
                "SELECT DISTINCT CONCAT('Q',wbit_item_id) AS term_full_entity_id FROM wbt_item_terms t1 WHERE 1=1".to_string();
        } else if *namespace_id == 120 {
            ret.0 = "SELECT DISTINCT CONACT('P',wbit_property_id) AS term_full_entity_id FROM wbt_property_terms t1 WHERE 1=1"
                .to_string();
        } else {
            return None;
        }

        for s in &yes {
            ret.0 += " AND EXISTS (";
            self.get_label_sql_subquery_new(&mut ret, "yes", &langs_yes, s);
            ret.0 += ")";
        }

        if !langs_any.is_empty() {
            ret.0 += " AND (0=1";
            for s in &any {
                ret.0 += " OR EXISTS (";
                self.get_label_sql_subquery_new(&mut ret, "any", &langs_any, s);
                ret.0 += ")";
            }
            ret.0 += ")";
        }

        for s in &no {
            ret.0 += " AND NOT EXISTS (";
            self.get_label_sql_subquery_new(&mut ret, "no", &langs_no, s);
            ret.0 += ")";
        }
        Some(ret)
    }

    // ─── Post-processing pipeline ─────────────────────────────────────────────

    /// Applies wikidata-centric filters (`filter_wikidata`, `sitelinks`, `labels`).
    async fn apply_wikidata_filters(
        &self,
        result: &PageList,
        available_sources: &[String],
    ) -> Result<()> {
        Platform::profile("before filter_wikidata", Some(result.len()));
        self.filter_wikidata(result).await?;
        Platform::profile("after filter_wikidata", Some(result.len()));

        if available_sources.to_vec() != vec!["sitelinks".to_string()] {
            self.process_sitelinks(result).await?;
            Platform::profile("after process_sitelinks", None);
        }
        if available_sources.to_vec() != vec!["labels".to_string()] {
            self.process_labels(result).await?;
            Platform::profile("after process_labels", Some(result.len()));
        }
        Ok(())
    }

    /// Applies page-enrichment steps (files, pages, namespace, subpages, wikidata annotation).
    async fn apply_page_enrichments(&self, result: &PageList) -> Result<()> {
        self.process_files(result).await?;
        Platform::profile("after process_files", Some(result.len()));
        self.process_pages(result).await?;
        Platform::profile("after process_pages", Some(result.len()));
        self.process_namespace_conversion(result).await?;
        Platform::profile("after process_namespace_conversion", Some(result.len()));
        self.process_subpages(result).await?;
        Platform::profile("after process_subpages", Some(result.len()));
        self.annotate_with_wikidata_item(result).await?;
        Platform::profile("after annotate_with_wikidata_item [2]", Some(result.len()));
        Ok(())
    }

    /// Applies text-based filters (regexp, search) and final creator/redlink pass.
    async fn apply_text_filters_and_creator(&self, result: &PageList) -> Result<()> {
        let wikidata_label_language = self.get_param_default(
            "wikidata_label_language",
            &self.get_param_default("interface_language", "en"),
        );
        result
            .load_missing_metadata(Some(wikidata_label_language), self)
            .await?;
        Platform::profile("after load_missing_metadata", Some(result.len()));

        if let Some(regexp) = self.get_param("rxp_filter") {
            result.regexp_filter(&regexp);
        }
        if let Some(search) = self.get_param("search_filter") {
            result.search_filter(self, &search).await?;
        }
        self.process_redlinks(result).await?;
        Platform::profile("after process_redlinks", Some(result.len()));
        self.process_creator(result).await?;
        Platform::profile("after process_creator", Some(result.len()));
        Ok(())
    }

    pub(super) async fn post_process_result(&self, available_sources: &[String]) -> Result<()> {
        Platform::profile("post_process_result begin", None);
        let result = match self.result.as_ref() {
            Some(res) => res,
            None => return Ok(()),
        };

        self.apply_wikidata_filters(result, available_sources)
            .await?;

        self.convert_to_common_wiki(result).await?;
        Platform::profile("after convert_to_common_wiki", Some(result.len()));

        if !available_sources.contains(&"categories".to_string()) {
            self.process_missing_database_filters(result).await?;
            Platform::profile("after process_missing_database_filters", Some(result.len()));
        }
        self.process_by_wikidata_item(result).await?;
        Platform::profile("after process_by_wikidata_item", Some(result.len()));

        self.apply_page_enrichments(result).await?;
        self.apply_text_filters_and_creator(result).await?;
        Ok(())
    }

    /// Resolves which wiki string to convert to, based on the `common_wiki` parameter.
    fn resolve_common_wiki_target(&self) -> Result<Option<String>> {
        let mode = self.get_param_default("common_wiki", "auto");
        match mode.as_str() {
            "auto" => Ok(None),
            "wikidata" => Ok(Some("wikidatawiki".to_string())),
            "cats" => Ok(Some(
                self.wiki_by_source
                    .get("categories")
                    .ok_or_else(|| anyhow!("categories wiki requested as output, but not set"))?
                    .clone(),
            )),
            "pagepile" => Ok(Some(
                self.wiki_by_source
                    .get("pagepile")
                    .ok_or_else(|| anyhow!("pagepile wiki requested as output, but not set"))?
                    .clone(),
            )),
            "manual" => Ok(Some(
                self.wiki_by_source
                    .get("manual")
                    .map(|s| s.to_string())
                    .or_else(|| self.get_param("common_wiki_other"))
                    .ok_or_else(|| anyhow!("manual wiki requested as output, but not set"))?,
            )),
            "other" => Ok(Some(self.get_param("common_wiki_other").ok_or_else(
                || anyhow!("Other wiki for output expected, but not given in text field"),
            )?)),
            unknown => Err(anyhow!("Unknown output wiki type '{unknown}'")),
        }
    }

    async fn convert_to_common_wiki(&self, result: &PageList) -> Result<()> {
        if let Some(target_wiki) = self.resolve_common_wiki_target()? {
            result.convert_to_wiki(&target_wiki, self).await?;
        }
        Ok(())
    }

    pub(super) fn apply_results_limit(&self, pages: &mut Vec<PageListEntry>) {
        let limit = self
            .get_param_default("output_limit", "0")
            .parse::<usize>()
            .unwrap_or(0);
        if limit != 0 && limit < pages.len() {
            pages.resize(limit, PageListEntry::new(Title::new("", 0)));
        }
    }

    // ─── Creator / labels ────────────────────────────────────────────────────

    /// Builds the SQL batch that queries for existing Wikidata labels matching the page titles.
    fn build_creator_sql_batch(sql_batch: &mut SQLtuple) {
        sql_batch.0 = "SELECT wbx_text
            FROM wbt_text
            WHERE EXISTS (SELECT * FROM wbt_item_terms,wbt_type,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id = wbtl_id AND wbtl_type_id IN (1,3) AND wbtl_text_in_lang_id = wbxl_id AND wbxl_text_id = wbx_id)
            AND wbx_text IN ("
            .to_string();
        sql_batch.0 += &crate::datasource::get_placeholders(sql_batch.1.len());
        sql_batch.0 += ")";
        // Labels use spaces, not underscores
        for element in sql_batch.1.iter_mut() {
            if let MyValue::Bytes(x) = element {
                let u2s = Title::underscores_to_spaces(&String::from_utf8_lossy(x));
                *element = MyValue::Bytes(u2s.into());
            }
        }
    }

    /// Prepares for JS "creator" mode: checks which labels already exist on Wikidata
    async fn process_creator(&self, result: &PageList) -> Result<()> {
        if result.is_empty() || result.is_wikidata() {
            return Ok(());
        }
        if !self.has_param("show_redlinks") && self.get_param_blank("wikidata_item") != "without" {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql_batch| {
                Self::build_creator_sql_batch(sql_batch);
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        let state = self.state();
        let mut conn = state.get_x3_db_connection().await?;

        for sql in batches {
            let rows = conn
                .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
                .await?
                .map_and_drop(from_row::<Vec<u8>>)
                .await?;

            let mut el = self.existing_labels.write().map_err(|e| anyhow!("{e}"))?;
            for wbx_text in rows {
                el.insert(String::from_utf8_lossy(&wbx_text).into_owned());
            }
        }
        conn.disconnect().await.map_err(|e| anyhow!("{e}"))?;
        Ok(())
    }

    // ─── Redlinks ────────────────────────────────────────────────────────────

    /// Builds the SQL for a single redlink batch, applying namespace and template filters.
    fn build_redlinks_sql(
        sql_batch: &mut SQLtuple,
        ns0_only: bool,
        remove_template_redlinks: bool,
    ) {
        let mut sql = "SELECT lt0.lt_title,lt0.lt_namespace,(SELECT COUNT(*) FROM page p1 WHERE p1.page_title=lt0.lt_title AND p1.page_namespace=lt0.lt_namespace) AS cnt from page p0,pagelinks pl0,linktarget lt0 WHERE pl0.pl_target_id=lt0.lt_id AND pl_from=p0.page_id AND ".to_string();
        sql += &sql_batch.0;
        if ns0_only {
            sql += " AND lt0.lt_namespace=0";
        } else {
            sql += " AND lt0.lt_namespace>=0";
        }
        if remove_template_redlinks {
            sql += " AND NOT EXISTS (SELECT * FROM pagelinks pl1,linktarget lt1 WHERE pl1.pl_target_id=lt1.lt_id AND pl1.pl_from_namespace=10 AND lt0.lt_namespace=lt1.lt_namespace AND lt0.lt_title=lt1.lt_title LIMIT 1)";
        }
        sql += " GROUP BY page_id,lt0.lt_namespace,lt_title";
        sql += " HAVING cnt=0";
        sql_batch.0 = sql;
    }

    async fn process_redlinks(&self, result: &PageList) -> Result<()> {
        if result.is_empty() || !self.do_output_redlinks() || result.is_wikidata() {
            return Ok(());
        }
        let ns0_only = self.has_param("article_redlinks_only");
        let remove_template_redlinks = self.has_param("remove_template_redlinks");

        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE / 20)
            .par_iter_mut()
            .map(|sql_batch| {
                Self::build_redlinks_sql(sql_batch, ns0_only, remove_template_redlinks);
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        let mut redlink_counter: HashMap<Title, LinkCount> = HashMap::new();

        let wiki = result
            .wiki()
            .ok_or_else(|| anyhow!("Platform::process_redlinks: no wiki set in result"))?
            .to_owned();

        let mut conn = self
            .state
            .get_wiki_db_connection(&wiki)
            .await
            .map_err(|e| anyhow!(e))?;

        for sql in batches {
            self.process_redlinks_batch(&mut conn, sql, &mut redlink_counter)
                .await?;
        }
        conn.disconnect().await.map_err(|e| anyhow!(e))?;

        let min_redlinks = self
            .get_param_default("min_redlink_count", "1")
            .parse::<LinkCount>()
            .unwrap_or(1);
        redlink_counter.retain(|_, &mut v| v >= min_redlinks);
        result.set_entries(
            redlink_counter
                .par_iter()
                .map(|(k, redlink_count)| {
                    let mut ret = PageListEntry::new(k.to_owned());
                    ret.set_redlink_count(Some(*redlink_count));
                    ret
                })
                .collect(),
        );
        Ok(())
    }

    async fn process_redlinks_batch(
        &self,
        conn: &mut mysql_async::Conn,
        sql: SQLtuple,
        redlink_counter: &mut HashMap<Title, LinkCount>,
    ) -> Result<()> {
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!("{e}"))?
            .map_and_drop(from_row::<(Vec<u8>, i64, usize)>)
            .await
            .map_err(|e| anyhow!("{e}"))?;

        for (page_title, namespace_id, _count) in rows {
            let page_title = String::from_utf8_lossy(&page_title).to_string();
            let title = Title::new(&page_title, namespace_id);
            *redlink_counter.entry(title).or_insert(0) += 1;
        }
        Ok(())
    }

    // ─── Namespace conversion ─────────────────────────────────────────────────

    async fn process_namespace_conversion(&self, result: &PageList) -> Result<()> {
        let namespace_conversion = self.get_param_default("namespace_conversion", "keep");
        let use_talk = match namespace_conversion.as_str() {
            "topic" => false,
            "talk" => true,
            _ => return Ok(()),
        };
        result.change_namespaces(use_talk);
        Ok(())
    }

    // ─── Subpages ─────────────────────────────────────────────────────────────

    /// Queries the wiki DB and adds subpages of every entry in `result`.
    async fn add_subpages_from_db(&self, result: &PageList) -> Result<()> {
        let title_ns = result.to_titles_namespaces();
        let wiki = result
            .wiki()
            .ok_or_else(|| anyhow!("Platform::process_subpages: no wiki set in result"))?
            .to_owned();
        let mut conn = self.state.get_wiki_db_connection(&wiki).await?;

        for (title, namespace_id) in title_ns {
            let sql: SQLtuple = (
                "SELECT page_title,page_namespace FROM page WHERE page_namespace=? AND page_title LIKE ?"
                    .to_string(),
                vec![
                    MyValue::Int(namespace_id),
                    MyValue::Bytes(format!("{title}/%").into()),
                ],
            );

            let rows = conn
                .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
                .await
                .map_err(|e| anyhow!("{e}"))?
                .map_and_drop(from_row::<(Vec<u8>, i64)>)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            for (page_title, page_namespace) in rows {
                let page_title = String::from_utf8_lossy(&page_title);
                result.add_entry(PageListEntry::new(Title::new(&page_title, page_namespace)));
            }
        }
        conn.disconnect().await.map_err(|e| anyhow!("{e}"))?;
        Ok(())
    }

    async fn process_subpages(&self, result: &PageList) -> Result<()> {
        let add_subpages = self.has_param("add_subpages");
        let subpage_filter = self.get_param_default("subpage_filter", "either");

        if !add_subpages && subpage_filter != "subpages" && subpage_filter != "no_subpages" {
            return Ok(());
        }

        if add_subpages {
            self.add_subpages_from_db(result).await?;
            // TODO: if new pages were added, they should get some post_process_result treatment
        }

        if subpage_filter != "subpages" && subpage_filter != "no_subpages" {
            return Ok(());
        }

        let keep_subpages = subpage_filter == "subpages";
        result.retain_entries(&|entry: &PageListEntry| {
            entry.title().pretty().contains('/') == keep_subpages
        });
        Ok(())
    }

    // ─── Process pages (coordinates, image, defaultsort, …) ──────────────────

    async fn process_pages(&self, result: &PageList) -> Result<()> {
        let is_kml = self.get_param_blank("format") == "kml";
        let fields = PageFields {
            add_image: self.has_param("add_image") || is_kml,
            add_coordinates: self.has_param("add_coordinates") || is_kml,
            add_defaultsort: self.has_param("add_defaultsort")
                || self.get_param_blank("sortby") == "defaultsort",
            add_disambiguation: self.has_param("add_disambiguation"),
            add_incoming_links: self.get_param_blank("sortby") == "incoming_links",
            add_sitelinks: self.get_param_blank("sortby") == "sitelinks"
                && !result.has_sitelink_counts(),
            is_wikidata: result.is_wikidata(),
        };

        if !fields.any() {
            return Ok(());
        }

        let select_cols = fields.build_select_columns();
        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql_batch| {
                sql_batch.0 = select_cols.clone() + &sql_batch.0;
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        let col_title: usize = 0;
        let col_ns: usize = 1;
        result
            .run_batch_queries(&self.state(), batches)
            .await?
            .iter()
            .filter_map(|row| {
                result
                    .entry_from_row(row, col_title, col_ns)
                    .map(|entry| (row, entry))
            })
            .filter_map(|(row, entry)| result.get_entry(&entry).map(|e| (row, e)))
            .for_each(|(row, mut entry)| {
                let mut parts = row.clone().unwrap();
                parts.remove(0); // page_title
                parts.remove(0); // page_namespace
                fields.apply_row_to_entry(&mut parts, &mut entry);
                result.add_entry(entry);
            });
        Ok(())
    }

    // ─── File usage / file data ───────────────────────────────────────────────

    async fn file_usage(&self, result: &PageList, file_usage_data_ns0: bool) -> Result<()> {
        let mut batch_size = PAGE_BATCH_SIZE;
        loop {
            if batch_size == 0 {
                return Err(anyhow!(
                    "file_usage: Too much file usage to report back from MySQL"
                ));
            }
            let batches: Vec<SQLtuple> = result
                .to_sql_batches_namespace(batch_size, 6)
                .par_iter_mut()
                .map(|sql_batch| {
                    sql_batch.0 = "SELECT gil_to,6 AS namespace_id,GROUP_CONCAT(gil_wiki,':',gil_page_namespace_id,':',gil_page,':',gil_page_namespace,':',gil_page_title SEPARATOR '|') AS gil_group FROM globalimagelinks WHERE gil_to IN (".to_string();
                    sql_batch.0 += &crate::datasource::get_placeholders(sql_batch.1.len());
                    sql_batch.0 += ")";
                    if file_usage_data_ns0 {
                        sql_batch.0 += " AND gil_page_namespace_id=0";
                    }
                    sql_batch.0 += " GROUP BY gil_to";
                    sql_batch.to_owned()
                })
                .collect::<Vec<SQLtuple>>();

            let the_f = |row: my::Row, entry: &mut PageListEntry| {
                if let Some(gil_group) = PageList::string_from_row(&row, 2) {
                    let fi = FileInfo::new_from_gil_group(&gil_group);
                    entry.set_file_info(Some(fi));
                }
            };
            let col_title: usize = 0;
            let col_ns: usize = 1;
            let batch_results = match result.run_batch_queries(&self.state(), batches).await {
                Ok(res) => res,
                Err(e) => {
                    if e.to_string().contains("packet too large") {
                        batch_size = std::cmp::min(batch_size, result.len()) / 2;
                        continue;
                    }
                    return Err(e);
                }
            };
            batch_results
                .iter()
                .filter_map(|row| {
                    result
                        .entry_from_row(row, col_title, col_ns)
                        .map(|entry| (row, entry))
                })
                .filter_map(|(row, entry)| result.get_entry(&entry).map(|e| (row, e)))
                .for_each(|(row, mut entry)| {
                    the_f(row.clone(), &mut entry);
                    result.add_entry(entry);
                });
            return Ok(());
        }
    }

    async fn process_files(&self, result: &PageList) -> Result<()> {
        let giu = self.has_param("giu");
        let file_data = self.has_param("ext_image_data")
            || self.get_param("sortby") == Some("filesize".to_string())
            || self.get_param("sortby") == Some("uploaddate".to_string());
        let file_usage = giu || self.has_param("file_usage_data");
        let file_usage_data_ns0 = self.has_param("file_usage_data_ns0");

        if file_usage {
            self.file_usage(result, file_usage_data_ns0).await?;
        }

        if file_data {
            self.process_file_data(result).await?;
        }
        Ok(())
    }

    async fn process_file_data(&self, result: &PageList) -> Result<()> {
        let sql = if self.state.using_file_table() {
            "SELECT file.file_name AS img_name,6 AS namespace_id,
            fr_size AS img_size,
            fr_width AS img_width,
            fr_height AS img_height,
            ft_media_type AS img_media_type,
            ft_major_mime AS img_major_mime,
            ft_minor_mime AS img_minor_mime,
            actor_name AS img_user_text,
            fr_timestamp AS img_timestamp,
            fr_sha1 AS img_sha1
            FROM file,filerevision,filetypes,actor
            WHERE file.file_latest=filerevision.fr_id
            AND fr_actor=actor_id
            AND file.file_type=filetypes.ft_id
            AND filerevision.fr_deleted=0
            AND file.file_name IN ("
        } else {
            "SELECT img_name,6 AS namespace_id,img_size,img_width,img_height,img_media_type,img_major_mime,img_minor_mime,img_user_text,img_timestamp,img_sha1 FROM image_compat WHERE img_name IN ("
        };
        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql_batch| {
                sql_batch.0 = sql.to_string();
                sql_batch.0 += &crate::datasource::get_placeholders(sql_batch.1.len());
                sql_batch.0 += ")";
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        let the_f = |row: my::Row, entry: &mut PageListEntry| {
            let (
                _img_name,
                _namespace_id,
                img_size,
                img_width,
                img_height,
                img_media_type,
                img_major_mime,
                img_minor_mime,
                img_user_text,
                img_timestamp,
                img_sha1,
            ) = my::from_row::<(
                String,
                usize,
                usize,
                usize,
                usize,
                String,
                String,
                String,
                String,
                String,
                String,
            )>(row);
            let mut file_info = entry.get_file_info().unwrap_or_default();
            file_info.img_size = Some(img_size);
            file_info.img_width = Some(img_width);
            file_info.img_height = Some(img_height);
            file_info.img_media_type = Some(img_media_type);
            file_info.img_major_mime = Some(img_major_mime);
            file_info.img_minor_mime = Some(img_minor_mime);
            file_info.img_user_text = Some(img_user_text);
            file_info.img_timestamp = Some(img_timestamp);
            file_info.img_sha1 = Some(img_sha1);
            entry.set_file_info(Some(file_info));
        };
        let col_title: usize = 0;
        let col_ns: usize = 1;
        result
            .run_batch_queries(&self.state(), batches)
            .await?
            .iter()
            .filter_map(|row| {
                result
                    .entry_from_row(row, col_title, col_ns)
                    .map(|entry| (row, entry))
            })
            .filter_map(|(row, entry)| result.get_entry(&entry).map(|e| (row, e)))
            .for_each(|(row, mut entry)| {
                the_f(row.clone(), &mut entry);
                result.add_entry(entry);
            });
        Ok(())
    }

    // ─── Wikidata item annotation ─────────────────────────────────────────────

    /// Applies row data from `wb_items_per_site` to entries in `result`.
    fn apply_wikidata_item_rows(
        rows: &[my::Row],
        result: &PageList,
        api: &wikimisc::mediawiki::api::Api,
    ) {
        rows.iter().for_each(|row| {
            let full_page_title = match row.get(0) {
                Some(Bytes(uv)) => match String::from_utf8(uv) {
                    Ok(s) => s,
                    Err(_) => return,
                },
                _ => return,
            };
            let ips_item_id = match row.get(1) {
                Some(my::Value::Int(i)) => i,
                _ => return,
            };
            let title = Title::new_from_full(&full_page_title, api);
            let tmp_entry = PageListEntry::new(title);
            let mut entry = match result.get_entry(&tmp_entry) {
                Some(entry) => entry,
                None => return,
            };
            entry.set_wikidata_item(Some(format!("Q{ips_item_id}")));
            result.add_entry(entry);
        });
    }

    async fn annotate_with_wikidata_item(&self, result: &PageList) -> Result<()> {
        if result.is_wikidata() {
            return Ok(());
        }
        let wiki = match result.wiki() {
            Some(wiki) => wiki.to_string(),
            None => return Ok(()),
        };
        let api = self.state.get_api_for_wiki(wiki.to_owned()).await?;

        let titles = result.to_full_pretty_titles(&api);
        let mut batches: Vec<SQLtuple> = vec![];
        titles.chunks(PAGE_BATCH_SIZE).for_each(|chunk| {
            let escaped: Vec<MyValue> = chunk
                .par_iter()
                .filter_map(|s| match s.trim() {
                    "" => None,
                    other => Some(other.to_string()),
                })
                .map(|s| s.into())
                .collect();
            let placeholders = crate::datasource::get_placeholders(escaped.len());
            let query = format!(
                "SELECT ips_site_page,ips_item_id FROM wb_items_per_site WHERE ips_site_id='{}' and ips_site_page IN ({})",
                &wiki, &placeholders
            );
            batches.push((query, escaped));
        });

        let rows: TokioMutex<Vec<my::Row>> = TokioMutex::new(vec![]);
        for sql in batches {
            let mut conn = self
                .state
                .get_wiki_db_connection("wikidatawiki")
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let mut subresult = conn
                .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
                .await
                .map_err(|e| anyhow!("{e}"))?
                .collect_and_drop()
                .await
                .map_err(|e| anyhow!("{e}"))?;
            conn.disconnect().await.map_err(|e| anyhow!("{e}"))?;
            rows.lock().await.append(&mut subresult);
        }

        let locked = rows.lock().await;
        Self::apply_wikidata_item_rows(&locked, result, &api);
        Ok(())
    }

    /// Filters on whether a page has a Wikidata item, depending on the `wikidata_item` parameter
    async fn process_by_wikidata_item(&self, result: &PageList) -> Result<()> {
        if result.is_wikidata() {
            return Ok(());
        }
        let wdi = self.get_param_default("wikidata_item", "no");
        if wdi != "any" && wdi != "with" && wdi != "without" {
            return Ok(());
        }
        self.annotate_with_wikidata_item(result).await?;
        if wdi == "with" {
            result.retain_entries(&|entry| entry.get_wikidata_item().is_some());
        }
        if wdi == "without" {
            result.retain_entries(&|entry| entry.get_wikidata_item().is_none());
        }
        Ok(())
    }

    /// Adds page properties that might be missing if none of the original sources was "categories"
    async fn process_missing_database_filters(&self, result: &PageList) -> Result<()> {
        let mut params = SourceDatabaseParameters::db_params(self).await;
        params.set_wiki(Some(result.wiki().ok_or_else(|| {
            anyhow!("Platform::process_missing_database_filters: result has no wiki")
        })?));
        let mut db = SourceDatabase::new(params);
        let new_result = db.get_pages(&self.state, Some(result)).await?;
        result.set_from(new_result);
        Ok(())
    }

    // ─── Labels (new wbt_item_terms) ──────────────────────────────────────────

    /// Using new `wbt_item_terms`
    async fn process_labels(&self, result: &PageList) -> Result<()> {
        if self.get_label_sql_new(&0).is_none() {
            return Ok(());
        }
        result.convert_to_wiki("wikidatawiki", self).await?;
        if result.is_empty() {
            return Ok(());
        }

        let batches: Vec<SQLtuple> = result
            .group_by_namespace()
            .par_iter()
            .filter_map(|(namespace_id, titles)| {
                let mut sql = self.get_label_sql_new(namespace_id)?;
                if *namespace_id == 0 {
                    sql.0 += " AND wbit_item_id IN (";
                } else if *namespace_id == 120 {
                    sql.0 += " AND wbit_property_id IN (";
                } else {
                    return None;
                }
                sql.0 += &titles
                    .par_iter()
                    .map(|title| title[1..].to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                sql.0 += ")";
                Some(sql)
            })
            .collect();

        result.clear_entries();
        let the_f = |row: my::Row| {
            let term_full_entity_id = my::from_row::<String>(row);
            Platform::entry_from_entity(&term_full_entity_id)
        };
        result
            .run_batch_queries_with_cluster(&self.state(), batches, DatabaseCluster::X3)
            .await?
            .iter()
            .filter_map(|row| the_f(row.to_owned()))
            .for_each(|entry| result.add_entry(entry));
        Ok(())
    }

    // ─── Sitelinks ────────────────────────────────────────────────────────────

    /// Builds the base SQL (SELECT + WHERE) and the HAVING/GROUP BY postfix for sitelink filtering.
    fn build_sitelinks_sql(
        sitelinks_yes: &[String],
        sitelinks_any: &[String],
        sitelinks_no: &[String],
        sitelinks_min: &str,
        sitelinks_max: &str,
    ) -> (SQLtuple, String) {
        let use_min_max = !sitelinks_min.is_empty() || !sitelinks_max.is_empty();

        let mut sql: SQLtuple = (String::new(), vec![]);
        sql.0 += "SELECT ";
        if use_min_max {
            sql.0 += "page_title,(SELECT count(*) FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1) AS sitelink_count";
        } else {
            sql.0 += "DISTINCT page_title,0";
        }
        sql.0 += " FROM page WHERE page_namespace=0";

        for site in sitelinks_yes {
            sql.0 += " AND EXISTS (SELECT * FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1 AND ips_site_id=? LIMIT 1)";
            sql.1.push(site.into());
        }
        if !sitelinks_any.is_empty() {
            sql.0 += " AND EXISTS (SELECT * FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1 AND ips_site_id IN (";
            let tmp = crate::datasource::prep_quote(sitelinks_any);
            crate::datasource::append_sql(&mut sql, tmp);
            sql.0 += ") LIMIT 1)";
        }
        for site in sitelinks_no {
            sql.0 += " AND NOT EXISTS (SELECT * FROM wb_items_per_site WHERE ips_item_id=substr(page_title,2)*1 AND ips_site_id=? LIMIT 1)";
            sql.1.push(site.into());
        }
        sql.0 += " AND ";

        let mut having: Vec<String> = vec![];
        if let Ok(s) = sitelinks_min.parse::<usize>() {
            having.push(format!("sitelink_count>={s}"));
        }
        if let Ok(s) = sitelinks_max.parse::<usize>() {
            having.push(format!("sitelink_count<={s}"));
        }
        let mut sql_post = String::new();
        if use_min_max {
            sql_post += " GROUP BY page_title";
        }
        if !having.is_empty() {
            sql_post += " HAVING ";
            sql_post += &having.join(" AND ");
        }

        (sql, sql_post)
    }

    async fn process_sitelinks(&self, result: &PageList) -> Result<()> {
        if result.is_empty() {
            return Ok(());
        }

        let sitelinks_yes = self.get_param_as_vec("sitelinks_yes", "\n");
        let sitelinks_any = self.get_param_as_vec("sitelinks_any", "\n");
        let sitelinks_no = self.get_param_as_vec("sitelinks_no", "\n");
        let sitelinks_min = self.get_param_blank("min_sitelink_count");
        let sitelinks_max = self.get_param_blank("max_sitelink_count");

        if sitelinks_yes.is_empty()
            && sitelinks_any.is_empty()
            && sitelinks_no.is_empty()
            && sitelinks_min.is_empty()
            && sitelinks_max.is_empty()
        {
            return Ok(());
        }

        let old_wiki = result.wiki().to_owned();
        result.convert_to_wiki("wikidatawiki", self).await?;
        if result.is_empty() {
            return Ok(());
        }

        let (sql, sql_post) = Self::build_sitelinks_sql(
            &sitelinks_yes,
            &sitelinks_any,
            &sitelinks_no,
            &sitelinks_min,
            &sitelinks_max,
        );

        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .par_iter_mut()
            .map(|sql_batch| {
                sql_batch.0 = sql.0.to_owned() + &sql_batch.0 + &sql_post;
                sql_batch.1.splice(..0, sql.1.to_owned());
                sql_batch.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        let state = self.state();
        let the_f = |row: my::Row| {
            let (page_title, _sitelinks_count) = my::from_row::<(String, usize)>(row);
            Some(PageListEntry::new(Title::new(&page_title, 0)))
        };
        result
            .run_batch_queries(&state, batches)
            .await?
            .iter()
            .filter_map(|row| the_f(row.to_owned()))
            .for_each(|entry| result.add_entry(entry));

        if let Some(wiki) = old_wiki {
            result.convert_to_wiki(&wiki, self).await?;
        }
        Ok(())
    }

    // ─── Wikidata property/item filter ────────────────────────────────────────

    /// Builds the `sql_post` snippet that encodes all Wikidata property/item filters.
    fn build_filter_wikidata_sql_post(
        &self,
        parts: &[SQLtuple],
        no_statements: bool,
        no_sitelinks: bool,
        min_statements: Option<usize>,
        max_statements: Option<usize>,
        min_identifiers: Option<usize>,
        max_identifiers: Option<usize>,
        wpiu: &str,
    ) -> SQLtuple {
        let mut sql_post: SQLtuple = (String::new(), vec![]);

        if let Some(m) = min_statements {
            sql_post.0 += &format!(
                " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-claims' AND pp_value*1>={m})"
            );
        }
        if let Some(m) = max_statements {
            sql_post.0 += &format!(
                " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-claims' AND pp_value*1<={m})"
            );
        }
        if let Some(m) = min_identifiers {
            sql_post.0 += &format!(
                " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-identifiers' AND pp_value*1>={m})"
            );
        }
        if let Some(m) = max_identifiers {
            sql_post.0 += &format!(
                " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-identifiers' AND pp_value*1<={m})"
            );
        }
        if no_statements {
            sql_post.0 += " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-claims' AND pp_sortkey=0)";
        }
        if no_sitelinks {
            sql_post.0 += " AND EXISTS (SELECT * FROM page_props WHERE page_id=pp_page AND pp_propname='wb-sitelinks' AND pp_sortkey=0)";
        }

        if !parts.is_empty() {
            match wpiu {
                "all" => {
                    for sql in parts {
                        sql_post.0 += &format!(" AND EXISTS {}", sql.0);
                        sql_post.1.append(&mut sql.1.to_owned());
                    }
                }
                "any" => {
                    sql_post.0 += " AND (0";
                    for sql in parts {
                        sql_post.0 += &format!(" OR EXISTS {}", sql.0);
                        sql_post.1.append(&mut sql.1.to_owned());
                    }
                    sql_post.0 += ")";
                }
                "none" => {
                    for sql in parts {
                        sql_post.0 += &format!(" AND NOT EXISTS {}", sql.0);
                        sql_post.1.append(&mut sql.1.to_owned());
                    }
                }
                _ => {}
            }
        }
        sql_post
    }

    async fn filter_wikidata(&self, result: &PageList) -> Result<()> {
        if result.is_empty() {
            return Ok(());
        }
        let no_statements = self.has_param("wpiu_no_statements");
        let no_sitelinks = self.has_param("wpiu_no_sitelinks");
        let wpiu = self.get_param_default("wpiu", "any");
        let min_statements = self.usize_option_from_param("min_statements");
        let max_statements = self.usize_option_from_param("max_statements");
        let min_identifiers = self.usize_option_from_param("min_identifiers");
        let max_identifiers = self.usize_option_from_param("max_identifiers");
        let list = self.get_param_blank("wikidata_prop_item_use");
        let list = list.trim().to_string();

        if list.is_empty()
            && !no_statements
            && !no_sitelinks
            && min_statements.is_none()
            && max_statements.is_none()
            && min_identifiers.is_none()
            && max_identifiers.is_none()
        {
            return Ok(());
        }

        let original_wiki = result.wiki();
        Platform::profile("before filter_wikidata:convert_to_wiki", Some(result.len()));
        result.convert_to_wiki("wikidatawiki", self).await?;
        Platform::profile("after filter_wikidata:convert_to_wiki", Some(result.len()));

        if result.is_empty() {
            if let Some(wiki) = original_wiki {
                result.convert_to_wiki(&wiki, self).await?;
            }
            return Ok(());
        }

        // Build per-item/property existence subqueries
        let parts = list
            .split_terminator(',')
            .filter_map(|s| match s.chars().next() {
                Some('Q') => Some((
                    "(SELECT * FROM pagelinks,linktarget WHERE pl_target_id=lt_id AND pl_from=page_id AND lt_namespace=0 AND lt_title=?)".to_string(),
                    vec![s.into()],
                )),
                Some('P') => Some((
                    "(SELECT * FROM pagelinks,linktarget WHERE pl_target_id=lt_id AND pl_from=page_id AND lt_namespace=120 AND lt_title=?)".to_string(),
                    vec![s.into()],
                )),
                _ => None,
            })
            .collect::<Vec<SQLtuple>>();

        let sql_post = self.build_filter_wikidata_sql_post(
            &parts,
            no_statements,
            no_sitelinks,
            min_statements,
            max_statements,
            min_identifiers,
            max_identifiers,
            &wpiu,
        );

        let batches: Vec<SQLtuple> = result
            .to_sql_batches(PAGE_BATCH_SIZE)
            .iter_mut()
            .map(|sql| {
                sql.0 =
                    "SELECT DISTINCT page_title FROM page WHERE ".to_owned() + &sql.0 + &sql_post.0;
                sql.1.append(&mut sql_post.1.to_owned());
                sql.to_owned()
            })
            .collect::<Vec<SQLtuple>>();

        result.clear_entries();
        let state = self.state();
        let the_f = |row: my::Row| {
            let pp_value: String = my::from_row(row);
            Some(PageListEntry::new(Title::new(&pp_value, 0)))
        };
        result
            .run_batch_queries(&state, batches)
            .await?
            .iter()
            .filter_map(|row| the_f(row.to_owned()))
            .for_each(|entry| result.add_entry(entry));

        if let Some(wiki) = original_wiki {
            result.convert_to_wiki(&wiki, self).await?;
        }
        Ok(())
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use crate::pagelist_entry::PageListEntry;
    use std::collections::HashMap;
    use std::sync::Arc;
    use wikimisc::mediawiki::title::Title;

    fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

    // ─── entry_from_entity ────────────────────────────────────────────────────

    #[test]
    fn test_entry_from_entity_q_item() {
        let entry = Platform::entry_from_entity("Q42").unwrap();
        assert_eq!(entry.title().pretty(), "Q42");
        assert_eq!(entry.title().namespace_id(), 0);
    }

    #[test]
    fn test_entry_from_entity_p_property() {
        let entry = Platform::entry_from_entity("P31").unwrap();
        assert_eq!(entry.title().pretty(), "P31");
        assert_eq!(entry.title().namespace_id(), 120);
    }

    #[test]
    fn test_entry_from_entity_l_lexeme() {
        let entry = Platform::entry_from_entity("L1234").unwrap();
        assert_eq!(entry.title().pretty(), "L1234");
        assert_eq!(entry.title().namespace_id(), 146);
    }

    #[test]
    fn test_entry_from_entity_unknown_returns_none() {
        assert!(Platform::entry_from_entity("X999").is_none());
        assert!(Platform::entry_from_entity("").is_none());
        assert!(Platform::entry_from_entity("42").is_none());
    }

    // ─── apply_results_limit ──────────────────────────────────────────────────

    #[test]
    fn test_apply_results_limit_no_limit() {
        let p = make_platform(vec![("output_limit", "0")]);
        let mut pages: Vec<PageListEntry> = (0..10)
            .map(|i| PageListEntry::new(Title::new(&format!("Page{i}"), 0)))
            .collect();
        p.apply_results_limit(&mut pages);
        assert_eq!(pages.len(), 10);
    }

    #[test]
    fn test_apply_results_limit_truncates() {
        let p = make_platform(vec![("output_limit", "3")]);
        let mut pages: Vec<PageListEntry> = (0..10)
            .map(|i| PageListEntry::new(Title::new(&format!("Page{i}"), 0)))
            .collect();
        p.apply_results_limit(&mut pages);
        assert_eq!(pages.len(), 3);
    }

    #[test]
    fn test_apply_results_limit_larger_than_pages() {
        let p = make_platform(vec![("output_limit", "20")]);
        let mut pages: Vec<PageListEntry> = (0..5)
            .map(|i| PageListEntry::new(Title::new(&format!("Page{i}"), 0)))
            .collect();
        p.apply_results_limit(&mut pages);
        assert_eq!(pages.len(), 5);
    }

    #[test]
    fn test_apply_results_limit_missing_param() {
        let p = make_platform(vec![]);
        let mut pages: Vec<PageListEntry> = (0..5)
            .map(|i| PageListEntry::new(Title::new(&format!("Page{i}"), 0)))
            .collect();
        p.apply_results_limit(&mut pages);
        assert_eq!(pages.len(), 5);
    }

    // ─── get_label_sql_new ────────────────────────────────────────────────────

    #[test]
    fn test_get_label_sql_new_returns_none_when_no_labels() {
        let p = make_platform(vec![]);
        assert!(p.get_label_sql_new(&0).is_none());
    }

    #[test]
    fn test_get_label_sql_new_returns_none_for_unknown_namespace() {
        let p = make_platform(vec![("labels_yes", "foo")]);
        assert!(p.get_label_sql_new(&1).is_none());
        assert!(p.get_label_sql_new(&4).is_none());
    }

    #[test]
    fn test_get_label_sql_new_returns_some_for_ns0() {
        let p = make_platform(vec![("labels_yes", "foo")]);
        let sql = p.get_label_sql_new(&0);
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.0.contains("wbt_item_terms"));
    }

    #[test]
    fn test_get_label_sql_new_yes_adds_exists() {
        let p = make_platform(vec![("labels_yes", "Magnus")]);
        let sql = p.get_label_sql_new(&0).unwrap();
        assert!(sql.0.contains("AND EXISTS"));
    }

    #[test]
    fn test_get_label_sql_new_no_adds_not_exists() {
        let p = make_platform(vec![("labels_no", "Foo")]);
        let sql = p.get_label_sql_new(&0).unwrap();
        assert!(sql.0.contains("AND NOT EXISTS"));
    }

    // ─── PageFields ───────────────────────────────────────────────────────────

    #[test]
    fn test_page_fields_any_false_when_all_off() {
        let f = PageFields {
            add_image: false,
            add_coordinates: false,
            add_defaultsort: false,
            add_disambiguation: false,
            add_incoming_links: false,
            add_sitelinks: false,
            is_wikidata: false,
        };
        assert!(!f.any());
    }

    #[test]
    fn test_page_fields_any_true_when_one_on() {
        let f = PageFields {
            add_image: true,
            add_coordinates: false,
            add_defaultsort: false,
            add_disambiguation: false,
            add_incoming_links: false,
            add_sitelinks: false,
            is_wikidata: false,
        };
        assert!(f.any());
    }

    #[test]
    fn test_page_fields_build_select_columns_includes_image() {
        let f = PageFields {
            add_image: true,
            add_coordinates: false,
            add_defaultsort: false,
            add_disambiguation: false,
            add_incoming_links: false,
            add_sitelinks: false,
            is_wikidata: false,
        };
        let sql = f.build_select_columns();
        assert!(sql.contains("page_image"));
        assert!(sql.contains("FROM page WHERE"));
    }

    #[test]
    fn test_page_fields_build_select_columns_sitelinks_wikidata() {
        let f = PageFields {
            add_image: false,
            add_coordinates: false,
            add_defaultsort: false,
            add_disambiguation: false,
            add_incoming_links: false,
            add_sitelinks: true,
            is_wikidata: true,
        };
        let sql = f.build_select_columns();
        assert!(sql.contains("wb_items_per_site"));
    }

    #[test]
    fn test_page_fields_build_select_columns_sitelinks_non_wikidata() {
        let f = PageFields {
            add_image: false,
            add_coordinates: false,
            add_defaultsort: false,
            add_disambiguation: false,
            add_incoming_links: false,
            add_sitelinks: true,
            is_wikidata: false,
        };
        let sql = f.build_select_columns();
        assert!(sql.contains("langlinks"));
    }

    // ─── resolve_common_wiki_target ───────────────────────────────────────────

    #[test]
    fn test_resolve_common_wiki_auto_returns_none() {
        let p = make_platform(vec![("common_wiki", "auto")]);
        assert!(p.resolve_common_wiki_target().unwrap().is_none());
    }

    #[test]
    fn test_resolve_common_wiki_wikidata() {
        let p = make_platform(vec![("common_wiki", "wikidata")]);
        assert_eq!(
            p.resolve_common_wiki_target().unwrap(),
            Some("wikidatawiki".to_string())
        );
    }

    #[test]
    fn test_resolve_common_wiki_other_missing_param_errors() {
        let p = make_platform(vec![("common_wiki", "other")]);
        assert!(p.resolve_common_wiki_target().is_err());
    }

    #[test]
    fn test_resolve_common_wiki_other_with_param() {
        let p = make_platform(vec![
            ("common_wiki", "other"),
            ("common_wiki_other", "frwiki"),
        ]);
        assert_eq!(
            p.resolve_common_wiki_target().unwrap(),
            Some("frwiki".to_string())
        );
    }

    #[test]
    fn test_resolve_common_wiki_unknown_errors() {
        let p = make_platform(vec![("common_wiki", "bogus")]);
        assert!(p.resolve_common_wiki_target().is_err());
    }

    // ─── build_sitelinks_sql ──────────────────────────────────────────────────

    #[test]
    fn test_build_sitelinks_sql_no_filters_distinct() {
        let (sql, post) = Platform::build_sitelinks_sql(&[], &[], &[], "", "");
        assert!(sql.0.contains("DISTINCT page_title,0"));
        assert!(post.is_empty());
    }

    #[test]
    fn test_build_sitelinks_sql_with_min() {
        let (sql, post) = Platform::build_sitelinks_sql(&[], &[], &[], "5", "");
        assert!(sql.0.contains("sitelink_count"));
        assert!(post.contains("HAVING"));
        assert!(post.contains("sitelink_count>=5"));
    }

    #[test]
    fn test_build_sitelinks_sql_yes_adds_exists() {
        let yes = vec!["enwiki".to_string()];
        let (sql, _) = Platform::build_sitelinks_sql(&yes, &[], &[], "", "");
        assert!(sql.0.contains("AND EXISTS"));
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn test_build_sitelinks_sql_no_adds_not_exists() {
        let no = vec!["dewiki".to_string()];
        let (sql, _) = Platform::build_sitelinks_sql(&[], &[], &no, "", "");
        assert!(sql.0.contains("AND NOT EXISTS"));
        assert_eq!(sql.1.len(), 1);
    }

    // ─── build_filter_wikidata_sql_post ───────────────────────────────────────

    #[test]
    fn test_build_filter_wikidata_sql_post_empty() {
        let p = make_platform(vec![]);
        let sql =
            p.build_filter_wikidata_sql_post(&[], false, false, None, None, None, None, "any");
        assert!(sql.0.is_empty());
        assert!(sql.1.is_empty());
    }

    #[test]
    fn test_build_filter_wikidata_sql_post_min_statements() {
        let p = make_platform(vec![]);
        let sql =
            p.build_filter_wikidata_sql_post(&[], false, false, Some(3), None, None, None, "any");
        assert!(sql.0.contains("wb-claims"));
        assert!(sql.0.contains("3"));
    }

    #[test]
    fn test_build_filter_wikidata_sql_post_no_statements_flag() {
        let p = make_platform(vec![]);
        let sql = p.build_filter_wikidata_sql_post(&[], true, false, None, None, None, None, "any");
        assert!(sql.0.contains("pp_sortkey=0"));
    }

    // ─── build_creator_sql_batch ──────────────────────────────────────────────

    #[test]
    fn test_build_creator_sql_batch_sets_sql_string() {
        let mut batch: SQLtuple = (String::new(), vec![MyValue::Bytes("Some_Page".into())]);
        Platform::build_creator_sql_batch(&mut batch);
        assert!(batch.0.contains("wbt_text"));
        assert!(batch.0.contains("wbx_text IN ("));
        // Underscore should have been replaced by space in the value
        if let MyValue::Bytes(v) = &batch.1[0] {
            assert_eq!(String::from_utf8_lossy(v), "Some Page");
        } else {
            panic!("Expected Bytes value");
        }
    }

    // ─── build_redlinks_sql ───────────────────────────────────────────────────

    #[test]
    fn test_build_redlinks_sql_ns0_only() {
        let mut batch: SQLtuple = ("page_title=?".to_string(), vec![]);
        Platform::build_redlinks_sql(&mut batch, true, false);
        assert!(batch.0.contains("lt0.lt_namespace=0"));
        assert!(!batch.0.contains("lt0.lt_namespace>=0"));
    }

    #[test]
    fn test_build_redlinks_sql_all_ns() {
        let mut batch: SQLtuple = ("page_title=?".to_string(), vec![]);
        Platform::build_redlinks_sql(&mut batch, false, false);
        assert!(batch.0.contains("lt0.lt_namespace>=0"));
    }

    #[test]
    fn test_build_redlinks_sql_remove_template() {
        let mut batch: SQLtuple = ("page_title=?".to_string(), vec![]);
        Platform::build_redlinks_sql(&mut batch, false, true);
        assert!(batch.0.contains("pl_from_namespace=10"));
    }
}
