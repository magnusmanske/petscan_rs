use crate::content_type::ContentType;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MyResponse, Platform};
use crate::render::Render;
use crate::render_params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;
use chrono::prelude::*;

/// Renders wiki text
#[derive(Debug, Clone, Copy)]
pub struct RenderWiki;

#[async_trait]
impl Render for RenderWiki {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let mut rows: Vec<String> = vec![];
        rows.push("== ".to_string() + &platform.combination().to_string() + " ==");

        let petscan_query_url =
            "https://petscan.wmflabs.org/?".to_string() + &platform.form_parameters().to_string();
        let petscan_query_url_no_doit = "https://petscan.wmflabs.org/?".to_string()
            + &platform.form_parameters().to_string_no_doit();

        let utc: DateTime<Utc> = Utc::now();
        rows.push(format!("Last updated on {}.", utc.to_rfc2822()));

        rows.push(format!(
            "[{} Regenerate this table] or [{} edit the query].\n",
            &petscan_query_url, &petscan_query_url_no_doit
        ));
        rows.push("{| border=1 class='wikitable'".to_string());
        let mut header: Vec<(&str, &str)> = vec![
            ("title", "Title"),
            ("page_id", "Page ID"),
            ("namespace", "Namespace"),
            ("size", "Size (bytes)"),
            ("timestamp", "Last change"),
        ];
        if params.show_wikidata_item() {
            header.push(("wikidata_item", "Wikidata"));
        }
        if params.file_data() {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k, k)));
        }
        if params.do_output_redlinks() {
            header = vec![("redlink_count", "Wanted"), ("title", "Title")];
        }
        let mut header: Vec<(String, String)> = header
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for col in self.get_initial_columns(&params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        rows.push(
            "!".to_string()
                + &header
                    .iter()
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<String>>()
                    .join(" !! "),
        );

        for entry in entries {
            *params.row_number_mut() += 1;
            rows.push("|-".to_string());
            let row = self.row_from_entry(&entry, &header, &params, platform);
            let row = "| ".to_string() + &row.join(" || ");
            rows.push(row);
        }

        rows.push("|}".to_string());

        Ok(MyResponse {
            s: rows.join("\n"),
            content_type: ContentType::Plain,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        if entry.title().namespace_id() == 6 {
            if params.thumbnails_in_wiki_output() {
                match entry.title().full_pretty(params.api()) {
                    Some(file) => format!("[[{}|120px|]]", &file),
                    None => format!("[[File:{}|120px|]]", entry.title().pretty()),
                }
            } else {
                match entry.title().full_pretty(params.api()) {
                    Some(file) => format!("[[:{}|]]", &file),
                    None => format!("[[:File:{}|]]", entry.title().pretty()),
                }
            }
        } else {
            Self::render_wikilink(entry, params)
        }
    }

    fn render_cell_wikidata_item(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match entry.get_wikidata_item() {
            Some(q) => format!("[[:d:{}|]]", q),
            None => String::new(),
        }
    }

    fn render_user_name(&self, user: &str, _params: &RenderParams) -> String {
        format!("[[User:{user}|]]")
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => format!("[[File:{}|120px|]]", img),
            None => String::new(),
        }
    }

    fn render_cell_namespace(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().namespace_id().to_string()
    }
}

impl RenderWiki {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }

    fn render_wikilink(entry: &PageListEntry, params: &RenderParams) -> String {
        if params.is_wikidata() {
            match &entry.get_wikidata_label() {
                Some(label) => format!("[[{}|{}]]", &entry.title().pretty(), label),
                None => format!("[[{}]]", entry.title().pretty()),
            }
        } else {
            let mut ret = "[[".to_string();
            if entry.title().namespace_id() == 14 {
                ret += ":";
            }
            ret += &entry
                .title()
                .full_pretty(params.api())
                .unwrap_or_else(|| entry.title().pretty().to_string());
            if !params.do_output_redlinks() {
                ret += "|";
            }
            ret += "]]";
            ret
        }
    }
}
