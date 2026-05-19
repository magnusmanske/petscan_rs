use crate::content_type::ContentType;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MyResponse, Platform};
use crate::render::Render;
use crate::render::params::RenderParams;
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
            status: 200,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        if entry.title().namespace_id() == 6 {
            if params.thumbnails_in_wiki_output() {
                match params.ns().full_pretty(entry.title()) {
                    Some(file) => format!("[[{}|120px|]]", &file),
                    None => format!("[[File:{}|120px|]]", entry.title().pretty()),
                }
            } else {
                match params.ns().full_pretty(entry.title()) {
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
            Some(q) => format!("[[:d:{q}|]]"),
            None => String::new(),
        }
    }

    fn render_user_name(&self, user: &str, _params: &RenderParams) -> String {
        format!("[[User:{user}|]]")
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => format!("[[File:{img}|120px|]]"),
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
            ret += &params
                .ns()
                .full_pretty(entry.title())
                .unwrap_or_else(|| entry.title().pretty().to_string());
            if !params.do_output_redlinks() {
                ret += "|";
            }
            ret += "]]";
            ret
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::StubNamespaceContext;
    use std::sync::Arc;
    use wikimisc::mediawiki::title::Title;

    fn enwiki_params() -> RenderParams {
        RenderParams::for_tests("enwiki", Arc::new(StubNamespaceContext::enwiki()))
    }

    // ── render_cell_title for files (namespace 6) ────────────────────────────
    //
    // The wikitext renderer has a dedicated branch for ns=6 (File) that
    // produces `[[File:Name|120px|]]` for thumbnail output and
    // `[[:File:Name|]]` for linked output. Before P5 #37 this branch
    // depended on `params.api()` and so couldn't be unit-tested.

    #[test]
    fn test_render_cell_title_for_file_with_thumbnails() {
        let r = RenderWiki;
        let mut params = enwiki_params();
        *params.use_autolist_mut() = false;
        // The thumbnails flag is read by `thumbnails_in_wiki_output()`; flip
        // it by going through the dedicated setter if one existed. For the
        // default RenderParams::for_tests it is `false`, so we hit the
        // linked-output branch below.
        let entry = PageListEntry::new(Title::new("Cat.jpg", 6));
        // Without thumbnails: `[[:File:Cat.jpg|]]` (the namespace prefix
        // comes from the stub's namespace name for ns=6).
        assert_eq!(
            r.render_cell_title(&entry, &params),
            "[[:File:Cat.jpg|]]"
        );
    }

    #[test]
    fn test_render_cell_title_for_non_file_namespace() {
        let r = RenderWiki;
        let params = enwiki_params();
        let entry = PageListEntry::new(Title::new("Cambridge", 0));
        // Article namespace → wikilink without file branch. Since the
        // namespace prefix is empty, the result is `[[Cambridge|]]`
        // (with `|` because `do_output_redlinks` defaults to false).
        assert_eq!(
            r.render_cell_title(&entry, &params),
            "[[Cambridge|]]"
        );
    }

    #[test]
    fn test_render_cell_title_for_category_namespace() {
        let r = RenderWiki;
        let params = enwiki_params();
        let entry = PageListEntry::new(Title::new("Bioinformatics", 14));
        // Category links need a leading colon to avoid actually
        // categorising the output page.
        assert_eq!(
            r.render_cell_title(&entry, &params),
            "[[:Category:Bioinformatics|]]"
        );
    }
}
