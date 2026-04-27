use crate::content_type::ContentType;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MyResponse, Platform};
use crate::render::Render;
use crate::render::params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;

/// Renders CSV and TSV
#[derive(Debug, Clone)]
pub struct RenderTSV {
    separator: String,
}

#[async_trait]
impl Render for RenderTSV {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let mut rows: Vec<String> = vec![];
        let mut header: Vec<(&str, &str)> = vec![
            ("number", "number"),
            ("title", "title"),
            ("page_id", "pageid"),
            ("namespace", "namespace"),
            ("size", "length"),
            ("timestamp", "touched"),
        ];
        if params.show_wikidata_item() {
            header.push(("wikidata_item", "Wikidata"));
        }
        if params.file_data() {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k, k)));
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
            header
                .iter()
                .map(|(_, v)| self.escape_cell(v))
                .collect::<Vec<String>>()
                .join(&self.separator),
        );

        for entry in entries {
            *params.row_number_mut() += 1;
            let row = self.row_from_entry(&entry, &header, &params, platform);
            let row: Vec<String> = row.iter().map(|s| self.escape_cell(s)).collect();
            let row = row.join(&self.separator);
            rows.push(row);
        }

        Ok(MyResponse {
            s: rows.join("\n"),
            content_type: match self.separator.as_str() {
                "," => ContentType::CSV,
                "\t" => ContentType::TSV,
                _ => ContentType::Plain, // Fallback
            },
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().with_underscores()
    }

    fn render_cell_wikidata_item(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.get_wikidata_item().unwrap_or_default()
    }

    fn render_user_name(&self, user: &str, _params: &RenderParams) -> String {
        user.to_string()
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => img.to_string(),
            None => String::new(),
        }
    }

    fn render_cell_namespace(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        entry
            .title()
            .namespace_name(params.api())
            .unwrap_or("UNKNOWN_NAMESPACE")
            .to_string()
    }
}

impl RenderTSV {
    pub fn new(separator: &str) -> Box<Self> {
        Box::new(Self {
            separator: separator.to_string(),
        })
    }

    fn escape_cell(&self, s: &str) -> String {
        if self.separator == "," {
            format!("\"{}\"", s.replace('\"', "\\\""))
        } else {
            s.replace('\t', " ")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tsv() -> RenderTSV {
        RenderTSV { separator: "\t".to_string() }
    }

    fn csv() -> RenderTSV {
        RenderTSV { separator: ",".to_string() }
    }

    // ── escape_cell ──────────────────────────────────────────────────────────

    #[test]
    fn test_escape_cell_tsv_replaces_tab_with_space() {
        let r = tsv();
        assert_eq!(r.escape_cell("hello\tworld"), "hello world");
    }

    #[test]
    fn test_escape_cell_tsv_leaves_plain_text_unchanged() {
        let r = tsv();
        assert_eq!(r.escape_cell("plain text"), "plain text");
    }

    #[test]
    fn test_escape_cell_csv_wraps_in_double_quotes() {
        let r = csv();
        assert_eq!(r.escape_cell("hello"), "\"hello\"");
    }

    #[test]
    fn test_escape_cell_csv_escapes_internal_quotes() {
        let r = csv();
        assert_eq!(r.escape_cell("say \"hi\""), "\"say \\\"hi\\\"\"");
    }

    #[test]
    fn test_escape_cell_csv_empty_string() {
        let r = csv();
        assert_eq!(r.escape_cell(""), "\"\"");
    }

    #[test]
    fn test_escape_cell_tsv_multiple_tabs() {
        let r = tsv();
        assert_eq!(r.escape_cell("a\tb\tc"), "a b c");
    }

    // ── Render default helpers (called through RenderTSV) ────────────────────

    #[test]
    fn test_opt_usize() {
        let r = tsv();
        assert_eq!(r.opt_usize(&Some(42_usize)), "42");
        assert_eq!(r.opt_usize(&None::<usize>), "");
    }

    #[test]
    fn test_opt_u32() {
        let r = tsv();
        assert_eq!(r.opt_u32(&Some(100_u32)), "100");
        assert_eq!(r.opt_u32(&None::<u32>), "");
    }

    #[test]
    fn test_opt_linkcount() {
        let r = tsv();
        assert_eq!(r.opt_linkcount(&Some(7_u32)), "7");
        assert_eq!(r.opt_linkcount(&None::<u32>), "");
    }

    #[test]
    fn test_opt_bool() {
        let r = tsv();
        assert_eq!(r.opt_bool(&Some(true)), "Y");
        assert_eq!(r.opt_bool(&Some(false)), "N");
        assert_eq!(r.opt_bool(&None), "");
    }

    #[test]
    fn test_opt_string() {
        let r = tsv();
        assert_eq!(r.opt_string(&Some("hello".to_string())), "hello");
        assert_eq!(r.opt_string(&None::<String>), "");
    }

    #[test]
    fn test_file_data_keys() {
        let r = tsv();
        let keys = r.file_data_keys();
        assert_eq!(keys.len(), 9);
        assert!(keys.contains(&"img_size"));
        assert!(keys.contains(&"img_width"));
        assert!(keys.contains(&"img_height"));
        assert!(keys.contains(&"img_media_type"));
        assert!(keys.contains(&"img_major_mime"));
        assert!(keys.contains(&"img_minor_mime"));
        assert!(keys.contains(&"img_user_text"));
        assert!(keys.contains(&"img_timestamp"));
        assert!(keys.contains(&"img_sha1"));
    }
}
