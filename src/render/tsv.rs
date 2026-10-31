use crate::content_type::ContentType;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MyResponse, Platform};
use crate::render::Render;
use crate::render::params::RenderParams;
use crate::render::stream::{self, StreamResponse};
use anyhow::Result;
use async_trait::async_trait;
use std::borrow::Cow;

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
        let header = self.header(&params);
        let mut rows: Vec<String> = vec![self.render_header(&header)];
        for entry in entries {
            rows.push(self.render_row(&entry, &header, &mut params, Some(platform)));
        }

        Ok(MyResponse {
            s: rows.join("\n"),
            content_type: self.content_type(),
            status: 200,
        })
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn stream(
        self: Box<Self>,
        _platform: &Platform,
        mut params: RenderParams,
        _wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<StreamResponse> {
        let header = self.header(&params);
        let head = Cow::Owned(self.render_header(&header));
        let content_type = self.content_type();
        let renderer = *self;
        let mut entries = entries.into_iter();
        let lines = std::iter::once(head).chain(std::iter::from_fn(move || {
            let entry = entries.next()?;
            Some(Cow::Owned(renderer.render_row(
                &entry,
                &header,
                &mut params,
                None,
            )))
        }));
        Ok(StreamResponse {
            stream: stream::to_stream(stream::newline_separated(lines)),
            content_type,
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
        params
            .ns()
            .local_namespace_name(entry.title().namespace_id())
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

    fn content_type(&self) -> ContentType {
        match self.separator.as_str() {
            "," => ContentType::CSV,
            "\t" => ContentType::TSV,
            _ => ContentType::Plain, // Fallback
        }
    }

    fn header(&self, params: &RenderParams) -> Vec<(String, String)> {
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
        for col in self.get_initial_columns(params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        header
    }

    fn render_header(&self, header: &[(String, String)]) -> String {
        header
            .iter()
            .map(|(_, v)| self.escape_cell(v))
            .collect::<Vec<String>>()
            .join(&self.separator)
    }

    fn render_row(
        &self,
        entry: &PageListEntry,
        header: &[(String, String)],
        params: &mut RenderParams,
        platform: Option<&Platform>,
    ) -> String {
        *params.row_number_mut() += 1;
        let row = match platform {
            Some(platform) => self.row_from_entry(entry, header, params, platform),
            None => self.row_from_entry_streaming(entry, header, params),
        };
        row.iter()
            .map(|s| self.escape_cell(s))
            .collect::<Vec<String>>()
            .join(&self.separator)
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
        RenderTSV {
            separator: "\t".to_string(),
        }
    }

    fn csv() -> RenderTSV {
        RenderTSV {
            separator: ",".to_string(),
        }
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

    // ── render_cell_* via NamespaceContext stub ─────────────────────────────
    //
    // These tests went unwritten before because `RenderParams` owned an
    // `Api` that only had async network constructors. P5 #37 swapped the
    // field for an `Arc<dyn NamespaceContext>`, letting us drive the
    // renderer with the in-memory `StubNamespaceContext`.

    use crate::pagelist_entry::PageListEntry;
    use crate::render::params::RenderParams;
    use crate::test_support::StubNamespaceContext;
    use futures::TryStreamExt;
    use std::sync::Arc;
    use wikimisc::mediawiki::title::Title;

    fn enwiki_params() -> RenderParams {
        RenderParams::for_tests("enwiki", Arc::new(StubNamespaceContext::enwiki()))
    }

    #[test]
    fn test_render_cell_namespace_for_article() {
        let r = tsv();
        let entry = PageListEntry::new(Title::new("Magnus_Manske", 0));
        // Article namespace (0) has an empty local name on enwiki.
        assert_eq!(r.render_cell_namespace(&entry, &enwiki_params()), "");
    }

    #[test]
    fn test_render_cell_namespace_for_user_talk() {
        let r = tsv();
        let entry = PageListEntry::new(Title::new("Foo", 3));
        assert_eq!(
            r.render_cell_namespace(&entry, &enwiki_params()),
            "User talk"
        );
    }

    #[test]
    fn test_render_cell_namespace_for_category() {
        let r = tsv();
        let entry = PageListEntry::new(Title::new("Some_cat", 14));
        assert_eq!(
            r.render_cell_namespace(&entry, &enwiki_params()),
            "Category"
        );
    }

    #[test]
    fn test_render_cell_namespace_unknown_namespace_falls_back() {
        // Namespace 999 isn't in the enwiki stub; render_cell_namespace
        // pins the fallback string `UNKNOWN_NAMESPACE`.
        let r = tsv();
        let entry = PageListEntry::new(Title::new("Whatever", 999));
        assert_eq!(
            r.render_cell_namespace(&entry, &enwiki_params()),
            "UNKNOWN_NAMESPACE"
        );
    }

    #[test]
    fn test_render_cell_title_uses_with_underscores() {
        // Default TSV `render_cell_title` just emits the title's
        // with_underscores form — verify that for a non-trivial title.
        let r = tsv();
        let entry = PageListEntry::new(Title::new("Foo bar baz", 0));
        assert_eq!(r.render_cell_title(&entry, &enwiki_params()), "Foo_bar_baz");
    }

    #[tokio::test]
    async fn streamed_tsv_matches_the_buffered_layout() {
        let platform = crate::test_support::make_platform(vec![]);
        let mut first = PageListEntry::new(Title::new("Foo_bar", 0));
        first.set_page_id(Some(42));
        first.set_page_bytes(Some(7));
        first.set_page_timestamp(Some("2024-01-02".to_string()));
        let second = PageListEntry::new(Title::new("Talk:Baz", 1));

        let response = RenderTSV::new("\t")
            .stream(&platform, enwiki_params(), "enwiki", vec![first, second])
            .unwrap();
        assert_eq!(response.content_type, crate::content_type::ContentType::TSV);
        let chunks = response.stream.try_collect::<Vec<_>>().await.unwrap();
        let body = String::from_utf8(chunks.concat()).unwrap();
        assert_eq!(
            body,
            "number\ttitle\tpageid\tnamespace\tlength\ttouched\n\
             1\tFoo_bar\t42\t\t7\t2024-01-02\n\
             2\tTalk:Baz\t\tTalk\t\t"
        );
    }
}
