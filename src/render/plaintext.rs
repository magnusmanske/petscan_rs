use crate::content_type::ContentType;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MyResponse, Platform};
use crate::render::Render;
use crate::render::params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;

/// Renders `PlainText`
#[derive(Debug, Clone, Copy)]
pub struct RenderPlainText;

#[async_trait]
impl Render for RenderPlainText {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let params = RenderParams::new(platform, wiki).await?;
        let output = entries
            .iter()
            .filter_map(|entry| params.ns().full_pretty(entry.title()))
            .collect::<Vec<String>>()
            .join("\n");
        Ok(MyResponse {
            s: output,
            content_type: ContentType::Plain,
            status: 200,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().pretty().to_string()
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

    fn render_cell_namespace(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().namespace_id().to_string()
    }
}

impl RenderPlainText {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
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

    // The original cell helpers emitted wikitext markup
    // (`[[User:..|]]`, `[[File:..|120px|]]`); P0 #3 stripped that
    // because plaintext shouldn't contain wikilinks. These tests pin
    // the bare-value output — and demonstrate the P5 #37 unblock for
    // the previously-untested plaintext renderer.

    #[test]
    fn test_render_cell_title_is_pretty() {
        let r = RenderPlainText;
        let entry = PageListEntry::new(Title::new("Magnus Manske", 0));
        assert_eq!(r.render_cell_title(&entry, &enwiki_params()), "Magnus Manske");
    }

    #[test]
    fn test_render_cell_wikidata_item_some_returns_bare_qid() {
        let r = RenderPlainText;
        let mut entry = PageListEntry::new(Title::new("Test", 0));
        entry.set_wikidata_item(Some("Q42".to_string()));
        // Pinned at "Q42" rather than "[[:d:Q42|]]" — see P0 #3.
        assert_eq!(
            r.render_cell_wikidata_item(&entry, &enwiki_params()),
            "Q42"
        );
    }

    #[test]
    fn test_render_cell_wikidata_item_none_returns_empty() {
        let r = RenderPlainText;
        let entry = PageListEntry::new(Title::new("Test", 0));
        assert_eq!(r.render_cell_wikidata_item(&entry, &enwiki_params()), "");
    }

    #[test]
    fn test_render_user_name_is_bare() {
        let r = RenderPlainText;
        // Pinned at "Alice" rather than "[[User:Alice|]]" — P0 #3.
        assert_eq!(r.render_user_name("Alice", &enwiki_params()), "Alice");
    }

    #[test]
    fn test_render_cell_image_some_is_bare_filename() {
        let r = RenderPlainText;
        // Pinned at "Foo.jpg" rather than "[[File:Foo.jpg|120px|]]" — P0 #3.
        assert_eq!(
            r.render_cell_image(&Some("Foo.jpg".to_string()), &enwiki_params()),
            "Foo.jpg"
        );
    }

    #[test]
    fn test_render_cell_image_none_returns_empty() {
        let r = RenderPlainText;
        assert_eq!(r.render_cell_image(&None, &enwiki_params()), "");
    }

    #[test]
    fn test_render_cell_namespace_returns_id_as_string() {
        // Plaintext's namespace cell emits the numeric ID, not the name.
        let r = RenderPlainText;
        let user_entry = PageListEntry::new(Title::new("Foo", 2));
        assert_eq!(
            r.render_cell_namespace(&user_entry, &enwiki_params()),
            "2"
        );
    }
}
