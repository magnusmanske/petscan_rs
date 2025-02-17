use crate::pagelist_entry::PageListEntry;
use crate::platform::*;
use crate::render::Render;
use crate::render_params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;

/// Renders PlainText
pub struct RenderPlainText {}

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
            .filter_map(|entry| entry.title().full_pretty(params.api()))
            .collect::<Vec<String>>()
            .join("\n");
        Ok(MyResponse {
            s: output,
            content_type: ContentType::Plain,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().pretty().to_string()
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

impl RenderPlainText {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
