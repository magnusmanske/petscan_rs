use crate::content_type::ContentType;
use crate::platform::MyResponse;
use crate::render::Render;
use crate::{pagelist_entry::PageListEntry, platform::Platform};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use pagepile::client::NewPage;

/// Renders `PagePile`
#[derive(Clone, Copy, Debug)]
pub struct RenderPagePile;

#[async_trait]
impl Render for RenderPagePile {
    async fn response(
        &self,
        _platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let pages: Vec<NewPage> = entries
            .iter()
            .map(|e| NewPage::new(e.title().pretty(), e.title().namespace_id()))
            .collect();
        let client = crate::datasource::pagepile::client()?;
        let id = client
            .create(wiki, &pages)
            .await
            .map_err(|e| anyhow!("PagePile generation failed: {e}"))?;
        let url = client.view_url(id);
        let html = format!(
            "<html><head><meta http-equiv=\"refresh\" content=\"0; url={url}\" /></head><BODY><H1>Redirect</H1>The document can be found <A HREF='{url}'>here</A>.</BODY></html>"
        );
        Ok(MyResponse {
            s: html,
            content_type: ContentType::HTML,
            status: 200,
        })
    }
}

impl RenderPagePile {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
