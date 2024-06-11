use crate::pagelist_entry::PageListEntry;
use crate::platform::*;
use crate::render::Render;
use crate::render_params::RenderParams;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;

/// Renders PagePile
pub struct RenderPagePile {}

#[async_trait]
impl Render for RenderPagePile {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let url = "https://pagepile.toolforge.org/api.php";
        let data: String = entries
            .iter()
            .map(|e| format!("{}\t{}", e.title().pretty(), e.title().namespace_id()))
            .collect::<Vec<String>>()
            .join("\n");
        let mut params: HashMap<String, String> =
            [("action", "create_pile_with_data"), ("wiki", wiki)]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();
        params.insert("data".to_string(), data);

        let result = match api.query_raw(url, &params, "POST").await {
            Ok(r) => r,
            Err(e) => return Err(anyhow!("PagePile generation failed: {:?}", e)),
        };
        let json: serde_json::value::Value = match serde_json::from_str(&result) {
            Ok(j) => j,
            Err(e) => {
                return Err(anyhow!(
                    "PagePile generation did not return valid JSON: {:?}",
                    e
                ))
            }
        };
        let pagepile_id = match json["pile"]["id"].as_u64() {
            Some(id) => id,
            None => {
                return Err(anyhow!(
                    "PagePile generation did not return a pagepile ID: {:?}",
                    json.clone()
                ))
            }
        };
        let url = format!(
            "https://tools.wmflabs.org/pagepile/api.php?action=get_data&id={}",
            pagepile_id
        );
        let html = format!("<html><head><meta http-equiv=\"refresh\" content=\"0; url={}\" /></head><BODY><H1>Redirect</H1>The document can be found <A HREF='{}'>here</A>.</BODY></html>",&url,&url) ;
        Ok(MyResponse {
            s: html,
            content_type: ContentType::HTML,
        })
    }

    fn render_cell_title(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        String::new()
    }
    fn render_cell_wikidata_item(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        String::new()
    }
    fn render_user_name(&self, _user: &str, _params: &RenderParams) -> String {
        String::new()
    }
    fn render_cell_image(&self, _image: &Option<String>, _params: &RenderParams) -> String {
        String::new()
    }
    fn render_cell_namespace(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        String::new()
    }
}

impl RenderPagePile {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
