use crate::pagelist_entry::PageListEntry;
use crate::platform::*;
use crate::render::Render;
use crate::render_json::RenderJSON;
use crate::render_params::RenderParams;
use async_trait::async_trait;
use serde_json::Value;

/// Renders JSON
pub struct RenderJSONL {}

#[async_trait]
impl Render for RenderJSONL {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let content_type = ContentType::Plain;

        let rj = RenderJSON::new();

        let value = rj.generate_json(platform, &mut params, entries).await?;

        let value: &Value = match params.json_output_compatability() {
            "quick-intersection" => &value["pages"],
            _ => &value["*"][0]["a"]["*"],
        };

        let parts = match value.as_array() {
            Some(p) => p,
            None => return Err("JSON value is not an array".to_string()),
        };

        let mut out: String = String::new();
        for part in parts {
            let output = ::serde_json::to_string(&part);
            match output {
                Ok(o) => out += &o,
                Err(e) => return Err(format!("JSON encoding failed: {:?}", e)),
            };
            out += "\n";
        }

        Ok(MyResponse {
            s: out,
            content_type,
        })
    }

    fn render_cell_wikidata_item(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "N/A".to_string()
    }
    fn render_user_name(&self, _user: &str, _params: &RenderParams) -> String {
        "N/A".to_string()
    }
    fn render_cell_image(&self, _image: &Option<String>, _params: &RenderParams) -> String {
        "N/A".to_string()
    }
    fn render_cell_namespace(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "N/A".to_string()
    }
    fn render_cell_title(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "N/A".to_string()
    }
}

impl RenderJSONL {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
