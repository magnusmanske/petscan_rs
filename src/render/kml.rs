use crate::content_type::ContentType;
use crate::pagelist_entry::PageListEntry;
use crate::platform::{MyResponse, Platform};
use crate::render::Render;
use crate::render::params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;
use wikimisc::{commons, escape};

/// Width of the placemark images, in pixels.
const THUMBNAIL_WIDTH: u32 = 120;

/// Renders KML
#[derive(Clone, Copy, Debug)]
pub struct RenderKML;

#[async_trait]
impl Render for RenderKML {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let params = RenderParams::new(platform, wiki).await?;
        let server = params
            .state()
            .site_matrix()
            .get_server_url_for_wiki(wiki)
            .unwrap_or_default();
        let mut kml = String::new();
        kml += r#"<?xml version="1.0" encoding="UTF-8"?>
        <kml xmlns="http://www.opengis.net/kml/2.2"><Document>"#;

        for entry in entries {
            if let Some(coords) = &entry.get_coordinates() {
                let title = entry.title();
                let label = if let "wikidatawiki" = wiki {
                    match entry.get_wikidata_label() {
                        Some(s) => s,
                        None => title.pretty().to_string(),
                    }
                } else {
                    title.pretty().to_string()
                };
                kml += r#"<Placemark>"#;
                kml += format!("<name>{}</name>", escape::xml(&label)).as_str();
                if let Some(desc) = entry.get_wikidata_description() {
                    kml += format!("<description>{}</description>", escape::xml(&desc)).as_str();
                }

                kml += "<ExtendedData>";
                if let Some(q) = entry.get_wikidata_item() {
                    kml += format!("<Data name=\"q\"><value>{}</value></Data>", escape::xml(&q))
                        .as_str();
                }

                let full_title = match params.ns().full_with_underscores(title) {
                    Some(ft) => ft,
                    None => format!("{title:?}"),
                };
                let url = format!("{}/wiki/{}", server, super::escape_attribute(&full_title));
                kml += format!(
                    "<Data name=\"url\"><value>{}</value></Data>",
                    escape::xml(&url)
                )
                .as_str();

                if let Some(img) = entry.get_page_image() {
                    let src = commons::redirect_file_url(&server, &img, Some(THUMBNAIL_WIDTH));
                    kml += format!(
                        "<Data name=\"image\"><value>{}</value></Data>",
                        escape::xml(&src)
                    )
                    .as_str();
                }

                kml += "</ExtendedData>";

                kml += format!(
                    "<Point><coordinates>{}, {}, 0.</coordinates></Point>",
                    coords.lon, coords.lat
                )
                .as_str();
                kml += r#"</Placemark>"#;
            }
        }

        kml += r#"</Document></kml>"#;

        Ok(MyResponse {
            s: kml,
            content_type: ContentType::Plain,
            status: 200,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().pretty().to_string()
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

impl RenderKML {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

#[cfg(test)]
mod tests {
    use crate::render::escape_attribute;

    #[test]
    fn test_escape_attribute() {
        assert_eq!(escape_attribute("<>&\"'"), "%3C%3E%26%22%27");
    }
}
