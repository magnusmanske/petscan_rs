use crate::form_parameters::FormParameters;
use crate::pagelist_entry::PageListEntry;
use crate::platform::*;
use crate::render::Render;
use crate::render_params::RenderParams;
use async_trait::async_trait;

/// Renders KML
pub struct RenderKML {}

#[async_trait]
impl Render for RenderKML {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let params = RenderParams::new(platform, wiki).await?;
        let server = match params.state().site_matrix().get_server_url_for_wiki(wiki) {
            Ok(url) => url,
            Err(_e) => String::new(),
        };
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
                kml += format!("<name>{}</name>", self.escape_xml(&label)).as_str();
                if let Some(desc) = entry.get_wikidata_description() {
                    kml +=
                        format!("<description>{}</description>", self.escape_xml(&desc)).as_str();
                }

                kml += "<ExtendedData>";
                if let Some(q) = entry.get_wikidata_item() {
                    kml += format!(
                        "<Data name=\"q\"><value>{}</value></Data>",
                        self.escape_xml(&q)
                    )
                    .as_str();
                }

                let full_title = match title.full_with_underscores(params.api()) {
                    Some(ft) => ft,
                    None => format!("{:?}", title),
                };
                let url = format!("{}/wiki/{}", &server, &self.escape_attribute(&full_title));
                kml += format!(
                    "<Data name=\"url\"><value>{}</value></Data>",
                    self.escape_xml(&url)
                )
                .as_str();

                if let Some(img) = entry.get_page_image() {
                    let file = self.escape_attribute(&img);
                    let src = format!(
                        "{}/wiki/Special:Redirect/file/{}?width={}",
                        &server, &file, 120
                    );
                    kml += format!(
                        "<Data name=\"image\"><value>{}</value></Data>",
                        self.escape_xml(&src)
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

impl RenderKML {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }

    fn escape_xml(&self, s: &str) -> String {
        s.replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
            .replace('&', "&amp;")
    }

    fn escape_attribute(&self, s: &str) -> String {
        FormParameters::percent_encode(s)
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_xml() {
        let r = RenderKML::new();
        assert_eq!(
            r.escape_xml("<>&\"'"),
            "&amp;lt;&amp;gt;&amp;&amp;quot;&amp;apos;"
        );
    }

    #[test]
    fn test_escape_attribute() {
        let r = RenderKML::new();
        assert_eq!(r.escape_attribute("<>&\"'"), "%3C%3E%26%22%27");
    }
}
