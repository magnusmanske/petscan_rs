pub mod html;
pub mod json;
pub mod jsonl;
pub mod kml;
pub mod pagepile;
pub mod params;
pub mod plaintext;
pub mod tsv;
pub mod wikitext;

use crate::form_parameters::FormParameters;
use crate::pagelist_entry::{LinkCount, PageListEntry};
use crate::platform::{MyResponse, Platform};
use crate::render::params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use wikimisc::mediawiki::api::{Api, NamespaceID};
use wikimisc::mediawiki::title::Title;

pub static AUTOLIST_WIKIDATA: &str = "www.wikidata.org";
pub static AUTOLIST_COMMONS: &str = "commons.wikimedia.org";

/// Namespace-and-title operations that the renderers need from a
/// MediaWiki `Api`. Extracting these behind a trait lets `RenderParams`
/// hold an `Arc<dyn NamespaceContext>` instead of an `Api` directly,
/// which unblocks unit tests for renderer code: production injects an
/// `ApiNamespaceContext` wrapping a real network-backed `Api`; tests
/// inject a `StubNamespaceContext` driven by a `HashMap`.
///
/// Methods take `&self` and return borrowed `&str` where possible so the
/// hot path (one call per rendered row) doesn't allocate.
pub trait NamespaceContext: std::fmt::Debug + Send + Sync {
    /// Local (UI-language) namespace name for `namespace_id`, e.g. "User"
    /// or "Benutzer" depending on the wiki.
    fn local_namespace_name(&self, namespace_id: NamespaceID) -> Option<&str>;

    /// Canonical namespace name (the wiki-independent English form).
    fn canonical_namespace_name(&self, namespace_id: NamespaceID) -> Option<&str>;

    /// `"Talk:Foo"` etc., with spaces. Delegates to `Title::full_pretty`.
    fn full_pretty(&self, title: &Title) -> Option<String>;

    /// `"Talk:Foo_bar"`, with underscores. Delegates to
    /// `Title::full_with_underscores`.
    fn full_with_underscores(&self, title: &Title) -> Option<String>;

    /// Iterate `(namespace_id_string, local_name)` pairs for the JSON
    /// renderer's namespaces section. Functional callback to avoid
    /// committing to an iterator type on the trait.
    fn for_each_local_namespace(&self, f: &mut dyn FnMut(&str, &str));
}

/// Production [`NamespaceContext`] backed by a real `wikimisc` `Api`.
#[derive(Debug, Clone)]
pub struct ApiNamespaceContext {
    api: Arc<Api>,
}

impl ApiNamespaceContext {
    pub fn new(api: Api) -> Arc<Self> {
        Arc::new(Self { api: Arc::new(api) })
    }

    pub fn api(&self) -> &Api {
        &self.api
    }
}

impl NamespaceContext for ApiNamespaceContext {
    fn local_namespace_name(&self, namespace_id: NamespaceID) -> Option<&str> {
        self.api.get_local_namespace_name(namespace_id)
    }

    fn canonical_namespace_name(&self, namespace_id: NamespaceID) -> Option<&str> {
        self.api.get_canonical_namespace_name(namespace_id)
    }

    fn full_pretty(&self, title: &Title) -> Option<String> {
        title.full_pretty(&self.api)
    }

    fn full_with_underscores(&self, title: &Title) -> Option<String> {
        title.full_with_underscores(&self.api)
    }

    fn for_each_local_namespace(&self, f: &mut dyn FnMut(&str, &str)) {
        if let Some(namespaces) =
            self.api.get_site_info()["query"]["namespaces"].as_object()
        {
            for (k, v) in namespaces {
                if let Some(local_name) = v["*"].as_str() {
                    f(k, local_name);
                }
            }
        }
    }
}


/// Percent-encode `s` and then escape the four XML/HTML attribute specials
/// (`<`, `>`, `"`, `'`). Used by the HTML and KML renderers when building
/// `href`/`src`/`name="..."`-style attributes.
pub(crate) fn escape_attribute(s: &str) -> String {
    FormParameters::percent_encode(s)
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[async_trait]
pub trait Render {
    async fn response(
        &self,
        _platform: &Platform,
        _wiki: &str,
        _pages: Vec<PageListEntry>,
    ) -> Result<MyResponse>;

    fn file_data_keys(&self) -> Vec<&str> {
        vec![
            "img_size",
            "img_width",
            "img_height",
            "img_media_type",
            "img_major_mime",
            "img_minor_mime",
            "img_user_text",
            "img_timestamp",
            "img_sha1",
        ]
    }

    fn get_initial_columns(&self, params: &RenderParams) -> Vec<&str> {
        let mut columns = vec![];
        if params.use_autolist() {
            columns.push("checkbox");
        }
        columns.push("number");
        if params.add_image() {
            columns.push("image");
        }
        columns.push("title");
        if params.do_output_redlinks() {
            //columns.push("namespace");
            columns.push("redlink_count");
        } else {
            columns.push("page_id");
            columns.push("namespace");
            columns.push("size");
            columns.push("timestamp");
        }
        if params.show_wikidata_item() {
            columns.push("wikidata_item");
        }
        if params.add_coordinates() {
            columns.push("coordinates");
        }
        if params.add_defaultsort() {
            columns.push("defaultsort");
        }
        if params.add_disambiguation() {
            columns.push("disambiguation");
        }
        if params.add_incoming_links() {
            columns.push("incoming_links");
        }
        if params.add_sitelinks() {
            columns.push("sitelinks");
        }
        if params.file_data() {
            self.file_data_keys().iter().for_each(|k| columns.push(*k));
        }
        if params.file_usage() {
            columns.push("fileusage");
        }
        columns
    }

    // The five cell-render methods below are only invoked by the default
    // `row_from_entry` implementation, which non-tabular renderers (JSON,
    // JSONL, PagePile) bypass entirely. Each has a `String::new()` default
    // so renderers that produce structured output don't have to carry
    // stub impls. Tabular renderers (HTML, TSV, Wikitext) override every
    // method they actually need.
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
    fn render_cell_checkbox(
        &self,
        _entry: &PageListEntry,
        _params: &RenderParams,
        _platform: &Platform,
    ) -> String {
        String::new()
    }
    fn render_cell_fileusage(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match &entry.get_file_info() {
            Some(fi) => {
                let mut rows: Vec<String> = vec![];
                for fu in &fi.file_usage {
                    let txt = format!(
                        "{}:{}:{}:{}",
                        fu.wiki(),
                        fu.title().namespace_id(),
                        fu.namespace_name(),
                        fu.title().pretty()
                    );
                    rows.push(txt);
                }
                rows.join("|")
            }
            None => String::new(),
        }
    }
    fn render_coordinates(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match &entry.get_coordinates() {
            Some(coords) => format!("{}/{}", coords.lat, coords.lon),
            None => String::new(),
        }
    }

    fn opt_usize(&self, o: &Option<usize>) -> String {
        o.map(|x| x.to_string()).unwrap_or_default()
    }

    fn opt_u32(&self, o: &Option<u32>) -> String {
        o.map(|x| x.to_string()).unwrap_or_default()
    }

    fn opt_linkcount(&self, o: &Option<LinkCount>) -> String {
        o.map(|x| x.to_string()).unwrap_or_default()
    }

    fn opt_bool(&self, o: &Option<bool>) -> String {
        match o {
            Some(b) => {
                if *b {
                    "Y"
                } else {
                    "N"
                }
            }
            None => "",
        }
        .to_string()
    }

    fn opt_string(&self, o: &Option<String>) -> String {
        o.as_ref().map(|x| x.to_string()).unwrap_or_default()
    }

    fn row_from_entry(
        &self,
        entry: &PageListEntry,
        header: &[(String, String)],
        params: &RenderParams,
        platform: &Platform,
    ) -> Vec<String> {
        let mut ret = vec![];
        for (k, _) in header {
            let cell = match k.as_str() {
                "title" => self.render_cell_title(entry, params),
                "page_id" => self.opt_u32(&entry.page_id()),
                "namespace" => self.render_cell_namespace(entry, params),
                "size" => self.opt_u32(&entry.page_bytes()),
                "timestamp" => self.opt_string(&entry.get_page_timestamp()),
                "wikidata_item" => self.render_cell_wikidata_item(entry, params),
                "image" => self.render_cell_image(&entry.get_page_image(), params),
                "number" => params.row_number().to_string(),
                "defaultsort" => self.opt_string(&entry.get_defaultsort()),
                "disambiguation" => self.opt_bool(&entry.disambiguation().as_option_bool()),
                "incoming_links" => self.opt_linkcount(&entry.incoming_links()),
                "sitelinks" => self.opt_linkcount(&entry.sitelink_count()),

                // `img_user_text` routes through `render_user_name` so a
                // tabular renderer can wrap it (e.g. HTML's [[User:…]]
                // wikilink). All other img_* fields share a single helper.
                "img_user_text" => entry
                    .get_file_info()
                    .as_ref()
                    .and_then(|fi| fi.img_user_text.as_deref())
                    .map(|user| self.render_user_name(user, params))
                    .unwrap_or_default(),
                "img_size" | "img_width" | "img_height" | "img_media_type"
                | "img_major_mime" | "img_minor_mime" | "img_timestamp" | "img_sha1" => entry
                    .get_file_info()
                    .as_ref()
                    .and_then(|fi| fi.field_as_str(k.as_str()))
                    .unwrap_or_default(),

                "checkbox" => self.render_cell_checkbox(entry, params, platform),
                "linknumber" => match entry.link_count() {
                    Some(lc) => format!("{lc}"),
                    None => String::new(),
                },
                "redlink_count" => match entry.redlink_count() {
                    Some(lc) => format!("{lc}"),
                    None => String::new(),
                },
                "coordinates" => self.render_coordinates(entry, params),
                "fileusage" => self.render_cell_fileusage(entry, params),

                _ => "<".to_string() + k + ">",
            };
            ret.push(cell);
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Security invariant: after `escape_attribute`, the output is safe to
    /// drop into an HTML/XML attribute value without further escaping.
    /// None of the four attribute-breaking characters may appear raw.
    fn assert_no_unescaped_attr_specials(s: &str) {
        for c in &['<', '>', '"', '\''] {
            assert!(
                !s.contains(*c),
                "found unescaped {c:?} in escape_attribute output: {s:?}"
            );
        }
    }

    #[test]
    fn escape_attribute_handles_script_tag_injection() {
        // Classic XSS attempt — the literal `<script>` must not survive.
        let out = escape_attribute("<script>alert(1)</script>");
        assert_no_unescaped_attr_specials(&out);
        assert_eq!(out, "%3Cscript%3Ealert%281%29%3C%2Fscript%3E");
    }

    #[test]
    fn escape_attribute_handles_attribute_breakout() {
        // An attacker who can control a `value="..."` substring tries to
        // close the attribute and inject an event handler. Both ' and "
        // and the surrounding whitespace must be encoded so the injected
        // characters cannot terminate the attribute. The identifier
        // (`onmouseover`) itself remains visible — that is fine; it only
        // becomes dangerous if accompanied by the unescaped specials.
        let out = escape_attribute("\" onmouseover='x' \"");
        assert_no_unescaped_attr_specials(&out);
        assert!(out.contains("%22")); // "
        assert!(out.contains("%27")); // '
        assert!(out.contains("%20")); // space
    }

    #[test]
    fn escape_attribute_encodes_ampersand() {
        // Ampersand must be encoded so a downstream HTML parser cannot
        // interpret the remainder as a character reference.
        let out = escape_attribute("a&b");
        assert_no_unescaped_attr_specials(&out);
        assert!(!out.contains('&'));
        assert_eq!(out, "a%26b");
    }

    #[test]
    fn escape_attribute_encodes_javascript_url() {
        // Even though we only use the output in attribute *values* (not
        // href targets), pinning behavior here documents the contract: no
        // executable URL scheme survives intact for an attacker to
        // hand-decode and re-inject.
        let out = escape_attribute("javascript:alert(1)");
        assert_no_unescaped_attr_specials(&out);
        assert_eq!(out, "javascript%3Aalert%281%29");
    }

    #[test]
    fn escape_attribute_preserves_plain_alphanumeric() {
        assert_eq!(escape_attribute("hello"), "hello");
        assert_eq!(escape_attribute("HelloWorld42"), "HelloWorld42");
    }

    #[test]
    fn escape_attribute_handles_empty_string() {
        assert_eq!(escape_attribute(""), "");
    }

    #[test]
    fn escape_attribute_handles_single_quote() {
        let out = escape_attribute("O'Brien");
        assert_no_unescaped_attr_specials(&out);
        assert_eq!(out, "O%27Brien");
    }

    #[test]
    fn escape_attribute_handles_unicode() {
        // Non-ASCII characters get percent-encoded as their UTF-8 bytes;
        // the invariant still holds.
        let out = escape_attribute("名前");
        assert_no_unescaped_attr_specials(&out);
        assert!(out.starts_with('%'));
        // 名 = E5 90 8D, 前 = E5 89 8D
        assert_eq!(out, "%E5%90%8D%E5%89%8D");
    }

    #[test]
    fn escape_attribute_invariant_holds_on_pathological_inputs() {
        // Fuzz-style sweep of nasty characters. The invariant must hold
        // regardless of input.
        let payloads = [
            "<>'\"&",
            "</style><script>",
            "\0\u{0001}\u{0002}",
            "\\\"//><",
            "\n\r\t",
            ";--",
            "data:text/html,<script>alert(1)</script>",
        ];
        for p in payloads {
            let out = escape_attribute(p);
            assert_no_unescaped_attr_specials(&out);
            // No ampersand either — attribute parsers can interpret it
            // as the start of a character reference.
            assert!(
                !out.contains('&'),
                "found unescaped & in escape_attribute({p:?}) = {out:?}"
            );
        }
    }
}
