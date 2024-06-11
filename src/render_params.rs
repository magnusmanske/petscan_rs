use crate::app_state::AppState;
use crate::platform::*;
use crate::render::AUTOLIST_WIKIDATA;
use anyhow::Result;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;

#[derive(Debug, Clone)]
pub struct RenderParams {
    wiki: String,
    file_data: bool,
    file_usage: bool,
    thumbnails_in_wiki_output: bool,
    wdi: String,
    show_wikidata_item: bool,
    is_wikidata: bool,
    add_coordinates: bool,
    add_image: bool,
    add_defaultsort: bool,
    add_disambiguation: bool,
    add_incoming_links: bool,
    add_sitelinks: bool,
    do_output_redlinks: bool,
    use_autolist: bool,
    autolist_creator_mode: bool,
    autolist_wiki_server: String,
    api: Api,
    state: Arc<AppState>,
    row_number: usize,
    json_output_compatability: String,
    json_callback: String,
    json_sparse: bool,
    json_pretty: bool,
    giu: bool,
}

impl RenderParams {
    pub async fn new(platform: &Platform, wiki: &str) -> Result<Self> {
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let mut ret = Self {
            wiki: wiki.to_string(),
            file_data: platform.has_param("ext_image_data"),
            file_usage: platform.has_param("file_usage_data"),
            thumbnails_in_wiki_output: platform.has_param("thumbnails_in_wiki_output"),
            wdi: platform.get_param_default("wikidata_item", "no"),
            add_coordinates: platform.has_param("add_coordinates"),
            add_image: platform.has_param("add_image")
                || platform.get_param_blank("format") == "kml",
            add_defaultsort: platform.has_param("add_defaultsort"),
            add_disambiguation: platform.has_param("add_disambiguation"),
            add_incoming_links: platform.get_param_blank("sortby") == "incoming_links",
            add_sitelinks: platform.get_param_blank("sortby") == "sitelinks",
            show_wikidata_item: false,
            is_wikidata: wiki == "wikidatawiki",
            do_output_redlinks: platform.do_output_redlinks(),
            use_autolist: false,          // Possibly set downstream
            autolist_creator_mode: false, // Possibly set downstream
            autolist_wiki_server: AUTOLIST_WIKIDATA.to_string(), // Possibly set downstream
            api,
            state: platform.state(),
            row_number: 0,
            json_output_compatability: platform
                .get_param_default("output_compatability", "catscan"), // Default; "quick-intersection" ?
            json_callback: platform.get_param_blank("callback"),
            json_sparse: platform.has_param("sparse"),
            json_pretty: platform.has_param("json-pretty"),
            giu: platform.has_param("giu"),
        };
        ret.show_wikidata_item = ret.wdi == "any" || ret.wdi == "with";
        Ok(ret)
    }

    pub fn show_wikidata_item(&self) -> bool {
        self.show_wikidata_item
    }

    pub fn file_data(&self) -> bool {
        self.file_data
    }

    pub fn do_output_redlinks(&self) -> bool {
        self.do_output_redlinks
    }

    pub fn row_number_mut(&mut self) -> &mut usize {
        &mut self.row_number
    }

    pub fn thumbnails_in_wiki_output(&self) -> bool {
        self.thumbnails_in_wiki_output
    }

    pub fn api(&self) -> &Api {
        &self.api
    }

    pub fn is_wikidata(&self) -> bool {
        self.is_wikidata
    }

    pub fn use_autolist_mut(&mut self) -> &mut bool {
        &mut self.use_autolist
    }

    pub fn use_autolist(&self) -> bool {
        self.use_autolist
    }

    pub fn autolist_creator_mode_mut(&mut self) -> &mut bool {
        &mut self.autolist_creator_mode
    }

    pub fn autolist_wiki_server(&self) -> &str {
        &self.autolist_wiki_server
    }

    pub fn set_autolist_wiki_server(&mut self, autolist_wiki_server: &str) {
        self.autolist_wiki_server = autolist_wiki_server.to_string();
    }

    pub fn row_number(&self) -> usize {
        self.row_number
    }

    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn wiki(&self) -> &str {
        &self.wiki
    }

    pub fn autolist_creator_mode(&self) -> bool {
        self.autolist_creator_mode
    }

    pub fn json_pretty(&self) -> bool {
        self.json_pretty
    }

    pub fn file_usage(&self) -> bool {
        self.file_usage
    }

    pub fn giu(&self) -> bool {
        self.giu
    }

    pub fn set_json_sparse(&mut self, json_sparse: bool) {
        self.json_sparse = json_sparse;
    }

    pub fn set_file_usage(&mut self, file_usage: bool) {
        self.file_usage = file_usage;
    }

    pub fn json_output_compatability(&self) -> &str {
        &self.json_output_compatability
    }

    pub fn json_callback(&self) -> &str {
        &self.json_callback
    }

    pub fn json_sparse(&self) -> bool {
        self.json_sparse
    }

    pub fn add_image(&self) -> bool {
        self.add_image
    }

    pub fn add_coordinates(&self) -> bool {
        self.add_coordinates
    }

    pub fn add_defaultsort(&self) -> bool {
        self.add_defaultsort
    }

    pub fn add_disambiguation(&self) -> bool {
        self.add_disambiguation
    }

    pub fn add_incoming_links(&self) -> bool {
        self.add_incoming_links
    }

    pub fn add_sitelinks(&self) -> bool {
        self.add_sitelinks
    }
}
