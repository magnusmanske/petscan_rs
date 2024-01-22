use async_trait::async_trait;
use crate::app_state::AppState;
use crate::form_parameters::FormParameters;
use crate::pagelist_entry::{LinkCount, PageListEntry};
use crate::platform::*;
use chrono::prelude::*;
use htmlescape::encode_minimal;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wikibase::mediawiki::api::Api;
use wikibase::mediawiki::title::Title;

static MAX_HTML_RESULTS: usize = 10000;
static AUTOLIST_WIKIDATA: &str = "www.wikidata.org";
static AUTOLIST_COMMONS: &str = "commons.wikimedia.org";

//________________________________________________________________________________________________________________________

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
    pub async fn new(platform: &Platform, wiki: &str) -> Result<Self, String> {
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let mut ret = Self {
            wiki: wiki.to_string(),
            file_data: platform.has_param("ext_image_data"),
            file_usage: platform.has_param("file_usage_data"),
            thumbnails_in_wiki_output: platform.has_param("thumbnails_in_wiki_output"),
            wdi: platform.get_param_default("wikidata_item", "no"),
            add_coordinates: platform.has_param("add_coordinates"),
            add_image: platform.has_param("add_image")||platform.get_param_blank("format")=="kml",
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
}

//________________________________________________________________________________________________________________________

#[async_trait]
pub trait Render {
    async fn response(
        &self,
        _platform: &Platform,
        _wiki: &str,
        _pages: Vec<PageListEntry>,
    ) -> Result<MyResponse, String>;

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
        if params.use_autolist {
            columns.push("checkbox");
        }
        columns.push("number");
        if params.add_image {
            columns.push("image");
        }
        columns.push("title");
        if params.do_output_redlinks {
            //columns.push("namespace");
            columns.push("redlink_count");
        } else {
            columns.push("page_id");
            columns.push("namespace");
            columns.push("size");
            columns.push("timestamp");
        }
        if params.show_wikidata_item {
            columns.push("wikidata_item");
        }
        if params.add_coordinates {
            columns.push("coordinates");
        }
        if params.add_defaultsort {
            columns.push("defaultsort");
        }
        if params.add_disambiguation {
            columns.push("disambiguation");
        }
        if params.add_incoming_links {
            columns.push("incoming_links");
        }
        if params.add_sitelinks {
            columns.push("sitelinks");
        }
        if params.file_data {
            self.file_data_keys().iter().for_each(|k| columns.push(*k));
        }
        if params.file_usage {
            columns.push("fileusage");
        }
        columns
    }

    fn render_cell_title(&self, _entry: &PageListEntry, _params: &RenderParams) -> String;
    fn render_cell_wikidata_item(&self, _entry: &PageListEntry, _params: &RenderParams) -> String;
    fn render_user_name(&self, _user: &String, _params: &RenderParams) -> String;
    fn render_cell_image(&self, _image: &Option<String>, _params: &RenderParams) -> String;
    fn render_cell_namespace(&self, _entry: &PageListEntry, _params: &RenderParams) -> String;
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
                    let txt = format!("{}:{}:{}:{}",fu.wiki(),fu.title().namespace_id(),fu.namespace_name(),fu.title().pretty());
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
        o.map(|x| x.to_string()).unwrap_or_else(String::new)
    }

    fn opt_u32(&self, o: &Option<u32>) -> String {
        o.map(|x| x.to_string()).unwrap_or_else(String::new)
    }

    fn opt_linkcount(&self, o: &Option<LinkCount>) -> String {
        o.map(|x| x.to_string()).unwrap_or_else(String::new)
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
        o.as_ref().map(|x| x.to_string()).unwrap_or_else(String::new)
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
                "page_id" => self.opt_u32(&entry.page_id),
                "namespace" => self.render_cell_namespace(entry, params),
                "size" => self.opt_u32(&entry.page_bytes),
                "timestamp" => self.opt_string(&entry.get_page_timestamp()),
                "wikidata_item" => self.render_cell_wikidata_item(entry, params),
                "image" => self.render_cell_image(&entry.get_page_image(), params),
                "number" => params.row_number.to_string(),
                "defaultsort" => self.opt_string(&entry.get_defaultsort()),
                "disambiguation" => self.opt_bool(&entry.disambiguation.as_option_bool()),
                "incoming_links" => self.opt_linkcount(&entry.incoming_links),
                "sitelinks" => self.opt_linkcount(&entry.sitelink_count),

                "img_size" => match &entry.get_file_info() {
                    Some(fi) => self.opt_usize(&fi.img_size),
                    None => String::new(),
                },
                "img_width" => match &entry.get_file_info() {
                    Some(fi) => self.opt_usize(&fi.img_width),
                    None => String::new(),
                },
                "img_height" => match &entry.get_file_info() {
                    Some(fi) => self.opt_usize(&fi.img_height),
                    None => String::new(),
                },
                "img_media_type" => match &entry.get_file_info() {
                    Some(fi) => self.opt_string(&fi.img_media_type),
                    None => String::new(),
                },
                "img_major_mime" => match &entry.get_file_info() {
                    Some(fi) => self.opt_string(&fi.img_major_mime),
                    None => String::new(),
                },
                "img_minor_mime" => match &entry.get_file_info() {
                    Some(fi) => self.opt_string(&fi.img_minor_mime),
                    None => String::new(),
                },
                "img_user_text" => match &entry.get_file_info() {
                    Some(fi) => self.render_user_name(&self.opt_string(&fi.img_user_text), &params),
                    None => String::new(),
                },
                "img_timestamp" => match &entry.get_file_info() {
                    Some(fi) => self.opt_string(&fi.img_timestamp),
                    None => String::new(),
                },
                "img_sha1" => match &entry.get_file_info() {
                    Some(fi) => self.opt_string(&fi.img_sha1),
                    None => String::new(),
                },

                "checkbox" => self.render_cell_checkbox(entry, params, platform),
                "linknumber" => match &entry.link_count {
                    Some(lc) => format!("{}", &lc),
                    None => String::new(),
                },
                "redlink_count" => match &entry.redlink_count {
                    Some(lc) => format!("{}", &lc),
                    None => String::new(),
                },
                "coordinates" => self.render_coordinates(entry, params),
                "fileusage" => self.render_cell_fileusage(&entry, &params),

                _ => "<".to_string() + k + ">",
            };
            ret.push(cell);
        }
        ret
    }
}

//________________________________________________________________________________________________________________________

/// Renders wiki text
pub struct RenderWiki {}

#[async_trait]
impl Render for RenderWiki {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let mut rows: Vec<String> = vec![];
        rows.push("== ".to_string() + &platform.combination().to_string() + " ==");

        let petscan_query_url =
            "https://petscan.wmflabs.org/?".to_string() + &platform.form_parameters().to_string();
        let petscan_query_url_no_doit = "https://petscan.wmflabs.org/?".to_string()
            + &platform.form_parameters().to_string_no_doit();

        let utc: DateTime<Utc> = Utc::now();
        rows.push(format!("Last updated on {}.", utc.to_rfc2822()));

        rows.push(format!(
            "[{} Regenerate this table] or [{} edit the query].\n",
            &petscan_query_url, &petscan_query_url_no_doit
        ));
        rows.push("{| border=1 class='wikitable'".to_string());
        let mut header: Vec<(&str, &str)> = vec![
            ("title", "Title"),
            ("page_id", "Page ID"),
            ("namespace", "Namespace"),
            ("size", "Size (bytes)"),
            ("timestamp", "Last change"),
        ];
        if params.show_wikidata_item {
            header.push(("wikidata_item", "Wikidata"));
        }
        if params.file_data {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k, k)));
        }
        if params.do_output_redlinks {
            header = vec![("redlink_count", "Wanted"), ("title", "Title")];
        }
        let mut header: Vec<(String, String)> = header
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for col in self.get_initial_columns(&params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        rows.push(
            "!".to_string()
                + &header
                    .iter()
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<String>>()
                    .join(" !! "),
        );

        for entry in entries {
            params.row_number += 1;
            rows.push("|-".to_string());
            let row = self.row_from_entry(&entry, &header, &params, &platform);
            let row = "| ".to_string() + &row.join(" || ");
            rows.push(row);
        }

        rows.push("|}".to_string());

        Ok(MyResponse {
            s: rows.join("\n"),
            content_type: ContentType::Plain,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        if entry.title().namespace_id() == 6  {
            if params.thumbnails_in_wiki_output {
                match entry.title().full_pretty(&params.api) {
                    Some(file) => format!("[[{}|120px|]]",&file),
                    None => format!("[[File:{}|120px|]]",entry.title().pretty()),
                }
            } else {
                match entry.title().full_pretty(&params.api) {
                    Some(file) => format!("[[:{}|]]",&file),
                    None => format!("[[:File:{}|]]",entry.title().pretty()),
                }
            }
        } else {
            self.render_wikilink(&entry, &params)
        }
    }

    fn render_cell_wikidata_item(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match entry.get_wikidata_item() {
            Some(q) => format!("[[:d:{}|]]",q),
            None => String::new(),
        }
    }

    fn render_user_name(&self, user: &String, _params: &RenderParams) -> String {
        format!("[[User:{}|]]",user)
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => format!("[[File:{}|120px|]]",img),
            None => String::new()
        }
    }

    fn render_cell_namespace(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().namespace_id().to_string()
    }
}

impl RenderWiki {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }

    fn render_wikilink(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        if params.is_wikidata {
            match &entry.get_wikidata_label() {
                Some(label) => format!("[[{}|{}]]", &entry.title().pretty(),label),
                None => format!("[[{}]]",entry.title().pretty())
            }
        } else {
            let mut ret = "[[".to_string();
            if entry.title().namespace_id() == 14 {
                ret += ":";
            }
            ret += &entry
                .title()
                .full_pretty(&params.api)
                .unwrap_or_else(|| entry.title().pretty().to_string());
            if !params.do_output_redlinks {
                ret += "|";
            }
            ret += "]]";
            ret
        }
    }
}

//________________________________________________________________________________________________________________________

/// Renders CSV and TSV
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
    ) -> Result<MyResponse, String> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let mut rows: Vec<String> = vec![];
        let mut header: Vec<(&str, &str)> = vec![
            ("number", "number"),
            ("title", "title"),
            ("page_id", "pageid"),
            ("namespace", "namespace"),
            ("size", "length"),
            ("timestamp", "touched"),
        ];
        if params.show_wikidata_item {
            header.push(("wikidata_item", "Wikidata"));
        }
        if params.file_data {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k, k)));
        }
        let mut header: Vec<(String, String)> = header
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for col in self.get_initial_columns(&params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        rows.push(
            header
                .iter()
                .map(|(_, v)| self.escape_cell(v))
                .collect::<Vec<String>>()
                .join(&self.separator),
        );

        for entry in entries {
            params.row_number += 1;
            let row = self.row_from_entry(&entry, &header, &params, &platform);
            let row: Vec<String> = row.iter().map(|s| self.escape_cell(s)).collect();
            let row = row.join(&self.separator);
            rows.push(row);
        }

        Ok(MyResponse {
            s: rows.join("\n"),
            content_type: match self.separator.as_str() {
                "," => ContentType::CSV,
                "\t" => ContentType::TSV,
                _ => ContentType::Plain, // Fallback
            },
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().with_underscores()
    }

    fn render_cell_wikidata_item(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match entry.get_wikidata_item() {
            Some(q) => q,
            None => String::new(),
        }
    }

    fn render_user_name(&self, user: &String, _params: &RenderParams) -> String {
        user.to_string()
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => img.to_string(),
            None => String::new(),
        }
    }

    fn render_cell_namespace(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        entry
            .title()
            .namespace_name(&params.api)
            .unwrap_or(&"UNKNOWN_NAMESPACE".to_string())
            .to_string()
    }
}

impl RenderTSV {
    pub fn new(separator: &str) -> Box<Self> {
        Box::new(Self {
            separator: separator.to_string(),
        })
    }

    fn escape_cell(&self, s: &str) -> String {
        if self.separator == "," {
            format!("\"{}\"",s.replace("\"", "\\\""))
        } else {
            s.replace("\t", " ")
        }
    }
}

//________________________________________________________________________________________________________________________

/// Renders HTML
pub struct RenderHTML {}

#[async_trait]
impl Render for RenderHTML {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        mut entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let mut rows = vec![];

        rows.push("<hr/>".to_string());
        rows.push("<script>var output_wiki='".to_string() + wiki + "';</script>");

        /*
        // TODO
        for ( auto a:platform->errors ) {
            ret += "<div class='alert alert-danger' role='alert'>" + a + "</div>" ;
        }
        */

        // Wikidata edit box?
        if params.do_output_redlinks {
            // Yeah no
        } else if wiki != "wikidatawiki" && platform.get_param_blank("wikidata_item") == "without" {
            rows.push("<div id='autolist_box' mode='creator'></div>".to_string());
            params.use_autolist = true;
            params.autolist_creator_mode = true;
        } else if wiki == "wikidatawiki" {
            rows.push("<div id='autolist_box' mode='autolist'></div>".to_string());
            params.use_autolist = true;
        } else if wiki != "wikidatawiki" && params.do_output_redlinks {
            rows.push("<div id='autolist_box' mode='creator'></div>".to_string());
            params.use_autolist = true;
            params.autolist_creator_mode = true;
        } else if wiki == "commonswiki" && entries.iter().all(|e| e.title().namespace_id() == 6) {
            // If it's Commons, and all results are files
            rows.push("<div id='autolist_box' mode='autolist'></div>".to_string());
            params.use_autolist = true;
            params.autolist_wiki_server = AUTOLIST_COMMONS.to_string();
        }

        if params.use_autolist {
            rows.push(format!(
                "<script>\nvar autolist_wiki_server='{}';\n</script>",
                params.autolist_wiki_server
            ));
        }

        // Gallery?
        let only_files = entries
            .iter()
            .any(|entry| entry.title().namespace_id() == 6);
        if only_files && (!params.use_autolist || params.autolist_wiki_server == AUTOLIST_COMMONS) {
            rows.push( "<div id='file_results' style='float:right;clear:right;' class='btn-group' data-toggle='buttons'>".to_string());
            rows.push( "<label class='btn btn-light active'><input type='radio' checked name='results_mode' value='titles' autocomplete='off' /><span tt='show_titles'></span></label>".to_string());
            rows.push( "<label class='btn btn-light'><input type='radio' name='results_mode' value='thumbnails' autocomplete='off' /><span tt='show_thumbnails'></span></label>".to_string());
            rows.push("</div>".to_string());
        }

        rows.push(format!(
            "<h2><a name='results'></a><span id='num_results' num='{}'></span></h2>",
            entries.len()
        ));

        for warning in platform.warnings()? {
            rows.push(format!(
                "<div class='alert alert-warning' style='clear:both'>{}</div>",
                warning
            ));
        }

        let header = self.get_initial_columns(&params);
        rows.push("<div style='clear:both;overflow:auto'>".to_string());
        rows.push(self.get_table_header(&header, &params));
        rows.push("<tbody>".to_string());

        let header: Vec<(String, String)> = header
            .iter()
            .map(|x| (x.to_string(), x.to_string()))
            .collect();

        let entries_len = entries.len();
        let mut output = rows.join("\n");
        entries.drain(..).for_each(|entry| {
            if params.row_number < MAX_HTML_RESULTS {
                params.row_number += 1;
                let row = self.row_from_entry(&entry, &header, &params, &platform);
                let row = self.render_html_row(&row, &header);
                output += &row;
            }
        });

        let mut rows = vec![];
        rows.push("</tbody></table></div>".to_string());

        if entries_len > MAX_HTML_RESULTS {
            rows.push( format!("<div class='alert alert-warning' style='clear:both'>Only the first {} results are shown in HTML, so as to not crash your browser; other formats will have complete results.</div>",MAX_HTML_RESULTS) );
        }

        if let Some(duration) = platform.query_time() {
            let seconds = (duration.as_millis() as f32) / 1000_f32;
            rows.push(format!(
                "<div style='font-size:8pt' id='query_length' sec='{}'></div>",
                seconds
            ));
        }
        rows.push("<script src='autolist.js'></script>".to_string());
        output += &rows.join("\n");
        let interface_language = platform.get_param_default("interface_language", "en");
        let state = platform.state();
        let html = state.get_main_page(interface_language);
        let html = html.replace(
            "<!--querystring-->",
            encode_minimal(&platform.form_parameters().to_string()).as_str(),
        );
        let mut html = html.replace("<!--output-->", &output);
        if let Some(psid) = platform.psid {
            let psid_string = format!("<span name='psid' style='display:none'>{}</span>", psid);
            html = html.replace("<!--psid-->", &psid_string);
        };

        Ok(MyResponse {
            s: html,
            content_type: ContentType::HTML,
        })
    }

    fn render_cell_title(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        self.render_wikilink(
            &entry.title(),
            &params.wiki,
            &entry.get_wikidata_label(),
            params,
            true,
            &entry.get_wikidata_description(),
            entry.redlink_count.is_some(),
        )
    }
    fn render_cell_wikidata_item(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        match entry.get_wikidata_item() {
            Some(q) => self.render_wikilink(
                &Title::new(&q, 0),
                &"wikidatawiki".to_string(),
                &None,
                params,
                false,
                &entry.get_wikidata_description(),
                entry.redlink_count.is_some(),
            ),
            None => String::new(),
        }
    }
    fn render_user_name(&self, user: &String, params: &RenderParams) -> String {
        let title = Title::new(user, 2);
        self.render_wikilink(&title, &params.wiki, &None, params, false, &None, false)
    }
    fn render_cell_image(&self, image: &Option<String>, params: &RenderParams) -> String {
        match image {
            Some(img) => {
                let thumnail_size = "120px"; // TODO
                let server_url = match params.state.get_server_url_for_wiki(&params.wiki) {
                    Ok(url) => url,
                    _ => return String::new(),
                };
                let file = self.escape_attribute(img);
                let url = format!("{}/wiki/File:{}", &server_url, &file);
                let src = format!(
                    "{}/wiki/Special:Redirect/file/{}?width={}",
                    &server_url, &file, &thumnail_size
                );
                format!("<div class='card thumbcard'><a target='_blank' href='{}'><img class='card-img thumbcard-img' src='{}' loading='lazy' /></a></div>",url,src)
            }
            None => String::new(),
        }
    }
    fn render_cell_namespace(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        let namespace_name = entry
            .title()
            .namespace_name(&params.api)
            .unwrap_or("UNKNOWN NAMESPACE")
            .to_string();
        if namespace_name.is_empty() {
            "<span tt='namespace_0'>Article</span>".to_string()
        } else {
            namespace_name
        }
    }

    fn render_cell_fileusage(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        match &entry.get_file_info() {
            Some(fi) => {
                let mut rows: Vec<String> = vec![];
                for fu in &fi.file_usage {
                    let html = "<div class='fileusage'>".to_string()
                        + &fu.wiki().to_owned()
                        + ":"
                        + &self.render_wikilink(
                            fu.title(),
                            fu.wiki(),
                            &None,
                            params,
                            false,
                            &entry.get_wikidata_description(),
                            entry.redlink_count.is_some(),
                        )
                        + "</div>";
                    rows.push(html);
                }
                rows.join("\n")
            }
            None => String::new(),
        }
    }

    fn render_coordinates(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match &entry.get_coordinates() {
            Some(coords) => {
                let lang = "en"; // TODO
                let mut url = format!(
                    "https://tools.wmflabs.org/geohack/geohack.php?language={}&params=",
                    &lang
                );
                if coords.lat < 0.0 {
                    url += &format!("{}_S_", -coords.lat);
                } else {
                    url += &format!("{}_N_", coords.lat);
                };
                if coords.lon < 0.0 {
                    url += &format!("{}_W_", -coords.lon)
                } else {
                    url += &format!("{}_E_", coords.lon)
                };
                url += "globe:earth";
                format!(
                    "<a class='smaller' target='_blank' href='{}'>{}/{}</a>",
                    url, &coords.lat, &coords.lon
                )
            }
            None => String::new(),
        }
    }

    fn render_cell_checkbox(
        &self,
        entry: &PageListEntry,
        params: &RenderParams,
        platform: &Platform,
    ) -> String {
        let mut q = String::new();
        let checked: &str;
        if params.autolist_creator_mode {
            if platform.label_exists(&entry.title().pretty().to_string()) || entry.title().pretty().contains('(') {
                checked = "";
            } else {
                checked = "checked";
            }
            q = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(since) => format!("create_item_{}_{}", &params.row_number, since.as_micros()),
                _ => String::new(),
            }
        } else {
            if params.autolist_wiki_server == AUTOLIST_COMMONS {
                q = match entry.page_id {
                    Some(id) => id.to_string(),
                    None => String::new(),
                }
            } else if params.autolist_wiki_server == AUTOLIST_WIKIDATA {
                q = entry.title().pretty().to_string();
                if q.is_empty() {
                    panic!("RenderHTML::render_cell_checkbox q is blank")
                }
                q.remove(0);
            } else {
                // TODO paranoia
            }
            checked = "checked";
        };
        format!(
            "<input type='checkbox' class='qcb' q='{}' id='autolist_checkbox_{}' {} />",
            &q, &q, &checked
        )
    }
}

impl RenderHTML {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }

    fn escape_attribute(&self, s: &str) -> String {
        FormParameters::percent_encode(s)
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
    }

    fn render_wikilink(
        &self,
        title: &Title,
        wiki: &str,
        alt_label: &Option<String>,
        params: &RenderParams,
        is_page_link: bool,
        wikidata_description: &Option<String>,
        is_redlink: bool,
    ) -> String {
        let server = match params.state.get_server_url_for_wiki(wiki) {
            Ok(url) => url,
            Err(_e) => return String::new(),
        };
        let full_title = match title.full_with_underscores(&params.api) {
            Some(ft) => ft,
            None => format!("{:?}", title),
        };
        let full_title_pretty = match title.full_pretty(&params.api) {
            Some(ft) => ft,
            None => format!("{:?}", title),
        };
        let url = server + "/wiki/" + &self.escape_attribute(&full_title);
        let label = match alt_label {
            Some(label) => label.to_string(),
            None => match is_page_link {
                true => title.pretty().to_string(),
                false => full_title_pretty,
            },
        };
        let mut ret = "<a".to_string();
        if is_redlink {
            ret += " class='redlink'";
        } else if is_page_link {
            ret += " class='pagelink'";
        }
        ret += &(" target='_blank' href='".to_string() + &url + "'>" + &label + "</a>");

        // TODO properties?
        if is_page_link && wiki == "wikidatawiki" && title.namespace_id() == 0 {
            ret += &format!("&nbsp;<small><tt>[{}]</tt></small>", title.pretty());
            match &wikidata_description {
                Some(desc) => ret += &format!("<div class='smaller'>{}</div>", &desc),
                None => {}
            }
        }
        ret
    }

    fn render_html_row(&self, row: &[String], header: &[(String, String)]) -> String {
        let mut ret = "<tr>".to_string();
        for (col_num, item) in row.iter().enumerate() {
            let header_key = match header.get(col_num) {
                Some(x) => x.0.to_string(),
                None => "UNKNOWN".to_string(),
            };
            let class_name = match header_key.as_str() {
                "number" | "page_id" | "timestamp" | "size" => "text-right text-monospace",
                "title" => "link_container",
                _ => "",
            };
            if class_name.is_empty() {
                ret += "<td>";
            } else {
                ret += "<td class='";
                ret += class_name;
                ret += "'>";
            }
            ret += &item;
            ret += "</td>";
        }
        ret += "</tr>";
        ret
    }

    fn get_table_header(&self, columns: &[&str], _params: &RenderParams) -> String {
        let mut ret = "<table class='table table-sm table-striped' id='main_table'>".to_string();
        ret += "<thead><tr>";
        let fdk = self.file_data_keys();
        for col in columns {
            let col = col.to_string();
            let x = match col.as_str() {
                "checkbox" => "<th></th>".to_string(),
                "number" => "<th class='text-right text-monospace'>#</th>".to_string(),
                "image" => "<th tt='h_image'></th>".to_string(),
                "title" => "<th class='text-nowrap' tt='h_title'></th>".to_string(),
                "page_id" => "<th class='text-nowrap' tt='h_id'></th>".to_string(),
                "namespace" => "<th class='text-nowrap' tt='h_namespace'></th>".to_string(),
                "linknumber" => "<th tt='link_number'></th>".to_string(),
                "redlink_count" => "<th tt='link_number'></th>".to_string(),
                "size" => "<th class='text-nowrap' tt='h_len'></th>".to_string(),
                "timestamp" => "<th class='text-nowrap' tt='h_touched'></th>".to_string(),
                "wikidata_item" => "<th tt='h_wikidata'></th>".to_string(),
                "coordinates" => "<th tt='h_coordinates'></th>".to_string(),
                "defaultsort" => "<th tt='h_defaultsort' style='white-space: nowrap;'></th>".to_string(),
                "disambiguation" => "<th tt='h_disambiguation'></th>".to_string(),
                "incoming_links" => "<th tt='h_incoming_links'></th>".to_string(),
                "sitelinks" => "<th tt='h_sitelinks'></th>".to_string(),
                "fileusage" => "<th tt='file_usage_data'></th>".to_string(),
                other => {
                    // File data etc.
                    if fdk.contains(&other) {
                        format!("<th tt='h_{}'></th>", &other)
                    } else {
                        format!("<th>UNKNOWN:'{}'</th>", &other)
                    }
                }
            };
            ret += &(&x.to_owned()).to_string();
        }
        ret += "</tr></thead>";
        ret
    }
}

//________________________________________________________________________________________________________________________

/// Renders JSON
pub struct RenderJSON {}

#[async_trait]
impl Render for RenderJSON {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let mut content_type = ContentType::JSON;
        if params.json_pretty {
            content_type = ContentType::Plain;
        }
        params.file_usage = params.giu || params.file_usage;
        if params.giu {
            params.json_sparse = false;
        }

        // Header
        let mut header: Vec<(&str, &str)> = vec![
            ("title", "Title"),
            ("page_id", "Page ID"),
            ("namespace", "Namespace"),
            ("size", "Size (bytes)"),
            ("timestamp", "Last change"),
        ];
        if params.show_wikidata_item {
            header.push(("wikidata_item", "Wikidata"));
        }
        let mut header: Vec<(String, String)> = header
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for col in self.get_initial_columns(&params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        let mut header: Vec<(String, String)> = header
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for col in self.get_initial_columns(&params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        if params.file_data {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k.to_string(), k.to_string())));
        }

        let value: Value = match params.json_output_compatability.as_str() {
            "quick-intersection" => self.quick_intersection(platform, entries, &params, &header),
            _ => self.cat_scan(platform, entries, &params, &header), // Default
        };

        let mut out: String = String::new();
        if !params.json_callback.is_empty() {
            out += &params.json_callback;
            out += "(";
        }

        let output = if params.json_pretty {
            ::serde_json::to_string_pretty(&value)
        } else {
            ::serde_json::to_string(&value)
        };
        match output {
            Ok(o) => out += &o,
            Err(e) => return Err(format!("JSON encoding failed: {:?}", e)),
        };

        if !params.json_callback.is_empty() {
            out += ")";
        }

        Ok(MyResponse {
            s: out,
            content_type,
        })
    }

    fn render_cell_wikidata_item(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "N/A".to_string()
    }
    fn render_user_name(&self, _user: &String, _params: &RenderParams) -> String {
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

impl RenderJSON {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }

    fn get_query_string(&self, platform: &Platform) -> String {
        "https://petscan.wmflabs.org/?".to_string() + &platform.form_parameters().to_string()
    }

    fn cat_scan(
        &self,
        platform: &Platform,
        entries: Vec<PageListEntry>,
        params: &RenderParams,
        header: &[(String, String)],
    ) -> Value {
        let entry_data: Vec<Value> = if params.json_sparse {
            entries
                .iter()
                .filter_map(|entry| {
                    Some(json!(entry.title().full_with_underscores(&params.api)?))
                })
                .collect()
        } else {
            entries.iter().map(|entry| {
                let mut o = json!({
                    "n":"page",
                    "title":entry.title().with_underscores(),
                    "id":entry.page_id.unwrap_or(0),
                    "namespace":entry.title().namespace_id(),
                    "len":entry.page_bytes.unwrap_or(0),
                    "touched":entry.get_page_timestamp().unwrap_or_else(String::new),
                    "nstext":params.api.get_canonical_namespace_name(entry.title().namespace_id()).unwrap_or("")
                });
                if let Some(q) = entry.get_wikidata_item() {
                    o["q"] = json!(q);
                    o["metadata"]["wikidata"] = json!(q);
                }
                self.add_metadata(&mut o, &entry, header);
                if params.file_data {
                    match &o["metadata"].get("fileusage") {
                        Some(_) => o["gil"] = o["metadata"]["fileusage"].to_owned(),
                        None => {}
                    }
                    self.file_data_keys().iter().for_each(|k|{
                        match &o["metadata"].get(k) {
                            Some(_) => o[k] = o["metadata"][k].to_owned(),
                            None => {}
                        }
                    });
                }
                o
            }).collect()
        };
        let seconds: f32 = match platform.query_time() {
            Some(duration) => (duration.as_millis() as f32) / (1000_f32),
            None => 0.0,
        };
        json!({"n":"result","a":{"query":self.get_query_string(platform),"querytime_sec":seconds},"*":[{"n":"combination","a":{"type":platform.get_param_default("combination","subset"),"*":entry_data}}]})
    }

    fn quick_intersection(
        &self,
        platform: &Platform,
        entries: Vec<PageListEntry>,
        params: &RenderParams,
        header: &[(String, String)],
    ) -> Value {
        let mut ret = json!({
            "namespaces":{},
            "status":"OK",
            "start":0,
            "max":entries.len()+1,
            "query":self.get_query_string(platform),
            "pagecount":entries.len(),
            "pages":[]
        });
        if let Some(duration) = platform.query_time() {
            ret["querytime"] = json!((duration.as_millis() as f32) / 1000_f32)
        }

        // Namespaces
        if let Some(namespaces) = params.api.get_site_info()["query"]["namespaces"].as_object() {
            for (k, v) in namespaces {
                if let Some(ns_local_name) = v["*"].as_str() { ret["namespaces"][k] = json!(ns_local_name) }
            }
        }

        // Entries
        if params.json_sparse {
            ret["pages"] = entries
                .iter()
                .filter_map(|entry| entry.title().full_with_underscores(&params.api))
                .collect();
        } else {
            ret["pages"] = entries
                .iter()
                .map(|entry| {
                    let mut o = json!({
                        "page_id" : entry.page_id.unwrap_or(0),
                        "page_namespace" : entry.title().namespace_id(),
                        "page_title" : entry.title().with_underscores(),
                        "page_latest" : entry.get_page_timestamp().unwrap_or_else(String::new),
                        "page_len" : entry.page_bytes.unwrap_or(0),
                        //"meta" : {}
                    });
                    if params.giu || params.file_usage {
                        if let Some(fu) = self.get_file_usage(&entry) { o["giu"] = fu }
                    }
                    self.add_metadata(&mut o, &entry, header);
                    o
                })
                .collect();
        }

        ret
    }

    fn get_file_info_value(&self, entry: &PageListEntry, key: &str) -> Option<Value> {
        match &entry.get_file_info() {
            Some(fi) => match key {
                "img_size" => fi.img_size.as_ref().map(|s| json!(s)),
                "img_width" => fi.img_width.as_ref().map(|s| json!(s)),
                "img_height" => fi.img_height.as_ref().map(|s| json!(s)),
                "img_media_type" => fi.img_media_type.as_ref().map(|s| json!(s)),
                "img_major_mime" => fi.img_major_mime.as_ref().map(|s| json!(s)),
                "img_minor_mime" => fi.img_minor_mime.as_ref().map(|s| json!(s)),
                "img_user_text" => fi.img_user_text.as_ref().map(|s| json!(s)),
                "img_timestamp" => fi.img_timestamp.as_ref().map(|s| json!(s)),
                "img_sha1" => fi.img_sha1.as_ref().map(|s| json!(s)),
                other => {
                    println!("KEY NOT FOUND:{}", &other);
                    None
                }
            },
            None => None,
        }
    }

    fn get_file_usage(&self, entry: &PageListEntry) -> Option<Value> {
        match &entry.get_file_info() {
            Some(fi) => match fi.file_usage.is_empty() {
                true => None,
                false => Some(
                    fi.file_usage
                        .iter()
                        .map(|fu| {
                            json!({
                                "ns":fu.title().namespace_id(),
                                "page":fu.title().with_underscores(),
                                "wiki":fu.wiki()
                            })
                        })
                        .collect(),
                ),
            },
            None => None,
        }
    }

    fn get_file_usage_as_string(&self, entry: &PageListEntry) -> Option<Value> {
        match &entry.get_file_info() {
            Some(fi) => match fi.file_usage.is_empty() {
                true => None,
                false => Some(json!(fi
                    .file_usage
                    .iter()
                    .map(|fu| {
                        format!(
                            "{}:{}:{}:{}",
                            fu.wiki(),
                            fu.title().namespace_id(),
                            fu.namespace_name(),
                            fu.title().with_underscores()
                        )
                    })
                    .collect::<Vec<String>>()
                    .join("|"))),
            },
            None => None,
        }
    }

    fn add_metadata(&self, o: &mut Value, entry: &PageListEntry, header: &[(String, String)]) {
        header.iter().for_each(|(head, _)| {
            let value = match head.to_string().as_str() {
                "checkbox" | "number" | "page_id" | "title" | "namespace" | "size"
                | "timestamp" => None,
                "image" => entry.get_page_image().map(|s| json!(s)),
                "linknumber" => entry.link_count.as_ref().map(|s| json!(s)),
                "wikidata" => entry.get_wikidata_item().map(|s| json!(s)),
                "defaultsort" => entry.get_defaultsort().map(|s| json!(s)),
                "disambiguation" => Some(entry.disambiguation.as_json()),
                "incoming_links" => entry.incoming_links.as_ref().map(|s| json!(s)),
                "sitelinks" => entry.sitelink_count.as_ref().map(|s| json!(s)),
                "coordinates" => match &entry.get_coordinates() {
                    Some(coord) => Some(json!(format!("{}/{}", coord.lat, coord.lon))),
                    None => None,
                },
                "fileusage" => self.get_file_usage_as_string(entry),
                other => self.get_file_info_value(entry, other),
            };
            if let Some(v) = value { o["metadata"][head] = v }
        });
    }
}

//________________________________________________________________________________________________________________________

/// Renders PagePile
pub struct RenderPagePile {}

#[async_trait]
impl Render for RenderPagePile {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let url = "https://pagepile.toolforge.org/api.php";
        let data: String = entries
            .iter()
            .map(|e| format!("{}\t{}", e.title().pretty(), e.title().namespace_id()))
            .collect::<Vec<String>>()
            .join("\n");
        let mut params: HashMap<String, String> =
            vec![("action", "create_pile_with_data"), ("wiki", wiki)]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();
        params.insert("data".to_string(), data);

        let result = match api.query_raw(url, &params, "POST").await {
            Ok(r) => r,
            Err(e) => return Err(format!("PagePile generation failed: {:?}", e)),
        };
        let json: serde_json::value::Value = match serde_json::from_str(&result) {
            Ok(j) => j,
            Err(e) => {
                return Err(format!(
                    "PagePile generation did not return valid JSON: {:?}",
                    e
                ))
            }
        };
        let pagepile_id = match json["pile"]["id"].as_u64() {
            Some(id) => id,
            None => {
                return Err(format!(
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
    fn render_user_name(&self, _user: &String, _params: &RenderParams) -> String {
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


//________________________________________________________________________________________________________________________

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
        let server = match params.state.get_server_url_for_wiki(wiki) {
            Ok(url) => url,
            Err(_e) => String::new(),
        };
        let mut kml = String::new();
        kml += r#"<?xml version="1.0" encoding="UTF-8"?>
        <kml xmlns="http://www.opengis.net/kml/2.2"><Document>"# ;

        for entry in entries {
            if let Some(coords) = &entry.get_coordinates() {
                let title = entry.title();
                let label = if let "wikidatawiki" = wiki {
                    match entry.get_wikidata_label() {
                        Some(s) => s,
                        None => title.pretty().to_string()
                    }
                } else {
                    title.pretty().to_string()
                } ;
                kml += r#"<Placemark>"# ;
                kml += format!("<name>{}</name>",self.escape_xml(&label)).as_str() ;
                if let Some(desc) = entry.get_wikidata_description() {
                    kml += format!("<description>{}</description>",self.escape_xml(&desc)).as_str() ;
                }

                kml += "<ExtendedData>";
                if let Some(q) = entry.get_wikidata_item() {
                    kml += format!("<Data name=\"q\"><value>{}</value></Data>",self.escape_xml(&q)).as_str() ;
                }

                let full_title = match title.full_with_underscores(&params.api) {
                    Some(ft) => ft,
                    None => format!("{:?}", title),
                };
                let url = format!("{}/wiki/{}",&server,&self.escape_attribute(&full_title));
                kml += format!("<Data name=\"url\"><value>{}</value></Data>",self.escape_xml(&url)).as_str();

                if let Some(img) = entry.get_page_image() {
                    let file = self.escape_attribute(&img);
                    let src = format!(
                        "{}/wiki/Special:Redirect/file/{}?width={}",
                        &server, &file, 120
                    );
                    kml += format!("<Data name=\"image\"><value>{}</value></Data>",self.escape_xml(&src)).as_str();
                }

                kml += "</ExtendedData>";

                kml += format!("<Point><coordinates>{}, {}, 0.</coordinates></Point>",coords.lon,coords.lat).as_str();
                kml += r#"</Placemark>"# ;
            }
        }

        kml += r#"</Document></kml>"# ;

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
            Some(q) => format!("[[:d:{}|]]",q),
            None => String::new(),
        }
    }

    fn render_user_name(&self, user: &String, _params: &RenderParams) -> String {
        format!("[[User:{}|]]",user)
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => format!("[[File:{}|120px|]]",img),
            None => String::new()
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

    fn escape_xml(&self, s:&str) -> String{
        s
            .replace("<","&lt;")
            .replace(">","&gt;")
            .replace('"',"&quot;")
            .replace("'","&apos;")
            .replace("&","&amp;")
    }

    fn escape_attribute(&self, s: &str) -> String {
        FormParameters::percent_encode(s)
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
    }
}


//________________________________________________________________________________________________________________________

/// Renders PlainText
pub struct RenderPlainText {}

#[async_trait]
impl Render for RenderPlainText {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse, String> {
        let params = RenderParams::new(platform, wiki).await?;
        let output = entries
            .iter()
            .filter_map(|entry|entry.title().full_pretty(&params.api))
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
            Some(q) => format!("[[:d:{}|]]",q),
            None => String::new(),
        }
    }

    fn render_user_name(&self, user: &String, _params: &RenderParams) -> String {
        format!("[[User:{}|]]",user)
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => format!("[[File:{}|120px|]]",img),
            None => String::new()
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
