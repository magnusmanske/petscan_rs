use crate::pagelist::PageListEntry;
use crate::platform::*;
use mediawiki::api::Api;
use rocket::http::ContentType;

//________________________________________________________________________________________________________________________

pub struct RenderParams {
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
    do_output_redlinks: bool,
    use_autolist: bool,
    api: Api,
    row_number: usize,
}

impl RenderParams {
    pub fn new(platform: &Platform, wiki: &String) -> Self {
        let mut ret = Self {
            file_data: platform.has_param("ext_image_data"),
            file_usage: platform.has_param("file_usage_data"),
            thumbnails_in_wiki_output: platform.has_param("thumbnails_in_wiki_output"),
            wdi: platform.get_param_default("wikidata_item", "no"),
            add_coordinates: platform.has_param("add_coordinates"),
            add_image: platform.has_param("add_image"),
            add_defaultsort: platform.has_param("add_defaultsort"),
            add_disambiguation: platform.has_param("add_disambiguation"),
            add_incoming_links: platform.get_param_blank("sortby") == "incoming_links".to_string(),
            show_wikidata_item: false,
            is_wikidata: wiki == "wikidatawiki",
            do_output_redlinks: platform.do_output_redlinks(),
            use_autolist: false, // Possibly set downstream
            api: platform.state().get_api_for_wiki(wiki.to_string()).unwrap(),
            row_number: 0,
        };
        ret.show_wikidata_item = ret.wdi == "any" || ret.wdi == "with";
        ret
    }
}

//________________________________________________________________________________________________________________________

pub trait Render {
    fn response(
        &self,
        _platform: &Platform,
        _wiki: &String,
        _pages: Vec<PageListEntry>,
    ) -> MyResponse {
        MyResponse {
            s: format!("Oh well"),
            content_type: ContentType::Plain,
        }
    }

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
            columns.push("namespace");
            columns.push("linknumber");
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
        if params.file_data {
            self.file_data_keys().iter().for_each(|k| columns.push(*k));
        }
        if params.file_usage {
            columns.push("fileusage");
        }
        columns
    }

    fn render_cell_title(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "IMPLEMENT THIS!".to_string()
    }

    fn render_cell_wikidata_item(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "IMPLEMENT THIS!".to_string()
    }

    fn render_user_name(&self, _user: &String, _params: &RenderParams) -> String {
        "IMPLEMENT THIS!".to_string()
    }

    fn render_cell_image(&self, _image: &Option<String>, _params: &RenderParams) -> String {
        "IMPLEMENT THIS!".to_string()
    }

    fn render_cell_namespace(&self, _entry: &PageListEntry, _params: &RenderParams) -> String {
        "IMPLEMENT THIS!".to_string()
    }

    fn opt_usize(&self, o: &Option<usize>) -> String {
        o.map(|x| x.to_string()).unwrap_or("".to_string())
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
        o.as_ref().map(|x| x.to_string()).unwrap_or("".to_string())
    }

    fn row_from_entry(
        &self,
        entry: &PageListEntry,
        header: &Vec<(String, String)>,
        params: &RenderParams,
    ) -> Vec<String> {
        let mut ret = vec![];
        for (k, _) in header {
            let cell = match k.as_str() {
                "title" => self.render_cell_title(entry, params),
                "page_id" => self.opt_usize(&entry.page_id),
                "namespace" => self.render_cell_namespace(entry, params),
                "size" => self.opt_usize(&entry.page_bytes),
                "timestamp" => self.opt_string(&entry.page_timestamp),
                "wikidata_item" => self.render_cell_wikidata_item(entry, params),
                "image" => self.render_cell_image(&entry.page_image, params),
                "number" => params.row_number.to_string(),
                "defaultsort" => self.opt_string(&entry.defaultsort),
                "disambiguation" => self.opt_bool(&entry.disambiguation),
                "incoming_links" => self.opt_usize(&entry.incoming_links),

                "img_size" => match &entry.file_info {
                    Some(fi) => self.opt_usize(&fi.img_size),
                    None => "".to_string(),
                },
                "img_width" => match &entry.file_info {
                    Some(fi) => self.opt_usize(&fi.img_width),
                    None => "".to_string(),
                },
                "img_height" => match &entry.file_info {
                    Some(fi) => self.opt_usize(&fi.img_height),
                    None => "".to_string(),
                },
                "img_media_type" => match &entry.file_info {
                    Some(fi) => self.opt_string(&fi.img_media_type),
                    None => "".to_string(),
                },
                "img_major_mime" => match &entry.file_info {
                    Some(fi) => self.opt_string(&fi.img_major_mime),
                    None => "".to_string(),
                },
                "img_minor_mime" => match &entry.file_info {
                    Some(fi) => self.opt_string(&fi.img_minor_mime),
                    None => "".to_string(),
                },
                "img_user_text" => match &entry.file_info {
                    Some(fi) => self.render_user_name(&self.opt_string(&fi.img_user_text), &params),
                    None => "".to_string(),
                },
                "img_timestamp" => match &entry.file_info {
                    Some(fi) => self.opt_string(&fi.img_timestamp),
                    None => "".to_string(),
                },
                "img_sha1" => match &entry.file_info {
                    Some(fi) => self.opt_string(&fi.img_sha1),
                    None => "".to_string(),
                },

                "checkbox" => "TODO".to_string(), // auto-creator
                "linknumber" => "TODO".to_string(),
                "coordinates" => "TODO".to_string(),
                "fileusage" => "TODO".to_string(),

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

impl Render for RenderWiki {
    fn response(
        &self,
        platform: &Platform,
        wiki: &String,
        entries: Vec<PageListEntry>,
    ) -> MyResponse {
        let mut params = RenderParams::new(platform, wiki);
        let mut rows: Vec<String> = vec![];
        rows.push("== ".to_string() + &platform.combination().to_string() + " ==");
        rows.push(
            "[https://petscan.wmflabs.org/?".to_string()
                + &platform.form_parameters().to_string()
                + " Regenerate this table].\n",
        );
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
            let row = self.row_from_entry(&entry, &header, &params);
            let row = "| ".to_string() + &row.join(" || ");
            rows.push(row);
        }

        rows.push("|}".to_string());

        MyResponse {
            s: rows.join("\n"),
            content_type: ContentType::Plain,
        }
    }

    fn render_cell_title(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        if entry.title().namespace_id() == 6 && params.thumbnails_in_wiki_output {
            "[[".to_string() + &entry.title().full_pretty(&params.api).unwrap() + "|120px|]]"
        } else {
            self.render_wikilink(&entry, &params)
        }
    }

    fn render_cell_wikidata_item(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match &entry.wikidata_item {
            Some(q) => "[[:d:".to_string() + &q + "|]]",
            None => "".to_string(),
        }
    }

    fn render_user_name(&self, user: &String, _params: &RenderParams) -> String {
        "[[User:".to_string() + user + "|]]"
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => "[[File:".to_string() + img + "|120px|]]",
            None => "".to_string(),
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
            match &entry.wikidata_label {
                Some(label) => "[[".to_string() + entry.title().pretty() + "|" + &label + "]]",
                None => "[[".to_string() + entry.title().pretty() + "]]",
            }
        } else {
            let mut ret = "[[".to_string();
            if entry.title().namespace_id() == 14 {
                ret += ":";
            }
            ret += &entry.title().full_pretty(&params.api).unwrap();
            ret += "|]]";
            ret
        }
    }
}

//________________________________________________________________________________________________________________________

/// Renders CSV and TSV
pub struct RenderTSV {
    separator: String,
}

impl Render for RenderTSV {
    fn response(
        &self,
        platform: &Platform,
        wiki: &String,
        entries: Vec<PageListEntry>,
    ) -> MyResponse {
        let mut params = RenderParams::new(platform, wiki);
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
            let row = self.row_from_entry(&entry, &header, &params);
            let row: Vec<String> = row.iter().map(|s| self.escape_cell(s)).collect();
            let row = row.join(&self.separator);
            rows.push(row);
        }

        MyResponse {
            s: rows.join("\n"),
            content_type: match self.separator.as_str() {
                "," => ContentType::parse_flexible("text/csv; charset=utf-8").unwrap(),
                "\t" => {
                    ContentType::parse_flexible("text/tab-separated-values; charset=utf-8").unwrap()
                }
                _ => ContentType::Plain, // Fallback
            },
        }
    }

    fn render_cell_title(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        entry.title().with_underscores()
    }

    fn render_cell_wikidata_item(&self, entry: &PageListEntry, _params: &RenderParams) -> String {
        match &entry.wikidata_item {
            Some(q) => q.to_string(),
            None => "".to_string(),
        }
    }

    fn render_user_name(&self, user: &String, _params: &RenderParams) -> String {
        user.to_string()
    }

    fn render_cell_image(&self, image: &Option<String>, _params: &RenderParams) -> String {
        match image {
            Some(img) => img.to_string(),
            None => "".to_string(),
        }
    }

    fn render_cell_namespace(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        entry
            .title()
            .namespace_name(&params.api)
            .unwrap_or("UNKNOWN_NAMESPACE".to_string())
    }
}

impl RenderTSV {
    pub fn new(separator: &str) -> Box<Self> {
        Box::new(Self {
            separator: separator.to_string(),
        })
    }

    // TODO properly
    fn escape_cell(&self, s: &String) -> String {
        if self.separator == "," {
            "\"".to_string() + &s.replace("\"", "\\\"") + "\""
        } else {
            s.replace("\t", " ")
        }
    }
}

//________________________________________________________________________________________________________________________

/// Renders HTML
pub struct RenderHTML {}

impl Render for RenderHTML {}

impl RenderHTML {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
