use crate::pagelist_entry::{LinkCount, PageListEntry};
use crate::platform::*;
use crate::render_params::RenderParams;
use async_trait::async_trait;

pub static AUTOLIST_WIKIDATA: &str = "www.wikidata.org";
pub static AUTOLIST_COMMONS: &str = "commons.wikimedia.org";

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

    fn render_cell_title(&self, _entry: &PageListEntry, _params: &RenderParams) -> String;
    fn render_cell_wikidata_item(&self, _entry: &PageListEntry, _params: &RenderParams) -> String;
    fn render_user_name(&self, _user: &str, _params: &RenderParams) -> String;
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
                "page_id" => self.opt_u32(&entry.page_id),
                "namespace" => self.render_cell_namespace(entry, params),
                "size" => self.opt_u32(&entry.page_bytes),
                "timestamp" => self.opt_string(&entry.get_page_timestamp()),
                "wikidata_item" => self.render_cell_wikidata_item(entry, params),
                "image" => self.render_cell_image(&entry.get_page_image(), params),
                "number" => params.row_number().to_string(),
                "defaultsort" => self.opt_string(&entry.get_defaultsort()),
                "disambiguation" => self.opt_bool(&entry.disambiguation.as_option_bool()),
                "incoming_links" => self.opt_linkcount(&entry.incoming_links),
                "sitelinks" => self.opt_linkcount(&entry.sitelink_count()),

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
                    Some(fi) => self.render_user_name(&self.opt_string(&fi.img_user_text), params),
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
                "fileusage" => self.render_cell_fileusage(entry, params),

                _ => "<".to_string() + k + ">",
            };
            ret.push(cell);
        }
        ret
    }
}
