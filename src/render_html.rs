use crate::form_parameters::FormParameters;
use crate::pagelist_entry::PageListEntry;
use crate::platform::*;
use crate::render::{Render, AUTOLIST_COMMONS, AUTOLIST_WIKIDATA};
use crate::render_params::RenderParams;
use anyhow::Result;
use async_trait::async_trait;
use htmlescape::encode_minimal;
use std::time::{SystemTime, UNIX_EPOCH};
use wikimisc::mediawiki::title::Title;

static MAX_HTML_RESULTS: usize = 10000;

/// Renders HTML
pub struct RenderHTML {}

#[async_trait]
impl Render for RenderHTML {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        mut entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
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
        if params.do_output_redlinks() {
            // Yeah no
        } else if wiki != "wikidatawiki" && platform.get_param_blank("wikidata_item") == "without" {
            rows.push("<div id='autolist_box' mode='creator'></div>".to_string());
            *params.use_autolist_mut() = true;
            *params.autolist_creator_mode_mut() = true;
        } else if wiki == "wikidatawiki" {
            rows.push("<div id='autolist_box' mode='autolist'></div>".to_string());
            *params.use_autolist_mut() = true;
        } else if wiki != "wikidatawiki" && params.do_output_redlinks() {
            rows.push("<div id='autolist_box' mode='creator'></div>".to_string());
            *params.use_autolist_mut() = true;
            *params.autolist_creator_mode_mut() = true;
        } else if wiki == "commonswiki" && entries.iter().all(|e| e.title().namespace_id() == 6) {
            // If it's Commons, and all results are files
            rows.push("<div id='autolist_box' mode='autolist'></div>".to_string());
            *params.use_autolist_mut() = true;
            params.set_autolist_wiki_server(AUTOLIST_COMMONS);
        }

        if params.use_autolist() {
            rows.push(format!(
                "<script>\nvar autolist_wiki_server='{}';\n</script>",
                params.autolist_wiki_server()
            ));
        }

        // Gallery?
        let only_files = entries
            .iter()
            .any(|entry| entry.title().namespace_id() == 6);
        if only_files
            && (!params.use_autolist() || params.autolist_wiki_server() == AUTOLIST_COMMONS)
        {
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
            if params.row_number() < MAX_HTML_RESULTS {
                *params.row_number_mut() += 1;
                let row = self.row_from_entry(&entry, &header, &params, platform);
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
            entry.title(),
            params.wiki(),
            &entry.get_wikidata_label(),
            params,
            true,
            &entry.get_wikidata_description(),
            entry.redlink_count().is_some(),
        )
    }
    fn render_cell_wikidata_item(&self, entry: &PageListEntry, params: &RenderParams) -> String {
        match entry.get_wikidata_item() {
            Some(q) => self.render_wikilink(
                &Title::new(&q, 0),
                "wikidatawiki",
                &None,
                params,
                false,
                &entry.get_wikidata_description(),
                entry.redlink_count().is_some(),
            ),
            None => String::new(),
        }
    }
    fn render_user_name(&self, user: &str, params: &RenderParams) -> String {
        let title = Title::new(user, 2);
        self.render_wikilink(&title, params.wiki(), &None, params, false, &None, false)
    }
    fn render_cell_image(&self, image: &Option<String>, params: &RenderParams) -> String {
        match image {
            Some(img) => {
                let thumnail_size = "120px"; // TODO
                let server_url = match params
                    .state()
                    .site_matrix()
                    .get_server_url_for_wiki(params.wiki())
                {
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
            .namespace_name(params.api())
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
                            entry.redlink_count().is_some(),
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
        if params.autolist_creator_mode() {
            if platform.label_exists(entry.title().pretty()) || entry.title().pretty().contains('(')
            {
                checked = "";
            } else {
                checked = "checked";
            }
            q = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(since) => format!("create_item_{}_{}", params.row_number(), since.as_micros()),
                _ => String::new(),
            }
        } else {
            if params.autolist_wiki_server() == AUTOLIST_COMMONS {
                q = match entry.page_id() {
                    Some(id) => id.to_string(),
                    None => String::new(),
                }
            } else if params.autolist_wiki_server() == AUTOLIST_WIKIDATA {
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
            .replace('\'', "&#39;")
    }

    /* trunk-ignore(clippy/too_many_arguments) */
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
        let server = match params.state().site_matrix().get_server_url_for_wiki(wiki) {
            Ok(url) => url,
            Err(_e) => return String::new(),
        };
        let full_title = match title.full_with_underscores(params.api()) {
            Some(ft) => ft,
            None => format!("{:?}", title),
        };
        let full_title_pretty = match title.full_pretty(params.api()) {
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
                "defaultsort" => {
                    "<th tt='h_defaultsort' style='white-space: nowrap;'></th>".to_string()
                }
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
            ret += &x.to_string();
        }
        ret += "</tr></thead>";
        ret
    }
}
