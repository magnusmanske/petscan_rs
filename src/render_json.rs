use crate::pagelist_entry::PageListEntry;
use crate::platform::*;
use crate::render::Render;
use crate::render_params::RenderParams;
use async_trait::async_trait;
use serde_json::Value;

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
        if params.json_pretty() {
            content_type = ContentType::Plain;
        }
        params.set_file_usage(params.giu() || params.file_usage());
        if params.giu() {
            params.set_json_sparse(false);
        }

        // Header
        let mut header: Vec<(&str, &str)> = vec![
            ("title", "Title"),
            ("page_id", "Page ID"),
            ("namespace", "Namespace"),
            ("size", "Size (bytes)"),
            ("timestamp", "Last change"),
        ];
        if params.show_wikidata_item() {
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
        if params.file_data() {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k.to_string(), k.to_string())));
        }

        let value: Value = match params.json_output_compatability() {
            "quick-intersection" => self.quick_intersection(platform, entries, &params, &header),
            _ => self.cat_scan(platform, entries, &params, &header), // Default
        };

        let mut out: String = String::new();
        if !params.json_callback().is_empty() {
            out += params.json_callback();
            out += "(";
        }

        let output = if params.json_pretty() {
            ::serde_json::to_string_pretty(&value)
        } else {
            ::serde_json::to_string(&value)
        };
        match output {
            Ok(o) => out += &o,
            Err(e) => return Err(format!("JSON encoding failed: {:?}", e)),
        };

        if !params.json_callback().is_empty() {
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
        let entry_data: Vec<Value> = if params.json_sparse() {
            entries
                .iter()
                .filter_map(|entry| {
                    Some(json!(entry.title().full_with_underscores(params.api())?))
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
                    "touched":entry.get_page_timestamp().unwrap_or_default(),
                    "nstext":params.api().get_canonical_namespace_name(entry.title().namespace_id()).unwrap_or("")
                });
                if let Some(q) = entry.get_wikidata_item() {
                    o["q"] = json!(q);
                    o["metadata"]["wikidata"] = json!(q);
                }
                self.add_metadata(&mut o, entry, header);
                if params.file_data() {
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
        if let Some(namespaces) = params.api().get_site_info()["query"]["namespaces"].as_object() {
            for (k, v) in namespaces {
                if let Some(ns_local_name) = v["*"].as_str() {
                    ret["namespaces"][k] = json!(ns_local_name)
                }
            }
        }

        // Entries
        if params.json_sparse() {
            ret["pages"] = entries
                .iter()
                .filter_map(|entry| entry.title().full_with_underscores(params.api()))
                .collect();
        } else {
            ret["pages"] = entries
                .iter()
                .map(|entry| {
                    let mut o = json!({
                        "page_id" : entry.page_id.unwrap_or(0),
                        "page_namespace" : entry.title().namespace_id(),
                        "page_title" : entry.title().with_underscores(),
                        "page_latest" : entry.get_page_timestamp().unwrap_or_default(),
                        "page_len" : entry.page_bytes.unwrap_or(0),
                        //"meta" : {}
                    });
                    if params.giu() || params.file_usage() {
                        if let Some(fu) = self.get_file_usage(entry) {
                            o["giu"] = fu
                        }
                    }
                    self.add_metadata(&mut o, entry, header);
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
                "coordinates" => entry
                    .get_coordinates()
                    .as_ref()
                    .map(|coord| json!(format!("{}/{}", coord.lat, coord.lon))),
                "fileusage" => self.get_file_usage_as_string(entry),
                other => self.get_file_info_value(entry, other),
            };
            if let Some(v) = value {
                o["metadata"][head] = v
            }
        });
    }
}
