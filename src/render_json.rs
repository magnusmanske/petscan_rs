use crate::content_type::ContentType;
use crate::platform::MyResponse;
use crate::render::Render;
use crate::render_params::RenderParams;
use crate::{pagelist_entry::PageListEntry, platform::Platform};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;

/// Renders JSON
#[derive(Clone, Copy, Debug)]
pub struct RenderJSON;

#[async_trait]
impl Render for RenderJSON {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let content_type = if params.json_pretty() {
            ContentType::Plain
        } else {
            ContentType::JSON
        };

        let value = self.generate_json(platform, &mut params, entries).await?;

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
            Err(e) => return Err(anyhow!("JSON encoding failed: {e}")),
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

    pub async fn generate_json(
        &self,
        platform: &Platform,
        params: &mut RenderParams,
        entries: Vec<PageListEntry>,
    ) -> Result<Value> {
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
        for col in self.get_initial_columns(params) {
            if !header.iter().any(|(k, _)| col == k) && col != "number" {
                header.push((col.to_string(), col.to_string()));
            }
        }
        let mut header: Vec<(String, String)> = header
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for col in self.get_initial_columns(params) {
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
            "quick-intersection" => Self::quick_intersection(platform, entries, params, &header),
            _ => self.cat_scan(platform, entries, params, &header), // Default
        };
        Ok(value)
    }

    fn get_query_string(platform: &Platform) -> String {
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
                    "id":entry.page_id().unwrap_or(0),
                    "namespace":entry.title().namespace_id(),
                    "len":entry.page_bytes().unwrap_or(0),
                    "touched":entry.get_page_timestamp().unwrap_or_default(),
                    "nstext":params.api().get_canonical_namespace_name(entry.title().namespace_id()).unwrap_or("")
                });
                if let Some(q) = entry.get_wikidata_item() {
                    o["q"] = json!(q);
                    o["metadata"]["wikidata"] = json!(q);
                }
                Self::add_metadata(&mut o, entry, header);
                if params.file_data() {
                    if o["metadata"].get("fileusage").is_some() { o["gil"] = o["metadata"]["fileusage"].to_owned() }
                    self.file_data_keys().iter().for_each(|k|{
                        if o["metadata"].get(k).is_some() { o[k] = o["metadata"][k].to_owned() }
                    });
                }
                o
            }).collect()
        };
        let seconds: f32 = match platform.query_time() {
            Some(duration) => (duration.as_millis() as f32) / (1000_f32),
            None => 0.0,
        };
        json!({"n":"result","a":{"query":Self::get_query_string(platform),"querytime_sec":seconds},"*":[{"n":"combination","a":{"type":platform.get_param_default("combination","subset"),"*":entry_data}}]})
    }

    fn quick_intersection(
        platform: &Platform,
        entries: Vec<PageListEntry>,
        params: &RenderParams,
        header: &[(String, String)],
    ) -> Value {
        let mut ret = json!({
            "namespaces":{},
            "wiki": platform.get_main_wiki(),
            "status":"OK",
            "start":0,
            "max":entries.len()+1,
            "query":Self::get_query_string(platform),
            "pagecount":entries.len(),
            "pages":[]
        });
        if let Some(duration) = platform.query_time() {
            ret["querytime"] = json!((duration.as_millis() as f32) / 1000_f32);
        }

        // Namespaces
        if let Some(namespaces) = params.api().get_site_info()["query"]["namespaces"].as_object() {
            for (k, v) in namespaces {
                if let Some(ns_local_name) = v["*"].as_str() {
                    ret["namespaces"][k] = json!(ns_local_name);
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
                        "page_id" : entry.page_id().unwrap_or(0),
                        "page_namespace" : entry.title().namespace_id(),
                        "page_title" : entry.title().with_underscores(),
                        "page_latest" : entry.get_page_timestamp().unwrap_or_default(),
                        "page_len" : entry.page_bytes().unwrap_or(0),
                        //"meta" : {}
                    });
                    if params.giu() || params.file_usage() {
                        if let Some(fu) = Self::get_file_usage(entry) {
                            o["giu"] = fu;
                        }
                    }
                    Self::add_metadata(&mut o, entry, header);
                    if let Some(q) = entry.get_wikidata_item() {
                        o["q"] = json!(q);
                        o["metadata"]["wikidata"] = json!(q);
                    }
                    o
                })
                .collect();
        }

        ret
    }

    fn get_file_info_value(entry: &PageListEntry, key: &str) -> Option<Value> {
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
                _other => {
                    // println!("KEY NOT FOUND:{}", &other);
                    None
                }
            },
            None => None,
        }
    }

    fn get_file_usage(entry: &PageListEntry) -> Option<Value> {
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

    fn get_file_usage_as_string(entry: &PageListEntry) -> Option<Value> {
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

    fn add_metadata(o: &mut Value, entry: &PageListEntry, header: &[(String, String)]) {
        header.iter().for_each(|(head, _)| {
            let value = match head.to_string().as_str() {
                "checkbox" | "number" | "page_id" | "title" | "namespace" | "size"
                | "timestamp" => None,
                "image" => entry.get_page_image().map(|s| json!(s)),
                "linknumber" => entry.link_count().map(|s| json!(s)),
                "wikidata" => entry.get_wikidata_item().map(|s| json!(s)),
                "defaultsort" => entry.get_defaultsort().map(|s| json!(s)),
                "disambiguation" => Some(entry.disambiguation().as_json()),
                "incoming_links" => entry.incoming_links().map(|s| json!(s)),
                "sitelinks" => entry.sitelink_count().map(|s| json!(s)),
                "coordinates" => entry
                    .get_coordinates()
                    .as_ref()
                    .map(|coord| json!(format!("{}/{}", coord.lat, coord.lon))),
                "fileusage" => Self::get_file_usage_as_string(entry),
                other => Self::get_file_info_value(entry, other),
            };
            if let Some(v) = value {
                o["metadata"][head] = v;
            }
        });
    }
}
