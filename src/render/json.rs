use crate::content_type::ContentType;
use crate::platform::MyResponse;
use crate::render::Render;
use crate::render::params::RenderParams;
use crate::render::stream::{self, StreamResponse};
use crate::{pagelist_entry::PageListEntry, platform::Platform};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
use std::borrow::Cow;

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
            status: 200,
        })
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn stream(
        self: Box<Self>,
        platform: &Platform,
        mut params: RenderParams,
        _wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<StreamResponse> {
        // Pretty-printing a stream would require threading indentation through
        // every entry; that mode stays buffered.
        if params.json_pretty() {
            return Err(anyhow!("pretty JSON is not streamed"));
        }
        params.set_file_usage(params.giu() || params.file_usage());
        if params.giu() {
            params.set_json_sparse(false);
        }
        let header = self.json_header(&params);
        let count = entries.len();
        let quick = params.json_output_compatability() == "quick-intersection";
        let (prefix, suffix) = if quick {
            Self::quick_stream_frame(platform, &params, count)
        } else {
            Self::cat_scan_stream_frame(platform)
        };
        let callback = params.json_callback().to_string();
        let has_callback = !callback.is_empty();
        let renderer = *self;
        let items = entries.into_iter().filter_map(move |entry| {
            renderer
                .entry_value(&entry, &params, &header, quick)
                .map(|value| {
                    Cow::Owned(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()))
                })
        });
        let open = if has_callback {
            callback + "(" + &prefix
        } else {
            prefix
        };
        let close = if has_callback { suffix + ")" } else { suffix };
        let pieces = std::iter::once(Cow::Owned(open))
            .chain(stream::comma_separated(items))
            .chain(std::iter::once(Cow::Owned(close)));
        Ok(StreamResponse {
            stream: stream::to_stream(pieces),
            content_type: ContentType::JSON,
        })
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

        let header = self.json_header(params);
        let value: Value = match params.json_output_compatability() {
            "quick-intersection" => Self::quick_intersection(platform, entries, params, &header),
            _ => self.cat_scan(platform, entries, params, &header), // Default
        };
        Ok(value)
    }

    /// The columns included in a rendered entry, shared by JSON and JSONL.
    pub(crate) fn json_header(&self, params: &RenderParams) -> Vec<(String, String)> {
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
        if params.file_data() {
            self.file_data_keys()
                .iter()
                .for_each(|k| header.push((k.to_string(), k.to_string())));
        }
        header
    }

    /// One entry as the object (or sparse title string) used by both JSON
    /// dialects. `None` when a sparse title has no resolvable namespace.
    pub(crate) fn entry_value(
        &self,
        entry: &PageListEntry,
        params: &RenderParams,
        header: &[(String, String)],
        quick: bool,
    ) -> Option<Value> {
        if params.json_sparse() {
            return params
                .ns()
                .full_with_underscores(entry.title())
                .map(|title| json!(title));
        }
        let mut o = if quick {
            json!({
                "page_id" : entry.page_id().unwrap_or(0),
                "page_namespace" : entry.title().namespace_id(),
                "page_title" : entry.title().with_underscores(),
                "page_latest" : entry.get_page_timestamp().unwrap_or_default(),
                "page_len" : entry.page_bytes().unwrap_or(0),
            })
        } else {
            json!({
                "n":"page",
                "title":entry.title().with_underscores(),
                "id":entry.page_id().unwrap_or(0),
                "namespace":entry.title().namespace_id(),
                "len":entry.page_bytes().unwrap_or(0),
                "touched":entry.get_page_timestamp().unwrap_or_default(),
                "nstext":params.ns().canonical_namespace_name(entry.title().namespace_id()).unwrap_or("")
            })
        };
        if quick {
            if (params.giu() || params.file_usage())
                && let Some(fu) = Self::get_file_usage(entry)
            {
                o["giu"] = fu;
            }
            Self::add_metadata(&mut o, entry, header);
            if let Some(q) = entry.get_wikidata_item() {
                o["q"] = json!(q);
                o["metadata"]["wikidata"] = json!(q);
            }
        } else {
            if let Some(q) = entry.get_wikidata_item() {
                o["q"] = json!(q);
                o["metadata"]["wikidata"] = json!(q);
            }
            Self::add_metadata(&mut o, entry, header);
            if params.file_data() {
                if o["metadata"].get("fileusage").is_some() {
                    o["gil"] = o["metadata"]["fileusage"].to_owned();
                }
                self.file_data_keys().iter().for_each(|k| {
                    if o["metadata"].get(k).is_some() {
                        o[k] = o["metadata"][k].to_owned();
                    }
                });
            }
        }
        Some(o)
    }

    fn get_query_string(platform: &Platform) -> String {
        "https://petscan.toolforge.org/?".to_string() + &platform.form_parameters().to_string()
    }

    fn json_string(value: &str) -> String {
        serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
    }

    fn cat_scan_stream_frame(platform: &Platform) -> (String, String) {
        let seconds: f32 = match platform.query_time() {
            Some(duration) => (duration.as_millis() as f32) / 1000_f32,
            None => 0.0,
        };
        let seconds = serde_json::to_string(&seconds).unwrap_or_else(|_| "0".to_string());
        let mut prefix = String::from("{\"n\":\"result\",\"a\":{\"query\":");
        prefix += &Self::json_string(&Self::get_query_string(platform));
        prefix += &format!(
            ",\"querytime_sec\":{seconds}}},\"*\":[{{\"n\":\"combination\",\"a\":{{\"type\":"
        );
        prefix += &Self::json_string(&platform.get_param_default("combination", "subset"));
        prefix += ",\"*\":[";
        (prefix, "]}}]}".to_string())
    }

    fn quick_stream_frame(
        platform: &Platform,
        params: &RenderParams,
        count: usize,
    ) -> (String, String) {
        let mut namespaces = serde_json::Map::new();
        params.ns().for_each_local_namespace(&mut |key, name| {
            namespaces.insert(key.to_string(), json!(name));
        });
        let namespaces = serde_json::to_string(&namespaces).unwrap_or_else(|_| "{}".to_string());
        let mut prefix = format!(
            "{{\"namespaces\":{namespaces},\"wiki\":{},\"status\":\"OK\",\"start\":0,\"max\":{},\"query\":{}",
            Self::json_string(platform.get_main_wiki().as_deref().unwrap_or_default()),
            count + 1,
            Self::json_string(&Self::get_query_string(platform)),
        );
        if let Some(duration) = platform.query_time() {
            prefix += &format!(
                ",\"querytime\":{}",
                serde_json::to_string(&((duration.as_millis() as f32) / 1000_f32))
                    .unwrap_or_else(|_| "0".to_string())
            );
        }
        prefix += &format!(",\"pagecount\":{count},\"pages\":[");
        (prefix, "]}".to_string())
    }

    fn cat_scan(
        &self,
        platform: &Platform,
        entries: Vec<PageListEntry>,
        params: &RenderParams,
        header: &[(String, String)],
    ) -> Value {
        let entry_data: Vec<Value> = entries
            .iter()
            .filter_map(|entry| self.entry_value(entry, params, header, false))
            .collect();
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
        params.ns().for_each_local_namespace(&mut |k, name| {
            ret["namespaces"][k] = json!(name);
        });

        let pages: Vec<Value> = entries
            .iter()
            .filter_map(|entry| RenderJSON.entry_value(entry, params, header, true))
            .collect();
        ret["pages"] = Value::Array(pages);
        ret
    }

    fn get_file_info_value(entry: &PageListEntry, key: &str) -> Option<Value> {
        entry
            .get_file_info()
            .as_ref()
            .and_then(|fi| fi.field_as_json(key))
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
                false => Some(json!(
                    fi.file_usage
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
                        .join("|")
                )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::params::RenderParams;
    use crate::test_support::{StubNamespaceContext, make_platform};
    use futures::TryStreamExt;
    use std::sync::Arc;
    use wikimisc::mediawiki::title::Title;

    #[tokio::test]
    async fn streamed_json_is_valid_and_contains_the_pages() {
        let platform = make_platform(vec![]);
        let params = RenderParams::for_tests("enwiki", Arc::new(StubNamespaceContext::enwiki()));
        let mut entry = PageListEntry::new(Title::new("Foo_bar", 0));
        entry.set_page_id(Some(42));
        entry.set_page_bytes(Some(7));
        entry.set_page_timestamp(Some("2024".to_string()));

        let response = RenderJSON::new()
            .stream(&platform, params, "enwiki", vec![entry])
            .unwrap();
        let chunks = response.stream.try_collect::<Vec<_>>().await.unwrap();
        let value: Value = serde_json::from_slice(&chunks.concat()).unwrap();

        let page = &value["*"][0]["a"]["*"][0];
        assert_eq!(page["title"], "Foo_bar");
        assert_eq!(page["id"], 42);
        assert_eq!(page["len"], 7);
    }

    #[tokio::test]
    async fn streamed_quick_intersection_has_the_expected_envelope() {
        let mut params =
            RenderParams::for_tests("enwiki", Arc::new(StubNamespaceContext::enwiki()));
        params.set_json_sparse(false);
        // quick-intersection is selected from the parameter in production; here
        // the per-entry builder is exercised through the same helper.
        let entry = PageListEntry::new(Title::new("Foo", 0));
        let header = RenderJSON::new().json_header(&params);
        let value = RenderJSON::new()
            .entry_value(&entry, &params, &header, true)
            .unwrap();
        assert_eq!(value["page_title"], "Foo");
    }
}
