use crate::content_type::ContentType;
use crate::platform::MyResponse;
use crate::render::Render;
use crate::render::json::RenderJSON;
use crate::render::params::RenderParams;
use crate::render::stream::{self, StreamResponse};
use crate::{pagelist_entry::PageListEntry, platform::Platform};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
use std::borrow::Cow;

/// Renders JSON
#[derive(Clone, Copy, Debug)]
pub struct RenderJSONL;

#[async_trait]
impl Render for RenderJSONL {
    async fn response(
        &self,
        platform: &Platform,
        wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<MyResponse> {
        let mut params = RenderParams::new(platform, wiki).await?;
        let content_type = ContentType::JSONL;

        let rj = RenderJSON::new();

        let value = rj.generate_json(platform, &mut params, entries).await?;

        let value: &Value = match params.json_output_compatability() {
            "quick-intersection" => &value["pages"],
            _ => &value["*"][0]["a"]["*"],
        };

        let parts = match value.as_array() {
            Some(p) => p,
            None => return Err(anyhow!("JSON value is not an array")),
        };

        let mut out: String = String::new();
        for part in parts {
            let output = ::serde_json::to_string(&part);
            match output {
                Ok(o) => out += &o,
                Err(e) => return Err(anyhow!("JSON encoding failed: {e}")),
            };
            out += "\n";
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
        _platform: &Platform,
        mut params: RenderParams,
        _wiki: &str,
        entries: Vec<PageListEntry>,
    ) -> Result<StreamResponse> {
        params.set_file_usage(params.giu() || params.file_usage());
        if params.giu() {
            params.set_json_sparse(false);
        }
        let renderer = *RenderJSON::new();
        let header = renderer.json_header(&params);
        let quick = params.json_output_compatability() == "quick-intersection";
        let items = entries.into_iter().filter_map(move |entry| {
            renderer
                .entry_value(&entry, &params, &header, quick)
                .map(|value| {
                    Cow::Owned(format!(
                        "{}\n",
                        serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
                    ))
                })
        });
        Ok(StreamResponse {
            stream: stream::to_stream(items),
            content_type: ContentType::JSONL,
        })
    }
}

impl RenderJSONL {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
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
    async fn streamed_jsonl_has_one_object_per_line() {
        let platform = make_platform(vec![]);
        let params = RenderParams::for_tests("enwiki", Arc::new(StubNamespaceContext::enwiki()));
        let entries = vec![
            PageListEntry::new(Title::new("Foo", 0)),
            PageListEntry::new(Title::new("Bar", 0)),
        ];

        let response = RenderJSONL::new()
            .stream(&platform, params, "enwiki", entries)
            .unwrap();
        let chunks = response.stream.try_collect::<Vec<_>>().await.unwrap();
        let body = String::from_utf8(chunks.concat()).unwrap();
        let lines: Vec<Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["title"], "Foo");
        assert_eq!(lines[1]["title"], "Bar");
    }
}
