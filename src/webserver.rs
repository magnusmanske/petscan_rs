use crate::app_state::AppState;
use crate::config::Config;
use crate::content_type::ContentType;
use crate::form_parameters::FormParameters;
use crate::platform::{MyResponse, Platform};
use anyhow::Result;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use url::form_urlencoded;

const MAX_POST_SIZE: usize = 1024 * 1024 * 128; // MB
static NOTFOUND: &[u8] = b"Not Found";

/// Server-side wall-clock budget for a single request. Long enough to cover
/// legitimately heavy PetScan queries (category traversals across the
/// English Wikipedia replica routinely take several minutes); short enough
/// that nothing pins a worker indefinitely if the replica or an upstream
/// API stalls. Note: orphaned `started_queries` rows whose corresponding
/// request was cancelled mid-flight are not cleaned up here; operators
/// should sweep them by `started` timestamp on a schedule.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Default)]
pub struct WebServer {
    app_state: Arc<AppState>,
    petscan_config: Arc<Config>,
    /// Static files cached at startup: URL path -> (bytes, content-type).
    static_files: Arc<HashMap<&'static str, (Bytes, &'static str)>>,
}

impl WebServer {
    pub fn new(app_state: Arc<AppState>, petscan_config: Config) -> Self {
        const STATIC: &[(&str, &str)] = &[
            ("/index.html", "text/html; charset=utf-8"),
            ("/autolist.js", "application/javascript; charset=utf-8"),
            ("/main.js", "application/javascript; charset=utf-8"),
            ("/favicon.ico", "image/x-icon"),
            ("/robots.txt", "text/plain; charset=utf-8"),
        ];
        let mut static_files = HashMap::with_capacity(STATIC.len());
        for (url_path, content_type) in STATIC {
            let disk_path = format!("html{url_path}");
            if let Ok(bytes) = std::fs::read(&disk_path) {
                static_files.insert(*url_path, (Bytes::from(bytes), *content_type));
            } else {
                tracing::warn!("Could not pre-load static file: {disk_path}");
            }
        }
        WebServer {
            app_state,
            petscan_config: Arc::new(petscan_config),
            static_files: Arc::new(static_files),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = self.start_webserver().await?;
        axum::serve(listener, self.router()).await?;
        Ok(())
    }

    /// Build the axum `Router` for this `WebServer`. Extracted from [`Self::run`]
    /// so integration tests can drive it via `tower::ServiceExt::oneshot`
    /// without binding a real TCP listener.
    pub fn router(&self) -> Router {
        Router::new()
            .fallback(any(handle))
            .layer(CorsLayer::permissive())
            .with_state(self.clone())
    }

    async fn start_webserver(&self) -> Result<TcpListener> {
        use anyhow::Context;
        let port = self.petscan_config.http_port.unwrap_or(80);
        let ip_address = self
            .petscan_config
            .http_server
            .clone()
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let ip_address: std::net::Ipv4Addr = ip_address
            .parse()
            .with_context(|| format!("Invalid http_server IP address: '{ip_address}'"))?;
        let addr = SocketAddr::from((ip_address, port));
        tracing::info!("Listening on http://{addr}");

        TcpListener::bind(addr)
            .await
            .with_context(|| format!("web_server: Cannot bind to {addr}"))
    }

    async fn process_request(&self, req: Request) -> Response {
        let (parts, body) = req.into_parts();
        let path = parts.uri.path().to_string();

        // URL GET query
        if let Some(query) = parts.uri.query()
            && !query.is_empty()
        {
            return self.process_from_query(query).await.into_response();
        }

        // POST – cap the body during streaming. `to_bytes` aborts once the
        // limit is reached; we map a body-limit error to 413 and any other
        // I/O failure to 500. The error type is opaque, so we walk the
        // source chain looking for the textual signature emitted by
        // http-body-util's `LengthLimitError` (transitively used by axum).
        if parts.method == Method::POST {
            let collected = match axum::body::to_bytes(body, MAX_POST_SIZE).await {
                Ok(b) => b,
                Err(e) => {
                    if error_chain_mentions(&e, "length limit exceeded") {
                        tracing::warn!("POST body exceeded {MAX_POST_SIZE} bytes – rejecting");
                        return (StatusCode::PAYLOAD_TOO_LARGE, "POST body too large")
                            .into_response();
                    }
                    tracing::error!("Failed to read POST body: {e}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
                        .into_response();
                }
            };
            if !collected.is_empty() {
                let query = String::from_utf8_lossy(&collected);
                return self.process_from_query(&query).await.into_response();
            }
        }

        // Fallback: Static file
        self.serve_file_path(&path)
    }

    async fn process_from_query(&self, query: &str) -> MyResponse {
        // Apply the per-request wall-clock budget. On timeout, drop the
        // in-flight `process_form` future (its RAII guards clean up state)
        // and return a 504 with a custom plaintext body.
        match tokio::time::timeout(REQUEST_TIMEOUT, self.process_form(query)).await {
            Ok(response) => response,
            Err(_elapsed) => {
                tracing::warn!(
                    timeout_secs = REQUEST_TIMEOUT.as_secs(),
                    "Request exceeded the server-side wall-clock budget; cancelling"
                );
                MyResponse {
                    s: format!(
                        "Request exceeded the server-side time budget of {} seconds.",
                        REQUEST_TIMEOUT.as_secs()
                    ),
                    content_type: ContentType::Plain,
                    status: StatusCode::GATEWAY_TIMEOUT.as_u16(),
                }
            }
        }
    }

    async fn process_form(&self, parameters: &str) -> MyResponse {
        let parameter_pairs = form_urlencoded::parse(parameters.as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let mut form_parameters = FormParameters::new_from_pairs(parameter_pairs);

        // Restart command?
        if let Some(code) = form_parameters.params.get("restart") {
            let given_code = code.to_string();
            if let Some(config_code) = self.app_state.get_restart_code()
                && given_code == config_code
            {
                self.app_state.shut_down();
            }
        }

        // In the process of shutting down?
        if self.app_state.is_shutting_down() {
            self.app_state.try_shutdown();
            return MyResponse {
                s: "Temporary maintenance".to_string(),
                content_type: ContentType::Plain,
                status: 200,
            };
        }

        // Just show the main page
        if form_parameters.params.contains_key("show_main_page") {
            let interface_language = form_parameters
                .params
                .get("interface_language")
                .map(|s| s.to_string())
                .unwrap_or_else(|| "en".to_string());
            return MyResponse {
                s: self.app_state.get_main_page(interface_language),
                content_type: ContentType::HTML,
                status: 200,
            };
        }

        // "psid" parameter? Load, and patch in, existing query
        let mut single_psid: Option<u64> = None;
        if let Some(psid) = form_parameters.params.get("psid")
            && !psid.trim().is_empty()
        {
            if form_parameters.params.len() == 1 {
                single_psid = psid.parse::<u64>().ok();
            }
            match self.app_state.get_query_from_psid(&psid.to_string()).await {
                Ok(psid_query) => {
                    let psid_params = match FormParameters::outcome_from_query(&psid_query) {
                        Ok(pp) => pp,
                        Err(e) => {
                            return self.app_state.render_error(e.to_string(), &form_parameters);
                        }
                    };
                    form_parameters.rebase(&psid_params);
                }
                Err(e) => return self.app_state.render_error(e.to_string(), &form_parameters),
            }
        }

        // No "doit" parameter, just display the HTML form with the current query
        if form_parameters
            .params
            .get("format")
            .unwrap_or(&"html".to_string())
            == "html"
            && (!form_parameters.params.contains_key("doit")
                || form_parameters.params.contains_key("norun"))
        {
            let interface_language = form_parameters
                .params
                .get("interface_language")
                .map(|s| s.to_string())
                .unwrap_or_else(|| "en".to_string());
            let html = self.app_state.get_main_page(interface_language);
            let html = html.replace("<!--querystring-->", form_parameters.to_string().as_str());
            return MyResponse {
                s: html,
                content_type: ContentType::HTML,
                status: 200,
            };
        }

        let started_query_id = match self
            .app_state
            .log_query_start(&form_parameters.to_string())
            .await
        {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!("Could not log query start: {e}\n{form_parameters}");
                0
            }
        };

        // Actually do something useful!
        // Cap the number of requests doing real work at once. If 50 are
        // already running, additional callers wait here until one finishes
        // (or the outer 30-minute wall-clock budget aborts them).
        let _request_permit = self.app_state.acquire_request_permit().await;
        // The guard increments the in-flight counter now and decrements on
        // drop — including unwind — so a panic anywhere below cannot leave
        // the counter permanently inflated.
        let _thread_guard = self.app_state.track_thread();
        let mut platform = Platform::new_from_parameters(&form_parameters, self.app_state.clone());
        Platform::profile("platform initialized", None);
        let platform_result = platform.run().await;
        match self.app_state.log_query_end(started_query_id).await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Could not log query {started_query_id} end:{e}\n{form_parameters}");
            }
        }
        Platform::profile("platform run complete", None);

        // Successful run?
        match platform_result {
            Ok(_) => {}
            Err(error) => {
                drop(platform);
                return self
                    .app_state
                    .render_error(error.to_string(), &form_parameters);
            }
        }

        // Generate and store a new PSID

        platform.psid = match single_psid {
            Some(psid) => Some(psid),
            None => match self
                .app_state
                .get_or_create_psid_for_query(&form_parameters.to_string())
                .await
            {
                Ok(psid) => Some(psid),
                Err(e) => {
                    // log_query_end was already called above after platform.run(); do not call it again
                    return self.app_state.render_error(e.to_string(), &form_parameters);
                }
            },
        };
        Platform::profile("PSID set", None);

        // Render response
        let response = match platform.get_response().await {
            Ok(response) => response,
            Err(e) => self.app_state.render_error(e.to_string(), &form_parameters),
        };
        drop(platform);
        response
    }

    /// Serve a static file from the in-memory cache populated at startup.
    /// "/" is an alias for "/index.html".
    fn serve_file_path(&self, path: &str) -> Response {
        let key = if path == "/" { "/index.html" } else { path };
        match self.static_files.get(key) {
            Some((bytes, content_type)) => Response::builder()
                .header(header::CONTENT_TYPE, *content_type)
                .body(bytes.clone().into())
                .unwrap_or_else(|_| (StatusCode::NOT_FOUND, NOTFOUND).into_response()),
            None => (StatusCode::NOT_FOUND, NOTFOUND).into_response(),
        }
    }
}

/// Axum entry point. Defers to [`WebServer::process_request`] so the handler
/// body stays a method on `WebServer` rather than a free function juggling
/// `State` everywhere.
async fn handle(State(server): State<WebServer>, req: Request) -> Response {
    server.process_request(req).await
}

/// Walks the `std::error::Error` source chain looking for a substring in any
/// node's `Display` output. Used to identify a body-limit overflow inside
/// the opaque `axum::Error` returned by `axum::body::to_bytes`.
fn error_chain_mentions(err: &(dyn std::error::Error + 'static), needle: &str) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if e.to_string().contains(needle) {
            return true;
        }
        cur = e.source();
    }
    false
}

impl IntoResponse for MyResponse {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::OK);
        let content_type = self.content_type.as_str();
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .body(self.s.into())
            .unwrap_or_else(|e| {
                tracing::error!("Failed to build HTTP response: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn my_response_into_response_maps_status_content_type_and_body() {
        let mr = MyResponse {
            s: "hello".to_string(),
            content_type: ContentType::JSON,
            status: 201,
        };
        let resp = mr.into_response();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"hello");
    }

    #[tokio::test]
    async fn my_response_into_response_falls_back_to_200_for_invalid_status() {
        let mr = MyResponse {
            s: String::new(),
            content_type: ContentType::Plain,
            status: 0,
        };
        let resp = mr.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // End-to-end Router tests via `tower::ServiceExt::oneshot`.
    //
    // These exercise the full axum dispatch pipeline (method-agnostic
    // routing, query-string vs body decoding, body size limit, CORS layer,
    // static-file fallback) without binding a real TCP listener.
    //
    // None of these need a populated `SiteMatrix`: the paths under test
    // either don't consult one (shutdown, restart, 404), or only touch
    // `is_language_rtl(lang)` which returns `false` on the default empty
    // matrix — correct LTR for these test inputs. Decoupling `SiteMatrix`
    // is deferred until a test actually asserts on its behaviour.
    // -----------------------------------------------------------------------

    use axum::body::Body;
    use axum::http::Request as AxumRequest;
    use tower::ServiceExt;

    /// Build a `WebServer` whose `main_page` body is a known marker, so
    /// assertions can pin the response body exactly.
    fn test_server(main_page: &str) -> WebServer {
        WebServer::new(
            Arc::new(AppState::for_test_with_main_page(main_page)),
            Config::default(),
        )
    }

    /// Convenience: send a request through the Router and return
    /// `(status, content-type header, body string)`. Body limit is
    /// generous so test assertions don't accidentally truncate.
    async fn send(server: &WebServer, req: AxumRequest<Body>) -> (StatusCode, String, String) {
        let resp = server.router().oneshot(req).await.expect("router dispatch");
        let status = resp.status();
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("collect body");
        (status, ct, String::from_utf8_lossy(&bytes).to_string())
    }

    #[tokio::test]
    async fn show_main_page_returns_html_with_configured_body() {
        let server = test_server("<html>HELLO-WORLD</html>");
        let req = AxumRequest::builder()
            .uri("/?show_main_page=1")
            .body(Body::empty())
            .unwrap();
        let (status, ct, body) = send(&server, req).await;
        assert_eq!(status, StatusCode::OK);
        assert!(ct.starts_with("text/html"), "content-type was {ct:?}");
        // The `<html>` tag is rewritten with `dir` + `lang` attributes; the
        // body content survives. Default `SiteMatrix` means LTR.
        assert!(
            body.contains("dir='ltr'"),
            "body missing dir attribute: {body}"
        );
        assert!(
            body.contains("lang='en'"),
            "body missing lang attribute: {body}"
        );
        assert!(body.contains("HELLO-WORLD"), "body missing marker: {body}");
    }

    #[tokio::test]
    async fn no_doit_param_falls_back_to_form_display() {
        let server = test_server("<html><!--querystring--></html>");
        // No `doit`, default format=html → form-display path. The
        // `<!--querystring-->` marker should be replaced with the current
        // querystring (here: `categories=Foo`).
        let req = AxumRequest::builder()
            .uri("/?categories=Foo")
            .body(Body::empty())
            .unwrap();
        let (status, _ct, body) = send(&server, req).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("categories=Foo"),
            "querystring not threaded into body: {body}"
        );
    }

    // NB: the `is_shutting_down() → "Temporary maintenance"` branch in
    // `process_form` cannot be exercised in-process. Once `shut_down()`
    // is called, the very next `try_shutdown()` (invoked from the
    // shutdown branch itself, or from any `ThreadGuard::drop`) will
    // `process::exit(0)` as soon as `threads_running` reaches zero —
    // killing the test runner. This exit is preserved by design (audit
    // P1 #12: the `/?restart=CODE` flow needs it to actually terminate).
    // The shutdown branch is short and self-contained; a dedicated
    // out-of-process test would be the only way to cover it.

    #[tokio::test]
    async fn unknown_static_path_returns_404() {
        let server = test_server("<html></html>");
        // No query string, GET, path not in the static cache → 404.
        let req = AxumRequest::builder()
            .uri("/this-path-does-not-exist")
            .body(Body::empty())
            .unwrap();
        let (status, _ct, body) = send(&server, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "Not Found");
    }

    #[tokio::test]
    async fn oversized_post_body_is_rejected_with_413() {
        let server = test_server("<html></html>");
        // POST a body larger than `MAX_POST_SIZE` (128 MiB). Constructing
        // 128+ MiB in memory is wasteful, so we cheat: send `Content-Length`
        // beyond the limit. axum's `to_bytes` aborts as soon as the limit
        // is exceeded by streamed bytes — we ensure that by sending exactly
        // one byte past it.
        let oversized = vec![b'x'; MAX_POST_SIZE + 1];
        let req = AxumRequest::builder()
            .method(Method::POST)
            .uri("/")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(oversized))
            .unwrap();
        let (status, _ct, body) = send(&server, req).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body, "POST body too large");
    }

    #[tokio::test]
    async fn cors_layer_advertises_permissive_origin() {
        // The `CorsLayer::permissive()` wrap should add
        // `Access-Control-Allow-Origin` to every response.
        let server = test_server("<html></html>");
        let req = AxumRequest::builder()
            .uri("/?show_main_page=1")
            .header("origin", "https://example.test")
            .body(Body::empty())
            .unwrap();
        let resp = server.router().oneshot(req).await.unwrap();
        let allow_origin = resp
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        // `permissive()` echoes back the request's `origin` or sends `*`.
        assert!(
            allow_origin == "*" || allow_origin == "https://example.test",
            "unexpected ACAO header: {allow_origin:?}"
        );
    }
}
