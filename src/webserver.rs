use crate::app_state::AppState;
use crate::content_type::ContentType;
use crate::form_parameters::FormParameters;
use crate::platform::{MyResponse, Platform};
use anyhow::Result;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use std::collections::HashMap;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use url::form_urlencoded;

const MAX_POST_SIZE: u64 = 1024 * 1024 * 128; // MB
static NOTFOUND: &[u8] = b"Not Found";
static BODY_TOO_BIG: &[u8] = b"POST body too large";

#[derive(Debug, Clone, Default)]
pub struct WebServer {
    app_state: Arc<AppState>,
    petscan_config: Arc<Value>,
    /// Static files cached at startup: URL path -> (bytes, content-type).
    static_files: Arc<HashMap<&'static str, (Bytes, &'static str)>>,
}

impl WebServer {
    pub fn new(app_state: Arc<AppState>, petscan_config: Value) -> Self {
        const STATIC: &[(&str, &str)] = &[
            ("/index.html",  "text/html; charset=utf-8"),
            ("/autolist.js", "application/javascript; charset=utf-8"),
            ("/main.js",     "application/javascript; charset=utf-8"),
            ("/favicon.ico", "image/x-icon"),
            ("/robots.txt",  "text/plain; charset=utf-8"),
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

        // We start a loop to continuously accept incoming connections
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("web_server: Cannot accept request: {e}");
                    continue;
                }
            };

            // Use an adapter to access something implementing `tokio::io` traits as if they implement
            // `hyper::rt` IO traits.
            let io = TokioIo::new(stream);
            let me = self.clone();

            // Spawn a tokio task to serve multiple connections concurrently
            tokio::task::spawn(async move {
                // Finally, we bind the incoming connection to our `hello` service
                if let Err(err) = http1::Builder::new()
                    // `service_fn` converts our function in a `Service`
                    .serve_connection(io, service_fn(|req| me.process_request(req)))
                    .await
                {
                    tracing::error!("Error serving connection: {err}");
                }
            });
        }
    }

    async fn start_webserver(&self) -> Result<TcpListener> {
        use anyhow::Context;
        // Run on IP/port
        let port = self.petscan_config["http_port"].as_u64().unwrap_or(80) as u16;
        let ip_address = self.petscan_config["http_server"]
            .as_str()
            .unwrap_or("0.0.0.0")
            .to_string();
        let ip_address: std::net::Ipv4Addr = ip_address
            .parse()
            .with_context(|| format!("Invalid http_server IP address: '{ip_address}'"))?;
        let addr = SocketAddr::from((ip_address, port));
        tracing::info!("Listening on http://{addr}");

        // We create a TcpListener and bind it to IP:port
        TcpListener::bind(addr)
            .await
            .with_context(|| format!("web_server: Cannot bind to {addr}"))
    }

    async fn process_request(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let (parts, body) = req.into_parts();
        let path = parts.uri.path().to_string();

        // URL GET query
        if let Some(query) = parts.uri.query()
            && !query.is_empty()
        {
            return self.process_from_query(query).await;
        }

        // POST – enforce body size limit during streaming via Limited
        if parts.method == Method::POST {
            let limited = Limited::new(body, MAX_POST_SIZE as usize);
            let collected = match limited.collect().await {
                Ok(c) => c,
                Err(e) => {
                    // Check whether the limit was exceeded or it is a genuine I/O error
                    if (&*e as &dyn std::error::Error).is::<http_body_util::LengthLimitError>() {
                        tracing::warn!("POST body exceeded {MAX_POST_SIZE} bytes – rejecting");
                        let mut resp = Response::new(Full::from(BODY_TOO_BIG));
                        *resp.status_mut() = hyper::StatusCode::PAYLOAD_TOO_LARGE;
                        return Ok(resp);
                    }
                    tracing::error!("Failed to read POST body: {e}");
                    let mut resp = Response::new(Full::from(b"Internal Server Error".as_ref()));
                    *resp.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
                    return Ok(resp);
                }
            };
            let query = collected.to_bytes();
            if !query.is_empty() {
                let query = String::from_utf8_lossy(&query);
                return self.process_from_query(&query).await;
            }
        }

        // Fallback: Static file
        self.serve_file_path(&path).await
    }

    async fn process_from_query(&self, query: &str) -> Result<Response<Full<Bytes>>, Infallible> {
        let ret = self.process_form(query).await;
        let response = Response::builder()
            .header(header::CONTENT_TYPE, ret.content_type.as_str())
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .body(Full::from(ret.s))
            .unwrap_or_else(|e| {
                tracing::error!("Failed to build HTTP response: {e}");
                Response::new(Full::from(b"Internal Server Error".as_ref()))
            });
        Ok(response)
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
            .get("psid")
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
        self.app_state.modify_threads_running(1);
        let mut platform = Platform::new_from_parameters(&form_parameters, self.app_state.clone());
        Platform::profile("platform initialized", None);
        let platform_result = platform.run().await;
        match self.app_state.log_query_end(started_query_id).await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Could not log query {started_query_id} end:{e}\n{form_parameters}");
            }
        }
        self.app_state.modify_threads_running(-1);
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
    async fn serve_file_path(&self, path: &str) -> Result<Response<Full<Bytes>>, Infallible> {
        let key = if path == "/" { "/index.html" } else { path };
        match self.static_files.get(key) {
            Some((bytes, content_type)) => {
                let response = Response::builder()
                    .header(header::CONTENT_TYPE, *content_type)
                    .body(Full::from(bytes.clone()))
                    .unwrap_or_else(|_| Response::new(Full::from(NOTFOUND)));
                Ok(response)
            }
            None => Self::not_found(),
        }
    }

    /// HTTP status code 404
    fn not_found() -> Result<Response<Full<Bytes>>, Infallible> {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(NOTFOUND.into())
            .unwrap_or_else(|_| Response::new(Full::from(NOTFOUND))))
    }
}
