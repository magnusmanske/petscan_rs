extern crate chrono;
extern crate reqwest;
#[macro_use]
extern crate lazy_static;
extern crate regex;
#[macro_use]
extern crate serde_json;

pub mod app_state;
pub mod datasource;
pub mod datasource_database;
pub mod form_parameters;
pub mod pagelist;
pub mod platform;
pub mod render;
pub mod wdfist;


use tokio::fs::File as TokioFile;
use tokio_util::codec::{BytesCodec, FramedRead};
use qstring::QString;
use crate::form_parameters::FormParameters;
use app_state::AppState;
use platform::{MyResponse, Platform, ContentType};
use serde_json::Value;
use std::env;
use std::fs::File;
use std::sync::Arc;
use std::{net::SocketAddr};
use hyper::{header, Body, Request, Response, Server, Error, StatusCode, Method};
use hyper::service::{make_service_fn, service_fn};
//type GenericError = Box<dyn std::error::Error + Send + Sync>;

static NOTFOUND: &[u8] = b"Not Found";

async fn process_form(parameters:&str, state: Arc<AppState>) -> MyResponse {
    let parameter_pairs = QString::from(parameters) ;
    let parameter_pairs = parameter_pairs.to_pairs() ;
    let mut form_parameters = FormParameters::new_from_pairs ( parameter_pairs ) ;

    // Restart command?
    match form_parameters.params.get("restart") {
        Some(code) => {
            let given_code = code.to_string();
            match state.config["restart-code"].as_str() {
                Some(config_code) => {
                    if given_code == config_code {
                        state.shut_down();
                    }
                }
                None => {}
            }
        }
        None => {}
    }

    // In the process of shutting down?
    if state.is_shutting_down() {
        state.try_shutdown();
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
            .unwrap_or("en".to_string());
        return MyResponse {
            s: state
                .get_main_page(interface_language.to_string())
                .to_owned(),
            content_type: ContentType::HTML,
        };
    }

    // "psid" parameter? Load, and patch in, existing query
    let mut single_psid: Option<u64> = None;
    match form_parameters.params.get("psid") {
        Some(psid) => {
            if !psid.trim().is_empty() {
                if form_parameters.params.len() == 1 {
                    single_psid = psid.parse::<u64>().ok()
                }
                match state.get_query_from_psid(&psid.to_string()).await {
                    Ok(psid_query) => {
                        let psid_params = match FormParameters::outcome_from_query(&psid_query) {
                            Ok(pp) => pp,
                            Err(e) => return state.render_error(e, &form_parameters),
                        };
                        form_parameters.rebase(&psid_params);
                    }
                    Err(e) => return state.render_error(e, &form_parameters),
                }
            }
        }
        None => {}
    }

    // No "doit" parameter, just display the HTML form with the current query
    if form_parameters
        .params
        .get("psid")
        .unwrap_or(&"html".to_string())
        == "html"
    {
        if !form_parameters.params.contains_key("doit")
            || form_parameters.params.contains_key("norun")
        {
            let interface_language = form_parameters
                .params
                .get("interface_language")
                .map(|s| s.to_string())
                .unwrap_or("en".to_string());
            let html = state.get_main_page(interface_language.to_string());
            let html = html.replace("<!--querystring-->", form_parameters.to_string().as_str());
            return MyResponse {
                s: html,
                content_type: ContentType::HTML,
            };
        }
    }

    let started_query_id = match state.log_query_start(&form_parameters.to_string()).await {
        Ok(id) => id,
        Err(e) => return state.render_error(e, &form_parameters),
    };

    // Actually do something useful!
    state.modify_threads_running(1);
    let mut platform = Platform::new_from_parameters(&form_parameters, state.clone());
    Platform::profile("platform initialized", None);
    let platform_result = platform.run().await;
    match state.log_query_end(started_query_id).await {
        _ => {} // Ignore error
    }
    state.modify_threads_running(-1);
    Platform::profile("platform run complete", None);

    // Successful run?
    match platform_result {
        Ok(_) => {}
        Err(error) => {
            drop(platform);
            return state.render_error(error, &form_parameters);
        }
    }

    // Generate and store a new PSID

    platform.psid = match single_psid {
        Some(psid) => Some(psid),
        None => match state.get_or_create_psid_for_query(&form_parameters.to_string()).await {
            Ok(psid) => Some(psid),
            Err(e) => {
                match state.log_query_end(started_query_id).await {
                    _ => {} // Ignore error
                }
                return state.render_error(e, &form_parameters);
            }
        },
    };
    Platform::profile("PSID set", None);

    // Render response
    let response = match platform.get_response().await {
        Ok(response) => response,
        Err(error) => state.render_error(error, &form_parameters),
    };
    drop(platform);
    response
}



/// HTTP status code 404
fn not_found() -> Result<Response<Body>,Error> {
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(NOTFOUND.into())
        .unwrap())
}

async fn simple_file_send(filename: &str,content_type: &str) -> Result<Response<Body>,Error> {
    // Serve a file by asynchronously reading it by chunks using tokio-util crate.
    let filename = format!("html{}",filename);
    if let Ok(file) = TokioFile::open(filename).await {
        let stream = FramedRead::new(file, BytesCodec::new());
        let body = Body::wrap_stream(stream);
        let response = Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .body(body)
        .unwrap();
        return Ok(response);
    }

    not_found()
}

async fn serve_file_path(filename:&str) -> Result<Response<Body>,Error> {
    match filename {
        "/" => simple_file_send("/index.html","text/html; charset=utf-8").await,
        "/index.html" => simple_file_send(filename,"text/html; charset=utf-8").await,
        "/autolist.js" => simple_file_send(filename,"application/javascript; charset=utf-8").await,
        "/main.js" => simple_file_send(filename,"application/javascript; charset=utf-8").await,
        "/favicon.ico" => simple_file_send(filename,"image/x-icon; charset=utf-8").await,
        "/robots.txt" => simple_file_send(filename,"text/plain; charset=utf-8").await,
        _ => not_found()
    }
}

async fn process_from_query(query:&str,app_state:Arc<AppState>) -> Result<Response<Body>,Error> {
    let ret = process_form(query,app_state).await;
    let response = Response::builder()
    .header(header::CONTENT_TYPE, ret.content_type.as_str())
    .body(Body::from(ret.s))
    .unwrap();
    return Ok(response);
}

async fn process_request(mut req: Request<Body>,app_state:Arc<AppState>) -> Result<Response<Body>,Error> {
    // URL GET query
    match req.uri().query() {
        Some(query) => {
            if !query.is_empty() {
                return process_from_query(query,app_state).await;
            }
        },
        None => {}
    } ;

    // POST
    if req.method() == Method::POST {
        let query = hyper::body::to_bytes(req.body_mut()).await.unwrap();
        if !query.is_empty() {
            let query = String::from_utf8_lossy(&query);
            return process_from_query(&query,app_state).await;
        }
    }

    // Fallback: Static file
    serve_file_path(req.uri().path()).await
}

#[tokio::main]
async fn main() -> Result<(),Error> {

    let basedir = env::current_dir()
        .expect("Can't get CWD")
        .to_str()
        .expect("Can't convert CWD to_str")
        .to_string();
    let path = basedir.to_owned() + "/config.json";
    let file = File::open(&path).expect(format!("Can not open config file at {}", &path).as_str());
    let petscan_config: Value =
        serde_json::from_reader(file).expect("Can not parse JSON from config file");

    let ip_address = petscan_config["http_server"].as_str().unwrap_or("0.0.0.0").to_string();
    let port = petscan_config["http_port"].as_u64().unwrap_or(80) as u16;    
    let app_state = Arc::new(AppState::new_from_config(&petscan_config).await) ;

    let ip_address : Vec<u8> = ip_address.split('.').map(|s|s.parse::<u8>().unwrap()).collect();
    let ip_address = std::net::Ipv4Addr::new(ip_address[0],ip_address[1],ip_address[2],ip_address[3],);
    let addr = SocketAddr::from((ip_address, port));

    let make_service = make_service_fn(move |_| {
        let app_state = app_state.clone();
        
        async {
            Ok::<_, Error>(service_fn(move |req|  {
                process_request(req,app_state.to_owned())
            }))
        }
    });
    
    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}
