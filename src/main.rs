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

use qstring::QString;
use crate::form_parameters::FormParameters;
use app_state::AppState;
use platform::{MyResponse, Platform, ContentType};
use serde_json::Value;
use std::env;
use std::fs::File;
use std::sync::Arc;
use std::{net::SocketAddr};
use hyper::{header, Body, Request, Response, Server, Error, StatusCode};
use hyper::service::{make_service_fn, service_fn};
//type GenericError = Box<dyn std::error::Error + Send + Sync>;


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
                match state.get_query_from_psid(&psid.to_string()) {
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

    let started_query_id = match state.log_query_start(&form_parameters.to_string()) {
        Ok(id) => id,
        Err(e) => return state.render_error(e, &form_parameters),
    };

    // Actually do something useful!
    state.modify_threads_running(1);
    let mut platform = Platform::new_from_parameters(&form_parameters, state.clone());
    Platform::profile("platform initialized", None);
    let platform_result = platform.run().await;
    state.log_query_end(started_query_id);
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
        None => match state.get_or_create_psid_for_query(&form_parameters.to_string()) {
            Ok(psid) => Some(psid),
            Err(e) => {
                state.log_query_end(started_query_id);
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

async fn process_request(req: Request<Body>,app_state:Arc<AppState>) -> Result<Response<Body>,Error> {
    // URL GET query
    match req.uri().query() {
        Some(query) => {
            if !query.is_empty() {
                let ret = process_form(query,app_state).await;
                let response = Response::builder()
                .header(header::CONTENT_TYPE, ret.content_type.as_str())
                .body(Body::from(ret.s))
                .unwrap();
                return Ok(response);
            }
        },
        None => {}
    } ;
    // Fallback
    Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("NOT FOUND".into())
                .unwrap())

                    /*
    let _x = process_form("test",app_state.clone());
    let query = match req.uri().query() {
        Some(q) => q,
        None => {
            //let _result = req.body().collect::<Vec<u8>>().await;
            let body = hyper::body::to_bytes(req.body_mut()).await.unwrap();
            "post"
        }
    } ;
    let body = Body::from(format!("Request:{:?}",query)) ;
    let response = Response::new(body) ;
    Ok(response)
    */


    /*
    Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    */
    //async move { Ok::<_, Error>(response) }.await
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

    let _ip_address = petscan_config["http_server"].as_str().unwrap_or("0.0.0.0").to_string();

    let port = petscan_config["http_port"].as_u64().unwrap_or(80) as u16;
    
    let app_state = Arc::new(AppState::new_from_config(&petscan_config).await) ;

    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    /*
    let make_service = make_service_fn(move |_| {
        let app_state = app_state.clone();

        async move {
            Ok::<_, Error>(service_fn(move |req: Request<Body>| {
                process_request(req,app_state)
            }))
        }
    });
    */

    let make_service = make_service_fn(move |_| {
        // Move a clone of `client` into the `service_fn`.
        //let client = client.clone();
        let app_state = app_state.clone();
        async {
            Ok::<_, Error>(service_fn(move |req|  {
                // Clone again to ensure that client outlives this closure.
                process_request(req,app_state.to_owned())
            }))
        }
    });
    
    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}
