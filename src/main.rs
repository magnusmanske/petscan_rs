#![feature(proc_macro_hygiene, decl_macro)]

extern crate chrono;
extern crate reqwest;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rocket;
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

use crate::form_parameters::FormParameters;
use app_state::AppState;
use platform::{MyResponse, Platform};
use rocket::config::{Config, Environment};
use rocket::http::ContentType;
use rocket::State;
use rocket_contrib::serve::StaticFiles;
use serde_json::Value;
use std::env;
use std::fs::File;
//use mysql as my;
//use std::sync::Arc;

fn process_form(mut form_parameters: FormParameters, state: State<AppState>) -> MyResponse {
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
        return MyResponse {
            s: state.get_main_page().to_owned(),
            content_type: ContentType::HTML,
        };
    }

    // "psid" parameter? Load, and patch in, existing query
    match form_parameters.params.get("psid") {
        Some(psid) => {
            if !psid.trim().is_empty() {
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
            let html = state.get_main_page();
            let html = html.replace("<!--querystring-->", form_parameters.to_string().as_str());
            return MyResponse {
                s: html,
                content_type: ContentType::HTML,
            };
        }
    }

    // Actually do something useful!
    state.modify_threads_running(1);
    let mut platform = Platform::new_from_parameters(&form_parameters, &state.inner());
    match platform.run() {
        Ok(_) => {}
        Err(error) => {
            platform.state().modify_threads_running(-1);
            return state.render_error(error, &form_parameters);
        }
    }
    platform.state().modify_threads_running(-1);

    // Generate and store a new PSID
    platform.psid = match state.get_or_create_psid_for_query(&form_parameters.to_string()) {
        Ok(psid) => Some(psid),
        Err(e) => return state.render_error(e, &form_parameters),
    };

    // Render response
    match platform.get_response() {
        Ok(response) => response,
        Err(error) => state.render_error(error, &form_parameters),
    }
}

#[post("/", data = "<params>")]
fn process_form_post(params: FormParameters, state: State<AppState>) -> MyResponse {
    process_form(params, state)
}

#[get("/")]
fn process_form_get(params: FormParameters, state: State<AppState>) -> MyResponse {
    process_form(params, state)
}

fn main() {
    let basedir = env::current_dir()
        .expect("Can't get CWD")
        .to_str()
        .expect("Can't convert CWD to_str")
        .to_string();
    let path = basedir.to_owned() + "/config.json";
    let file = File::open(&path).expect(format!("Can not open config file at {}", &path).as_str());
    let petscan_config: Value =
        serde_json::from_reader(file).expect("Can not parse JSON from config file");

    let mut rocket_config = Config::build(Environment::Production);
    match petscan_config["http_server"].as_str() {
        Some(address) => rocket_config = rocket_config.address(address),
        None => {} // 0.0.0.0; default
    }
    let rocket_config = rocket_config
        .workers(32)
        .log_level(rocket::config::LoggingLevel::Normal) // Critical
        .port(petscan_config["http_port"].as_u64().unwrap_or(80) as u16)
        .finalize()
        .expect("Can't finalize rocket_config");

    rocket::custom(rocket_config)
        .manage(AppState::new_from_config(&petscan_config))
        .mount("/", StaticFiles::from(basedir + "/html"))
        .mount("/", routes![process_form_get, process_form_post])
        //.attach(DbConn::fairing())
        .launch();
}
