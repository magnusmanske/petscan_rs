#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rocket;
extern crate regex;

pub mod app_state;
pub mod datasource;
pub mod datasource_database;
pub mod form_parameters;
pub mod pagelist;
pub mod platform;

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
    // TODO check restart-code
    if state.is_shutting_down() {
        return MyResponse {
            s: "Temporary maintenance".to_string(),
            content_type: ContentType::Plain,
        };
    }
    if form_parameters.params.contains_key("show_main_page") {
        return MyResponse {
            s: state.get_main_page().to_owned(),
            content_type: ContentType::HTML,
        };
    }
    if form_parameters.params.contains_key("psid") {
        let psid = form_parameters.params.get("psid").unwrap().to_string();
        if !psid.is_empty() {
            match state.get_query_from_psid(&psid) {
                Some(psid_query) => {
                    let psid_params = FormParameters::outcome_from_query(&psid_query);
                    form_parameters.rebase(&psid_params);
                }
                None => {
                    return MyResponse {
                        s: format!(
                            "ERROR: PSID {} was requested, but not found in database",
                            psid
                        ),
                        content_type: ContentType::Plain,
                    };
                }
            }
        }
    }
    state.modify_threads_running(1);
    let mut platform = Platform::new_from_parameters(&form_parameters, &state);
    platform.run();
    platform.state.modify_threads_running(-1);

    let psid = state.get_new_psid_for_query(&form_parameters.to_string());
    println!("New PSID: {:?}", psid);

    platform.get_response()
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
        .unwrap()
        .to_string();
    let path = basedir.to_owned() + "/config.json";
    let file = File::open(&path).expect(format!("Can not open config file at {}", &path).as_str());
    let petscan_config: Value =
        serde_json::from_reader(file).expect("Can not parse JSON from config file");

    let rocket_config = Config::build(Environment::Staging)
        .address("127.0.0.1")
        .port(petscan_config["http_port"].as_u64().unwrap_or(80) as u16)
        .finalize()
        .unwrap();

    rocket::custom(rocket_config)
        .manage(AppState::new_from_config(&petscan_config))
        .mount("/", StaticFiles::from(basedir + "/html"))
        .mount("/", routes![process_form_get, process_form_post])
        //.attach(DbConn::fairing())
        .launch();
}
