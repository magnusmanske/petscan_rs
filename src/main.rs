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
use platform::Platform;
use rocket::config::{Config, Environment};
use rocket::State;
use rocket_contrib::serve::StaticFiles;
use serde_json::Value;
use std::env;
use std::fs::File;
//use mysql as my;
//use std::sync::Arc;

fn process_form(form_parameters: FormParameters, state: State<AppState>) -> String {
    // TODO check restart-code
    if state.is_shutting_down() {
        return "Temporary maintenance".to_string();
    }
    state.modify_threads_running(1);
    let mut platform = Platform::new_from_parameters(&form_parameters, state);
    platform.run();
    platform.state.modify_threads_running(-1);
    let ret = format!("{:#?}", platform.result());
    ret
}

/*
#[get("/?<form_parameters..>")]
fn process_form_get(
    form_parameters: LenientForm<FormParameters>,
    state: State<AppState>,
) -> String {
    process_form(form_parameters.into_inner(), state)
}

#[post("/", data = "<form_parameters>")]
fn process_form_post(
    form_parameters: LenientForm<FormParameters>,
    state: State<AppState>,
) -> String {
    process_form(form_parameters.into_inner(), state)
}
*/

#[get("/?")]
fn process_form_get(params: FormParameters, state: State<AppState>) -> String {
    process_form(params, state)
}

fn main() {
    let basedir = env::current_dir()
        .expect("Can't get CWD")
        .to_str()
        .unwrap()
        .to_string();
    let path = basedir.to_owned() + "/config.json";
    let file = File::open(path).expect("Can not open config file");
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
        .mount("/", routes![process_form_get]) // process_form_post
        //.attach(DbConn::fairing())
        .launch();
}
