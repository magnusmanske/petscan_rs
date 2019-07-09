#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

pub mod form_parameters;

use form_parameters::FormParameters;
use mysql as my;
use rocket::config::{Config, Environment};
use rocket::request::LenientForm;
use rocket_contrib::serve::StaticFiles;
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;

pub struct AppState {
    pub db: Arc<my::Pool>,
    pub config: Value,
}
//unsafe impl Send for AppState {}
//unsafe impl Sync for AppState {}

impl AppState {
    pub fn new_from_config(config: &Value) -> Self {
        Self {
            db: Arc::new(AppState::db_pool_from_config(config)),
            config: config.to_owned(),
        }
    }

    fn db_pool_from_config(config: &Value) -> my::Pool {
        let mut builder = my::OptsBuilder::new();
        //println!("{}", &self.params);
        builder
            .ip_or_hostname(config["host"].as_str())
            .db_name(config["schema"].as_str())
            .user(config["user"].as_str())
            .pass(config["pass"].as_str());
        builder.tcp_port(config["db_port"].as_u64().unwrap_or(3306) as u16);

        // Min 1, max 7 connections
        match my::Pool::new_manual(1, 7, builder) {
            Ok(pool) => pool,
            Err(e) => panic!("Could not initialize DB connection pool: {}", &e),
        }
    }
}

use rocket::State;

fn process_form(form_parameters: LenientForm<FormParameters>, _state: State<AppState>) -> String {
    format!(
        "Hello, {}!",
        form_parameters
            .name
            .as_ref()
            .unwrap_or(&"ANON".to_string())
            .as_str()
    )
}

#[get("/?<form_parameters..>")]
fn process_form_get(
    form_parameters: LenientForm<FormParameters>,
    state: State<AppState>,
) -> String {
    process_form(form_parameters, state)
}

#[post("/", data = "<form_parameters>")]
fn process_form_post(
    form_parameters: LenientForm<FormParameters>,
    state: State<AppState>,
) -> String {
    process_form(form_parameters, state)
}

fn main() {
    let basedir = "/Users/mm6/rust/petscan_rs/".to_string();
    let path = basedir.to_owned() + "config.json";
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
        .mount("/", StaticFiles::from(basedir + "html"))
        .mount("/", routes![process_form_get, process_form_post])
        //.attach(DbConn::fairing())
        .launch();
}
