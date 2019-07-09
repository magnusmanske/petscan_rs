#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

use rocket::config::{Config, Environment};
use rocket::request::LenientForm;
use rocket_contrib::serve::StaticFiles;

#[derive(FromForm)]
struct User {
    name: Option<String>,
    //account: Option<usize>,
}

fn process_form(user: LenientForm<User>) -> String {
    format!(
        "Hello, {}!",
        user.name.as_ref().unwrap_or(&"ANON".to_string()).as_str()
    )
}

#[get("/?<user..>")]
fn process_form_get(user: LenientForm<User>) -> String {
    process_form(user)
}

#[post("/", data = "<user>")]
fn process_form_post(user: LenientForm<User>) -> String {
    process_form(user)
}

fn main() {
    let config = Config::build(Environment::Staging)
        .address("127.0.0.1")
        .port(3000)
        .finalize()
        .unwrap();

    rocket::custom(config)
        .mount("/", StaticFiles::from("/Users/mm6/rust/petscan_rs/html"))
        .mount("/", routes![process_form_get, process_form_post])
        .launch();
}
