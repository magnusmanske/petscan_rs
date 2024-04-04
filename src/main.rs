#![type_length_limit = "4276799"]

extern crate chrono;
extern crate reqwest;
#[macro_use]
extern crate lazy_static;
extern crate regex;
#[macro_use]
extern crate serde_json;

pub mod app_state;
pub mod command_line;
pub mod datasource;
pub mod datasource_database;
pub mod datasource_labels;
pub mod datasource_manual;
pub mod datasource_pagepile;
pub mod datasource_search;
pub mod datasource_sitelinks;
pub mod datasource_sparql;
pub mod datasource_wikidata;
pub mod form_parameters;
pub mod pagelist;
pub mod pagelist_entry;
pub mod platform;
pub mod render;
pub mod render_html;
pub mod render_json;
pub mod render_kml;
pub mod render_pagepile;
pub mod render_params;
pub mod render_plaintext;
pub mod render_tsv;
pub mod render_wikitext;
pub mod wdfist;
pub mod webserver;

use app_state::AppState;
use command_line::*;
use std::sync::Arc;
use webserver::WebServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let petscan_config = get_petscan_config();
    let app_state = Arc::new(AppState::new_from_config(&petscan_config).await);

    let args = std::env::args();
    if args.len() > 1 {
        let _ = command_line_useage(app_state).await;
    } else {
        let webserver = WebServer::new(app_state, petscan_config);
        let _ = webserver.run().await;
    }
    Ok(())
}
