#![type_length_limit = "4276799"]
#![forbid(unsafe_code)]
#![warn(
    clippy::cognitive_complexity,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_link_with_quotes,
    clippy::doc_markdown,
    clippy::empty_line_after_outer_attr,
    clippy::empty_structs_with_brackets,
    clippy::float_cmp,
    clippy::float_cmp_const,
    clippy::float_equality_without_abs,
    keyword_idents,
    clippy::missing_const_for_fn,
    missing_copy_implementations,
    missing_debug_implementations,
    // clippy::missing_errors_doc,
    // clippy::missing_panics_doc,
    clippy::mod_module_files,
    non_ascii_idents,
    noop_method_call,
    // clippy::option_if_let_else,
    clippy::print_stderr,
    // clippy::print_stdout,
    clippy::semicolon_if_nothing_returned,
    clippy::unseparated_literal_suffix,
    clippy::shadow_unrelated,
    clippy::similar_names,
    clippy::suspicious_operation_groupings,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    clippy::unused_self,
    clippy::use_debug,
    clippy::used_underscore_binding,
    clippy::useless_let_if_seq,
    clippy::wildcard_dependencies,
    clippy::wildcard_imports
)]

#[macro_use]
extern crate serde_json;

pub mod app_state;
pub mod combination;
pub mod command_line;
pub mod content_type;
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
pub mod render_jsonl;
pub mod render_kml;
pub mod render_pagepile;
pub mod render_params;
pub mod render_plaintext;
pub mod render_tsv;
pub mod render_wikitext;
pub mod wdfist;
pub mod webserver;

use tracing_subscriber as _;
