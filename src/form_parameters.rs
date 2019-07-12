use regex::Regex;
use rocket::http::Status;
use rocket::request::{self, FromRequest, Request};
use rocket::Outcome;
use std::collections::HashMap;
use std::collections::HashSet;
use url::*;

#[derive(Debug, Clone)]
pub struct FormParameters {
    pub params: HashMap<String, String>,
    pub ns: HashSet<usize>,
}

impl<'a, 'r> FromRequest<'a, 'r> for FormParameters {
    type Error = String;

    fn from_request(request: &'a Request<'r>) -> request::Outcome<Self, Self::Error> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r#"^ns\[(\d+)\]$"#).unwrap();
        }
        // TODO IMPORTANT for parsing see https://api.rocket.rs/v0.4/rocket/request/struct.Request.html#method.uri
        match request.uri().query() {
            Some(query) => {
                /*
                let params = json!({});
                query.split("&").for_each(|s|{
                    let parts : Vec<&str> = s.split("=").collect();

                });
                */
                let parsed_url = Url::parse(&("https://127.0.0.1/?".to_string() + query)).unwrap();
                println!("{:?}", &parsed_url);
                let params: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
                println!("{:?}", &params);
                let mut ns: HashSet<usize> = HashSet::new();
                params
                    .iter()
                    .filter(|(_k, v)| *v == "1")
                    .for_each(|(k, _v)| {
                        for cap in RE.captures_iter(k) {
                            match cap[1].parse::<usize>() {
                                Ok(ns_num) => {
                                    ns.insert(ns_num);
                                }
                                _ => {}
                            }
                        }
                    });
                println!("{:?}", &ns);
                Outcome::Success(FormParameters {
                    params: params,
                    ns: ns,
                })
            }
            None => Outcome::Failure((Status::BadRequest, "No query found".to_string())),
        }
        /*
        let keys: Vec<_> = request.headers().get("x-api-key").collect();
        match keys.len() {
            0 => Outcome::Failure((Status::BadRequest, ApiKeyError::Missing)),
            1 if is_valid(keys[0]) => Outcome::Success(ApiKey(keys[0].to_string())),
            1 => Outcome::Failure((Status::BadRequest, ApiKeyError::Invalid)),
            _ => Outcome::Failure((Status::BadRequest, ApiKeyError::BadCount)),
        }
        */
    }
}
