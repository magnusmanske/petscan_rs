use regex::Regex;
use rocket::data::Outcome as DataOutcome;
use rocket::data::{FromData, Transform, Transformed};
use rocket::http::{Method, Status};
use rocket::request::{self, FromRequest};
use rocket::Outcome;
use rocket::{Data, Outcome::*, Request};
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use url::*;

static FORM_SIZE_LIMIT: u64 = 1024 * 1024 * 50; // 50MB

#[derive(Debug, Clone)]
pub struct FormParameters {
    pub params: HashMap<String, String>,
    pub ns: HashSet<usize>,
}

impl FormParameters {
    pub fn outcome_from_query(query: &str) -> Self {
        // TODO PSID IMPORTANT for parsing see https://api.rocket.rs/v0.4/rocket/request/struct.Request.html#method.uri
        lazy_static! {
            static ref RE: Regex = Regex::new(r#"^ns\[(\d+)\]$"#).unwrap();
        }
        let parsed_url = Url::parse(&("https://127.0.0.1/?".to_string() + query)).unwrap();
        let params: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
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
        FormParameters {
            params: params,
            ns: ns,
        }
    }
}

// GET
impl<'a, 'r> FromRequest<'a, 'r> for FormParameters {
    type Error = String;

    fn from_request(request: &'a Request<'r>) -> request::Outcome<Self, Self::Error> {
        match request.method() {
            // TODO Not sure if method check is really necessary
            Method::Get => {
                match request.uri().query() {
                    Some(query) => {
                        let form_params = FormParameters::outcome_from_query(query);
                        Outcome::Success(form_params)
                    }
                    None => {
                        let mut ret = FormParameters {
                            params: HashMap::new(),
                            ns: HashSet::new(),
                        };
                        ret.params
                            .insert("show_main_page".to_string(), "1".to_string());
                        Outcome::Success(ret)
                        //Outcome::Failure((Status::BadRequest, "No query found".to_string()))
                    }
                }
            }
            _ => Outcome::Failure((Status::BadRequest, "Unsupported method".to_string())),
        }
    }
}

// POST
impl<'b> FromData<'b> for FormParameters {
    type Error = String;
    type Owned = String;
    type Borrowed = str;

    fn transform(_: &Request, data: Data) -> Transform<DataOutcome<Self::Owned, Self::Error>> {
        let mut stream = data.open().take(FORM_SIZE_LIMIT);
        let mut string = String::with_capacity((FORM_SIZE_LIMIT / 2) as usize);
        let outcome = match stream.read_to_string(&mut string) {
            Ok(_) => Success(string),
            Err(e) => Failure((Status::InternalServerError, format!("{:?}", e))),
        };

        // Returning `Borrowed` here means we get `Borrowed` in `from_data`.
        Transform::Borrowed(outcome)
    }

    fn from_data(_: &Request, outcome: Transformed<'b, Self>) -> DataOutcome<Self, Self::Error> {
        let query = outcome.borrowed()?;
        Success(FormParameters::outcome_from_query(query))
    }
}
