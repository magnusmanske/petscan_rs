use crate::app_state::AppState;
use crate::form_parameters::FormParameters;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::env;
use std::fs::File;
use std::sync::Arc;
use url::form_urlencoded;

pub async fn command_line_useage(app_state: Arc<AppState>) -> Result<()> {
    let mut args = std::env::args();
    let _ = args.next(); // the actual command
    let argument: String = args
        .next()
        .ok_or_else(|| anyhow!("No command line argument provided"))?;

    let parameter_pairs = form_urlencoded::parse(argument.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let mut form_parameters = FormParameters::new_from_pairs(parameter_pairs);

    // Never output HTML, pick JSON instead as default
    let format: String = match form_parameters.params.get("format") {
        Some(format) => match format.as_str() {
            "html" | "" => "json".into(),
            other => other.into(),
        },
        None => "json".into(),
    };
    form_parameters.params.insert("format".into(), format);

    // Load PSID if set
    if let Some(psid) = form_parameters.params.get("psid") {
        if !psid.trim().is_empty() {
            match app_state.get_query_from_psid(&psid.to_string()).await {
                Ok(psid_query) => {
                    let psid_params = FormParameters::outcome_from_query(&psid_query)?;
                    form_parameters.rebase(&psid_params);
                }
                Err(e) => return Err(e),
            }
        }
    }

    let mut platform = Platform::new_from_parameters(&form_parameters, app_state.clone());
    let _ = platform.run().await;
    // println!("{:?}",platform.result().as_ref().unwrap().entries().read());

    let response = match platform.get_response().await {
        Ok(response) => response,
        Err(error) => app_state.render_error(error.to_string(), &form_parameters),
    };
    println!("{}", json!(response.s).as_str().unwrap_or(&response.s));

    Ok(())
}

/// # Panics
/// Panics if the config file can not be opened or parsed.
pub fn get_petscan_config() -> Value {
    let basedir = env::current_dir()
        .expect("Can't get CWD")
        .to_str()
        .expect("Can't convert CWD to_str")
        .to_string();
    let path = basedir.to_owned() + "/config.json";
    let file =
        File::open(&path).unwrap_or_else(|_| panic!("Can not open config file at {}", &path));
    let petscan_config: Value =
        serde_json::from_reader(file).expect("Can not parse JSON from config file");
    petscan_config
}
