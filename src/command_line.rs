use crate::app_state::AppState;
use crate::config::Config;
use crate::form_parameters::FormParameters;
use crate::platform::{Platform, WebResponse};
use anyhow::{Result, anyhow};
use futures::StreamExt;
use std::env;
use std::io::Write;
use std::sync::Arc;
use url::form_urlencoded;

pub async fn command_line_usage(app_state: Arc<AppState>) -> Result<()> {
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
    if let Some(psid) = form_parameters.params.get("psid")
        && !psid.trim().is_empty()
    {
        match app_state.get_query_from_psid(&psid.to_string()).await {
            Ok(psid_query) => {
                let psid_params = FormParameters::outcome_from_query(&psid_query)?;
                form_parameters.rebase(&psid_params);
            }
            Err(e) => return Err(e),
        }
    }

    let mut platform = Platform::new_from_parameters(&form_parameters, app_state.clone());

    // If `run()` fails, surface that error directly. Falling through to
    // `get_response()` would mask the real cause with a generic "No result".
    let response = match platform.run().await {
        Ok(()) => match platform.get_streaming_response().await {
            Ok(response) => response,
            Err(error) => WebResponse::Buffered(app_state.render_error(&error, &form_parameters)),
        },
        Err(error) => WebResponse::Buffered(app_state.render_error(&error, &form_parameters)),
    };
    match response {
        WebResponse::Buffered(response) => println!("{}", response.s),
        WebResponse::Streamed(response) => {
            let mut stream = response.stream;
            let mut stdout = std::io::stdout().lock();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| anyhow!("{e}"))?;
                stdout.write_all(&chunk)?;
            }
            stdout.write_all(b"\n")?;
        }
    }

    Ok(())
}

/// Names the config file; `config.json` in the working directory if unset.
const CONFIG_PATH_VAR: &str = "PETSCAN_CONFIG";

/// Loads the config file named by [`CONFIG_PATH_VAR`].
///
/// # Panics
/// Panics if the config file can not be opened or parsed.
pub fn get_petscan_config() -> Config {
    let path = env::var(CONFIG_PATH_VAR).unwrap_or_else(|_| "config.json".to_string());
    Config::from_file(path).expect("config load failed")
}
