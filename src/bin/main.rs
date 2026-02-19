use petscan_rs::app_state::AppState;
use petscan_rs::command_line::{command_line_useage, get_petscan_config};
use petscan_rs::webserver::WebServer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let petscan_config = get_petscan_config();
    let app_state = Arc::new(AppState::new_from_config(&petscan_config).await?);

    let args = std::env::args();
    if args.len() > 1 {
        let _ = command_line_useage(app_state).await;
    } else {
        let webserver = WebServer::new(app_state, petscan_config);
        let _ = webserver.run().await;
    }
    Ok(())
}
