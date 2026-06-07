use anyhow::Result;
use lambda_http::{Error, run, service_fn};

use telegram_rutracker_bot::app::handler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_rutracker_bot=info,tower_http=warn".into()),
        )
        .without_time()
        .init();

    run(service_fn(handler)).await
}
