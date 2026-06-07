use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use serde_json::Value;

use telegram_rutracker_bot::app::handle_worker_payload;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_rutracker_bot=info,tower_http=warn".into()),
        )
        .without_time()
        .init();

    run(service_fn(worker_handler)).await
}

async fn worker_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    handle_worker_payload(event.payload)
        .await
        .map_err(Into::into)
}
