use anyhow::Result;

use telegram_rutracker_bot::app::run_polling_from_env;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_rutracker_bot=info,tower_http=warn".into()),
        )
        .without_time()
        .init();

    run_polling_from_env().await
}
