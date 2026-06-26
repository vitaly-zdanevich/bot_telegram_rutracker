use anyhow::Result;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, X_CONTENT_TYPE_OPTIONS};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use std::path::PathBuf;
use telegram_rutracker_bot::app::{handle_vm_worker_payload, validate_vm_worker_payload};
use telegram_rutracker_bot::config::{Config, optional_env};
use telegram_rutracker_bot::downloader::{SeedConfig, TorrentDownloader};
use telegram_rutracker_bot::image_cache;
use tracing::{error, info, warn};

const DEFAULT_BIND: &str = "127.0.0.1:8080";
const VM_WORKER_BIND_ENV: &str = "VM_WORKER_BIND";
const VM_WORKER_SIGNATURE_HEADER: &str = "x-telegram-rutracker-signature";
const VM_WORKER_TIMESTAMP_HEADER: &str = "x-telegram-rutracker-timestamp";

#[derive(Clone)]
struct WorkerState {
    image_cache_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_rutracker_bot=info,tower_http=warn".into()),
        )
        .without_time()
        .init();

    let config = Config::from_env()?;
    initialize_seed_session_if_configured(&config).await?;

    let bind = optional_env(VM_WORKER_BIND_ENV).unwrap_or_else(|| DEFAULT_BIND.to_string());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = WorkerState {
        image_cache_dir: config.image_cache_dir,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/image-cache/{file}", get(image_cache_file))
        .route("/telegram", post(telegram_update))
        .with_state(state);

    tracing::info!(bind, "starting VM worker HTTP server");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn initialize_seed_session_if_configured(config: &Config) -> Result<()> {
    if !config.seed_torrents {
        return Ok(());
    }

    let root_dir = config.tmp_dir.join("seeds");
    let listen_port = config.torrent_listen_port;
    let session = TorrentDownloader::initialize_seed_session(
        SeedConfig {
            root_dir,
            listen_port,
            disk_reserve_bytes: config.seed_disk_reserve_mb.saturating_mul(1024 * 1024),
        },
        config.peer_limit,
    )
    .await?;
    info!(
        listen_addr = ?session.listen_addr(),
        "initialized persistent torrent seed session"
    );
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn image_cache_file(State(state): State<WorkerState>, Path(file): Path<String>) -> Response {
    match image_cache::read_cached_image(&state.image_cache_dir, &file).await {
        Ok(image) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, image.content_type)
            .header(CACHE_CONTROL, "public, max-age=2592000, immutable")
            .header(X_CONTENT_TYPE_OPTIONS, "nosniff")
            .body(Body::from(image.bytes))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }),
        Err(err) => {
            warn!(file, error = %err, "failed to read cached image");
            (StatusCode::NOT_FOUND, "not found").into_response()
        }
    }
}

async fn telegram_update(
    State(_state): State<WorkerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(timestamp) = header_value(&headers, VM_WORKER_TIMESTAMP_HEADER) else {
        return (StatusCode::UNAUTHORIZED, "missing timestamp").into_response();
    };
    let Some(signature) = header_value(&headers, VM_WORKER_SIGNATURE_HEADER) else {
        return (StatusCode::UNAUTHORIZED, "missing signature").into_response();
    };

    match validate_vm_worker_payload(&body, timestamp, signature) {
        Ok(()) => {
            let payload = body.to_vec();
            let timestamp = timestamp.to_string();
            let signature = signature.to_string();
            info!(payload_bytes = payload.len(), "accepted VM worker update");
            tokio::spawn(async move {
                if let Err(err) = handle_vm_worker_payload(&payload, &timestamp, &signature).await {
                    error!(error = %err, "failed to handle VM worker update");
                }
            });
            (StatusCode::OK, "{\"ok\":true}").into_response()
        }
        Err(err) => {
            warn!(error = %err, "rejected VM worker update");
            (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
        }
    }
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}
