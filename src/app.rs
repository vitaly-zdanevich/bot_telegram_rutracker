use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use lambda_http::http::{Method, StatusCode};
use lambda_http::{Body, Error, IntoResponse, Request, Response};
use serde_json::Value;
use sha1::{Digest, Sha1};
use sha2::{Digest as Sha256Digest, Sha256};
use time::OffsetDateTime;
use tracing::{error, info, warn};

use crate::catalog::OfflineCatalog;
use crate::config::Config;
use crate::downloader::{
    DownloadOutcome, DownloadRequest, SeedConfig, SeedTorrentStats, TorrentDownloader,
};
use crate::image_cache;
use crate::rutracker::{
    AuthorDetails, RutrackerClient, RutrackerCredentials, SearchResult, TopicComment, TopicDetails,
    TopicFile, TopicSpoilerImages, ensure_same_topic, require_magnet, validate_forum_query,
};
use crate::telegram::{
    CallbackQuery, InlineQuery, Message, ProgressMessage, RichMessageDelivery,
    TELEGRAM_CAPTION_LIMIT_CHARS, TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS,
    TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT, TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS, Telegram, Update,
    callback_button, copy_button, countdown_status_text, html_escape, inline_keyboard,
    truncate_for_telegram, url_button,
};
use crate::telegram_safe_file_bytes;
use crate::torrent::{TorrentFile, TorrentMetadata};

const RUTRACKER_NEWS_URL: &str = "https://t.me/rutracker_news";
const RUTRACKER_UNAVAILABLE_TEXT: &str = concat!(
    "RuTracker is unavailable from this bot backend right now. ",
    "Check their official news channel for status: ",
    "<a href=\"https://t.me/rutracker_news\">@rutracker_news</a>.\n",
    "RuTracker runs on low-cost community infrastructure; please consider donating to them."
);
const WORKER_FUNCTION_NAME_ENV: &str = "WORKER_FUNCTION_NAME";
const VM_WORKER_URL_ENV: &str = "VM_WORKER_URL";
const VM_WORKER_SECRET_ENV: &str = "VM_WORKER_SECRET";
const VM_WORKER_TIMEOUT_MS_ENV: &str = "VM_WORKER_TIMEOUT_MS";
const VM_WORKER_SIGNATURE_HEADER: &str = "x-telegram-rutracker-signature";
const VM_WORKER_TIMESTAMP_HEADER: &str = "x-telegram-rutracker-timestamp";
const VM_WORKER_SIGNATURE_TTL_SECONDS: u64 = 5 * 60;
const IMAGE_CACHE_ROUTE_PREFIX: &str = "/image-cache/";
const IMAGE_CACHE_PROXY_TIMEOUT_SECONDS: u64 = 10;
const AUTHOR_RELEASES_FORUM_ID: u64 = 1538;
const FILE_LIST_METADATA_TIMEOUT_SECONDS: u64 = 30;
const FILE_LIST_METADATA_ATTEMPTS: usize = 1;
const RESOLVING_FILES_STATUS_TEXT: &str =
    "Resolving magnet metadata for file list... This can take up to 30 seconds.";

type CacheStore<K, T> = Mutex<HashMap<K, CacheEntry<T>>>;

static UPDATE_CACHE: OnceLock<CacheStore<i64, ()>> = OnceLock::new();
static SEARCH_CACHE: OnceLock<CacheStore<String, Vec<SearchResult>>> = OnceLock::new();
static TOPIC_CACHE: OnceLock<CacheStore<u64, TopicDetails>> = OnceLock::new();
static COMMENT_PAGE_CACHE: OnceLock<CacheStore<(u64, u32), TopicDetails>> = OnceLock::new();
static CATEGORIES_CACHE: OnceLock<CacheStore<String, Vec<crate::rutracker::ForumNode>>> =
    OnceLock::new();
static SUBCATEGORIES_CACHE: OnceLock<CacheStore<u64, Vec<crate::rutracker::ForumNode>>> =
    OnceLock::new();
static LATEST_CACHE: OnceLock<CacheStore<u64, Vec<SearchResult>>> = OnceLock::new();
static VIEWFORUM_LATEST_CACHE: OnceLock<CacheStore<u64, Vec<SearchResult>>> = OnceLock::new();
static METADATA_CACHE: OnceLock<CacheStore<u64, TorrentMetadata>> = OnceLock::new();
static DOWNLOAD_SELECTION_CACHE: OnceLock<CacheStore<String, DownloadSelection>> = OnceLock::new();
static LOCAL_RESULT_CACHE: OnceLock<CacheStore<u64, SearchResult>> = OnceLock::new();

const RAM_CACHE_TTL_SECONDS: u64 = 30 * 60;
const SELECTION_PAGE_SIZE: usize = 8;
const SELECTION_FILE_BUTTON_LABEL_CHARS: usize = 48;

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    created_at: u64,
}

#[derive(Clone)]
struct DownloadSelection {
    topic_id: u64,
    selected: HashSet<usize>,
    page: usize,
}

struct RichDescriptionMessage {
    text: String,
    embedded_spoiler_images: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DownloadPromptLimit {
    Unknown,
    AllFilesFit,
    SomeFilesOversized,
}

pub async fn webhook_handler(request: Request) -> Result<impl IntoResponse, Error> {
    if is_image_cache_proxy_request(&request) {
        return Ok(proxy_image_cache_request(&request).await?);
    }

    if request.method() != Method::POST {
        return Ok(response(StatusCode::OK, "ok"));
    }

    let webhook_secret = crate::config::required_env("TELEGRAM_WEBHOOK_SECRET")?;
    let actual_secret = request
        .headers()
        .get("x-telegram-bot-api-secret-token")
        .and_then(|value| value.to_str().ok());
    if actual_secret != Some(webhook_secret.as_str()) {
        warn!("rejected webhook with missing or invalid secret token");
        return Ok(response(StatusCode::UNAUTHORIZED, "unauthorized"));
    }

    let payload = match request_body_bytes(request.body()) {
        Ok(payload) => payload,
        Err(err) => {
            warn!(error = %err, "ignored invalid Telegram request body");
            return Ok(response(StatusCode::OK, "ignored"));
        }
    };
    let update = match parse_update_bytes(&payload) {
        Ok(update) => update,
        Err(err) => {
            warn!(error = %err, "ignored invalid Telegram update");
            return Ok(response(StatusCode::OK, "ignored"));
        }
    };
    if forward_to_vm_worker_if_configured(payload.clone()).await? {
        info!(
            update_id = update.update_id,
            "forwarded Telegram update to VM worker"
        );
        return Ok(response(StatusCode::OK, "ok"));
    }

    let worker_function_name = crate::config::required_env(WORKER_FUNCTION_NAME_ENV)?;
    invoke_worker(&worker_function_name, payload).await?;
    info!(
        update_id = update.update_id,
        worker_function_name, "queued Telegram update for worker"
    );

    Ok(response(StatusCode::OK, "ok"))
}

fn is_image_cache_proxy_request(request: &Request) -> bool {
    matches!(request.method(), &Method::GET | &Method::HEAD)
        && request.uri().path().starts_with(IMAGE_CACHE_ROUTE_PREFIX)
}

async fn proxy_image_cache_request(request: &Request) -> Result<Response<Body>> {
    let file_name = request
        .uri()
        .path()
        .strip_prefix(IMAGE_CACHE_ROUTE_PREFIX)
        .unwrap_or_default();
    image_cache::validate_cache_file_name(file_name)?;
    let url = image_cache_proxy_url(&crate::config::required_env(VM_WORKER_URL_ENV)?, file_name)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(IMAGE_CACHE_PROXY_TIMEOUT_SECONDS))
        .build()
        .context("failed to build image cache proxy HTTP client")?;
    let upstream_response = client
        .get(url)
        .send()
        .await
        .context("failed to fetch proxied image cache file")?;
    let status = upstream_response.status();
    let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if !status.is_success() {
        warn!(%status, file_name, "VM image cache proxy returned non-success status");
        return Ok(response(status, "not found"));
    }
    let content_type = upstream_response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.starts_with("image/"))
        .ok_or_else(|| anyhow!("proxied image cache file has no image content type"))?
        .to_string();
    let content_length = upstream_response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = Body::Binary(
        upstream_response
            .bytes()
            .await
            .context("failed to read proxied image cache file")?
            .to_vec(),
    );
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .header("cache-control", "public, max-age=2592000, immutable")
        .header("x-content-type-options", "nosniff");
    if let Some(content_length) = content_length {
        builder = builder.header("content-length", content_length);
    }
    builder
        .body(body)
        .context("failed to build image cache proxy response")
}

fn image_cache_proxy_url(vm_worker_url: &str, file_name: &str) -> Result<String> {
    image_cache::validate_cache_file_name(file_name)?;
    let mut url = url::Url::parse(vm_worker_url)
        .with_context(|| format!("VM_WORKER_URL has invalid URL {vm_worker_url:?}"))?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("VM_WORKER_URL must use http or https, got {scheme:?}"),
    }
    url.set_path(&format!("image-cache/{file_name}"));
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

/// Handles the private Lambda worker payload after the webhook Lambda has
/// already acknowledged Telegram.
pub async fn handle_worker_payload(payload: Value) -> Result<Value> {
    let update: Update = serde_json::from_value(payload).context("failed to parse worker event")?;
    let config = Config::from_env()?;
    let app = App::new(config)?;
    if let Err(err) = app.handle_update(update).await {
        error!(error = %err, "failed to handle Telegram update");
    }

    Ok(serde_json::json!({ "ok": true }))
}

/// Handles a signed update forwarded from the public Lambda webhook to the VM.
///
/// The VM endpoint returns 2xx quickly and processes the Telegram update in the
/// background, so Lambda does not retry and duplicate the same user-visible work.
pub async fn handle_vm_worker_payload(
    payload: &[u8],
    timestamp: &str,
    signature: &str,
) -> Result<Value> {
    validate_vm_worker_payload(payload, timestamp, signature)?;
    let update = parse_update_bytes(payload)?;
    let config = Config::from_env()?;
    let app = App::new(config)?;
    if let Err(err) = app.handle_update(update).await {
        error!(error = %err, "failed to handle Telegram update on VM worker");
    }

    Ok(serde_json::json!({ "ok": true }))
}

/// Verifies that a VM worker request came from the Lambda webhook dispatcher.
pub fn validate_vm_worker_payload(payload: &[u8], timestamp: &str, signature: &str) -> Result<()> {
    let secret = crate::config::required_env(VM_WORKER_SECRET_ENV)?;
    verify_vm_worker_signature(&secret, timestamp, signature, payload)
}

/// Runs the same bot app through Telegram long polling for VM-only deployments.
pub async fn run_polling_from_env() -> Result<()> {
    let config = Config::from_env()?;
    let app = App::new(config)?;
    app.run_polling().await
}

/// Returns true when the VM worker accepted the update and Lambda should not
/// invoke the fallback worker.
async fn forward_to_vm_worker_if_configured(payload: Vec<u8>) -> Result<bool> {
    let Some(url) = crate::config::optional_env(VM_WORKER_URL_ENV) else {
        return Ok(false);
    };
    let secret = crate::config::required_env(VM_WORKER_SECRET_ENV)?;
    let timeout_ms = crate::config::parse_env(VM_WORKER_TIMEOUT_MS_ENV, 1500_u64)?;
    let timestamp = now_seconds().to_string();
    let signature = vm_worker_signature(&secret, &timestamp, &payload)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .context("failed to build VM worker HTTP client")?;
    let response = client
        .post(&url)
        .header(VM_WORKER_TIMESTAMP_HEADER, timestamp)
        .header(VM_WORKER_SIGNATURE_HEADER, signature)
        .body(payload)
        .send()
        .await;

    match response {
        Ok(response) if response.status().is_success() => Ok(true),
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!(%status, body = %body, "VM worker rejected Telegram update; falling back to Lambda worker");
            Ok(false)
        }
        Err(err) => {
            warn!(error = %err, "VM worker unavailable; falling back to Lambda worker");
            Ok(false)
        }
    }
}

async fn invoke_worker(function_name: &str, payload: Vec<u8>) -> Result<(), Error> {
    invoke_worker_signed(function_name, payload).await?;
    Ok(())
}

fn vm_worker_signature(secret: &str, timestamp: &str, payload: &[u8]) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .context("failed to create VM worker HMAC")?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload);
    Ok(format!(
        "sha256={}",
        hex_lower(&mac.finalize().into_bytes())
    ))
}

fn verify_vm_worker_signature(
    secret: &str,
    timestamp: &str,
    signature: &str,
    payload: &[u8],
) -> Result<()> {
    let timestamp_seconds = timestamp
        .parse::<u64>()
        .context("VM worker timestamp is invalid")?;
    let now = now_seconds();
    if now.abs_diff(timestamp_seconds) > VM_WORKER_SIGNATURE_TTL_SECONDS {
        bail!("VM worker signature timestamp is stale");
    }
    let expected = vm_worker_signature(secret, timestamp, payload)?;
    if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
        bail!("VM worker signature is invalid");
    }
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (left, right) in left.iter().zip(right) {
        diff |= left ^ right;
    }
    diff == 0
}

// Avoid the full AWS SDK here: the webhook only needs one signed Invoke call,
// and the SDK brings a large dependency tree into the latency-sensitive binary.
async fn invoke_worker_signed(function_name: &str, payload: Vec<u8>) -> Result<()> {
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .context("AWS_REGION is required to invoke worker Lambda")?;
    let access_key = crate::config::required_env("AWS_ACCESS_KEY_ID")?;
    let secret_key = crate::config::required_env("AWS_SECRET_ACCESS_KEY")?;
    let session_token = crate::config::optional_env("AWS_SESSION_TOKEN");
    let host = format!("lambda.{region}.amazonaws.com");
    let canonical_uri = format!(
        "/2015-03-31/functions/{}/invocations",
        urlencoding::encode(function_name)
    );
    let url = format!("https://{host}{canonical_uri}");
    let payload_hash = hex_lower(&Sha256::digest(&payload));
    let now = OffsetDateTime::now_utc();
    let date = format!(
        "{:04}{:02}{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    );
    let amz_date = format!(
        "{date}T{:02}{:02}{:02}Z",
        now.hour(),
        now.minute(),
        now.second()
    );

    let mut headers = vec![
        ("content-type", "application/json".to_string()),
        ("host", host.clone()),
        ("x-amz-content-sha256", payload_hash.clone()),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-invocation-type", "Event".to_string()),
    ];
    if let Some(token) = session_token.as_ref() {
        headers.push(("x-amz-security-token", token.clone()));
    }
    headers.sort_by_key(|(name, _)| *name);
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect::<String>();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request =
        format!("POST\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let credential_scope = format!("{date}/{region}/lambda/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        hex_lower(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signing_key = aws_v4_signing_key(&secret_key, &date, &region, "lambda")?;
    let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes())?);
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let client = reqwest::Client::new();
    let mut request = client
        .post(url)
        .header("content-type", "application/json")
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("x-amz-invocation-type", "Event")
        .header("authorization", authorization);
    if let Some(token) = session_token {
        request = request.header("x-amz-security-token", token);
    }
    let response = request
        .body(payload)
        .send()
        .await
        .context("failed to invoke worker Lambda")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("worker Lambda invoke failed with {status}: {body}");
    }
    Ok(())
}

fn aws_v4_signing_key(
    secret_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Result<Vec<u8>> {
    let date_key = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes())?;
    let region_key = hmac_sha256(&date_key, region.as_bytes())?;
    let service_key = hmac_sha256(&region_key, service.as_bytes())?;
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).context("failed to create HMAC")?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn response(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::Text(body.to_string()))
        .expect("static response is valid")
}

fn request_body_bytes(body: &Body) -> Result<Vec<u8>> {
    match body {
        Body::Text(text) => Ok(text.as_bytes().to_vec()),
        Body::Binary(bytes) => Ok(bytes.clone()),
        Body::Empty => bail!("empty body"),
        _ => bail!("unsupported request body type"),
    }
}

fn parse_update_bytes(bytes: &[u8]) -> Result<Update> {
    serde_json::from_slice(bytes).context("failed to parse Telegram update")
}

struct App {
    config: Config,
    telegram: Telegram,
    rutracker: RutrackerClient,
    offline_catalog: Option<OfflineCatalog>,
}

impl App {
    fn new(config: Config) -> Result<Self> {
        let telegram = Telegram::new(
            config.telegram_bot_token.clone(),
            config.telegram_api_base_url.clone(),
        );
        let rutracker_credentials = match (
            config.rutracker_username.as_deref(),
            config.rutracker_password.as_deref(),
        ) {
            (Some(username), Some(password)) => {
                Some(RutrackerCredentials::new(username, password)?)
            }
            _ => None,
        };
        let rutracker = RutrackerClient::new(
            &config.rutracker_base_urls,
            config.rutracker_cookie.as_deref(),
            rutracker_credentials,
            config.http_timeout_seconds,
            config.http_max_attempts,
        )?;
        let offline_catalog = config.rutracker_catalog_path.as_ref().map(|path| {
            OfflineCatalog::new(
                path.clone(),
                config
                    .rutracker_base_urls
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "https://rutracker.org/forum".to_string()),
            )
        });
        Ok(Self {
            config,
            telegram,
            rutracker,
            offline_catalog,
        })
    }

    async fn handle_update(&self, update: Update) -> Result<()> {
        if !mark_update_seen(update.update_id) {
            info!(
                update_id = update.update_id,
                "ignored duplicate Telegram update"
            );
            return Ok(());
        }

        if let Some(callback) = update.callback_query {
            return self.handle_callback(callback).await;
        }
        if let Some(inline_query) = update.inline_query {
            return self.handle_inline_query(inline_query).await;
        }
        if let Some(message) = update.message {
            return self.handle_message(message).await;
        }
        info!(update_id = update.update_id, "ignored unsupported update");
        Ok(())
    }

    async fn run_polling(&self) -> Result<()> {
        self.telegram
            .delete_webhook()
            .await
            .context("failed to delete Telegram webhook before polling")?;
        info!("started Telegram long polling");
        let mut offset = None;
        loop {
            match self.telegram.get_updates(offset, 50).await {
                Ok(updates) => {
                    for update in updates {
                        offset = Some(update.update_id + 1);
                        if let Err(err) = self.handle_update(update).await {
                            error!(error = %err, "failed to handle Telegram update");
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Telegram long polling failed");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, message: Message) -> Result<()> {
        let chat_id = message.chat.id;
        if !self.is_allowed_user(message.from.as_ref().map(|user| user.id)) {
            warn!(
                chat_id,
                user_id = message.from.as_ref().map(|user| user.id),
                "ignored unauthorized message"
            );
            return Ok(());
        }

        let Some(text) = message
            .text
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(());
        };

        if is_help_command(text) {
            self.telegram
                .send_message(chat_id, &help_text(self.config.max_file_mb), None)
                .await?;
            return Ok(());
        }
        if is_stat_command(text) {
            return self.handle_stat(chat_id).await;
        }
        if is_author_command(text) {
            return self.handle_author(chat_id).await;
        }
        if let Some(query) = text.strip_prefix("c ").or_else(|| text.strip_prefix("C ")) {
            return self.handle_category_query(chat_id, query.trim()).await;
        }
        if telegram_command_name(text).is_some() {
            self.telegram
                .send_message(
                    chat_id,
                    "Unknown command. Send /help, /author, /stat, or a RuTracker search string.",
                    None,
                )
                .await?;
            return Ok(());
        }

        self.run_search(chat_id, text, None).await
    }

    async fn handle_callback(&self, callback: CallbackQuery) -> Result<()> {
        if !self.is_allowed_user(Some(callback.from.id)) {
            self.telegram
                .answer_callback(&callback.id, "This bot is private.")
                .await?;
            return Ok(());
        }

        let chat_id = callback
            .message
            .as_ref()
            .map(|message| message.chat.id)
            .unwrap_or(callback.from.id);
        let Some(data) = callback.data.as_deref() else {
            return Ok(());
        };

        let callback_message = callback.message.as_ref().map(|message| ProgressMessage {
            chat_id: message.chat.id,
            message_id: message.message_id,
        });
        let callback_reply_markup = callback
            .message
            .as_ref()
            .and_then(|message| message.reply_markup.clone());
        let callback_text = callback
            .message
            .as_ref()
            .and_then(|message| message.text.clone());

        if let Some(topic_id) = data.strip_prefix("dl:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Choose download mode...")
                .await?;
            return self
                .handle_download_prompt(
                    chat_id,
                    topic_id,
                    callback_message,
                    callback_reply_markup,
                    callback_text,
                )
                .await;
        }
        if let Some(topic_id) = data.strip_prefix("dlall:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Starting download...")
                .await?;
            return self.handle_download(chat_id, topic_id, None).await;
        }
        if let Some(topic_id) = data.strip_prefix("sel:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Loading file list...")
                .await?;
            return self
                .handle_select_files(chat_id, topic_id, callback_message)
                .await;
        }
        if let Some((key, page)) = parse_selection_page_callback(data) {
            self.telegram
                .answer_callback(&callback.id, "Loading page...")
                .await?;
            return self
                .handle_selection_page(chat_id, &key, page, callback_message)
                .await;
        }
        if let Some((key, index)) = parse_selection_toggle_callback(data) {
            self.telegram
                .answer_callback(&callback.id, "Updated selection.")
                .await?;
            return self
                .handle_selection_toggle(chat_id, &key, index, callback_message)
                .await;
        }
        if let Some(key) = data.strip_prefix("go:") {
            self.telegram
                .answer_callback(&callback.id, "Starting selected download...")
                .await?;
            return self.handle_selected_download(chat_id, key).await;
        }
        if let Some((wrapper_topic_id, release_topic_id)) = parse_author_description_callback(data)
        {
            self.telegram
                .answer_callback(&callback.id, "Loading description...")
                .await?;
            return self
                .handle_author_description(
                    chat_id,
                    wrapper_topic_id,
                    release_topic_id,
                    callback_message,
                    callback_reply_markup,
                )
                .await;
        }
        if let Some(topic_id) = data.strip_prefix("desc:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Loading description...")
                .await?;
            return self
                .handle_description(chat_id, topic_id, callback_message, callback_reply_markup)
                .await;
        }
        if let Some(topic_id) = data.strip_prefix("files:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Loading files...")
                .await?;
            return self
                .handle_files(
                    chat_id,
                    topic_id,
                    callback_message,
                    callback_reply_markup,
                    callback_text,
                )
                .await;
        }
        if let Some(topic_id) = data.strip_prefix("mag:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Loading magnet...")
                .await?;
            return self.handle_magnet(chat_id, topic_id).await;
        }
        if let Some((topic_id, page, start_comment_index)) = parse_comments_callback(data) {
            self.telegram
                .answer_callback(&callback.id, "Loading comments...")
                .await?;
            return self
                .send_comments_page(
                    chat_id,
                    topic_id,
                    page,
                    start_comment_index,
                    callback_message,
                )
                .await;
        }
        if let Some(forum_id) = data.strip_prefix("cat:").and_then(parse_u64) {
            self.telegram
                .answer_callback(&callback.id, "Loading category...")
                .await?;
            return self.handle_category(chat_id, forum_id).await;
        }
        if let Some(rest) = data.strip_prefix("cs:") {
            let mut parts = rest.splitn(2, ':');
            let forum_id = parts
                .next()
                .and_then(parse_u64)
                .ok_or_else(|| anyhow!("invalid category search callback"))?;
            self.telegram
                .answer_callback(&callback.id, "Loading category...")
                .await?;
            return self.handle_category(chat_id, forum_id).await;
        }

        Ok(())
    }

    async fn handle_inline_query(&self, inline_query: InlineQuery) -> Result<()> {
        if !self.is_allowed_user(Some(inline_query.from.id)) {
            self.telegram
                .answer_inline_query(&inline_query.id, Vec::new())
                .await?;
            return Ok(());
        }

        let query = inline_query.query.trim();
        if query.is_empty() {
            self.telegram
                .answer_inline_query(&inline_query.id, Vec::new())
                .await?;
            return Ok(());
        }

        let results = match self.cached_search(query, None).await {
            Ok(results) => results,
            Err(err) => {
                warn!(error = %err, "RuTracker unavailable for inline query");
                self.telegram
                    .answer_inline_query(
                        &inline_query.id,
                        vec![rutracker_unavailable_inline_result()],
                    )
                    .await?;
                return Ok(());
            }
        };
        let mut inline_results = Vec::new();
        for result in results.into_iter().take(10) {
            if result.local_catalog {
                cache_set(
                    LOCAL_RESULT_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
                    result.topic_id,
                    result.clone(),
                );
            }
            let details = if result.local_catalog {
                None
            } else {
                self.cached_topic(result.topic_id).await.ok()
            };
            inline_results.push(self.inline_article(query, &result, details.as_ref())?);
        }
        self.telegram
            .answer_inline_query(&inline_query.id, inline_results)
            .await
    }

    fn inline_article(
        &self,
        query: &str,
        result: &SearchResult,
        details: Option<&TopicDetails>,
    ) -> Result<Value> {
        let title = details
            .filter(|details| !details.title.is_empty())
            .map(|details| details.title.as_str())
            .unwrap_or(result.title.as_str());
        let size = details
            .and_then(|details| details.total_size_bytes)
            .unwrap_or(result.size_bytes);
        let seeds = details
            .and_then(|details| details.seeds)
            .unwrap_or(result.seeds);
        let downloads = details
            .and_then(|details| details.downloads)
            .unwrap_or(result.downloads);
        let category_ref = result
            .category
            .as_ref()
            .or_else(|| details.and_then(|details| details.category_path.last()));
        let category_name = category_ref
            .map(|category| category.name.as_str())
            .unwrap_or("unknown");
        let category_line = match category_ref {
            Some(category) => self.category_link(category, result.category_url.as_deref())?,
            None => "unknown".to_string(),
        };
        let author = details
            .and_then(|details| details.author.as_ref())
            .map(format_author)
            .or_else(|| format_search_author(result))
            .unwrap_or_else(|| "unknown".to_string());
        let metadata_lines = details
            .map(format_topic_metadata_lines)
            .or_else(|| format_search_metadata_lines(result))
            .filter(|lines| !lines.is_empty())
            .map(|lines| format!("{lines}\n"))
            .unwrap_or_default();
        let message_text = format!(
            "{}\nCategory: {}\nUser: {}\n{}Size: {}\nSeeds: {}\nDownloads: {}",
            topic_title_link(title, &result.topic_url),
            category_line,
            author,
            metadata_lines,
            format_bytes(size),
            seeds,
            downloads,
        );
        let magnet = details
            .and_then(|details| details.magnet.as_deref())
            .or(result.magnet.as_deref());
        let mut buttons = vec![vec![url_button("RuTracker", &result.topic_url)]];
        if let Some(magnet) = magnet {
            buttons.push(vec![copy_button("Magnet", magnet)]);
        }
        if !query.is_empty()
            && let Some(category) = result.category.as_ref()
        {
            buttons.push(vec![callback_button(
                &format!("Category: {}", truncate_button_text(&category.name, 50)),
                &category_latest_callback(category.id),
            )]);
        }

        let mut article = serde_json::json!({
            "type": "article",
            "id": result.topic_id.to_string(),
            "title": title,
            "description": format!("{} | {} | {} seeds", category_name, format_bytes(size), seeds),
            "input_message_content": {
                "message_text": message_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": true,
            },
            "reply_markup": inline_keyboard(buttons),
        });
        if let Some(image) = first_post_image_url(details) {
            article["thumbnail_url"] = serde_json::json!(image);
        }
        Ok(article)
    }

    async fn run_search(&self, chat_id: i64, query: &str, forum_id: Option<u64>) -> Result<()> {
        self.try_send_typing(chat_id).await;
        let _typing = self.telegram.start_chat_action_heartbeat(chat_id, "typing");
        let progress = self
            .telegram
            .send_status_message(chat_id, "Searching RuTracker...")
            .await?;
        self.try_send_typing(chat_id).await;
        let results = match self.cached_search(query, forum_id).await {
            Ok(results) => results,
            Err(err) => {
                self.edit_rutracker_unavailable(progress, &err).await?;
                return Ok(());
            }
        };
        if results.is_empty() {
            self.telegram
                .edit_message(progress, "No RuTracker title matches found.", None)
                .await?;
            return Ok(());
        }
        if results.iter().any(|result| result.local_catalog) {
            self.telegram
                .edit_message(
                    progress,
                    "RuTracker is unavailable after retries; showing matches from the local XML catalog fallback. Comments and live topic details are not included.",
                    None,
                )
                .await?;
        } else {
            self.telegram.try_delete_message(progress).await;
        }

        for result in results {
            if result.local_catalog {
                cache_set(
                    LOCAL_RESULT_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
                    result.topic_id,
                    result.clone(),
                );
            }
            let details = if result.local_catalog {
                None
            } else {
                match self.cached_topic(result.topic_id).await {
                    Ok(details) => {
                        ensure_same_topic(&result, &details);
                        Some(details)
                    }
                    Err(err) => {
                        warn!(topic_id = result.topic_id, error = %err, "failed to fetch topic details for search result");
                        None
                    }
                }
            };
            self.send_search_result(chat_id, query, &result, details.as_ref(), None)
                .await?;
        }
        Ok(())
    }

    async fn handle_stat(&self, chat_id: i64) -> Result<()> {
        let Some(seed_config) = self.seed_config() else {
            self.telegram
                .send_message(
                    chat_id,
                    "Torrent seeding is disabled for this backend.",
                    None,
                )
                .await?;
            return Ok(());
        };
        match TorrentDownloader::seed_stats(seed_config, self.config.peer_limit).await {
            Ok(stats) => {
                self.telegram
                    .send_message(chat_id, &seed_stats_text(&stats), None)
                    .await
            }
            Err(err) => {
                self.telegram
                    .send_message(
                        chat_id,
                        &format!(
                            "Cannot read torrent stats: {}",
                            html_escape(&err.to_string())
                        ),
                        None,
                    )
                    .await
            }
        }
    }

    async fn handle_author(&self, chat_id: i64) -> Result<()> {
        self.try_send_typing(chat_id).await;
        let _typing = self.telegram.start_chat_action_heartbeat(chat_id, "typing");
        let progress = self
            .telegram
            .send_status_message(chat_id, "Loading author releases...")
            .await?;
        let results = match self.cached_viewforum_latest(AUTHOR_RELEASES_FORUM_ID).await {
            Ok(results) => results,
            Err(err) => {
                self.edit_rutracker_unavailable(progress, &err).await?;
                return Ok(());
            }
        };
        if results.is_empty() {
            self.telegram
                .edit_message(progress, "No recent author releases found.", None)
                .await?;
            return Ok(());
        }
        self.telegram.try_delete_message(progress).await;

        for result in results {
            let (result, details, description_callback) = match self
                .author_release_result(&result)
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    warn!(topic_id = result.topic_id, error = %err, "failed to fetch topic details for author release");
                    (result, None, None)
                }
            };
            self.send_search_result(
                chat_id,
                "",
                &result,
                details.as_ref(),
                description_callback.as_deref(),
            )
            .await?;
        }
        Ok(())
    }

    async fn author_release_result(
        &self,
        result: &SearchResult,
    ) -> Result<(SearchResult, Option<TopicDetails>, Option<String>)> {
        let wrapper = self.cached_topic(result.topic_id).await?;
        ensure_same_topic(result, &wrapper);
        let Some(release_topic_id) = wrapper.first_post_topic_links.first().copied() else {
            return Ok((result.clone(), Some(wrapper), None));
        };
        let release = self.cached_topic(release_topic_id).await?;
        let topic_url = self.rutracker.topic_url(release.topic_id)?;
        let category_url = release
            .category_path
            .last()
            .and_then(|category| self.rutracker.category_url(category.id).ok());
        let result = author_release_search_result(result, &release, topic_url, category_url);
        Ok((
            result,
            Some(release),
            Some(author_description_callback(
                wrapper.topic_id,
                release_topic_id,
            )),
        ))
    }

    async fn try_send_typing(&self, chat_id: i64) {
        if let Err(err) = self.telegram.send_chat_action(chat_id, "typing").await {
            warn!(chat_id, error = %err, "failed to send Telegram typing action");
        }
    }

    async fn send_search_result(
        &self,
        chat_id: i64,
        query: &str,
        result: &SearchResult,
        details: Option<&TopicDetails>,
        description_callback: Option<&str>,
    ) -> Result<()> {
        let title = details
            .filter(|details| !details.title.is_empty())
            .map(|details| details.title.as_str())
            .unwrap_or(result.title.as_str());
        let category = result
            .category
            .as_ref()
            .or_else(|| details.and_then(|details| details.category_path.last()));
        let author = details
            .and_then(|details| details.author.as_ref())
            .map(format_author)
            .or_else(|| format_search_author(result))
            .unwrap_or_else(|| "unknown".to_string());
        let size = details
            .and_then(|details| details.total_size_bytes)
            .unwrap_or(result.size_bytes);
        let seeds = details
            .and_then(|details| details.seeds)
            .unwrap_or(result.seeds);
        let downloads = details
            .and_then(|details| details.downloads)
            .unwrap_or(result.downloads);
        let category_line = match category {
            Some(category) => self.category_link(category, result.category_url.as_deref())?,
            None => "unknown".to_string(),
        };
        let magnet = details
            .and_then(|details| details.magnet.as_deref())
            .or(result.magnet.as_deref())
            .map(str::to_string);
        let metadata_lines = details
            .map(format_topic_metadata_lines)
            .or_else(|| format_search_metadata_lines(result))
            .filter(|lines| !lines.is_empty())
            .map(|lines| format!("{lines}\n"))
            .unwrap_or_default();

        let message = format!(
            "{}\nCategory: {}\nUser: {}\n{}Size: {}\nSeeds: {}\nDownloads: {}",
            topic_title_link(title, &result.topic_url),
            category_line,
            author,
            metadata_lines,
            format_bytes(size),
            seeds,
            downloads,
        );

        let mut rows = if result.local_catalog {
            vec![vec![
                callback_button("Download", &format!("dl:{}", result.topic_id)),
                magnet_button(magnet.as_deref(), result.topic_id),
            ]]
        } else {
            vec![
                vec![
                    callback_button("Description", &format!("desc:{}", result.topic_id)),
                    callback_button("Comments", &format!("comments:{}:1", result.topic_id)),
                    callback_button("Files", &format!("files:{}", result.topic_id)),
                ],
                vec![
                    callback_button("Download", &format!("dl:{}", result.topic_id)),
                    magnet_button(magnet.as_deref(), result.topic_id),
                ],
            ]
        };
        let default_description_callback = format!("desc:{}", result.topic_id);
        if let Some(description_callback) = description_callback
            && let Some(description_button) =
                rows.iter_mut()
                    .flat_map(|row| row.iter_mut())
                    .find(|button| {
                        button.get("callback_data").and_then(Value::as_str)
                            == Some(default_description_callback.as_str())
                    })
        {
            description_button["callback_data"] = serde_json::json!(description_callback);
        }
        if let Some(category) = category.filter(|_| !query.is_empty()) {
            rows.push(vec![callback_button(
                &format!("Category: {}", truncate_button_text(&category.name, 50)),
                &category_latest_callback(category.id),
            )]);
        }

        let reply_markup = inline_keyboard(rows);
        if let Some(image) = first_post_image_url(details) {
            match self
                .telegram
                .send_photo_url_or_upload(chat_id, image, None, None)
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    warn!(
                        topic_id = result.topic_id,
                        image,
                        error = %err,
                        "failed to send search result image; sending metadata without image"
                    );
                }
            }
        }

        self.telegram
            .send_message(chat_id, &message, Some(reply_markup))
            .await
    }

    async fn handle_category_query(&self, chat_id: i64, query: &str) -> Result<()> {
        validate_forum_query(query)?;
        let progress = self
            .telegram
            .send_status_message(chat_id, "Searching categories...")
            .await?;
        let needle = query.to_lowercase();
        let categories = match self.cached_categories().await {
            Ok(categories) => categories,
            Err(err) => {
                self.edit_rutracker_unavailable(progress, &err).await?;
                return Ok(());
            }
        }
        .into_iter()
        .filter(|node| node.name.to_lowercase().contains(&needle))
        .take(30)
        .collect::<Vec<_>>();
        if categories.is_empty() {
            self.telegram
                .edit_message(progress, "No matching RuTracker categories found.", None)
                .await?;
            return Ok(());
        }
        let rows = categories
            .chunks(2)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|node| {
                        callback_button(
                            &truncate_button_text(&node.name, 60),
                            &category_latest_callback(node.id),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        self.telegram
            .edit_message(
                progress,
                "Matching categories:",
                Some(inline_keyboard(rows)),
            )
            .await
    }

    async fn handle_category(&self, chat_id: i64, forum_id: u64) -> Result<()> {
        let subcategories = match self.cached_forum_subcategories(forum_id).await {
            Ok(subcategories) => subcategories,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        if !subcategories.is_empty() {
            let rows = subcategories
                .chunks(2)
                .take(10)
                .map(|chunk| {
                    chunk
                        .iter()
                        .map(|node| {
                            callback_button(
                                &truncate_button_text(&node.name, 60),
                                &category_latest_callback(node.id),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            self.telegram
                .send_message(chat_id, "Subcategories:", Some(inline_keyboard(rows)))
                .await?;
        }

        let latest = match self.cached_latest(forum_id).await {
            Ok(latest) => latest,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        if latest.is_empty() {
            self.telegram
                .send_message(chat_id, "No latest posts found in this category.", None)
                .await?;
            return Ok(());
        }
        for result in latest {
            let details = self.cached_topic(result.topic_id).await.ok();
            self.send_search_result(chat_id, "", &result, details.as_ref(), None)
                .await?;
        }
        Ok(())
    }

    async fn handle_description(
        &self,
        chat_id: i64,
        topic_id: u64,
        progress: Option<ProgressMessage>,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                if let Some(progress) = progress {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
                return Ok(());
            }
        };
        let rich_image_sections = self
            .rich_spoiler_image_sections(&topic.first_post_spoiler_image_sections)
            .await;
        let rich_message = self.topic_message_with_rich_description(
            &topic,
            &topic.description_text,
            &rich_image_sections,
        )?;
        let fallback_text =
            self.topic_message_with_classic_description(&topic, &topic.description_text)?;
        let reply_markup = remove_callback_button(reply_markup, &format!("desc:{topic_id}"));
        let delivery = if let Some(progress) = progress {
            self.telegram
                .edit_rich_message_or_text(
                    progress,
                    &rich_message.text,
                    &fallback_text,
                    reply_markup,
                )
                .await?
        } else {
            self.telegram
                .send_rich_message_or_text(chat_id, &rich_message.text, &fallback_text, None)
                .await?
        };
        let images = fallback_description_spoiler_images(
            &topic.first_post_spoiler_image_sections,
            delivery,
            rich_message.embedded_spoiler_images,
        );
        self.send_description_spoiler_image_urls(chat_id, topic.topic_id, &images)
            .await;
        Ok(())
    }

    async fn handle_author_description(
        &self,
        chat_id: i64,
        wrapper_topic_id: u64,
        release_topic_id: u64,
        progress: Option<ProgressMessage>,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let wrapper = match self.cached_topic(wrapper_topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                if let Some(progress) = progress {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
                return Ok(());
            }
        };
        let release = match self.cached_topic(release_topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                if let Some(progress) = progress {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
                return Ok(());
            }
        };
        let description = combined_author_description(&wrapper, &release);
        let image_sections = combined_author_spoiler_image_sections(&wrapper, &release);
        let rich_image_sections = self.rich_spoiler_image_sections(&image_sections).await;
        let rich_message =
            self.topic_message_with_rich_description(&release, &description, &rich_image_sections)?;
        let fallback_text = self.topic_message_with_classic_description(&release, &description)?;
        let callback = author_description_callback(wrapper_topic_id, release_topic_id);
        let reply_markup = remove_callback_button(reply_markup, &callback);
        let delivery = if let Some(progress) = progress {
            self.telegram
                .edit_rich_message_or_text(
                    progress,
                    &rich_message.text,
                    &fallback_text,
                    reply_markup,
                )
                .await?
        } else {
            self.telegram
                .send_rich_message_or_text(chat_id, &rich_message.text, &fallback_text, None)
                .await?
        };
        let images = fallback_description_spoiler_images(
            &image_sections,
            delivery,
            rich_message.embedded_spoiler_images,
        );
        self.send_description_spoiler_image_urls(chat_id, release.topic_id, &images)
            .await;
        Ok(())
    }

    async fn send_description_spoiler_image_urls(
        &self,
        chat_id: i64,
        topic_id: u64,
        images: &[String],
    ) {
        for image in images {
            if let Err(err) = self
                .telegram
                .send_photo_url_or_upload(chat_id, image, None, None)
                .await
            {
                warn!(
                    topic_id,
                    image,
                    error = %err,
                    "failed to send description spoiler image"
                );
            }
        }
    }

    async fn rich_spoiler_image_sections(
        &self,
        sections: &[TopicSpoilerImages],
    ) -> Vec<TopicSpoilerImages> {
        let Some(public_base_url) = self.config.image_cache_public_base_url.as_deref() else {
            return sections.to_vec();
        };
        let client = match image_cache::http_client() {
            Ok(client) => client,
            Err(err) => {
                warn!(error = %err, "failed to initialize image cache HTTP client");
                return Vec::new();
            }
        };
        let mut remaining = TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT;
        let mut cached_sections = Vec::new();
        for section in sections {
            if remaining == 0 {
                break;
            }
            let mut images = Vec::new();
            for image in section.images.iter().take(remaining) {
                match image_cache::cache_remote_image(
                    &client,
                    &self.config.image_cache_dir,
                    public_base_url,
                    image,
                )
                .await
                {
                    Ok(cached_url) => images.push(cached_url),
                    Err(err) => {
                        warn!(image, error = %err, "failed to cache rich description image; sending spoiler images separately");
                        return Vec::new();
                    }
                }
            }
            remaining = remaining.saturating_sub(images.len());
            if !images.is_empty() {
                cached_sections.push(TopicSpoilerImages {
                    title: section.title.clone(),
                    images,
                });
            }
        }
        cached_sections
    }

    fn topic_message_with_description(
        &self,
        topic: &TopicDetails,
        description: &str,
    ) -> Result<String> {
        let title = if topic.title.is_empty() {
            "RuTracker topic"
        } else {
            &topic.title
        };
        let category_line = match topic.category_path.last() {
            Some(category) => format!(
                "<a href=\"{}\">{}</a>",
                html_escape(&self.rutracker.category_url(category.id)?),
                html_escape(&category.name)
            ),
            None => "unknown".to_string(),
        };
        let metadata_lines = format_topic_metadata_lines(topic);
        let metadata_block = if metadata_lines.is_empty() {
            String::new()
        } else {
            format!("{metadata_lines}\n")
        };
        let author = topic
            .author
            .as_ref()
            .map(format_author)
            .unwrap_or_else(|| "unknown".to_string());
        let size = topic
            .total_size_bytes
            .map(format_bytes)
            .unwrap_or_else(|| "unknown".to_string());
        let seeds = topic
            .seeds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let downloads = topic
            .downloads
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        Ok(format!(
            "{}\nCategory: {}\nUser: {}\n{}Size: {}\nSeeds: {}\nDownloads: {}\n\n<b>Description</b>\n{}",
            topic_title_link(title, &self.rutracker.topic_url(topic.topic_id)?),
            category_line,
            author,
            metadata_block,
            size,
            seeds,
            downloads,
            description
        ))
    }

    fn topic_message_with_classic_description(
        &self,
        topic: &TopicDetails,
        description: &str,
    ) -> Result<String> {
        if description.trim().is_empty() {
            return self
                .topic_message_with_description(topic, "Description is empty or unavailable.");
        }
        self.topic_message_with_description(
            topic,
            &format_description_html(&truncate_for_telegram(description, 3000)),
        )
    }

    fn topic_message_with_rich_description(
        &self,
        topic: &TopicDetails,
        description: &str,
        image_sections: &[TopicSpoilerImages],
    ) -> Result<RichDescriptionMessage> {
        let total_images = spoiler_image_count(image_sections);
        let max_images = total_images.min(TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT);
        for embedded_images in (0..=max_images).rev() {
            let embedded_sections = take_spoiler_image_sections(image_sections, embedded_images);
            let mut low = 0;
            let mut high = description.chars().count();
            let mut best = None;
            while low <= high {
                let mid = low + (high - low) / 2;
                let text = self.topic_message_with_description(
                    topic,
                    &rich_description_html(description, mid, &embedded_sections),
                )?;
                if rich_message_payload_char_count(&text) <= TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS
                {
                    best = Some(text);
                    low = mid + 1;
                } else if mid == 0 {
                    break;
                } else {
                    high = mid - 1;
                }
            }
            if let Some(text) = best {
                return Ok(RichDescriptionMessage {
                    text,
                    embedded_spoiler_images: embedded_images,
                });
            }
        }
        Ok(RichDescriptionMessage {
            text: self.topic_message_with_classic_description(topic, description)?,
            embedded_spoiler_images: 0,
        })
    }

    fn category_link(
        &self,
        category: &crate::rutracker::CategoryRef,
        category_url: Option<&str>,
    ) -> Result<String> {
        let owned_url;
        let url = match category_url {
            Some(url) => url,
            None => {
                owned_url = self.rutracker.category_url(category.id)?;
                &owned_url
            }
        };
        Ok(format!(
            "<a href=\"{}\">{}</a>",
            html_escape(url),
            html_escape(&category.name)
        ))
    }

    fn topic_message_with_files(
        &self,
        topic: &TopicDetails,
        files: &str,
        include_description: bool,
    ) -> Result<String> {
        let description = if include_description && !topic.description_text.is_empty() {
            Some(format_description_html(&truncate_for_telegram(
                &topic.description_text,
                2400,
            )))
        } else {
            None
        };
        let mut text = match description.as_deref() {
            Some(description) => self.topic_message_with_description(topic, description)?,
            None => self.topic_message_header(topic)?,
        };
        text.push_str("\n\n");
        text.push_str(files);
        Ok(text)
    }

    fn topic_message_header(&self, topic: &TopicDetails) -> Result<String> {
        let title = if topic.title.is_empty() {
            "RuTracker topic"
        } else {
            &topic.title
        };
        let category_line = match topic.category_path.last() {
            Some(category) => format!(
                "<a href=\"{}\">{}</a>",
                html_escape(&self.rutracker.category_url(category.id)?),
                html_escape(&category.name)
            ),
            None => "unknown".to_string(),
        };
        let metadata_lines = format_topic_metadata_lines(topic);
        let metadata_block = if metadata_lines.is_empty() {
            String::new()
        } else {
            format!("{metadata_lines}\n")
        };
        let author = topic
            .author
            .as_ref()
            .map(format_author)
            .unwrap_or_else(|| "unknown".to_string());
        let size = topic
            .total_size_bytes
            .map(format_bytes)
            .unwrap_or_else(|| "unknown".to_string());
        let seeds = topic
            .seeds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let downloads = topic
            .downloads
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        Ok(format!(
            "{}\nCategory: {}\nUser: {}\n{}Size: {}\nSeeds: {}\nDownloads: {}",
            topic_title_link(title, &self.rutracker.topic_url(topic.topic_id)?),
            category_line,
            author,
            metadata_block,
            size,
            seeds,
            downloads
        ))
    }

    async fn handle_files(
        &self,
        chat_id: i64,
        topic_id: u64,
        message_to_edit: Option<ProgressMessage>,
        reply_markup: Option<Value>,
        current_text: Option<String>,
    ) -> Result<()> {
        let preserve_message_body =
            should_preserve_callback_message_body(message_to_edit, current_text.as_deref());
        let include_description = message_has_section(current_text.as_deref(), "Description");
        let files_reply_markup =
            remove_callback_button(reply_markup.clone(), &format!("files:{topic_id}"));
        let downloader = self.downloader(topic_id);
        if let Some(metadata) = self.cached_seed_metadata(topic_id, &downloader).await? {
            let files = files_from_metadata_text(&metadata.files, self.config.max_file_mb);
            return self
                .deliver_local_files_text(
                    chat_id,
                    message_to_edit,
                    current_text.as_deref(),
                    &files,
                    files_reply_markup,
                    preserve_message_body,
                )
                .await;
        }
        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                if let Some(message_to_edit) = message_to_edit {
                    warn!(error = %err, "RuTracker unavailable for Files request");
                    self.telegram
                        .send_message_reply_to(
                            chat_id,
                            message_to_edit.message_id,
                            RUTRACKER_UNAVAILABLE_TEXT,
                            Some(rutracker_unavailable_keyboard()),
                        )
                        .await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
                return Ok(());
            }
        };
        if !topic.first_post_files.is_empty() {
            let files = files_from_topic_text(&topic, self.config.max_file_mb);
            let text = self.topic_message_with_files(&topic, &files, include_description)?;
            if let Some(message_to_edit) = message_to_edit {
                if preserve_message_body {
                    self.telegram
                        .edit_message_reply_markup(message_to_edit, files_reply_markup)
                        .await?;
                    return self
                        .telegram
                        .send_message_reply_to(chat_id, message_to_edit.message_id, &text, None)
                        .await;
                }
                self.telegram
                    .edit_message(message_to_edit, &text, files_reply_markup)
                    .await?;
            } else {
                self.telegram.send_message(chat_id, &text, None).await?;
            }
            return Ok(());
        }

        let magnet = require_magnet(&topic)?;
        let progress = match message_to_edit {
            Some(progress) if preserve_message_body => {
                self.telegram
                    .edit_message_reply_markup(progress, files_reply_markup.clone())
                    .await?;
                self.telegram
                    .send_status_message_reply_to(
                        chat_id,
                        progress.message_id,
                        RESOLVING_FILES_STATUS_TEXT,
                    )
                    .await?
            }
            Some(progress) => {
                self.telegram
                    .send_status_message_reply_to(
                        chat_id,
                        progress.message_id,
                        RESOLVING_FILES_STATUS_TEXT,
                    )
                    .await?
            }
            None => {
                self.telegram
                    .send_status_message(chat_id, RESOLVING_FILES_STATUS_TEXT)
                    .await?
            }
        };
        match self
            .cached_metadata_with_attempts(
                topic_id,
                magnet,
                &downloader,
                FILE_LIST_METADATA_TIMEOUT_SECONDS,
                FILE_LIST_METADATA_ATTEMPTS,
            )
            .await
        {
            Ok(metadata) => {
                let files = files_from_metadata_text(&metadata.files, self.config.max_file_mb);
                let text = self.topic_message_with_files(&topic, &files, include_description)?;
                if preserve_message_body {
                    self.telegram.edit_message(progress, &text, None).await?;
                } else if let Some(message_to_edit) = message_to_edit {
                    match self
                        .telegram
                        .edit_message(message_to_edit, &text, files_reply_markup)
                        .await
                    {
                        Ok(()) => self.telegram.try_delete_message(progress).await,
                        Err(err) => {
                            warn!(
                                topic_id,
                                error = %err,
                                "failed to edit original message with file list; keeping status reply"
                            );
                            self.telegram.edit_message(progress, &text, None).await?;
                        }
                    }
                } else {
                    self.telegram.try_delete_message(progress).await;
                    self.telegram.send_message(chat_id, &text, None).await?;
                }
            }
            Err(err) => {
                self.telegram
                    .edit_message(
                        progress,
                        &format!(
                            "File list is unavailable from HTML and magnet metadata failed: {}",
                            html_escape(&err.to_string())
                        ),
                        None,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_magnet(&self, chat_id: i64, topic_id: u64) -> Result<()> {
        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        match topic.magnet.as_ref() {
            Some(magnet) => {
                self.telegram
                    .send_message(
                        chat_id,
                        &format!("<b>Magnet</b>\n<code>{}</code>", html_escape(magnet)),
                        None,
                    )
                    .await?;
            }
            None => {
                self.telegram
                    .send_message(
                        chat_id,
                        "Magnet link is not available in this topic HTML.",
                        None,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_download_prompt(
        &self,
        chat_id: i64,
        topic_id: u64,
        progress: Option<ProgressMessage>,
        reply_markup: Option<Value>,
        current_text: Option<String>,
    ) -> Result<()> {
        let preserve_message_body =
            should_preserve_callback_message_body(progress, current_text.as_deref());
        if let Some(local_result) = self.cached_local_result(topic_id) {
            return self
                .handle_local_download_prompt(
                    chat_id,
                    topic_id,
                    progress,
                    reply_markup,
                    local_result,
                    preserve_message_body,
                )
                .await;
        }
        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                if let Some(progress) = progress {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
                return Ok(());
            }
        };
        let include_description = message_has_section(current_text.as_deref(), "Description");
        let existing_files = self.existing_files_section(&topic, current_text.as_deref());
        let prompt_limit = topic_download_prompt_limit(&topic, self.config.max_file_mb);
        let text = self.topic_message_with_download_prompt(
            &topic,
            include_description,
            existing_files.as_deref(),
            prompt_limit,
        )?;
        let all_files_label = all_files_button_label(&topic, self.config.max_file_mb);
        let reply_markup = download_prompt_reply_markup(reply_markup, topic_id, &all_files_label);
        if let Some(progress) = progress {
            if preserve_message_body {
                return self
                    .telegram
                    .edit_message_reply_markup(progress, reply_markup)
                    .await;
            }
            match self
                .telegram
                .edit_message(progress, &text, reply_markup.clone())
                .await
            {
                Ok(()) => Ok(()),
                Err(err) => {
                    warn!(
                        topic_id,
                        error = %err,
                        "failed to edit download prompt text; trying to edit buttons only"
                    );
                    self.telegram
                        .edit_message_reply_markup(progress, reply_markup)
                        .await?;
                    self.telegram
                        .send_message_reply_to(
                            chat_id,
                            progress.message_id,
                            &download_prompt_text(self.config.max_file_mb, prompt_limit),
                            None,
                        )
                        .await
                        .map(|_| ())
                }
            }
        } else {
            self.telegram
                .send_message(chat_id, &text, reply_markup)
                .await
        }
    }

    async fn handle_local_download_prompt(
        &self,
        chat_id: i64,
        topic_id: u64,
        progress: Option<ProgressMessage>,
        reply_markup: Option<Value>,
        result: SearchResult,
        preserve_message_body: bool,
    ) -> Result<()> {
        let Some(magnet) = result.magnet.as_deref() else {
            self.telegram
                .send_message(
                    chat_id,
                    "Magnet link is unavailable in the local catalog.",
                    None,
                )
                .await?;
            return Ok(());
        };
        let downloader = self.downloader(topic_id);
        let metadata = match self
            .cached_metadata(topic_id, magnet, &downloader, 120)
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                let text = format!(
                    "Cannot load torrent metadata for download options: {}",
                    html_escape(&err.to_string())
                );
                if let Some(progress) = progress {
                    self.telegram
                        .edit_message(progress, &text, reply_markup)
                        .await?;
                } else {
                    self.telegram.send_message(chat_id, &text, None).await?;
                }
                return Ok(());
            }
        };
        let all_files_label =
            all_files_button_label_from_metadata(&metadata, self.config.max_file_mb);
        let prompt_limit = metadata_download_prompt_limit(&metadata, self.config.max_file_mb);
        let reply_markup = download_prompt_reply_markup(reply_markup, topic_id, &all_files_label);
        if let Some(progress) = progress {
            if preserve_message_body {
                return self
                    .telegram
                    .edit_message_reply_markup(progress, reply_markup)
                    .await;
            }
            self.telegram
                .edit_message_reply_markup(progress, reply_markup)
                .await?;
            self.telegram
                .send_message_reply_to(
                    chat_id,
                    progress.message_id,
                    &download_prompt_text(self.config.max_file_mb, prompt_limit),
                    None,
                )
                .await
                .map(|_| ())
        } else {
            self.telegram
                .send_message(
                    chat_id,
                    &download_prompt_text(self.config.max_file_mb, prompt_limit),
                    reply_markup,
                )
                .await
        }
    }

    fn topic_message_with_download_prompt(
        &self,
        topic: &TopicDetails,
        include_description: bool,
        existing_files: Option<&str>,
        prompt_limit: DownloadPromptLimit,
    ) -> Result<String> {
        let mut text = if include_description && !topic.description_text.is_empty() {
            self.topic_message_with_description(
                topic,
                &format_description_html(&truncate_for_telegram(&topic.description_text, 2400)),
            )?
        } else {
            self.topic_message_header(topic)?
        };
        if let Some(existing_files) = existing_files {
            let download_prompt = download_prompt_text(self.config.max_file_mb, prompt_limit);
            let with_files = format!("{text}\n\n{existing_files}\n\n{download_prompt}");
            if with_files.chars().count() <= TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS {
                return Ok(with_files);
            }
        }
        text.push_str("\n\n");
        text.push_str(&download_prompt_text(self.config.max_file_mb, prompt_limit));
        Ok(text)
    }

    fn existing_files_section(
        &self,
        topic: &TopicDetails,
        current_text: Option<&str>,
    ) -> Option<String> {
        if !message_has_files_section(current_text) {
            return None;
        }
        if !topic.first_post_files.is_empty() {
            return Some(files_from_topic_text(topic, self.config.max_file_mb));
        }
        cached_metadata_value(topic.topic_id)
            .map(|metadata| files_from_metadata_text(&metadata.files, self.config.max_file_mb))
    }

    async fn deliver_local_files_text(
        &self,
        chat_id: i64,
        message_to_edit: Option<ProgressMessage>,
        current_text: Option<&str>,
        files_text: &str,
        reply_markup: Option<Value>,
        preserve_message_body: bool,
    ) -> Result<()> {
        let Some(message_to_edit) = message_to_edit else {
            return self.telegram.send_message(chat_id, files_text, None).await;
        };
        if preserve_message_body {
            self.telegram
                .edit_message_reply_markup(message_to_edit, reply_markup)
                .await?;
            return self
                .telegram
                .send_message_reply_to(chat_id, message_to_edit.message_id, files_text, None)
                .await;
        }
        let Some(text) = current_message_with_files(current_text, files_text) else {
            self.telegram
                .edit_message_reply_markup(message_to_edit, reply_markup)
                .await?;
            return self
                .telegram
                .send_message_reply_to(chat_id, message_to_edit.message_id, files_text, None)
                .await;
        };
        match self
            .telegram
            .edit_message(message_to_edit, &text, reply_markup.clone())
            .await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to edit message with local file list; sending reply instead"
                );
                self.telegram
                    .edit_message_reply_markup(message_to_edit, reply_markup)
                    .await?;
                self.telegram
                    .send_message_reply_to(chat_id, message_to_edit.message_id, files_text, None)
                    .await
            }
        }
    }

    async fn handle_select_files(
        &self,
        chat_id: i64,
        topic_id: u64,
        progress: Option<ProgressMessage>,
    ) -> Result<()> {
        let (progress, preserve_message_body) = match progress {
            Some(progress) => {
                self.try_send_typing(chat_id).await;
                (progress, true)
            }
            None => (
                self.telegram
                    .send_status_message(chat_id, "Resolving magnet metadata for file selection...")
                    .await?,
                false,
            ),
        };
        let magnet = match self.download_magnet(topic_id).await {
            Ok(magnet) => magnet,
            Err(err) => {
                if preserve_message_body {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                } else {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                }
                return Ok(());
            }
        };
        let downloader = self.downloader(topic_id);
        let metadata = match self
            .cached_metadata(topic_id, &magnet, &downloader, 120)
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                let text = format!(
                    "Cannot load torrent metadata for selection: {}",
                    html_escape(&err.to_string())
                );
                if preserve_message_body {
                    self.telegram.send_message(chat_id, &text, None).await?;
                } else {
                    self.telegram.edit_message(progress, &text, None).await?;
                }
                return Ok(());
            }
        };
        if selectable_files(&metadata, self.config.max_file_mb).is_empty() {
            let text = format!(
                "No files smaller than the configured {} Telegram upload limit are available for selection.",
                upload_limit_label(self.config.max_file_mb)
            );
            if preserve_message_body {
                self.telegram.send_message(chat_id, &text, None).await?;
            } else {
                self.telegram.edit_message(progress, &text, None).await?;
            }
            return Ok(());
        }
        let key = store_download_selection(DownloadSelection {
            topic_id,
            selected: HashSet::new(),
            page: 0,
        });
        self.render_selection(progress, &key, &metadata).await
    }

    async fn handle_selection_page(
        &self,
        chat_id: i64,
        key: &str,
        page: usize,
        progress: Option<ProgressMessage>,
    ) -> Result<()> {
        let Some(mut selection) = load_download_selection(key) else {
            return self
                .telegram
                .send_message(
                    chat_id,
                    "This file selection expired after a Lambda cold start. Press Download again.",
                    None,
                )
                .await;
        };
        selection.page = page;
        save_download_selection(key, selection.clone());
        let metadata = match self.cached_selection_metadata(&selection).await {
            Ok(metadata) => metadata,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        self.render_or_send_selection(chat_id, progress, key, &metadata)
            .await
    }

    async fn handle_selection_toggle(
        &self,
        chat_id: i64,
        key: &str,
        index: usize,
        progress: Option<ProgressMessage>,
    ) -> Result<()> {
        let Some(mut selection) = load_download_selection(key) else {
            return self
                .telegram
                .send_message(
                    chat_id,
                    "This file selection expired after a Lambda cold start. Press Download again.",
                    None,
                )
                .await;
        };
        if !selection.selected.insert(index) {
            selection.selected.remove(&index);
        }
        save_download_selection(key, selection.clone());
        let metadata = match self.cached_selection_metadata(&selection).await {
            Ok(metadata) => metadata,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        self.render_or_send_selection(chat_id, progress, key, &metadata)
            .await
    }

    async fn handle_selected_download(&self, chat_id: i64, key: &str) -> Result<()> {
        let Some(selection) = load_download_selection(key) else {
            self.telegram
                .send_message(
                    chat_id,
                    "This file selection expired after a Lambda cold start. Press Download again.",
                    None,
                )
                .await?;
            return Ok(());
        };
        if selection.selected.is_empty() {
            let metadata = match self.cached_selection_metadata(&selection).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                    return Ok(());
                }
            };
            let progress = self
                .telegram
                .send_status_message(chat_id, "Select at least one file before starting.")
                .await?;
            return self.render_selection(progress, key, &metadata).await;
        }
        let mut selected = selection.selected.iter().copied().collect::<Vec<_>>();
        selected.sort_unstable();
        self.handle_download(chat_id, selection.topic_id, Some(selected))
            .await
    }

    async fn cached_selection_metadata(
        &self,
        selection: &DownloadSelection,
    ) -> Result<TorrentMetadata> {
        let magnet = self.download_magnet(selection.topic_id).await?;
        let downloader = self.downloader(selection.topic_id);
        self.cached_metadata(selection.topic_id, &magnet, &downloader, 120)
            .await
    }

    async fn render_or_send_selection(
        &self,
        chat_id: i64,
        progress: Option<ProgressMessage>,
        key: &str,
        metadata: &TorrentMetadata,
    ) -> Result<()> {
        match progress {
            Some(progress) => self.render_selection(progress, key, metadata).await,
            None => {
                let progress = self
                    .telegram
                    .send_status_message(chat_id, "Loading selection...")
                    .await?;
                self.render_selection(progress, key, metadata).await
            }
        }
    }

    async fn render_selection(
        &self,
        progress: ProgressMessage,
        key: &str,
        metadata: &TorrentMetadata,
    ) -> Result<()> {
        let Some(selection) = load_download_selection(key) else {
            self.telegram
                .send_message(
                    progress.chat_id,
                    "This file selection expired. Press Download again.",
                    None,
                )
                .await?;
            return Ok(());
        };
        let files = selectable_files(metadata, self.config.max_file_mb);
        let total_pages = files.len().div_ceil(SELECTION_PAGE_SIZE).max(1);
        let page = selection.page.min(total_pages - 1);
        if page != selection.page {
            save_download_selection(
                key,
                DownloadSelection {
                    page,
                    ..selection.clone()
                },
            );
        }
        let start = page * SELECTION_PAGE_SIZE;
        let end = (start + SELECTION_PAGE_SIZE).min(files.len());

        let mut rows = Vec::new();
        for file in &files[start..end] {
            rows.push(vec![callback_button(
                &selection_file_button_label(file, selection.selected.contains(&file.index)),
                &format!("tog:{key}:{}", file.index),
            )]);
        }
        let mut nav = Vec::new();
        if page > 0 {
            nav.push(callback_button("Prev", &format!("sp:{key}:{}", page - 1)));
        }
        if page + 1 < total_pages {
            nav.push(callback_button("Next", &format!("sp:{key}:{}", page + 1)));
        }
        if !nav.is_empty() {
            rows.push(nav);
        }
        rows.push(vec![callback_button(
            &format!("Start selected ({})", selection.selected.len()),
            &format!("go:{key}"),
        )]);

        self.telegram
            .edit_message_reply_markup(progress, Some(inline_keyboard(rows)))
            .await
    }

    async fn handle_download(
        &self,
        chat_id: i64,
        topic_id: u64,
        selected_indexes: Option<Vec<usize>>,
    ) -> Result<()> {
        let typing = self.telegram.start_chat_action_heartbeat(chat_id, "typing");
        let progress = self
            .telegram
            .send_status_message(chat_id, "Resolving magnet metadata...")
            .await?;
        let total_seconds = self.config.download_timeout_seconds();
        let countdown = self.telegram.start_status_countdown(
            progress,
            "Downloading selected files...",
            self.config.download_countdown_label.clone(),
            self.config.download_status_interval_seconds,
            total_seconds,
        );

        let magnet = match self.download_magnet(topic_id).await {
            Ok(magnet) => magnet.to_string(),
            Err(err) => {
                countdown.stop();
                typing.stop();
                self.telegram
                    .edit_message(
                        progress,
                        &format!("Cannot download: {}", html_escape(&err.to_string())),
                        None,
                    )
                    .await?;
                return Ok(());
            }
        };

        let downloader = self.downloader(topic_id);
        let metadata = match self
            .cached_metadata(topic_id, &magnet, &downloader, total_seconds.min(180))
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                countdown.stop();
                typing.stop();
                self.telegram
                    .edit_message(
                        progress,
                        &format!(
                            "Failed to resolve magnet metadata: {}",
                            html_escape(&err.to_string())
                        ),
                        None,
                    )
                    .await?;
                return Ok(());
            }
        };

        self.telegram
            .try_edit_message(
                progress,
                &countdown_status_text(
                    "Downloading selected files...",
                    &self.config.download_countdown_label,
                    total_seconds.div_ceil(60),
                ),
            )
            .await;
        let telegram = self.telegram.clone();
        let (title, topic_url) = match self.cached_local_result(topic_id) {
            Some(result) => (result.title, result.topic_url),
            None => {
                let topic = self.cached_topic(topic_id).await?;
                (topic.title, self.rutracker.topic_url(topic_id)?)
            }
        };
        let outcome = match downloader
            .download_small_files(
                DownloadRequest {
                    magnet: &magnet,
                    metadata: &metadata,
                    max_file_mb: self.config.max_file_mb,
                    selected_indexes: selected_indexes.as_deref(),
                    timeout_seconds: total_seconds,
                    final_window_seconds: 10,
                },
                move |file, completed, total| {
                    let telegram = telegram.clone();
                    let title = title.clone();
                    let topic_url = topic_url.clone();
                    async move {
                        let caption = downloaded_file_caption(
                            &title,
                            &topic_url,
                            file.size_bytes,
                            completed,
                            total,
                        );
                        telegram
                            .send_playable_or_document(
                                chat_id,
                                &file.path,
                                &file.display_name,
                                Some(&caption),
                            )
                            .await
                    }
                },
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                countdown.stop();
                typing.stop();
                self.telegram
                    .edit_message(
                        progress,
                        &format!("Download failed: {}", html_escape(&err.to_string())),
                        None,
                    )
                    .await?;
                return Ok(());
            }
        };
        countdown.stop();
        typing.stop();

        if outcome.timed_out {
            self.telegram
                .edit_message(
                    progress,
                    &download_timeout_message(&self.config, &outcome),
                    None,
                )
                .await?;
        } else if outcome.files.is_empty() {
            self.telegram
                .edit_message(
                    progress,
                    &format!(
                        "No files smaller than the configured {} Telegram upload limit were downloaded.",
                        upload_limit_label(self.config.max_file_mb)
                    ),
                    None,
                )
                .await?;
        } else {
            self.telegram
                .edit_message(
                    progress,
                    &format!("Downloaded and sent {} files.", outcome.files.len()),
                    None,
                )
                .await?;
            self.telegram.try_delete_message(progress).await;
        }

        Ok(())
    }

    async fn send_comments_page(
        &self,
        chat_id: i64,
        topic_id: u64,
        page: u32,
        start_comment_index: usize,
        callback_message: Option<ProgressMessage>,
    ) -> Result<()> {
        let topic = match self.cached_topic_page(topic_id, page).await {
            Ok(topic) => topic,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        self.send_comments_topic(chat_id, &topic, start_comment_index, callback_message)
            .await
    }

    async fn send_comments_topic(
        &self,
        chat_id: i64,
        topic: &TopicDetails,
        start_comment_index: usize,
        callback_message: Option<ProgressMessage>,
    ) -> Result<()> {
        let comments = comments_text(topic, start_comment_index);
        let reply_markup = comments_next_keyboard(topic, comments.next_comment_index);
        if (topic.comments_page > 1 || start_comment_index > 0)
            && let Some(progress) = callback_message
        {
            return self
                .telegram
                .edit_message(progress, &comments.text, reply_markup)
                .await;
        }
        if let Some(progress) = callback_message {
            return self
                .telegram
                .send_message_reply_to(chat_id, progress.message_id, &comments.text, reply_markup)
                .await;
        }
        self.telegram
            .send_message(chat_id, &comments.text, reply_markup)
            .await
    }

    fn downloader(&self, topic_id: u64) -> TorrentDownloader {
        // VM seeding mode keeps stable per-topic folders so rqbit can resume
        // and seed already-downloaded data across requests and restarts.
        let output_dir = if self.config.seed_torrents {
            self.config
                .tmp_dir
                .join("seeds")
                .join(format!("rutracker-{topic_id}"))
        } else {
            self.config
                .tmp_dir
                .join(format!("rutracker-{topic_id}-{}", std::process::id()))
        };
        let downloader = TorrentDownloader::new(output_dir, self.config.peer_limit);
        match self.seed_config() {
            Some(seed_config) => downloader.with_seed_config(seed_config),
            None => downloader,
        }
    }

    fn seed_config(&self) -> Option<SeedConfig> {
        self.config.seed_torrents.then(|| SeedConfig {
            root_dir: self.config.tmp_dir.join("seeds"),
            listen_port: self.config.torrent_listen_port,
            disk_reserve_bytes: self.config.seed_disk_reserve_mb.saturating_mul(1024 * 1024),
        })
    }

    fn is_allowed_user(&self, user_id: Option<i64>) -> bool {
        self.config.allowed_telegram_user_ids.is_empty()
            || user_id.is_some_and(|id| self.config.allowed_telegram_user_ids.contains(&id))
    }

    async fn send_rutracker_unavailable(&self, chat_id: i64, err: &anyhow::Error) -> Result<()> {
        warn!(error = %err, "RuTracker unavailable for Telegram request");
        self.telegram
            .send_message(
                chat_id,
                RUTRACKER_UNAVAILABLE_TEXT,
                Some(rutracker_unavailable_keyboard()),
            )
            .await
    }
    async fn edit_rutracker_unavailable(
        &self,
        progress: ProgressMessage,
        err: &anyhow::Error,
    ) -> Result<()> {
        warn!(error = %err, "RuTracker unavailable for Telegram request");
        self.telegram
            .edit_message(
                progress,
                RUTRACKER_UNAVAILABLE_TEXT,
                Some(rutracker_unavailable_keyboard()),
            )
            .await
    }

    async fn cached_search(&self, query: &str, forum_id: Option<u64>) -> Result<Vec<SearchResult>> {
        let key = format!("{}:{}", forum_id.unwrap_or(0), query);
        if let Some(value) = cache_get(
            SEARCH_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &key,
        ) {
            return Ok(value);
        }
        let value = match self
            .rutracker
            .search(query, forum_id, self.config.search_limit)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                if let Some(value) = self.offline_catalog_search(query, forum_id, &err)? {
                    value
                } else {
                    return Err(err);
                }
            }
        };
        cache_set(
            SEARCH_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            key,
            value.clone(),
        );
        Ok(value)
    }

    fn offline_catalog_search(
        &self,
        query: &str,
        forum_id: Option<u64>,
        live_error: &anyhow::Error,
    ) -> Result<Option<Vec<SearchResult>>> {
        let Some(catalog) = self.offline_catalog.as_ref() else {
            return Ok(None);
        };
        if !catalog.exists() {
            warn!(error = %live_error, "RuTracker unavailable and local catalog is not installed");
            return Ok(None);
        }
        match catalog.search(query, forum_id, self.config.search_limit) {
            Ok(results) if results.is_empty() => {
                warn!(error = %live_error, "RuTracker unavailable and local catalog returned no matches");
                Ok(None)
            }
            Ok(results) => {
                warn!(
                    error = %live_error,
                    results = results.len(),
                    "RuTracker unavailable; using local XML catalog fallback"
                );
                Ok(Some(results))
            }
            Err(err) => {
                warn!(
                    error = %err,
                    live_error = %live_error,
                    "RuTracker unavailable and local catalog search failed"
                );
                Ok(None)
            }
        }
    }

    async fn cached_topic(&self, topic_id: u64) -> Result<TopicDetails> {
        if let Some(value) = cache_get(
            TOPIC_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &topic_id,
        ) {
            return Ok(value);
        }
        let value = self.rutracker.topic(topic_id).await?;
        cache_set(
            TOPIC_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            topic_id,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_topic_page(&self, topic_id: u64, page: u32) -> Result<TopicDetails> {
        let page = page.max(1);
        if page == 1 {
            return self.cached_topic(topic_id).await;
        }
        let key = (topic_id, page);
        if let Some(value) = cache_get(
            COMMENT_PAGE_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &key,
        ) {
            return Ok(value);
        }
        let value = self.rutracker.topic_page(topic_id, page).await?;
        cache_set(
            COMMENT_PAGE_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            key,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_categories(&self) -> Result<Vec<crate::rutracker::ForumNode>> {
        let key = "all".to_string();
        if let Some(value) = cache_get(
            CATEGORIES_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &key,
        ) {
            return Ok(value);
        }
        let value = self.rutracker.categories().await?;
        cache_set(
            CATEGORIES_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            key,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_forum_subcategories(
        &self,
        forum_id: u64,
    ) -> Result<Vec<crate::rutracker::ForumNode>> {
        if let Some(value) = cache_get(
            SUBCATEGORIES_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &forum_id,
        ) {
            return Ok(value);
        }
        let value = self.rutracker.forum_subcategories(forum_id).await?;
        cache_set(
            SUBCATEGORIES_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            forum_id,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_latest(&self, forum_id: u64) -> Result<Vec<SearchResult>> {
        if let Some(value) = cache_get(
            LATEST_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &forum_id,
        ) {
            return Ok(value);
        }
        let value = self.rutracker.latest_in_forum(forum_id, 10).await?;
        cache_set(
            LATEST_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            forum_id,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_viewforum_latest(&self, forum_id: u64) -> Result<Vec<SearchResult>> {
        if let Some(value) = cache_get(
            VIEWFORUM_LATEST_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &forum_id,
        ) {
            return Ok(value);
        }
        let value = self.rutracker.latest_in_viewforum(forum_id, 10).await?;
        cache_set(
            VIEWFORUM_LATEST_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            forum_id,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_metadata(
        &self,
        topic_id: u64,
        magnet: &str,
        downloader: &TorrentDownloader,
        timeout_seconds: u64,
    ) -> Result<TorrentMetadata> {
        self.cached_metadata_with_attempts(
            topic_id,
            magnet,
            downloader,
            timeout_seconds,
            crate::downloader::MAGNET_METADATA_MAX_ATTEMPTS,
        )
        .await
    }

    async fn cached_metadata_with_attempts(
        &self,
        topic_id: u64,
        magnet: &str,
        downloader: &TorrentDownloader,
        timeout_seconds: u64,
        max_attempts: usize,
    ) -> Result<TorrentMetadata> {
        if let Some(value) = cache_get(
            METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &topic_id,
        ) {
            return Ok(value);
        }
        let value = downloader
            .metadata_from_magnet_with_attempts(magnet, timeout_seconds, max_attempts)
            .await?;
        cache_set(
            METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            topic_id,
            value.clone(),
        );
        Ok(value)
    }

    async fn cached_seed_metadata(
        &self,
        topic_id: u64,
        downloader: &TorrentDownloader,
    ) -> Result<Option<TorrentMetadata>> {
        if let Some(value) = cache_get(
            METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &topic_id,
        ) {
            return Ok(Some(value));
        }
        let Some(value) = downloader.metadata_from_seed_cache().await? else {
            return Ok(None);
        };
        cache_set(
            METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            topic_id,
            value.clone(),
        );
        Ok(Some(value))
    }

    async fn download_magnet(&self, topic_id: u64) -> Result<String> {
        if let Some(result) = self.cached_local_result(topic_id)
            && let Some(magnet) = result.magnet
        {
            return Ok(magnet);
        }
        let topic = self.cached_topic(topic_id).await?;
        require_magnet(&topic).map(str::to_string)
    }

    fn cached_local_result(&self, topic_id: u64) -> Option<SearchResult> {
        cache_get(
            LOCAL_RESULT_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &topic_id,
        )
    }
}

fn cache_get<K, T>(cache: &Mutex<HashMap<K, CacheEntry<T>>>, key: &K) -> Option<T>
where
    K: Eq + std::hash::Hash,
    T: Clone,
{
    let mut guard = cache.lock().ok()?;
    let now = now_seconds();
    guard.retain(|_, entry| now.saturating_sub(entry.created_at) < RAM_CACHE_TTL_SECONDS);
    guard.get(key).map(|entry| entry.value.clone())
}

fn cache_set<K, T>(cache: &Mutex<HashMap<K, CacheEntry<T>>>, key: K, value: T)
where
    K: Eq + std::hash::Hash,
{
    let mut guard = cache.lock().expect("RAM cache poisoned");
    let now = now_seconds();
    guard.retain(|_, entry| now.saturating_sub(entry.created_at) < RAM_CACHE_TTL_SECONDS);
    guard.insert(
        key,
        CacheEntry {
            value,
            created_at: now,
        },
    );
}

fn cached_metadata_value(topic_id: u64) -> Option<TorrentMetadata> {
    cache_get(
        METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
        &topic_id,
    )
}

fn format_author(author: &AuthorDetails) -> String {
    let mut text = author_name_link(&author.name, author.profile_url.as_deref());
    if let Some(posts) = author.posts_count {
        text.push_str(&format!(" ({} messages)", posts));
    }
    text
}

fn format_search_author(result: &SearchResult) -> Option<String> {
    result
        .author
        .as_ref()
        .map(|name| author_name_link(name, result.author_profile_url.as_deref()))
}

fn first_post_image_url(details: Option<&TopicDetails>) -> Option<&str> {
    // Search result metadata stays in a normal message because media captions
    // are limited to 1024 chars. The image is sent separately.
    details?
        .first_post_images
        .first()
        .map(String::as_str)
        .filter(|image| !image.trim().is_empty())
}

fn author_name_link(name: &str, profile_url: Option<&str>) -> String {
    match profile_url {
        Some(profile_url) => format!(
            "<a href=\"{}\">{}</a>",
            html_escape(profile_url),
            html_escape(name)
        ),
        None => html_escape(name),
    }
}

fn comment_author_line(author: Option<&AuthorDetails>) -> String {
    match author {
        Some(author) => format_author(author),
        None => "<b>unknown</b>".to_string(),
    }
}

struct CommentsText {
    text: String,
    next_comment_index: Option<usize>,
}

/// Formats comments for either a new reply message or a later edit when the
/// user presses the comments "Next" button.
fn comments_text(topic: &TopicDetails, start_comment_index: usize) -> CommentsText {
    let page = topic.comments_page.max(1);
    let total_pages = topic.comments_total_pages.max(page).max(1);
    if topic.comments.is_empty() {
        return CommentsText {
            text: format!("No comments found on page {page}/{total_pages}."),
            next_comment_index: None,
        };
    }
    let mut text = format!("<b>Comments {page}/{total_pages}</b>");
    let start_comment_index = start_comment_index.min(topic.comments.len().saturating_sub(1));
    for (index, comment) in topic.comments.iter().enumerate().skip(start_comment_index) {
        let block = comment_block_html(comment);
        if comments_text_fits(&text, &block) {
            push_comment_block(&mut text, &block);
            continue;
        }
        if index == start_comment_index {
            let block = oversized_comment_block_html(&text, comment);
            push_comment_block(&mut text, &block);
            return CommentsText {
                text,
                next_comment_index: (index + 1 < topic.comments.len()).then_some(index + 1),
            };
        }
        return CommentsText {
            text,
            next_comment_index: Some(index),
        };
    }
    CommentsText {
        text,
        next_comment_index: None,
    }
}

fn comment_block_html(comment: &TopicComment) -> String {
    format!(
        "{}\n{}",
        comment_author_line(comment.author.as_ref()),
        comment_body_html(comment)
    )
}

fn comment_body_html(comment: &TopicComment) -> String {
    let html = comment.html.trim();
    if !html.is_empty() {
        return html.to_string();
    }
    html_escape(&comment.text)
}

fn comments_text_fits(current: &str, block: &str) -> bool {
    current.chars().count() + 2 + block.chars().count() <= TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS
}

fn push_comment_block(text: &mut String, block: &str) {
    text.push_str("\n\n");
    text.push_str(block);
}

fn oversized_comment_block_html(current_text: &str, comment: &TopicComment) -> String {
    let author = comment_author_line(comment.author.as_ref());
    let fixed_chars = current_text.chars().count() + 2 + author.chars().count() + 1;
    let body_budget = TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS.saturating_sub(fixed_chars);
    format!(
        "{author}\n{}",
        html_escape_truncated_to(&comment.text, body_budget)
    )
}

fn html_escape_truncated_to(value: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let total_chars = value.chars().count();
    let mut out = String::new();
    let mut used = 0;
    let mut truncated = false;
    for (index, ch) in value.chars().enumerate() {
        let escaped = html_escape(&ch.to_string());
        let escaped_len = escaped.chars().count();
        let reserve_for_ellipsis = usize::from(index + 1 < total_chars);
        if used + escaped_len + reserve_for_ellipsis > limit {
            truncated = true;
            break;
        }
        out.push_str(&escaped);
        used += escaped_len;
    }
    if truncated && used < limit {
        out.push('…');
    }
    out
}

fn files_from_topic_text(topic: &TopicDetails, max_file_mb: u64) -> String {
    let mut text = format!(
        "<b>Files from first post: {}</b>",
        html_escape(&topic.title)
    );
    for file in topic.first_post_files.iter().take(80) {
        text.push('\n');
        text.push_str(&format_topic_file(file, max_file_mb));
    }
    text
}

fn files_from_metadata_text(files: &[TorrentFile], max_file_mb: u64) -> String {
    let mut text = String::from("<b>Files from torrent metadata</b>");
    for file in files.iter().take(120) {
        text.push('\n');
        let display = file.path.display().to_string();
        let safe_bytes = telegram_safe_file_bytes(max_file_mb);
        if file.size_bytes > safe_bytes {
            text.push_str(&format!(
                "<s>{}</s> — {} (over {} MB)",
                html_escape(&display),
                format_bytes(file.size_bytes),
                max_file_mb
            ));
        } else {
            text.push_str(&format!(
                "{} — {}",
                html_escape(&display),
                format_bytes(file.size_bytes)
            ));
        }
    }
    text
}

fn format_topic_file(file: &TopicFile, max_file_mb: u64) -> String {
    let safe_bytes = telegram_safe_file_bytes(max_file_mb);
    match file.size_bytes {
        Some(size) if size > safe_bytes => format!(
            "<s>{}</s> — {} (over {} MB)",
            html_escape(&file.path),
            format_bytes(size),
            max_file_mb
        ),
        Some(size) => format!("{} — {}", html_escape(&file.path), format_bytes(size)),
        None => format!("{} — size unknown", html_escape(&file.path)),
    }
}

fn magnet_button(magnet: Option<&str>, topic_id: u64) -> Value {
    match magnet {
        Some(magnet) => copy_button("Magnet", magnet),
        None => callback_button("Magnet", &format!("mag:{topic_id}")),
    }
}

fn selectable_files(metadata: &TorrentMetadata, max_file_mb: u64) -> Vec<&TorrentFile> {
    let safe_bytes = telegram_safe_file_bytes(max_file_mb);
    metadata
        .files
        .iter()
        .filter(|file| file.size_bytes <= safe_bytes)
        .collect()
}

fn selection_file_button_label(file: &TorrentFile, selected: bool) -> String {
    let file_name = file
        .path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| file.path.to_str().unwrap_or("file"));
    let label = format!(
        "{} ({})",
        truncate_for_telegram(file_name, SELECTION_FILE_BUTTON_LABEL_CHARS),
        format_bytes(file.size_bytes)
    );
    if selected {
        format!("✅ {label}")
    } else {
        label
    }
}

fn comments_next_keyboard(
    topic: &TopicDetails,
    next_comment_index: Option<usize>,
) -> Option<Value> {
    if let Some(next_comment_index) = next_comment_index {
        return Some(inline_keyboard(vec![vec![callback_button(
            "Next comments",
            &format!(
                "comments:{}:{}:{next_comment_index}",
                topic.topic_id, topic.comments_page
            ),
        )]]));
    }
    let next_page = topic.comments_page.saturating_add(1);
    (next_page <= topic.comments_total_pages).then(|| {
        inline_keyboard(vec![vec![callback_button(
            &format!("Next {}/{}", next_page, topic.comments_total_pages),
            &format!("comments:{}:{next_page}:0", topic.topic_id),
        )]])
    })
}

fn rutracker_unavailable_keyboard() -> Value {
    inline_keyboard(vec![vec![url_button("RuTracker news", RUTRACKER_NEWS_URL)]])
}

fn rutracker_unavailable_inline_result() -> Value {
    serde_json::json!({
        "type": "article",
        "id": "rutracker-unavailable",
        "title": "RuTracker unavailable",
        "description": "Check @rutracker_news for status",
        "input_message_content": {
            "message_text": RUTRACKER_UNAVAILABLE_TEXT,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        },
        "reply_markup": rutracker_unavailable_keyboard(),
    })
}

fn parse_comments_callback(value: &str) -> Option<(u64, u32, usize)> {
    let rest = value.strip_prefix("comments:")?;
    let mut parts = rest.split(':');
    let topic_id = parts.next().and_then(parse_u64)?;
    let page = parts.next()?.parse::<u32>().ok()?.max(1);
    let start_comment_index = parts
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    Some((topic_id, page, start_comment_index))
}

fn parse_selection_page_callback(value: &str) -> Option<(String, usize)> {
    let rest = value.strip_prefix("sp:")?;
    let mut parts = rest.splitn(2, ':');
    let key = parts.next()?.to_string();
    let page = parts.next()?.parse::<usize>().ok()?;
    Some((key, page))
}

fn parse_selection_toggle_callback(value: &str) -> Option<(String, usize)> {
    let rest = value.strip_prefix("tog:")?;
    let mut parts = rest.splitn(2, ':');
    let key = parts.next()?.to_string();
    let index = parts.next()?.parse::<usize>().ok()?;
    Some((key, index))
}

fn category_latest_callback(forum_id: u64) -> String {
    format!("cat:{forum_id}")
}

fn store_download_selection(selection: DownloadSelection) -> String {
    let mut hasher = Sha1::new();
    hasher.update(selection.topic_id.to_be_bytes());
    hasher.update(now_seconds().to_be_bytes());
    let digest = hasher.finalize();
    let key = URL_SAFE_NO_PAD.encode(&digest[..9]);
    save_download_selection(&key, selection);
    key
}

fn load_download_selection(key: &str) -> Option<DownloadSelection> {
    cache_get(
        DOWNLOAD_SELECTION_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
        &key.to_string(),
    )
}

fn save_download_selection(key: &str, selection: DownloadSelection) {
    cache_set(
        DOWNLOAD_SELECTION_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
        key.to_string(),
        selection,
    );
}

fn mark_update_seen(update_id: i64) -> bool {
    let cache = UPDATE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().expect("update cache poisoned");
    let now = now_seconds();
    guard.retain(|_, entry| now.saturating_sub(entry.created_at) < RAM_CACHE_TTL_SECONDS);
    if guard.contains_key(&update_id) {
        return false;
    }
    guard.insert(
        update_id,
        CacheEntry {
            value: (),
            created_at: now,
        },
    );
    true
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_help_command(text: &str) -> bool {
    matches!(
        telegram_command_name(text).as_deref(),
        Some("start" | "help")
    )
}

fn is_stat_command(text: &str) -> bool {
    matches!(
        telegram_command_name(text).as_deref(),
        Some("stat" | "stats")
    )
}

fn is_author_command(text: &str) -> bool {
    matches!(telegram_command_name(text).as_deref(), Some("author"))
}

fn help_text(max_file_mb: u64) -> String {
    let upload_text = if max_file_mb <= crate::TELEGRAM_MAX_FILE_MB_DEFAULT {
        format!(
            "Downloads are limited to files under {} because Telegram Bot API \
             <a href=\"https://core.telegram.org/bots/api#senddocument\">sendDocument</a> has that upload limit.",
            upload_limit_label(crate::TELEGRAM_MAX_FILE_MB_DEFAULT)
        )
    } else {
        format!(
            "Downloads are limited to files under {} by this deployment. Public Telegram Bot API \
             <a href=\"https://core.telegram.org/bots/api#senddocument\">sendDocument</a> uploads are limited to {}; \
             tdlib's local telegram-bot-api server can send larger files: https://github.com/tdlib/telegram-bot-api",
            upload_limit_label(max_file_mb),
            upload_limit_label(crate::TELEGRAM_MAX_FILE_MB_DEFAULT)
        )
    };

    format!(
        concat!(
            "This is an unofficial bot and is not affiliated with RuTracker.\n\n",
            "Send a RuTracker search string. I search rutracker.org titles and return matching topics.\n\n",
            "Use <code>c text</code> to search categories. Category buttons return the 10 most recent topics from that category.\n\n",
            "Use <code>/author</code> to show the 10 most recent topics from RuTracker's author releases forum.\n\n",
            "Use <code>/stat</code> to show torrents currently seeding from the VM, including uploaded bytes and ratio.\n\n",
            "{}\n\n",
            "Torrent downloads use librqbit, the rqbit torrent client library: ",
            "https://github.com/ikatson/rqbit\n\n",
            "When RuTracker is unavailable after retries, VM deployments can search a local SQLite index built from RuTracker's XML dump fallback: ",
            "https://rutracker.org/forum/viewtopic.php?t=5591249\n\n",
            "AWS Lambda can run one invocation for at most 15 minutes: ",
            "https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html\n\n",
            "If RuTracker is unavailable, I return the official news channel link: @rutracker_news. ",
            "RuTracker runs on low-cost community infrastructure; please consider donating to them.\n\n",
            "Please seed legal torrents when you can; it helps the ecosystem. ",
            "Respect creators, and consider supporting your favorite artists and authors with money or by buying from them. ",
            "Torrents are also culture preservation: many works are public domain, Creative Commons, abandoned, or otherwise unavailable to buy legally.\n\n",
            "If you know an indie artist, consider asking whether they want to release some work under Creative Commons and publish it legally on RuTracker, so more people can discover them.\n\n",
            "Source code: https://github.com/vitaly-zdanevich/bot_telegram_rutracker"
        ),
        upload_text
    )
}

fn download_prompt_text(max_file_mb: u64, limit: DownloadPromptLimit) -> String {
    let limit_label = upload_limit_label(max_file_mb);
    let body = match limit {
        DownloadPromptLimit::Unknown => format!(
            "Choose all files under {limit_label}, or select specific files from torrent metadata."
        ),
        DownloadPromptLimit::AllFilesFit => {
            "Choose all files, or select specific files from torrent metadata.".to_string()
        }
        DownloadPromptLimit::SomeFilesOversized => format!(
            "Some files are larger than {limit_label} and cannot be sent. Choose all files under {limit_label}, or select specific files from torrent metadata."
        ),
    };
    format!("<b>Download</b>\n{body}")
}

fn topic_download_prompt_limit(topic: &TopicDetails, max_file_mb: u64) -> DownloadPromptLimit {
    let safe_bytes = telegram_safe_file_bytes(max_file_mb);
    if topic.first_post_files.is_empty() {
        return DownloadPromptLimit::Unknown;
    }
    let mut saw_unknown_size = false;
    for file in &topic.first_post_files {
        match file.size_bytes {
            Some(size) if size > safe_bytes => return DownloadPromptLimit::SomeFilesOversized,
            Some(_) => {}
            None => saw_unknown_size = true,
        }
    }
    if saw_unknown_size {
        DownloadPromptLimit::Unknown
    } else {
        DownloadPromptLimit::AllFilesFit
    }
}

fn metadata_download_prompt_limit(
    metadata: &TorrentMetadata,
    max_file_mb: u64,
) -> DownloadPromptLimit {
    let safe_bytes = telegram_safe_file_bytes(max_file_mb);
    if metadata
        .files
        .iter()
        .any(|file| file.size_bytes > safe_bytes)
    {
        DownloadPromptLimit::SomeFilesOversized
    } else {
        DownloadPromptLimit::AllFilesFit
    }
}

fn upload_limit_label(max_file_mb: u64) -> String {
    format!("{max_file_mb} MB")
}

fn download_timeout_message(config: &Config, outcome: &DownloadOutcome) -> String {
    if config.download_countdown_label == "AWS Lambda lifetime" {
        return format!(
            "Downloaded and sent {} of {} files - sorry, AWS Lambda lifetime is only 15 minutes.",
            outcome.files.len(),
            outcome.total_files
        );
    }

    format!(
        "Downloaded and sent {} of {} files before {} expired.",
        outcome.files.len(),
        outcome.total_files,
        config.download_countdown_label.to_lowercase()
    )
}

fn telegram_command_name(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let command = trimmed.strip_prefix('/')?.split_whitespace().next()?;
    let command = command.split('@').next().unwrap_or(command);
    Some(command.to_ascii_lowercase())
}

fn parse_u64(value: &str) -> Option<u64> {
    value.parse::<u64>().ok()
}

fn truncate_button_text(value: &str, limit: usize) -> String {
    truncate_for_telegram(value, limit).replace('\n', " ")
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn seed_stats_text(stats: &[SeedTorrentStats]) -> String {
    if stats.is_empty() {
        return "No torrents are currently seeding from this VM.".to_string();
    }
    let total_uploaded = stats
        .iter()
        .map(|torrent| torrent.uploaded_bytes)
        .sum::<u64>();
    let total_size = stats.iter().map(|torrent| torrent.total_bytes).sum::<u64>();
    let mut text = format!(
        "<b>Seeding from VM</b>\nTorrents: {}\nUploaded: {}; ratio: {}",
        stats.len(),
        format_bytes(total_uploaded),
        format_ratio(total_uploaded, total_size)
    );

    for (index, torrent) in stats.iter().enumerate() {
        let line = seed_stat_line(index + 1, torrent);
        if text.chars().count() + line.chars().count() > TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS {
            let omitted = stats.len() - index;
            text.push_str(&format!("\n... {omitted} more torrents"));
            break;
        }
        text.push_str(&line);
    }
    text
}

fn seed_stat_line(index: usize, torrent: &SeedTorrentStats) -> String {
    format!(
        "\n\n{}. <b>{}</b>\nState: {}; progress: {} / {}\nUploaded: {}; ratio: {}; up: {}",
        index,
        html_escape(&truncate_for_telegram(&torrent.name, 120)),
        html_escape(&torrent.state),
        format_bytes(torrent.progress_bytes),
        format_bytes(torrent.total_bytes),
        format_bytes(torrent.uploaded_bytes),
        format_ratio(torrent.uploaded_bytes, torrent.total_bytes),
        html_escape(&torrent.upload_speed)
    )
}

fn format_ratio(uploaded_bytes: u64, total_bytes: u64) -> String {
    if total_bytes == 0 {
        return "N/A".to_string();
    }
    format!("{:.2}", uploaded_bytes as f64 / total_bytes as f64)
}

fn topic_title_link(title: &str, topic_url: &str) -> String {
    format!(
        "<a href=\"{}\">{}</a>",
        html_escape(topic_url),
        html_escape(title)
    )
}

fn downloaded_file_caption(
    title: &str,
    topic_url: &str,
    size_bytes: u64,
    completed: usize,
    total: usize,
) -> String {
    let suffix = format!(" ({}) - file {completed}/{total}", format_bytes(size_bytes));
    let escaped_url = html_escape(topic_url);
    let fixed_chars = "<a href=\"".len()
        + escaped_url.chars().count()
        + "\">".len()
        + "</a>".len()
        + suffix.chars().count();
    let title_budget = TELEGRAM_CAPTION_LIMIT_CHARS.saturating_sub(fixed_chars);
    format!(
        "<a href=\"{escaped_url}\">{}</a>{suffix}",
        html_escape_truncated_to(title, title_budget)
    )
}

fn message_has_section(text: Option<&str>, section: &str) -> bool {
    text.is_some_and(|text| text.lines().any(|line| line.trim() == section))
}

fn message_has_files_section(text: Option<&str>) -> bool {
    text.is_some_and(|text| text.lines().any(is_files_section_start))
}

fn is_files_section_start(line: &str) -> bool {
    let line = line.trim();
    line.starts_with("Files from first post:") || line.starts_with("Files from torrent metadata")
}

fn current_message_with_files(current_text: Option<&str>, files_text: &str) -> Option<String> {
    let current_text = current_text
        .map(str::trim)
        .filter(|text| !text.is_empty())?;
    let base = current_text
        .lines()
        .take_while(|line| !is_files_section_start(line))
        .collect::<Vec<_>>()
        .join("\n");
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    let text = format!("{}\n\n{files_text}", html_escape(base));
    if text.chars().count() <= TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS {
        Some(text)
    } else {
        None
    }
}

fn should_preserve_callback_message_body(
    progress: Option<ProgressMessage>,
    current_text: Option<&str>,
) -> bool {
    progress.is_some()
        && match current_text {
            Some(text) => text.trim().is_empty(),
            None => true,
        }
}

#[cfg(test)]
fn resolving_files_text(current_text: Option<&str>) -> String {
    let Some(current_text) = current_text.map(str::trim).filter(|text| !text.is_empty()) else {
        return RESOLVING_FILES_STATUS_TEXT.to_string();
    };
    let with_status = format!("{current_text}\n\n{RESOLVING_FILES_STATUS_TEXT}");
    if with_status.chars().count() <= TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS {
        with_status
    } else {
        RESOLVING_FILES_STATUS_TEXT.to_string()
    }
}

fn all_files_button_label(topic: &TopicDetails, max_file_mb: u64) -> String {
    let safe_bytes = telegram_safe_file_bytes(max_file_mb);
    let has_oversized_file = topic
        .first_post_files
        .iter()
        .any(|file| file.size_bytes.is_some_and(|size| size > safe_bytes));
    all_files_button_label_for_oversized(has_oversized_file, max_file_mb)
}

fn all_files_button_label_from_metadata(metadata: &TorrentMetadata, max_file_mb: u64) -> String {
    let safe_bytes = telegram_safe_file_bytes(max_file_mb);
    let has_oversized_file = metadata
        .files
        .iter()
        .any(|file| file.size_bytes > safe_bytes);
    all_files_button_label_for_oversized(has_oversized_file, max_file_mb)
}

fn all_files_button_label_for_oversized(has_oversized_file: bool, max_file_mb: u64) -> String {
    if has_oversized_file {
        format!("All under {}", upload_limit_label(max_file_mb))
    } else {
        "All".to_string()
    }
}

fn download_prompt_reply_markup(
    reply_markup: Option<Value>,
    topic_id: u64,
    all_files_label: &str,
) -> Option<Value> {
    let choice_row = vec![
        callback_button(all_files_label, &format!("dlall:{topic_id}")),
        callback_button("Select", &format!("sel:{topic_id}")),
    ];
    let Some(mut reply_markup) = reply_markup else {
        return Some(inline_keyboard(vec![choice_row]));
    };
    let Some(rows) = reply_markup
        .get_mut("inline_keyboard")
        .and_then(Value::as_array_mut)
    else {
        return Some(inline_keyboard(vec![choice_row]));
    };

    let download_callback = format!("dl:{topic_id}");
    let mut filtered_rows = Vec::with_capacity(rows.len() + 1);
    let mut insert_index = None;
    for row in std::mem::take(rows) {
        match row {
            Value::Array(buttons) => {
                let mut removed_download = false;
                let buttons = buttons
                    .into_iter()
                    .filter(|button| {
                        let should_remove = button.get("callback_data").and_then(Value::as_str)
                            == Some(download_callback.as_str());
                        removed_download |= should_remove;
                        !should_remove
                    })
                    .collect::<Vec<_>>();
                if removed_download && insert_index.is_none() {
                    insert_index = Some(filtered_rows.len());
                }
                if !buttons.is_empty() {
                    filtered_rows.push(Value::Array(buttons));
                }
            }
            other => filtered_rows.push(other),
        }
    }

    let insert_index = insert_index.unwrap_or(0).min(filtered_rows.len());
    filtered_rows.insert(insert_index, Value::Array(choice_row));
    *rows = filtered_rows;
    Some(reply_markup)
}

fn remove_callback_button(reply_markup: Option<Value>, callback_data: &str) -> Option<Value> {
    let mut reply_markup = reply_markup?;
    let Some(rows) = reply_markup
        .get_mut("inline_keyboard")
        .and_then(Value::as_array_mut)
    else {
        return Some(reply_markup);
    };

    let mut filtered_rows = Vec::with_capacity(rows.len());
    for row in std::mem::take(rows) {
        match row {
            Value::Array(buttons) => {
                let buttons = buttons
                    .into_iter()
                    .filter(|button| {
                        button.get("callback_data").and_then(Value::as_str) != Some(callback_data)
                    })
                    .collect::<Vec<_>>();
                if !buttons.is_empty() {
                    filtered_rows.push(Value::Array(buttons));
                }
            }
            other => filtered_rows.push(other),
        }
    }
    *rows = filtered_rows;
    Some(reply_markup)
}

fn format_topic_metadata_lines(topic: &TopicDetails) -> String {
    let mut lines = Vec::new();
    if let Some(release_type) = topic.release_type.as_ref() {
        if release_type.trim().to_lowercase() == "авторская" {
            lines.push("<b>Uploaded by author</b>".to_string());
        } else {
            lines.push(format!("Тип: {}", html_escape(release_type)));
        }
    }
    if let Some(publication_date) = topic.publication_date.as_ref() {
        lines.push(format!("Published: {}", html_escape(publication_date)));
    }
    lines.join("\n")
}

fn format_search_metadata_lines(result: &SearchResult) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(published) = result.published.as_ref() {
        lines.push(format!("Published: {}", html_escape(published)));
    }
    if result.local_catalog {
        lines.push("Found in local XML catalog fallback; comments are unavailable.".to_string());
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn author_release_search_result(
    fallback: &SearchResult,
    release: &TopicDetails,
    topic_url: String,
    category_url: Option<String>,
) -> SearchResult {
    let mut result = fallback.clone();
    result.topic_id = release.topic_id;
    if !release.title.is_empty() {
        result.title = release.title.clone();
    }
    result.topic_url = topic_url;
    if let Some(category) = release.category_path.last().cloned() {
        result.category = Some(category);
        result.category_url = category_url;
    }
    if let Some(size) = release.total_size_bytes {
        result.size_bytes = size;
    }
    if let Some(seeds) = release.seeds {
        result.seeds = seeds;
    }
    if let Some(downloads) = release.downloads {
        result.downloads = downloads;
    }
    if let Some(magnet) = release.magnet.as_ref() {
        result.magnet = Some(magnet.clone());
    }
    if let Some(published) = release.publication_date.as_ref() {
        result.published = Some(published.clone());
    }
    if let Some(author) = release.author.as_ref() {
        result.author = Some(author.name.clone());
        result.author_profile_url = author.profile_url.clone();
    }
    result
}

fn author_description_callback(wrapper_topic_id: u64, release_topic_id: u64) -> String {
    format!("authdesc:{wrapper_topic_id}:{release_topic_id}")
}

fn parse_author_description_callback(value: &str) -> Option<(u64, u64)> {
    let rest = value.strip_prefix("authdesc:")?;
    let (wrapper_topic_id, release_topic_id) = rest.split_once(':')?;
    Some((parse_u64(wrapper_topic_id)?, parse_u64(release_topic_id)?))
}

fn combined_author_description(wrapper: &TopicDetails, release: &TopicDetails) -> String {
    let mut sections = Vec::new();
    if !wrapper.description_text.trim().is_empty() {
        sections.push(format!(
            "== Author release page ==\n{}",
            wrapper.description_text.trim()
        ));
    }
    if !release.description_text.trim().is_empty() {
        sections.push(format!(
            "== Topic page ==\n{}",
            release.description_text.trim()
        ));
    }
    sections.join("\n\n")
}

fn combined_author_spoiler_image_sections(
    wrapper: &TopicDetails,
    release: &TopicDetails,
) -> Vec<TopicSpoilerImages> {
    wrapper
        .first_post_spoiler_image_sections
        .iter()
        .chain(release.first_post_spoiler_image_sections.iter())
        .cloned()
        .collect()
}

fn fallback_description_spoiler_images(
    sections: &[TopicSpoilerImages],
    delivery: RichMessageDelivery,
    embedded_images: usize,
) -> Vec<String> {
    let skip = match delivery {
        RichMessageDelivery::Rich => embedded_images,
        RichMessageDelivery::TextFallback => 0,
    };
    skip_spoiler_image_sections(sections, skip)
}

fn spoiler_image_count(sections: &[TopicSpoilerImages]) -> usize {
    sections.iter().map(|section| section.images.len()).sum()
}

fn take_spoiler_image_sections(
    sections: &[TopicSpoilerImages],
    limit: usize,
) -> Vec<TopicSpoilerImages> {
    let mut remaining = limit;
    let mut out = Vec::new();
    for section in sections {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(section.images.len());
        if take > 0 {
            out.push(TopicSpoilerImages {
                title: section.title.clone(),
                images: section.images.iter().take(take).cloned().collect(),
            });
            remaining -= take;
        }
    }
    out
}

fn skip_spoiler_image_sections(sections: &[TopicSpoilerImages], skip: usize) -> Vec<String> {
    sections
        .iter()
        .flat_map(|section| section.images.iter())
        .skip(skip)
        .cloned()
        .collect()
}

fn rich_description_html(
    description: &str,
    limit: usize,
    image_sections: &[TopicSpoilerImages],
) -> String {
    let mut out = if description.trim().is_empty() {
        "Description is empty or unavailable.".to_string()
    } else {
        format_description_html_with_rich_images(
            &truncate_for_telegram(description, limit),
            image_sections,
        )
    };
    if description.trim().is_empty() {
        let image_html = format_rich_spoiler_image_sections(image_sections);
        if !out.ends_with("\n\n") {
            if out.ends_with('\n') {
                out.push('\n');
            } else {
                out.push_str("\n\n");
            }
        }
        out.push_str(&image_html);
    }
    out
}

fn format_description_html_with_rich_images(
    description: &str,
    image_sections: &[TopicSpoilerImages],
) -> String {
    let lines = description.lines().collect::<Vec<_>>();
    let mut used_sections = vec![false; image_sections.len()];
    let mut out = String::new();
    let mut index = 0;
    while index < lines.len() {
        if let Some(title) = spoiler_title(lines[index]) {
            if !out.is_empty() && !out.ends_with("\n\n") {
                if out.ends_with('\n') {
                    out.push('\n');
                } else {
                    out.push_str("\n\n");
                }
            }
            out.push_str("<blockquote><b>");
            out.push_str(&html_escape(title));
            out.push_str("</b>");
            index += 1;
            while index < lines.len() {
                if lines[index].trim().is_empty() || spoiler_title(lines[index]).is_some() {
                    break;
                }
                out.push('\n');
                out.push_str(&html_escape(lines[index]));
                index += 1;
            }
            out.push_str("</blockquote>");
            if let Some(section_index) =
                matching_spoiler_image_section(image_sections, &used_sections, title)
            {
                used_sections[section_index] = true;
                push_rich_spoiler_image_tags(&mut out, &image_sections[section_index].images);
            }
            while index < lines.len() && lines[index].trim().is_empty() {
                index += 1;
            }
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&html_escape(lines[index]));
        index += 1;
    }

    // Image-only spoilers do not appear in description_text, so keep them as
    // explicit media blocks after the textual description.
    for (section_index, section) in image_sections.iter().enumerate() {
        if used_sections[section_index] {
            continue;
        }
        push_rich_spoiler_image_section(&mut out, section);
    }
    out
}

fn matching_spoiler_image_section(
    sections: &[TopicSpoilerImages],
    used_sections: &[bool],
    title: &str,
) -> Option<usize> {
    let title = compact_spoiler_title(title);
    sections
        .iter()
        .enumerate()
        .find(|(index, section)| {
            !used_sections[*index] && compact_spoiler_title(&section.title) == title
        })
        .map(|(index, _)| index)
}

fn compact_spoiler_title(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn format_rich_spoiler_image_sections(sections: &[TopicSpoilerImages]) -> String {
    let mut out = String::new();
    for section in sections {
        push_rich_spoiler_image_section(&mut out, section);
    }
    out
}

fn push_rich_spoiler_image_section(out: &mut String, section: &TopicSpoilerImages) {
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    if !section.title.trim().is_empty() {
        out.push_str("<blockquote><b>");
        out.push_str(&html_escape(section.title.trim()));
        out.push_str("</b></blockquote>");
    }
    push_rich_spoiler_image_tags(out, &section.images);
}

fn push_rich_spoiler_image_tags(out: &mut String, images: &[String]) {
    for image in images {
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("<img src=\"");
        out.push_str(&html_escape(image));
        out.push_str("\"/>");
    }
}

fn rich_message_payload_char_count(value: &str) -> usize {
    value.chars().map(|ch| if ch == '\n' { 4 } else { 1 }).sum()
}

fn format_description_html(description: &str) -> String {
    let lines = description.lines().collect::<Vec<_>>();
    let mut out = String::new();
    let mut index = 0;
    while index < lines.len() {
        if let Some(title) = spoiler_title(lines[index]) {
            if !out.is_empty() && !out.ends_with("\n\n") {
                if out.ends_with('\n') {
                    out.push('\n');
                } else {
                    out.push_str("\n\n");
                }
            }
            out.push_str("<blockquote><b>");
            out.push_str(&html_escape(title));
            out.push_str("</b>");
            index += 1;
            while index < lines.len() {
                if lines[index].trim().is_empty() || spoiler_title(lines[index]).is_some() {
                    break;
                }
                out.push('\n');
                out.push_str(&html_escape(lines[index]));
                index += 1;
            }
            out.push_str("</blockquote>");
            while index < lines.len() && lines[index].trim().is_empty() {
                index += 1;
            }
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&html_escape(lines[index]));
        index += 1;
    }
    out
}

fn spoiler_title(line: &str) -> Option<&str> {
    let line = line.trim();
    line.strip_prefix("== ")
        .and_then(|line| line.strip_suffix(" =="))
        .map(str::trim)
        .filter(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::PathBuf;

    use crate::config::Config;
    use crate::downloader::SeedTorrentStats;
    use crate::rutracker::{
        AuthorDetails, CategoryRef, RutrackerClient, SearchResult, TopicComment, TopicFile,
        TopicSpoilerImages,
    };
    use crate::telegram::{
        ProgressMessage, RichMessageDelivery, TELEGRAM_CAPTION_LIMIT_CHARS,
        TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS, TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT,
        TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS, Telegram,
    };
    use crate::torrent::{TorrentFile, TorrentMetadata};

    use super::{
        App, DownloadPromptLimit, RUTRACKER_NEWS_URL, RUTRACKER_UNAVAILABLE_TEXT, TopicDetails,
        all_files_button_label, author_description_callback, author_name_link,
        author_release_search_result, callback_button, category_latest_callback,
        combined_author_description, comments_next_keyboard, comments_text,
        current_message_with_files, download_prompt_reply_markup, download_prompt_text,
        downloaded_file_caption, fallback_description_spoiler_images, files_from_topic_text,
        first_post_image_url, format_author, format_bytes, format_description_html,
        format_rich_spoiler_image_sections, format_topic_metadata_lines, help_text,
        image_cache_proxy_url, inline_keyboard, is_author_command, mark_update_seen,
        message_has_files_section, message_has_section, metadata_download_prompt_limit,
        now_seconds, parse_author_description_callback, parse_comments_callback,
        remove_callback_button, resolving_files_text, rich_message_payload_char_count,
        seed_stats_text, selection_file_button_label, should_preserve_callback_message_body,
        telegram_command_name, topic_download_prompt_limit, topic_title_link,
        verify_vm_worker_signature, vm_worker_signature,
    };

    fn test_app() -> App {
        let base_urls = vec!["https://rutracker.org/forum".to_string()];
        let config = Config {
            telegram_bot_token: "telegram-token".to_string(),
            telegram_api_base_url: "https://api.telegram.org".to_string(),
            telegram_webhook_secret: "webhook-secret".to_string(),
            allowed_telegram_user_ids: HashSet::new(),
            rutracker_base_urls: base_urls.clone(),
            rutracker_cookie: None,
            rutracker_username: None,
            rutracker_password: None,
            search_limit: 10,
            http_timeout_seconds: 1,
            http_max_attempts: 1,
            tmp_dir: PathBuf::from("/tmp"),
            image_cache_public_base_url: None,
            image_cache_dir: PathBuf::from("/tmp/image-cache"),
            max_file_mb: 50,
            lambda_timeout_seconds: 900,
            download_margin_seconds: 20,
            download_countdown_label: "AWS Lambda lifetime".to_string(),
            download_status_interval_seconds: 60,
            peer_limit: 120,
            seed_torrents: false,
            torrent_listen_port: 49152,
            seed_disk_reserve_mb: 0,
            rutracker_catalog_path: None,
            rutracker_catalog_xml_topic_id: crate::catalog::DEFAULT_RUTRACKER_CATALOG_TOPIC_ID,
        };
        let rutracker = RutrackerClient::new(&base_urls, None, None, 1, 1).unwrap();
        App {
            config,
            telegram: Telegram::new(
                "telegram-token".to_string(),
                "https://api.telegram.org".to_string(),
            ),
            rutracker,
            offline_catalog: None,
        }
    }

    #[test]
    fn formats_bytes() {
        assert_eq!(format_bytes(42), "42 B");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
    }

    #[test]
    fn parses_telegram_command_name() {
        assert_eq!(
            telegram_command_name("/help@bot test").as_deref(),
            Some("help")
        );
        assert_eq!(
            telegram_command_name("/stat@bot test").as_deref(),
            Some("stat")
        );
        assert_eq!(
            telegram_command_name("/author@bot").as_deref(),
            Some("author")
        );
        assert!(is_author_command("/author"));
        assert_eq!(telegram_command_name("hello"), None);
    }

    #[test]
    fn parses_author_description_callbacks() {
        let callback = author_description_callback(6860489, 6844147);

        assert_eq!(callback, "authdesc:6860489:6844147");
        assert_eq!(
            parse_author_description_callback(&callback),
            Some((6860489, 6844147))
        );
        assert_eq!(parse_author_description_callback("authdesc:6860489"), None);
        assert_eq!(parse_author_description_callback("desc:6860489"), None);
    }

    #[test]
    fn author_release_result_uses_linked_topic_metadata() {
        let fallback = SearchResult {
            topic_id: 6860489,
            title: "ROOKAVA wrapper".to_string(),
            author: Some("hagnir".to_string()),
            author_profile_url: None,
            category: Some(CategoryRef {
                id: 1538,
                name: "Авторские раздачи".to_string(),
            }),
            size_bytes: 0,
            seeds: 0,
            downloads: 0,
            topic_url: "https://rutracker.org/forum/viewtopic.php?t=6860489".to_string(),
            category_url: Some("https://rutracker.org/forum/viewforum.php?f=1538".to_string()),
            magnet: None,
            published: None,
            local_catalog: false,
        };
        let release = TopicDetails {
            topic_id: 6844147,
            title: "ROOKAVA - 9 singles [FLAC]".to_string(),
            author: Some(AuthorDetails {
                name: "Аndy".to_string(),
                profile_url: Some(
                    "https://rutracker.org/forum/profile.php?mode=viewprofile&u=4474847"
                        .to_string(),
                ),
                posts_count: None,
                avatar_url: None,
            }),
            category_path: vec![CategoryRef {
                id: 1825,
                name: "lossless".to_string(),
            }],
            total_size_bytes: Some(308_176_486),
            seeds: Some(7),
            downloads: Some(42),
            magnet: Some("magnet:?xt=urn:btih:test".to_string()),
            publication_date: Some("2026-april-11".to_string()),
            ..TopicDetails::default()
        };

        let result = author_release_search_result(
            &fallback,
            &release,
            "https://rutracker.org/forum/viewtopic.php?t=6844147".to_string(),
            Some("https://rutracker.org/forum/viewforum.php?f=1825".to_string()),
        );

        assert_eq!(result.topic_id, 6844147);
        assert_eq!(result.title, "ROOKAVA - 9 singles [FLAC]");
        assert_eq!(result.author.as_deref(), Some("Аndy"));
        assert_eq!(result.category.as_ref().unwrap().id, 1825);
        assert_eq!(result.size_bytes, 308_176_486);
        assert_eq!(result.seeds, 7);
        assert_eq!(result.downloads, 42);
        assert_eq!(result.magnet.as_deref(), Some("magnet:?xt=urn:btih:test"));
    }

    #[test]
    fn combines_author_wrapper_and_release_descriptions() {
        let wrapper = TopicDetails {
            description_text: "Wrapper text from author releases.".to_string(),
            ..TopicDetails::default()
        };
        let release = TopicDetails {
            description_text: "Release topic text.".to_string(),
            ..TopicDetails::default()
        };

        assert_eq!(
            combined_author_description(&wrapper, &release),
            "== Author release page ==\nWrapper text from author releases.\n\n== Topic page ==\nRelease topic text."
        );
    }

    #[test]
    fn formats_seed_stats_text() {
        let text = seed_stats_text(&[SeedTorrentStats {
            id: 7,
            name: "Album & Singles".to_string(),
            state: "live".to_string(),
            progress_bytes: 50 * 1024 * 1024,
            total_bytes: 50 * 1024 * 1024,
            uploaded_bytes: 75 * 1024 * 1024,
            upload_speed: "1.25 MiB/s".to_string(),
        }]);

        assert!(text.contains("<b>Seeding from VM</b>"));
        assert!(text.contains("<b>Album &amp; Singles</b>"));
        assert!(text.contains("Uploaded: 75.0 MB; ratio: 1.50; up: 1.25 MiB/s"));
    }

    #[test]
    fn seed_stats_text_stays_within_telegram_limit() {
        let stats = (0..200)
            .map(|index| SeedTorrentStats {
                id: index,
                name: format!("Torrent {index} {}", "long name ".repeat(20)),
                state: "live".to_string(),
                progress_bytes: 1024 * 1024,
                total_bytes: 1024 * 1024,
                uploaded_bytes: 0,
                upload_speed: "0.00 MiB/s".to_string(),
            })
            .collect::<Vec<_>>();

        let text = seed_stats_text(&stats);

        assert!(text.chars().count() <= TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(text.contains("more torrents"));
    }

    #[test]
    fn formats_topic_title_as_link() {
        assert_eq!(
            topic_title_link(
                "A & B",
                "https://rutracker.org/forum/viewtopic.php?t=42&x=1"
            ),
            "<a href=\"https://rutracker.org/forum/viewtopic.php?t=42&amp;x=1\">A &amp; B</a>"
        );
    }

    #[test]
    fn formats_downloaded_file_caption_as_source_link() {
        let caption = downloaded_file_caption(
            "Album & Singles",
            "https://rutracker.org/forum/viewtopic.php?t=42",
            12 * 1024 * 1024,
            2,
            5,
        );

        assert!(caption.starts_with(
            "<a href=\"https://rutracker.org/forum/viewtopic.php?t=42\">Album &amp; Singles</a>"
        ));
        assert!(caption.contains("12.0 MB"));
        assert!(caption.contains("file 2/5"));
        assert!(caption.chars().count() <= TELEGRAM_CAPTION_LIMIT_CHARS);
    }

    #[test]
    fn formats_author_name_as_profile_link() {
        assert_eq!(
            author_name_link(
                "artist & author",
                Some("https://rutracker.org/forum/profile.php?mode=viewprofile&u=12")
            ),
            "<a href=\"https://rutracker.org/forum/profile.php?mode=viewprofile&amp;u=12\">artist &amp; author</a>"
        );
    }

    #[test]
    fn detects_plain_text_sections() {
        assert!(message_has_section(
            Some("Title\n\nDescription\ntext"),
            "Description"
        ));
        assert!(!message_has_section(
            Some("Title with Description word"),
            "Description"
        ));
    }

    #[test]
    fn preserves_callback_message_when_body_text_is_unavailable() {
        let progress = Some(ProgressMessage {
            chat_id: 7,
            message_id: 42,
        });

        assert!(should_preserve_callback_message_body(progress, None));
        assert!(should_preserve_callback_message_body(progress, Some("  ")));
        assert!(!should_preserve_callback_message_body(
            progress,
            Some("Title\nCategory: Music")
        ));
        assert!(!should_preserve_callback_message_body(None, None));
    }

    #[test]
    fn detects_file_sections() {
        assert!(message_has_files_section(Some(
            "Title\n\nFiles from first post: Album\ntrack.flac - 30 MB"
        )));
        assert!(message_has_files_section(Some(
            "Title\n\nFiles from torrent metadata\ntrack.flac - 30 MB"
        )));
        assert!(!message_has_files_section(Some(
            "Title\n\nDownload\nSelect files"
        )));
    }

    #[test]
    fn appends_local_files_to_current_message() {
        let text = current_message_with_files(
            Some("Title & Artist\nCategory: Music"),
            "<b>Files from torrent metadata</b>\ntrack.flac — 30.0 MB",
        )
        .unwrap();

        assert!(text.starts_with("Title &amp; Artist\nCategory: Music"));
        assert!(text.contains("<b>Files from torrent metadata</b>"));
        assert!(text.contains("track.flac"));
    }

    #[test]
    fn replaces_existing_files_section_in_current_message() {
        let text = current_message_with_files(
            Some("Title\n\nFiles from torrent metadata\nold.flac — 1.0 MB"),
            "<b>Files from torrent metadata</b>\nnew.flac — 2.0 MB",
        )
        .unwrap();

        assert!(text.contains("Title"));
        assert!(text.contains("new.flac"));
        assert!(!text.contains("old.flac"));
    }

    #[test]
    fn resolving_files_text_preserves_current_message_when_it_fits() {
        let text = resolving_files_text(Some("Title\nCategory: Music"));

        assert!(text.starts_with("Title\nCategory: Music"));
        assert!(text.contains("Resolving magnet metadata for file list..."));
    }

    #[test]
    fn resolving_files_text_falls_back_when_current_message_is_too_large() {
        let text = resolving_files_text(Some(&"x".repeat(TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS)));

        assert_eq!(
            text,
            "Resolving magnet metadata for file list... This can take up to 30 seconds."
        );
    }

    #[test]
    fn download_prompt_preserves_existing_files_section_when_it_fits() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            category_path: vec![CategoryRef {
                id: 23,
                name: "Music".to_string(),
            }],
            description_text: "Track list".to_string(),
            first_post_files: vec![TopicFile {
                path: "track.flac".to_string(),
                size_bytes: Some(30 * 1024 * 1024),
            }],
            ..TopicDetails::default()
        };
        let files = files_from_topic_text(&topic, app.config.max_file_mb);

        let text = app
            .topic_message_with_download_prompt(
                &topic,
                true,
                Some(&files),
                topic_download_prompt_limit(&topic, app.config.max_file_mb),
            )
            .unwrap();

        assert!(text.contains("<b>Description</b>"));
        assert!(text.contains("<b>Download</b>"));
        assert!(text.contains("Files from first post"));
        assert!(text.contains("track.flac"));
        assert!(text.contains("Choose all files, or select specific files"));
        assert!(!text.contains("Choose all files under"));
    }

    #[test]
    fn download_prompt_drops_existing_files_section_when_it_would_not_fit() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            category_path: vec![CategoryRef {
                id: 23,
                name: "Music".to_string(),
            }],
            description_text: "Track list".to_string(),
            ..TopicDetails::default()
        };
        let files = format!(
            "<b>Files from first post: Album</b>\ntrack.flac\n{}",
            "x".repeat(TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS)
        );

        let text = app
            .topic_message_with_download_prompt(
                &topic,
                true,
                Some(&files),
                topic_download_prompt_limit(&topic, app.config.max_file_mb),
            )
            .unwrap();

        assert!(text.contains("<b>Description</b>"));
        assert!(text.contains("<b>Download</b>"));
        assert!(!text.contains("Files from first post"));
        assert!(!text.contains("track.flac"));
    }

    #[test]
    fn download_prompt_mentions_limit_when_topic_has_oversized_file() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            first_post_files: vec![
                TopicFile {
                    path: "small.flac".to_string(),
                    size_bytes: Some(30 * 1024 * 1024),
                },
                TopicFile {
                    path: "huge.flac".to_string(),
                    size_bytes: Some(75 * 1024 * 1024),
                },
            ],
            ..TopicDetails::default()
        };

        let text = app
            .topic_message_with_download_prompt(
                &topic,
                false,
                None,
                topic_download_prompt_limit(&topic, app.config.max_file_mb),
            )
            .unwrap();

        assert!(text.contains("Some files are larger than 50 MB"));
        assert!(text.contains("Choose all files under 50 MB"));
    }

    #[test]
    fn download_prompt_keeps_limit_when_topic_file_sizes_are_unknown() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            first_post_files: vec![TopicFile {
                path: "track.flac".to_string(),
                size_bytes: None,
            }],
            ..TopicDetails::default()
        };

        assert_eq!(
            topic_download_prompt_limit(&topic, app.config.max_file_mb),
            DownloadPromptLimit::Unknown
        );
        assert!(
            app.topic_message_with_download_prompt(
                &topic,
                false,
                None,
                topic_download_prompt_limit(&topic, app.config.max_file_mb),
            )
            .unwrap()
            .contains("Choose all files under 50 MB")
        );
    }

    #[test]
    fn download_prompt_uses_metadata_limit_status() {
        let fit_metadata = TorrentMetadata {
            name: "Album".to_string(),
            info_hash: "hash".to_string(),
            magnet: None,
            files: vec![TorrentFile {
                index: 0,
                path: "track.flac".into(),
                size_bytes: 30 * 1024 * 1024,
            }],
            torrent_bytes: None,
        };
        let oversized_metadata = TorrentMetadata {
            files: vec![TorrentFile {
                index: 0,
                path: "huge.flac".into(),
                size_bytes: 75 * 1024 * 1024,
            }],
            ..fit_metadata.clone()
        };

        assert_eq!(
            download_prompt_text(50, metadata_download_prompt_limit(&fit_metadata, 50)),
            "<b>Download</b>\nChoose all files, or select specific files from torrent metadata."
        );
        assert!(
            download_prompt_text(50, metadata_download_prompt_limit(&oversized_metadata, 50))
                .contains("Some files are larger than 50 MB")
        );
    }

    #[test]
    fn marks_duplicate_updates_seen() {
        let update_id = i64::MIN + 4242;
        assert!(mark_update_seen(update_id));
        assert!(!mark_update_seen(update_id));
    }

    #[test]
    fn replaces_download_button_with_choice_buttons() {
        let markup = inline_keyboard(vec![
            vec![
                callback_button("Description", "desc:42"),
                callback_button("Comments", "comments:42:1"),
                callback_button("Files", "files:42"),
            ],
            vec![callback_button("Download", "dl:42")],
        ]);

        let filtered = download_prompt_reply_markup(Some(markup), 42, "All under 50 MB").unwrap();
        let rows = filtered
            .get("inline_keyboard")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[1][0]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("dlall:42")
        );
        assert_eq!(
            rows[1][1]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("sel:42")
        );
        assert_eq!(
            rows[0][0]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("desc:42")
        );
    }

    #[test]
    fn labels_download_all_button_from_known_file_sizes() {
        let all_files_fit_topic = TopicDetails {
            first_post_files: vec![TopicFile {
                path: "track.mp3".to_string(),
                size_bytes: Some(10 * 1024 * 1024),
            }],
            ..TopicDetails::default()
        };
        let oversized_file_topic = TopicDetails {
            first_post_files: vec![
                TopicFile {
                    path: "track.mp3".to_string(),
                    size_bytes: Some(10 * 1024 * 1024),
                },
                TopicFile {
                    path: "video.mkv".to_string(),
                    size_bytes: Some(100 * 1024 * 1024),
                },
            ],
            ..TopicDetails::default()
        };

        assert_eq!(all_files_button_label(&all_files_fit_topic, 50), "All");
        assert_eq!(
            all_files_button_label(&oversized_file_topic, 50),
            "All under 50 MB"
        );
        assert_eq!(all_files_button_label(&oversized_file_topic, 2000), "All");
    }

    #[test]
    fn selection_file_buttons_use_names_and_checkmark() {
        let file = TorrentFile {
            index: 3,
            path: PathBuf::from("Album/track.flac"),
            size_bytes: 30 * 1024 * 1024,
        };

        assert_eq!(
            selection_file_button_label(&file, false),
            "track.flac (30.0 MB)"
        );
        assert_eq!(
            selection_file_button_label(&file, true),
            "✅ track.flac (30.0 MB)"
        );
    }

    #[test]
    fn category_buttons_open_recent_topics() {
        assert_eq!(category_latest_callback(441), "cat:441");
    }

    #[test]
    fn first_post_image_does_not_depend_on_caption_length() {
        let topic = TopicDetails {
            first_post_images: vec![
                "https://rutracker.org/image-one.jpg".to_string(),
                "https://rutracker.org/image-two.jpg".to_string(),
            ],
            ..TopicDetails::default()
        };

        assert_eq!(
            first_post_image_url(Some(&topic)),
            Some("https://rutracker.org/image-one.jpg")
        );
        assert_eq!(first_post_image_url(None), None);
    }

    #[test]
    fn removes_used_callback_button_from_reply_markup() {
        let markup = inline_keyboard(vec![
            vec![
                callback_button("Description", "desc:42"),
                callback_button("Comments", "comments:42:1"),
                callback_button("Files", "files:42"),
            ],
            vec![callback_button("Download", "dl:42")],
        ]);

        let filtered = remove_callback_button(Some(markup), "desc:42").unwrap();
        let rows = filtered
            .get("inline_keyboard")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0][0]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("comments:42:1")
        );
        assert_eq!(rows[0].as_array().unwrap().len(), 2);
        assert_eq!(
            rows[0][1]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("files:42")
        );
        assert_eq!(
            rows[1][0]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("dl:42")
        );

        let filtered = remove_callback_button(
            Some(inline_keyboard(vec![vec![callback_button(
                "Files", "files:42",
            )]])),
            "files:42",
        )
        .unwrap();
        let rows = filtered
            .get("inline_keyboard")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn formats_topic_metadata_lines() {
        let topic = TopicDetails {
            release_type: Some("авторская".to_string()),
            publication_date: Some("2026-june-07".to_string()),
            ..TopicDetails::default()
        };
        assert_eq!(
            format_topic_metadata_lines(&topic),
            "<b>Uploaded by author</b>\nPublished: 2026-june-07"
        );
    }

    #[test]
    fn places_user_before_topic_metadata() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            category_path: vec![CategoryRef {
                id: 23,
                name: "Music".to_string(),
            }],
            author: Some(AuthorDetails {
                name: "artist".to_string(),
                profile_url: Some(
                    "https://rutracker.org/forum/profile.php?mode=viewprofile&u=12".to_string(),
                ),
                posts_count: Some(25),
                avatar_url: None,
            }),
            release_type: Some("авторская".to_string()),
            publication_date: Some("2026-june-07".to_string()),
            ..TopicDetails::default()
        };

        let text = app.topic_message_header(&topic).unwrap();

        assert!(text.contains("Category:"));
        assert!(text.contains("User:"));
        assert!(text.contains("<b>Uploaded by author</b>"));
        assert!(text.find("Category:").unwrap() < text.find("User:").unwrap());
        assert!(text.find("User:").unwrap() < text.find("<b>Uploaded by author</b>").unwrap());
    }

    #[test]
    fn rich_description_message_can_exceed_classic_message_limit() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Long Album".to_string(),
            category_path: vec![CategoryRef {
                id: 23,
                name: "Music".to_string(),
            }],
            total_size_bytes: Some(1024),
            seeds: Some(1),
            downloads: Some(2),
            ..TopicDetails::default()
        };
        let description = "Long description line.\n".repeat(350);

        let text = app
            .topic_message_with_rich_description(&topic, &description, &[])
            .unwrap()
            .text;

        assert!(text.chars().count() > TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(text.chars().count() <= TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(rich_message_payload_char_count(&text) <= TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(text.contains("Long description line."));
    }

    #[test]
    fn rich_description_embeds_spoiler_images() {
        let app = test_app();
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            first_post_spoiler_image_sections: vec![TopicSpoilerImages {
                title: "2026 - Single".to_string(),
                images: vec![
                    "https://img.example/cover.jpg".to_string(),
                    "https://img.example/back.jpg?x=1&y=2".to_string(),
                ],
            }],
            ..TopicDetails::default()
        };

        let message = app
            .topic_message_with_rich_description(
                &topic,
                "Album description.",
                &topic.first_post_spoiler_image_sections,
            )
            .unwrap();

        assert_eq!(message.embedded_spoiler_images, 2);
        assert!(
            message
                .text
                .contains("<blockquote><b>2026 - Single</b></blockquote>")
        );
        assert!(
            message
                .text
                .contains("<img src=\"https://img.example/cover.jpg\"/>")
        );
        assert!(
            message
                .text
                .contains("<img src=\"https://img.example/back.jpg?x=1&amp;y=2\"/>")
        );
    }

    #[test]
    fn rich_description_places_spoiler_images_after_matching_text() {
        let app = test_app();
        let sections = vec![
            TopicSpoilerImages {
                title: "2026 - Лиса".to_string(),
                images: vec!["https://img.example/lisa.jpg".to_string()],
            },
            TopicSpoilerImages {
                title: "2026 - Волк".to_string(),
                images: vec!["https://img.example/wolf.jpg".to_string()],
            },
        ];
        let topic = TopicDetails {
            topic_id: 42,
            title: "ROOKAVA".to_string(),
            first_post_spoiler_image_sections: sections.clone(),
            ..TopicDetails::default()
        };
        let description = concat!(
            "== 2026 - Лиса ==\n",
            "ROOKAVA - Лиса [5:05]\n",
            "Слияние электроники, фолка и загадки.\n\n",
            "== 2026 - Волк ==\n",
            "ROOKAVA - Волк [4:42]\n",
            "Темный фолк-техно трек."
        );

        let message = app
            .topic_message_with_rich_description(&topic, description, &sections)
            .unwrap();

        let lisa_text = message.text.find("ROOKAVA - Лиса").unwrap();
        let lisa_image = message
            .text
            .find("<img src=\"https://img.example/lisa.jpg\"/>")
            .unwrap();
        let wolf_text = message.text.find("ROOKAVA - Волк").unwrap();
        let wolf_image = message
            .text
            .find("<img src=\"https://img.example/wolf.jpg\"/>")
            .unwrap();

        assert!(lisa_text < lisa_image);
        assert!(lisa_image < wolf_text);
        assert!(wolf_text < wolf_image);
        assert_eq!(message.embedded_spoiler_images, 2);
    }

    #[test]
    fn formats_rich_spoiler_images_as_separate_media_blocks() {
        let sections = vec![TopicSpoilerImages {
            title: "Scans & Covers".to_string(),
            images: vec![
                "https://img.example/cover.jpg?x=1&y=2".to_string(),
                "https://img.example/back.jpg".to_string(),
            ],
        }];

        assert_eq!(
            format_rich_spoiler_image_sections(&sections),
            "<blockquote><b>Scans &amp; Covers</b></blockquote>\n<img src=\"https://img.example/cover.jpg?x=1&amp;y=2\"/>\n<img src=\"https://img.example/back.jpg\"/>"
        );
    }

    #[test]
    fn rich_description_sends_remaining_spoiler_images_separately() {
        let app = test_app();
        let sections = vec![TopicSpoilerImages {
            title: "Albums".to_string(),
            images: (0..(TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT + 2))
                .map(|index| format!("https://img.example/{index}.jpg"))
                .collect(),
        }];
        let topic = TopicDetails {
            topic_id: 42,
            title: "Album".to_string(),
            first_post_spoiler_image_sections: sections.clone(),
            ..TopicDetails::default()
        };

        let message = app
            .topic_message_with_rich_description(&topic, "Album description.", &sections)
            .unwrap();

        assert_eq!(
            message.embedded_spoiler_images,
            TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT
        );
        assert!(
            message
                .text
                .contains("<img src=\"https://img.example/49.jpg\"/>")
        );
        assert!(
            !message
                .text
                .contains("<img src=\"https://img.example/50.jpg\"/>")
        );
        assert_eq!(
            fallback_description_spoiler_images(
                &sections,
                RichMessageDelivery::Rich,
                message.embedded_spoiler_images,
            ),
            vec![
                "https://img.example/50.jpg".to_string(),
                "https://img.example/51.jpg".to_string(),
            ]
        );
        assert_eq!(
            fallback_description_spoiler_images(
                &sections,
                RichMessageDelivery::TextFallback,
                message.embedded_spoiler_images,
            )
            .len(),
            TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT + 2
        );
    }

    #[test]
    fn formats_comments_text() {
        let topic = TopicDetails {
            comments_page: 2,
            comments_total_pages: 6,
            comments: vec![TopicComment {
                author: Some(AuthorDetails {
                    name: "listener".to_string(),
                    profile_url: Some(
                        "https://rutracker.org/forum/profile.php?mode=viewprofile&u=77".to_string(),
                    ),
                    posts_count: Some(12),
                    avatar_url: None,
                }),
                text: "Nice & legal release".to_string(),
                html: "Nice <a href=\"https://example.invalid/release\">legal release</a>"
                    .to_string(),
            }],
            ..TopicDetails::default()
        };

        let comments = comments_text(&topic, 0);

        assert!(comments.text.starts_with("<b>Comments 2/6</b>"));
        assert!(comments.text.contains("listener"));
        assert!(comments.text.contains("(12 messages)"));
        assert!(
            comments
                .text
                .contains("Nice <a href=\"https://example.invalid/release\">legal release</a>")
        );
        assert_eq!(comments.next_comment_index, None);
    }

    #[test]
    fn keeps_long_comment_html_until_message_limit() {
        let long_link_text = "legal ".repeat(250);
        let topic = TopicDetails {
            comments_page: 1,
            comments_total_pages: 1,
            comments: vec![TopicComment {
                author: None,
                text: long_link_text.clone(),
                html: format!("<a href=\"https://example.invalid/release\">{long_link_text}</a>"),
            }],
            ..TopicDetails::default()
        };

        let comments = comments_text(&topic, 0);

        assert!(
            comments
                .text
                .contains("<a href=\"https://example.invalid/release\">")
        );
        assert!(comments.text.contains(&long_link_text));
        assert_eq!(comments.next_comment_index, None);
    }

    #[test]
    fn truncates_only_single_comment_that_exceeds_message_limit() {
        let huge = "comment & ".repeat(800);
        let topic = TopicDetails {
            comments_page: 1,
            comments_total_pages: 1,
            comments: vec![TopicComment {
                author: None,
                text: huge,
                html: "x".repeat(TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS + 100),
            }],
            ..TopicDetails::default()
        };

        let comments = comments_text(&topic, 0);

        assert!(comments.text.chars().count() <= TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(comments.text.contains("comment &amp;"));
        assert!(comments.text.ends_with('…'));
        assert_eq!(comments.next_comment_index, None);
    }

    #[test]
    fn paginates_comments_when_message_limit_would_be_exceeded() {
        let topic = TopicDetails {
            topic_id: 42,
            comments_page: 1,
            comments_total_pages: 3,
            comments: vec![
                TopicComment {
                    author: None,
                    text: "a".repeat(1800),
                    html: "a".repeat(1800),
                },
                TopicComment {
                    author: None,
                    text: "b".repeat(1800),
                    html: "b".repeat(1800),
                },
                TopicComment {
                    author: None,
                    text: "c".repeat(1800),
                    html: "c".repeat(1800),
                },
            ],
            ..TopicDetails::default()
        };

        let first = comments_text(&topic, 0);
        let second = comments_text(&topic, first.next_comment_index.unwrap());
        let same_page_markup = comments_next_keyboard(&topic, first.next_comment_index).unwrap();
        let next_page_markup = comments_next_keyboard(&topic, second.next_comment_index).unwrap();

        assert_eq!(first.next_comment_index, Some(2));
        assert!(first.text.contains(&"a".repeat(1800)));
        assert!(first.text.contains(&"b".repeat(1800)));
        assert!(!first.text.contains(&"c".repeat(1800)));
        assert_eq!(second.next_comment_index, None);
        assert!(second.text.contains(&"c".repeat(1800)));
        assert!(
            serde_json::to_string(&same_page_markup)
                .unwrap()
                .contains("comments:42:1:2")
        );
        assert!(
            serde_json::to_string(&next_page_markup)
                .unwrap()
                .contains("comments:42:2:0")
        );
    }

    #[test]
    fn parses_comment_callbacks_with_optional_offset() {
        assert_eq!(parse_comments_callback("comments:42:2"), Some((42, 2, 0)));
        assert_eq!(parse_comments_callback("comments:42:2:5"), Some((42, 2, 5)));
    }

    #[test]
    fn omits_author_avatar_from_metadata() {
        let author = AuthorDetails {
            name: "artist".to_string(),
            profile_url: Some(
                "https://rutracker.org/forum/profile.php?mode=viewprofile&u=12".to_string(),
            ),
            posts_count: Some(184),
            avatar_url: Some("https://rutracker.org/avatar.jpg".to_string()),
        };

        let text = format_author(&author);

        assert!(text.contains("(184 messages)"));
        assert!(!text.contains("avatar"));
        assert!(!text.contains("avatar: open"));
    }

    #[test]
    fn formats_spoilers_as_blockquotes() {
        let description = "Track list\n\n== Об исполнителе (группе) ==\nДепрессивно & честно.\n\nДоп. информация: source";

        assert_eq!(
            format_description_html(description),
            "Track list\n\n<blockquote><b>Об исполнителе (группе)</b>\nДепрессивно &amp; честно.</blockquote>\nДоп. информация: source"
        );
    }

    #[test]
    fn rutracker_unavailable_message_points_to_news_channel() {
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains("bot backend"));
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains("@rutracker_news"));
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains(RUTRACKER_NEWS_URL));
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains(".\nRuTracker runs"));
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains("donating"));
    }

    #[test]
    fn signs_and_verifies_vm_worker_payloads() {
        let payload = br#"{"update_id":1}"#;
        let timestamp = now_seconds().to_string();
        let signature = vm_worker_signature("secret", &timestamp, payload).unwrap();

        verify_vm_worker_signature("secret", &timestamp, &signature, payload).unwrap();
        assert!(verify_vm_worker_signature("other", &timestamp, &signature, payload).is_err());
        assert!(verify_vm_worker_signature("secret", &timestamp, "sha256=bad", payload).is_err());
    }

    #[test]
    fn builds_image_cache_proxy_url_from_vm_worker_url() {
        let file_name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg";

        assert_eq!(
            image_cache_proxy_url("http://203.0.113.10:8080/telegram", file_name).unwrap(),
            "http://203.0.113.10:8080/image-cache/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg"
        );
        assert!(image_cache_proxy_url("http://203.0.113.10:8080/telegram", "../bad.jpg").is_err());
    }

    #[test]
    fn help_mentions_limits_unofficial_status_and_donations() {
        let help = help_text(50);
        assert!(help.contains("https://github.com/ikatson/rqbit"));
        assert!(help.contains("https://rutracker.org/forum/viewtopic.php?t=5591249"));
        assert!(help.contains("local SQLite index built from RuTracker's XML dump fallback"));
        assert!(help.contains("<code>/author</code>"));
        assert!(help.contains("<code>/stat</code>"));
        assert!(help.contains("Category buttons return the 10 most recent topics"));
        assert!(help.contains(
            "<a href=\"https://core.telegram.org/bots/api#senddocument\">sendDocument</a>"
        ));
        assert!(
            help.contains(
                "https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html"
            )
        );
        assert!(help.contains("at most 15 minutes"));
        assert!(!help.contains("900 seconds"));
        assert!(help.contains("unofficial bot"));
        assert!(help.contains("donating"));
        assert!(help.contains("seed legal torrents"));
        assert!(help.contains("favorite artists and authors"));
        assert!(help.contains("culture preservation"));
        assert!(help.contains("Creative Commons"));
        assert!(help.contains("indie artist"));
        assert!(help.contains("publish it legally on RuTracker"));
    }

    #[test]
    fn help_mentions_local_bot_api_for_larger_uploads() {
        let help = help_text(2000);
        assert!(help.contains("2000 MB"));
        assert!(help.contains("https://github.com/tdlib/telegram-bot-api"));
    }
}
