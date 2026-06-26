use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::{info, warn};

// Telegram documents message text as 1-4096 characters after entity parsing.
// https://core.telegram.org/bots/api#editmessagetext
pub const TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS: usize = 4096;
// Telegram rich messages are limited to 32768 UTF-8 characters.
// https://core.telegram.org/bots/api#rich-message-limits
pub const TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS: usize = 32_768;
// Telegram rich messages can include up to 50 media blocks.
// https://core.telegram.org/bots/api#rich-message-limits
pub const TELEGRAM_RICH_MESSAGE_MEDIA_LIMIT: usize = 50;
// Telegram captions are limited to 0-1024 characters after entity parsing.
// https://core.telegram.org/bots/api#sendphoto
pub const TELEGRAM_CAPTION_LIMIT_CHARS: usize = 1024;
// Telegram Bot API documents sendPhoto uploads as limited to 10 MB.
// https://core.telegram.org/bots/api#sendphoto
const TELEGRAM_PHOTO_UPLOAD_MAX_BYTES: u64 = 10 * 1024 * 1024;
const TELEGRAM_RICH_REQUEST_TIMEOUT_SECONDS: u64 = 10;

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
    pub inline_query: Option<InlineQuery>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub from: Option<User>,
    pub chat: Chat,
    pub text: Option<String>,
    pub reply_markup: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    pub from: User,
    pub message: Option<Message>,
    pub inline_message_id: Option<String>,
    pub data: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InlineQuery {
    pub id: String,
    pub from: User,
    pub query: String,
}

#[derive(Clone, Copy, Debug)]
pub struct ProgressMessage {
    pub chat_id: i64,
    pub message_id: i64,
}

#[derive(Clone)]
pub struct Telegram {
    token: String,
    api_base_url: String,
    http: reqwest::Client,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlayableUploadKind {
    Audio,
    Video,
}

pub struct ChatActionHeartbeat {
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RichMessageDelivery {
    Rich,
    TextFallback,
}

pub struct StatusCountdown {
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl Drop for ChatActionHeartbeat {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for StatusCountdown {
    fn drop(&mut self) {
        self.stop();
    }
}

impl ChatActionHeartbeat {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

impl StatusCountdown {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

impl Telegram {
    pub fn new(token: String, api_base_url: String) -> Self {
        Self {
            token,
            api_base_url,
            http: reqwest::Client::new(),
        }
    }

    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        self.send_message_value(chat_id, text, reply_markup, Some("HTML"))
            .await
            .map(|_| ())
    }

    /// Sends a message as a Telegram reply to an existing message, keeping
    /// secondary content visually grouped with the result it belongs to.
    pub async fn send_message_reply_to(
        &self,
        chat_id: i64,
        reply_to_message_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let mut payload = self.send_message_payload(chat_id, text, reply_markup, Some("HTML"));
        payload["reply_to_message_id"] = json!(reply_to_message_id);
        payload["allow_sending_without_reply"] = json!(true);
        self.telegram_json_value("sendMessage", payload)
            .await
            .map(|_| ())
    }

    pub async fn send_message_value(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
        parse_mode: Option<&str>,
    ) -> Result<Value> {
        let payload = self.send_message_payload(chat_id, text, reply_markup, parse_mode);
        self.telegram_json_value("sendMessage", payload).await
    }

    pub async fn send_rich_message_or_text(
        &self,
        chat_id: i64,
        rich_html: &str,
        fallback_html: &str,
        reply_markup: Option<Value>,
    ) -> Result<RichMessageDelivery> {
        let payload = self.send_rich_message_payload(chat_id, rich_html, reply_markup.clone());
        match tokio::time::timeout(
            Duration::from_secs(TELEGRAM_RICH_REQUEST_TIMEOUT_SECONDS),
            self.telegram_json_value("sendRichMessage", payload),
        )
        .await
        {
            Err(_) => {
                warn!("timed out sending Telegram rich message; falling back to sendMessage");
                self.send_message(chat_id, fallback_html, reply_markup)
                    .await
                    .map(|_| RichMessageDelivery::TextFallback)
            }
            Ok(Ok(_)) => Ok(RichMessageDelivery::Rich),
            Ok(Err(err)) => {
                warn!(error = %err, "failed to send Telegram rich message; falling back to sendMessage");
                self.send_message(chat_id, fallback_html, reply_markup)
                    .await
                    .map(|_| RichMessageDelivery::TextFallback)
            }
        }
    }

    /// Builds the shared sendMessage JSON payload so normal messages and
    /// reply messages keep identical parsing, previews, and truncation.
    fn send_message_payload(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
        parse_mode: Option<&str>,
    ) -> Value {
        let mut payload = json!({
            "chat_id": chat_id,
            "text": truncate_for_telegram(text, TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS),
            "disable_web_page_preview": true
        });
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        if let Some(parse_mode) = parse_mode {
            payload["parse_mode"] = json!(parse_mode);
        }
        payload
    }

    fn send_rich_message_payload(
        &self,
        chat_id: i64,
        html: &str,
        reply_markup: Option<Value>,
    ) -> Value {
        let html = rich_message_html(html);
        log_rich_message_html_diagnostic("sendRichMessage", &html);
        let mut payload = json!({
            "chat_id": chat_id,
            "rich_message": {
                "html": html,
            }
        });
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        payload
    }

    pub async fn send_status_message(&self, chat_id: i64, text: &str) -> Result<ProgressMessage> {
        let value = self
            .send_message_value(chat_id, text, None, Some("HTML"))
            .await?;
        progress_message_from_send_response(chat_id, value)
    }

    pub async fn send_status_message_reply_to(
        &self,
        chat_id: i64,
        reply_to_message_id: i64,
        text: &str,
    ) -> Result<ProgressMessage> {
        let mut payload = self.send_message_payload(chat_id, text, None, Some("HTML"));
        payload["reply_to_message_id"] = json!(reply_to_message_id);
        payload["allow_sending_without_reply"] = json!(true);
        let value = self.telegram_json_value("sendMessage", payload).await?;
        progress_message_from_send_response(chat_id, value)
    }

    pub async fn edit_message(
        &self,
        progress: ProgressMessage,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let mut payload = json!({
            "chat_id": progress.chat_id,
            "message_id": progress.message_id,
            "text": truncate_for_telegram(text, TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS),
            "disable_web_page_preview": true,
            "parse_mode": "HTML"
        });
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        self.telegram_json_value("editMessageText", payload)
            .await
            .map(|_| ())
    }

    pub async fn edit_rich_message_or_text(
        &self,
        progress: ProgressMessage,
        rich_html: &str,
        fallback_html: &str,
        reply_markup: Option<Value>,
    ) -> Result<RichMessageDelivery> {
        let html = rich_message_html(rich_html);
        log_rich_message_html_diagnostic("editMessageText", &html);
        let mut payload = json!({
            "chat_id": progress.chat_id,
            "message_id": progress.message_id,
            "rich_message": {
                "html": html,
            }
        });
        if let Some(markup) = reply_markup.clone() {
            payload["reply_markup"] = markup;
        }
        match tokio::time::timeout(
            Duration::from_secs(TELEGRAM_RICH_REQUEST_TIMEOUT_SECONDS),
            self.telegram_json_value("editMessageText", payload),
        )
        .await
        {
            Err(_) => {
                warn!("timed out editing Telegram rich message; falling back to editMessageText");
                self.edit_message(progress, fallback_html, reply_markup)
                    .await
                    .map(|_| RichMessageDelivery::TextFallback)
            }
            Ok(Ok(_)) => Ok(RichMessageDelivery::Rich),
            Ok(Err(err)) => {
                warn!(error = %err, "failed to edit Telegram rich message; falling back to editMessageText");
                self.edit_message(progress, fallback_html, reply_markup)
                    .await
                    .map(|_| RichMessageDelivery::TextFallback)
            }
        }
    }

    pub async fn edit_message_reply_markup(
        &self,
        progress: ProgressMessage,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let mut payload = json!({
            "chat_id": progress.chat_id,
            "message_id": progress.message_id,
        });
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        self.telegram_json_value("editMessageReplyMarkup", payload)
            .await
            .map(|_| ())
    }

    pub async fn try_edit_message(&self, progress: ProgressMessage, text: &str) {
        if let Err(err) = self.edit_message(progress, text, None).await {
            warn!(error = %err, "failed to edit Telegram status message");
        }
    }

    pub async fn delete_message(&self, progress: ProgressMessage) -> Result<()> {
        self.telegram_json_value(
            "deleteMessage",
            json!({
                "chat_id": progress.chat_id,
                "message_id": progress.message_id,
            }),
        )
        .await
        .map(|_| ())
    }

    pub async fn try_delete_message(&self, progress: ProgressMessage) {
        if let Err(err) = self.delete_message(progress).await {
            warn!(error = %err, "failed to delete Telegram status message");
        }
    }

    pub async fn answer_callback(&self, callback_id: &str, text: &str) -> Result<()> {
        self.telegram_json_value(
            "answerCallbackQuery",
            json!({
                "callback_query_id": callback_id,
                "text": text,
            }),
        )
        .await
        .map(|_| ())
    }

    pub async fn answer_inline_query(
        &self,
        inline_query_id: &str,
        results: Vec<Value>,
    ) -> Result<()> {
        self.telegram_json_value(
            "answerInlineQuery",
            json!({
                "inline_query_id": inline_query_id,
                "results": results,
                "cache_time": 0,
                "is_personal": true,
            }),
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_webhook(&self) -> Result<()> {
        self.telegram_json_value(
            "deleteWebhook",
            json!({
                "drop_pending_updates": false,
            }),
        )
        .await
        .map(|_| ())
    }

    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_seconds: u64,
    ) -> Result<Vec<Update>> {
        let mut payload = json!({
            "timeout": timeout_seconds,
            "allowed_updates": ["message", "callback_query", "inline_query"],
        });
        if let Some(offset) = offset {
            payload["offset"] = json!(offset);
        }
        let value = self.telegram_json_value("getUpdates", payload).await?;
        let result = value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("Telegram getUpdates response has no result"))?;
        serde_json::from_value(result).context("failed to parse Telegram updates")
    }

    pub async fn send_chat_action(&self, chat_id: i64, action: &str) -> Result<()> {
        self.telegram_json_value(
            "sendChatAction",
            json!({
                "chat_id": chat_id,
                "action": action,
            }),
        )
        .await
        .map(|_| ())
    }

    pub fn start_chat_action_heartbeat(
        &self,
        chat_id: i64,
        action: &'static str,
    ) -> ChatActionHeartbeat {
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = stop.clone();
        let telegram = self.clone();
        let task = tokio::spawn(async move {
            while !task_stop.load(Ordering::Relaxed) {
                let _ = telegram.send_chat_action(chat_id, action).await;
                tokio::time::sleep(Duration::from_secs(4)).await;
            }
        });
        ChatActionHeartbeat { stop, task }
    }

    pub fn start_status_countdown(
        &self,
        progress: ProgressMessage,
        prefix: &'static str,
        label: String,
        update_interval_seconds: u64,
        total_seconds: u64,
    ) -> StatusCountdown {
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = stop.clone();
        let telegram = self.clone();
        let started_at = Instant::now();
        let task = tokio::spawn(async move {
            let update_interval_seconds = update_interval_seconds.max(1);
            let mut last_update_bucket = u64::MAX;
            while !task_stop.load(Ordering::Relaxed) {
                let elapsed = started_at.elapsed().as_secs();
                let remaining = total_seconds.saturating_sub(elapsed);
                let minutes = remaining.div_ceil(60);
                let update_bucket = elapsed / update_interval_seconds;
                if update_bucket != last_update_bucket {
                    last_update_bucket = update_bucket;
                    let text = countdown_status_text(prefix, &label, minutes.max(1));
                    telegram.try_edit_message(progress, &text).await;
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
        StatusCountdown { stop, task }
    }

    pub async fn send_document(
        &self,
        chat_id: i64,
        path: &Path,
        display_name: &str,
        caption: Option<&str>,
    ) -> Result<()> {
        self.send_multipart_file(
            "sendDocument",
            "document",
            chat_id,
            path,
            display_name,
            caption,
        )
        .await
    }

    pub async fn send_playable_or_document(
        &self,
        chat_id: i64,
        path: &Path,
        display_name: &str,
        caption: Option<&str>,
    ) -> Result<()> {
        let Some(kind) = playable_upload_kind(path, display_name) else {
            return self
                .send_document(chat_id, path, display_name, caption)
                .await;
        };
        let (method, field) = match kind {
            PlayableUploadKind::Audio => ("sendAudio", "audio"),
            PlayableUploadKind::Video => ("sendVideo", "video"),
        };
        let media_error = match self
            .send_multipart_file(method, field, chat_id, path, display_name, caption)
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        warn!(
            display_name,
            method,
            error = %media_error,
            "Telegram rejected playable media upload; sending as document"
        );
        self.send_document(chat_id, path, display_name, caption)
            .await
            .with_context(|| {
                format!(
                    "Telegram rejected {method} upload ({media_error}); document fallback failed"
                )
            })
    }

    async fn send_multipart_file(
        &self,
        method: &str,
        file_field: &str,
        chat_id: i64,
        path: &Path,
        display_name: &str,
        caption: Option<&str>,
    ) -> Result<()> {
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let body = reqwest::Body::wrap_stream(stream);
        let mime = upload_mime(path, display_name);
        let mut form = Form::new().text("chat_id", chat_id.to_string()).part(
            file_field.to_string(),
            Part::stream(body)
                .file_name(display_name.to_string())
                .mime_str(mime)?,
        );
        if let Some(caption) = caption {
            form = form
                .text(
                    "caption",
                    truncate_for_telegram(caption, TELEGRAM_CAPTION_LIMIT_CHARS),
                )
                .text("parse_mode", "HTML");
        }
        let response = self
            .http
            .post(self.method_url(method))
            .multipart(form)
            .send()
            .await
            .map_err(|err| anyhow!("Telegram {method} request failed: {}", err.without_url()))?;
        self.telegram_status_response(method, response).await?;
        Ok(())
    }

    pub async fn send_photo_url(
        &self,
        chat_id: i64,
        photo_url: &str,
        caption: Option<&str>,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let mut payload = json!({
            "chat_id": chat_id,
            "photo": photo_url,
        });
        if let Some(caption) = caption {
            payload["caption"] =
                json!(truncate_for_telegram(caption, TELEGRAM_CAPTION_LIMIT_CHARS));
            payload["parse_mode"] = json!("HTML");
        }
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        self.telegram_json_value("sendPhoto", payload)
            .await
            .map(|_| ())
    }

    /// Sends a photo by URL, falling back to multipart upload when Telegram
    /// cannot fetch the remote URL itself.
    ///
    /// This matters for RuTracker first-post images: some hosts are reachable
    /// from the VM but rejected by Telegram/local Bot API as remote URLs.
    pub async fn send_photo_url_or_upload(
        &self,
        chat_id: i64,
        photo_url: &str,
        caption: Option<&str>,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let url_error = match self
            .send_photo_url(chat_id, photo_url, caption, reply_markup.clone())
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        self.send_photo_upload_from_url(chat_id, photo_url, caption, reply_markup)
            .await
            .with_context(|| {
                format!("Telegram rejected image URL ({url_error}); upload fallback failed")
            })
    }

    async fn send_photo_upload_from_url(
        &self,
        chat_id: i64,
        photo_url: &str,
        caption: Option<&str>,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        // Download through the bot process, then upload as InputFile. This keeps
        // search result images working even when sendPhoto rejects the URL form.
        let response = self
            .http
            .get(photo_url)
            .send()
            .await
            .map_err(|err| anyhow!("failed to download image URL: {}", err.without_url()))?;
        let response = self
            .http_status_response("image download", response)
            .await?;
        if let Some(length) = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            && length > TELEGRAM_PHOTO_UPLOAD_MAX_BYTES
        {
            bail!("image is too large for sendPhoto: {length} bytes");
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = response
            .bytes()
            .await
            .map_err(|err| anyhow!("failed to read image bytes: {}", err.without_url()))?;
        if bytes.len() as u64 > TELEGRAM_PHOTO_UPLOAD_MAX_BYTES {
            bail!("image is too large for sendPhoto: {} bytes", bytes.len());
        }
        let file_name = remote_file_name(photo_url);
        let mime = image_mime_for_upload(content_type.as_deref(), &file_name)?;
        let mut form = Form::new().text("chat_id", chat_id.to_string()).part(
            "photo",
            Part::bytes(bytes.to_vec())
                .file_name(file_name)
                .mime_str(&mime)?,
        );
        if let Some(caption) = caption {
            form = form
                .text(
                    "caption",
                    truncate_for_telegram(caption, TELEGRAM_CAPTION_LIMIT_CHARS),
                )
                .text("parse_mode", "HTML");
        }
        if let Some(markup) = reply_markup {
            form = form.text("reply_markup", serde_json::to_string(&markup)?);
        }
        let response = self
            .http
            .post(self.method_url("sendPhoto"))
            .multipart(form)
            .send()
            .await
            .map_err(|err| {
                anyhow!(
                    "Telegram sendPhoto upload request failed: {}",
                    err.without_url()
                )
            })?;
        self.telegram_status_response("sendPhoto", response).await?;
        Ok(())
    }

    async fn telegram_json_value(&self, method: &str, payload: Value) -> Result<Value> {
        let response = self
            .http
            .post(self.method_url(method))
            .json(&payload)
            .send()
            .await
            .map_err(|err| anyhow!("Telegram {method} request failed: {}", err.without_url()))?;
        let response = self.telegram_status_response(method, response).await?;
        let value = response.json::<Value>().await?;
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(anyhow!("Telegram {method} returned {value}"));
        }
        Ok(value)
    }

    async fn telegram_status_response(
        &self,
        method: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        self.status_response(&format!("Telegram {method}"), response)
            .await
    }

    async fn http_status_response(
        &self,
        label: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        self.status_response(label, response).await
    }

    async fn status_response(
        &self,
        label: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        // Do not use reqwest::error_for_status here: its error includes the
        // full request URL, which contains the Telegram bot token.
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        bail!("{label} returned HTTP {status}: {body}")
    }

    fn method_url(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", self.api_base_url, self.token)
    }
}

pub fn html_escape(value: &str) -> String {
    html_escape::encode_text(value).to_string()
}

pub fn truncate_for_telegram(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    let mut truncated = value
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn progress_message_from_send_response(chat_id: i64, value: Value) -> Result<ProgressMessage> {
    let message_id = value
        .get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("Telegram sendMessage response has no result.message_id"))?;
    Ok(ProgressMessage {
        chat_id,
        message_id,
    })
}

fn rich_message_html(value: &str) -> String {
    let mut html = String::with_capacity(value.len());
    for (index, ch) in value.char_indices() {
        if ch == '\n' && starts_with_rich_block(value[index + ch.len_utf8()..].trim_start()) {
            html.push('\n');
        } else if ch == '\n' {
            html.push_str("<br>");
        } else {
            html.push(ch);
        }
    }
    truncate_for_telegram(&html, TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS)
}

fn log_rich_message_html_diagnostic(method: &str, html: &str) {
    let image_count = html.matches("<img").count();
    let video_count = html.matches("<video").count();
    let audio_count = html.matches("<audio").count();
    if image_count == 0 && video_count == 0 && audio_count == 0 {
        return;
    }
    let media_start = first_media_tag_start(html);
    let first_media_url = media_start
        .and_then(|start| first_media_src(&html[start..]))
        .unwrap_or_default();
    let media_context = media_start
        .map(|start| log_context_around(html, start, 180, 420))
        .unwrap_or_else(|| log_snippet(html, 600));
    info!(
        method,
        chars = html.chars().count(),
        image_count,
        video_count,
        audio_count,
        first_media_url = %first_media_url,
        media_context = %media_context,
        "prepared Telegram rich message HTML"
    );
}

fn first_media_tag_start(html: &str) -> Option<usize> {
    ["<img", "<video", "<audio"]
        .iter()
        .filter_map(|tag| html.find(tag))
        .min()
}

fn first_media_src(tag_and_tail: &str) -> Option<String> {
    let tag_end = tag_and_tail.find('>').unwrap_or(tag_and_tail.len());
    let tag = &tag_and_tail[..tag_end];
    quoted_attr(tag, "src=\"", '"').or_else(|| quoted_attr(tag, "src='", '\''))
}

fn quoted_attr(value: &str, needle: &str, quote: char) -> Option<String> {
    let start = value.find(needle)? + needle.len();
    let end = value[start..].find(quote)? + start;
    Some(value[start..end].to_string())
}

fn log_context_around(value: &str, byte_start: usize, before: usize, after: usize) -> String {
    let prefix = value[..byte_start]
        .chars()
        .rev()
        .take(before)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    let suffix = value[byte_start..].chars().take(after).collect::<String>();
    let mut snippet = String::new();
    if value[..byte_start].chars().count() > before {
        snippet.push('…');
    }
    snippet.push_str(&prefix);
    snippet.push_str(&suffix);
    if value[byte_start..].chars().count() > after {
        snippet.push('…');
    }
    sanitize_log_snippet(&snippet)
}

fn log_snippet(value: &str, limit: usize) -> String {
    let mut snippet = value.chars().take(limit).collect::<String>();
    if value.chars().count() > limit {
        snippet.push('…');
    }
    sanitize_log_snippet(&snippet)
}

fn sanitize_log_snippet(value: &str) -> String {
    value
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn starts_with_rich_block(value: &str) -> bool {
    [
        "<audio",
        "<blockquote",
        "<details",
        "<figure",
        "<footer",
        "<h1",
        "<h2",
        "<h3",
        "<h4",
        "<h5",
        "<h6",
        "<hr",
        "<img",
        "<ol",
        "<p",
        "<pre",
        "<table",
        "<tg-collage",
        "<tg-map",
        "<tg-math-block",
        "<tg-slideshow",
        "<ul",
        "<video",
    ]
    .iter()
    .any(|prefix| value.starts_with(prefix))
}

fn remote_file_name(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| {
            url.path_segments()
                .and_then(|mut segments| segments.next_back().map(str::to_string))
        })
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "photo.jpg".to_string())
}

fn image_mime_for_upload(content_type: Option<&str>, file_name: &str) -> Result<String> {
    if let Some(content_type) = content_type.and_then(|value| value.split(';').next()) {
        let content_type = content_type.trim();
        if content_type.starts_with("image/") {
            return Ok(content_type.to_string());
        }
        if content_type == "application/octet-stream"
            && let Some(mime) = mime_guess::from_path(file_name).first_raw()
            && mime.starts_with("image/")
        {
            return Ok(mime.to_string());
        }
        bail!("downloaded URL is not an image: {content_type}");
    }
    mime_guess::from_path(file_name)
        .first_raw()
        .filter(|mime| mime.starts_with("image/"))
        .map(str::to_string)
        .ok_or_else(|| anyhow!("downloaded URL has no image content type"))
}

fn upload_mime(path: &Path, display_name: &str) -> &'static str {
    mime_guess::from_path(display_name)
        .first_raw()
        .or_else(|| mime_guess::from_path(path).first_raw())
        .unwrap_or("application/octet-stream")
}

fn playable_upload_kind(path: &Path, display_name: &str) -> Option<PlayableUploadKind> {
    let mime = upload_mime(path, display_name);
    if mime.starts_with("audio/") {
        return Some(PlayableUploadKind::Audio);
    }
    if mime.starts_with("video/") {
        return Some(PlayableUploadKind::Video);
    }

    // Telegram accepts a narrower set for playable uploads than MIME databases
    // can describe, so still try known media extensions and keep document fallback.
    let extension = upload_extension(path, display_name)?;
    match extension.as_str() {
        "aac" | "aiff" | "alac" | "ape" | "flac" | "m4a" | "mp3" | "oga" | "ogg" | "opus"
        | "wav" | "wma" | "wv" => Some(PlayableUploadKind::Audio),
        "avi" | "flv" | "m2ts" | "m4v" | "mkv" | "mov" | "mp4" | "mpeg" | "mpg" | "ts" | "webm"
        | "wmv" => Some(PlayableUploadKind::Video),
        _ => None,
    }
}

fn upload_extension(path: &Path, display_name: &str) -> Option<String> {
    Path::new(display_name)
        .extension()
        .or_else(|| path.extension())
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
}

pub fn countdown_status_text(prefix: &str, label: &str, minutes_left: u64) -> String {
    format!(
        "{prefix}\n{label}: {} left",
        format_minutes_left(minutes_left.max(1))
    )
}

fn format_minutes_left(minutes_left: u64) -> String {
    let hours = minutes_left / 60;
    let minutes = minutes_left % 60;
    match (hours, minutes) {
        (0, minutes) => pluralize(minutes, "minute"),
        (hours, 0) => pluralize(hours, "hour"),
        (hours, minutes) => format!(
            "{} {}",
            pluralize(hours, "hour"),
            pluralize(minutes, "minute")
        ),
    }
}

fn pluralize(value: u64, unit: &str) -> String {
    if value == 1 {
        format!("1 {unit}")
    } else {
        format!("{value} {unit}s")
    }
}

pub fn inline_keyboard(rows: Vec<Vec<Value>>) -> Value {
    json!({ "inline_keyboard": rows })
}

pub fn callback_button(text: &str, data: &str) -> Value {
    json!({
        "text": text,
        "callback_data": data,
    })
}

pub fn url_button(text: &str, url: &str) -> Value {
    json!({
        "text": text,
        "url": url,
    })
}

pub fn copy_button(text: &str, copy_text: &str) -> Value {
    json!({
        "text": text,
        "copy_text": {
            "text": copy_text,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        PlayableUploadKind, TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS, Telegram,
        countdown_status_text, first_media_src, first_media_tag_start, log_context_around,
        playable_upload_kind, rich_message_html,
    };

    #[test]
    fn formats_countdown_with_lambda_lifetime() {
        assert_eq!(
            countdown_status_text("Downloading selected files...", "AWS Lambda lifetime", 15),
            "Downloading selected files...\nAWS Lambda lifetime: 15 minutes left"
        );
    }

    #[test]
    fn formats_countdown_with_custom_label() {
        assert_eq!(
            countdown_status_text("Downloading selected files...", "Download time budget", 60),
            "Downloading selected files...\nDownload time budget: 1 hour left"
        );
        assert_eq!(
            countdown_status_text(
                "Downloading selected files...",
                "Download time budget",
                1439
            ),
            "Downloading selected files...\nDownload time budget: 23 hours 59 minutes left"
        );
    }

    #[test]
    fn builds_method_url_from_configured_api_base_url() {
        let telegram = Telegram::new("token".to_string(), "http://127.0.0.1:8081".to_string());
        assert_eq!(
            telegram.method_url("sendDocument"),
            "http://127.0.0.1:8081/bottoken/sendDocument"
        );
    }

    #[test]
    fn detects_playable_upload_kind_from_display_name() {
        assert_eq!(
            playable_upload_kind(Path::new("/tmp/download"), "Album/track.FLAC"),
            Some(PlayableUploadKind::Audio)
        );
        assert_eq!(
            playable_upload_kind(Path::new("/tmp/download"), "clip.mp4"),
            Some(PlayableUploadKind::Video)
        );
        assert_eq!(
            playable_upload_kind(Path::new("/tmp/download"), "book.pdf"),
            None
        );
    }

    #[test]
    fn detects_playable_upload_kind_from_download_path() {
        assert_eq!(
            playable_upload_kind(Path::new("/tmp/track.ogg"), "download"),
            Some(PlayableUploadKind::Audio)
        );
        assert_eq!(
            playable_upload_kind(Path::new("/tmp/movie.mkv"), "download"),
            Some(PlayableUploadKind::Video)
        );
    }

    #[test]
    fn builds_rich_message_payload() {
        let telegram = Telegram::new("token".to_string(), "https://api.telegram.org".to_string());
        let payload = telegram.send_rich_message_payload(42, "<b>A</b>\nB", None);

        assert_eq!(payload["chat_id"], 42);
        assert_eq!(payload["rich_message"]["html"], "<b>A</b><br>B");
    }

    #[test]
    fn truncates_rich_message_html_to_rich_limit() {
        let html = rich_message_html(&"x".repeat(TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS + 10));

        assert_eq!(html.chars().count(), TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(html.ends_with('…'));
    }

    #[test]
    fn truncates_rich_message_html_after_line_break_expansion() {
        let html = rich_message_html(&"x\n".repeat(TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS));

        assert_eq!(html.chars().count(), TELEGRAM_RICH_MESSAGE_TEXT_LIMIT_CHARS);
        assert!(html.ends_with('…'));
    }

    #[test]
    fn keeps_media_tags_as_separate_rich_blocks() {
        assert_eq!(
            rich_message_html("</blockquote>\n<img src=\"https://example.invalid/photo.jpg\"/>"),
            "</blockquote>\n<img src=\"https://example.invalid/photo.jpg\"/>"
        );
        assert_eq!(
            rich_message_html(
                "Album description.\n\n<blockquote><b>Scans</b></blockquote>\n<img src=\"https://example.invalid/a.jpg\"/>\n<img src=\"https://example.invalid/b.jpg\"/>"
            ),
            "Album description.\n\n<blockquote><b>Scans</b></blockquote>\n<img src=\"https://example.invalid/a.jpg\"/>\n<img src=\"https://example.invalid/b.jpg\"/>"
        );
        assert_eq!(
            rich_message_html("line one\nline two"),
            "line one<br>line two"
        );
    }

    #[test]
    fn extracts_media_diagnostic_from_rich_html() {
        let html = "Intro<br><blockquote><b>Scans</b></blockquote>\n<img src=\"https://example.invalid/a.jpg\"/>\n<img src='https://example.invalid/b.jpg'/>";
        let media_start = first_media_tag_start(html).unwrap();

        assert_eq!(
            first_media_src(&html[media_start..]).as_deref(),
            Some("https://example.invalid/a.jpg")
        );
        assert_eq!(
            log_context_around(html, media_start, 20, 45),
            "…ns</b></blockquote>\\n<img src=\"https://example.invalid/a.jpg\"/>\\n<i…"
        );
    }
}
