use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::warn;

// Telegram documents message text as 1-4096 characters after entity parsing.
// https://core.telegram.org/bots/api#editmessagetext
pub const TELEGRAM_MESSAGE_TEXT_LIMIT_CHARS: usize = 4096;

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
    http: reqwest::Client,
}

pub struct ChatActionHeartbeat {
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
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
    pub fn new(token: String) -> Self {
        Self {
            token,
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

    pub async fn send_message_value(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<Value>,
        parse_mode: Option<&str>,
    ) -> Result<Value> {
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
        self.telegram_json_value("sendMessage", payload).await
    }

    pub async fn send_status_message(&self, chat_id: i64, text: &str) -> Result<ProgressMessage> {
        let value = self
            .send_message_value(chat_id, text, None, Some("HTML"))
            .await?;
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
        total_seconds: u64,
    ) -> StatusCountdown {
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = stop.clone();
        let telegram = self.clone();
        let started_at = Instant::now();
        let task = tokio::spawn(async move {
            let mut last_minutes = u64::MAX;
            while !task_stop.load(Ordering::Relaxed) {
                let elapsed = started_at.elapsed().as_secs();
                let remaining = total_seconds.saturating_sub(elapsed);
                let minutes = remaining.div_ceil(60);
                if minutes != last_minutes {
                    last_minutes = minutes;
                    let text = countdown_status_text(prefix, minutes.max(1));
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
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let body = reqwest::Body::wrap_stream(stream);
        let mime = mime_guess::from_path(path)
            .first_raw()
            .unwrap_or("application/octet-stream");
        let mut form = Form::new().text("chat_id", chat_id.to_string()).part(
            "document",
            Part::stream(body)
                .file_name(display_name.to_string())
                .mime_str(mime)?,
        );
        if let Some(caption) = caption {
            form = form.text("caption", truncate_for_telegram(caption, 1024));
        }
        self.http
            .post(self.method_url("sendDocument"))
            .multipart(form)
            .send()
            .await?
            .error_for_status()?;
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
            payload["caption"] = json!(truncate_for_telegram(caption, 1024));
            payload["parse_mode"] = json!("HTML");
        }
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        self.telegram_json_value("sendPhoto", payload)
            .await
            .map(|_| ())
    }

    async fn telegram_json_value(&self, method: &str, payload: Value) -> Result<Value> {
        let response = self
            .http
            .post(self.method_url(method))
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;
        let value = response.json::<Value>().await?;
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(anyhow!("Telegram {method} returned {value}"));
        }
        Ok(value)
    }

    fn method_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{method}", self.token)
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

pub fn countdown_status_text(prefix: &str, minutes_left: u64) -> String {
    format!(
        "{prefix}\nAWS Lambda lifetime: {} minutes left",
        minutes_left.max(1)
    )
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
    use super::countdown_status_text;

    #[test]
    fn formats_countdown_with_lambda_lifetime() {
        assert_eq!(
            countdown_status_text("Downloading selected files...", 15),
            "Downloading selected files...\nAWS Lambda lifetime: 15 minutes left"
        );
    }
}
