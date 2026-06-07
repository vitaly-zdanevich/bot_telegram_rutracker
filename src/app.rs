use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lambda_http::http::{Method, StatusCode};
use lambda_http::{Body, Error, IntoResponse, Request, Response};
use serde_json::Value;
use sha1::{Digest, Sha1};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::downloader::{DownloadRequest, TorrentDownloader};
use crate::rutracker::{
    AuthorDetails, RutrackerClient, RutrackerCredentials, SearchResult, TopicDetails, TopicFile,
    ensure_same_topic, require_magnet, validate_forum_query,
};
use crate::telegram::{
    CallbackQuery, InlineQuery, Message, ProgressMessage, Telegram, Update, callback_button,
    copy_button, html_escape, inline_keyboard, truncate_for_telegram, url_button,
};
use crate::telegram_safe_file_bytes;
use crate::torrent::{TorrentFile, TorrentMetadata};

const HELP_TEXT: &str = concat!(
    "This is an unofficial bot and is not affiliated with RuTracker.\n\n",
    "Send a RuTracker search string. I search rutracker.org titles and return matching topics.\n\n",
    "Use <code>c text</code> to search categories. Category buttons from normal search results rerun the same query inside that category.\n\n",
    "Downloads are limited to files under 50 MB because Telegram Bot API ",
    "<a href=\"https://core.telegram.org/bots/api#senddocument\">sendDocument</a> has that upload limit.\n\n",
    "Torrent downloads use librqbit, the rqbit torrent client library: ",
    "https://github.com/ikatson/rqbit\n\n",
    "AWS Lambda can run one invocation for at most 15 minutes: ",
    "https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html\n\n",
    "If RuTracker is unavailable, I return the official news channel link: @rutracker_news. ",
    "RuTracker runs on low-cost community infrastructure; please consider donating to them.\n\n",
    "Please seed legal torrents when you can; it helps the ecosystem. ",
    "Respect creators, and consider supporting your favorite artists and authors with money or by buying from them. ",
    "Torrents are also culture preservation: many works are public domain, Creative Commons, abandoned, or otherwise unavailable to buy legally.\n\n",
    "If you know an indie artist, consider asking whether they want to release some work under Creative Commons and publish it legally on RuTracker, so more people can discover them.\n\n",
    "Source code: https://github.com/vitaly-zdanevich/bot_telegram_rutracker"
);

const RUTRACKER_NEWS_URL: &str = "https://t.me/rutracker_news";
const RUTRACKER_UNAVAILABLE_TEXT: &str = concat!(
    "RuTracker is unavailable from this Lambda right now. ",
    "Check their official news channel for status: ",
    "<a href=\"https://t.me/rutracker_news\">@rutracker_news</a>. ",
    "RuTracker runs on low-cost community infrastructure; please consider donating to them."
);

type CacheStore<K, T> = Mutex<HashMap<K, CacheEntry<T>>>;

static QUERY_CACHE: OnceLock<Mutex<HashMap<String, QueryCacheEntry>>> = OnceLock::new();
static SEARCH_CACHE: OnceLock<CacheStore<String, Vec<SearchResult>>> = OnceLock::new();
static TOPIC_CACHE: OnceLock<CacheStore<u64, TopicDetails>> = OnceLock::new();
static COMMENT_PAGE_CACHE: OnceLock<CacheStore<(u64, u32), TopicDetails>> = OnceLock::new();
static CATEGORIES_CACHE: OnceLock<CacheStore<String, Vec<crate::rutracker::ForumNode>>> =
    OnceLock::new();
static SUBCATEGORIES_CACHE: OnceLock<CacheStore<u64, Vec<crate::rutracker::ForumNode>>> =
    OnceLock::new();
static LATEST_CACHE: OnceLock<CacheStore<u64, Vec<SearchResult>>> = OnceLock::new();
static METADATA_CACHE: OnceLock<CacheStore<u64, TorrentMetadata>> = OnceLock::new();
static DOWNLOAD_SELECTION_CACHE: OnceLock<CacheStore<String, DownloadSelection>> = OnceLock::new();

const RAM_CACHE_TTL_SECONDS: u64 = 30 * 60;
const SELECTION_PAGE_SIZE: usize = 8;

#[derive(Clone)]
struct QueryCacheEntry {
    query: String,
    created_at: u64,
}

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

pub async fn handler(request: Request) -> Result<impl IntoResponse, Error> {
    let config = Config::from_env()?;

    if request.method() != Method::POST {
        return Ok(response(StatusCode::OK, "ok"));
    }

    let actual_secret = request
        .headers()
        .get("x-telegram-bot-api-secret-token")
        .and_then(|value| value.to_str().ok());
    if actual_secret != Some(config.telegram_webhook_secret.as_str()) {
        warn!("rejected webhook with missing or invalid secret token");
        return Ok(response(StatusCode::UNAUTHORIZED, "unauthorized"));
    }

    let update = match parse_update(request.body()) {
        Ok(update) => update,
        Err(err) => {
            warn!(error = %err, "ignored invalid Telegram update");
            return Ok(response(StatusCode::OK, "ignored"));
        }
    };

    let app = App::new(config)?;
    if let Err(err) = app.handle_update(update).await {
        error!(error = %err, "failed to handle Telegram update");
    }

    Ok(response(StatusCode::OK, "ok"))
}

fn response(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::Text(body.to_string()))
        .expect("static response is valid")
}

fn parse_update(body: &Body) -> Result<Update> {
    match body {
        Body::Text(text) => serde_json::from_str(text).context("failed to parse text body"),
        Body::Binary(bytes) => serde_json::from_slice(bytes).context("failed to parse binary body"),
        Body::Empty => bail!("empty body"),
        _ => bail!("unsupported request body type"),
    }
}

struct App {
    config: Config,
    telegram: Telegram,
    rutracker: RutrackerClient,
}

impl App {
    fn new(config: Config) -> Result<Self> {
        let telegram = Telegram::new(config.telegram_bot_token.clone());
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
        Ok(Self {
            config,
            telegram,
            rutracker,
        })
    }

    async fn handle_update(&self, update: Update) -> Result<()> {
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
            self.telegram.send_message(chat_id, HELP_TEXT, None).await?;
            return Ok(());
        }
        if let Some(query) = text.strip_prefix("c ").or_else(|| text.strip_prefix("C ")) {
            return self.handle_category_query(chat_id, query.trim()).await;
        }
        if telegram_command_name(text).is_some() {
            self.telegram
                .send_message(
                    chat_id,
                    "Unknown command. Send /help or a RuTracker search string.",
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
                .handle_download_prompt(chat_id, topic_id, callback_message)
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
        if let Some((topic_id, page)) = parse_comments_callback(data) {
            self.telegram
                .answer_callback(&callback.id, "Loading comments...")
                .await?;
            return self.send_comments_page(chat_id, topic_id, page).await;
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
            let key = parts
                .next()
                .ok_or_else(|| anyhow!("category search callback missing cache key"))?;
            let Some(query) = load_query_cache(key) else {
                self.telegram
                    .send_message(
                        chat_id,
                        "That category filter expired after a Lambda cold start. Send the search again.",
                        None,
                    )
                    .await?;
                return Ok(());
            };
            self.telegram
                .answer_callback(&callback.id, "Searching inside category...")
                .await?;
            return self.run_search(chat_id, &query, Some(forum_id)).await;
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
            let details = self.cached_topic(result.topic_id).await.ok();
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
            Some(category) => format!(
                "<a href=\"{}\">{}</a>",
                html_escape(&self.rutracker.category_url(category.id)?),
                html_escape(&category.name)
            ),
            None => "unknown".to_string(),
        };
        let author = details
            .and_then(|details| details.author.as_ref())
            .map(format_author)
            .or_else(|| format_search_author(result))
            .unwrap_or_else(|| "unknown".to_string());
        let metadata_lines = details
            .map(format_topic_metadata_lines)
            .filter(|lines| !lines.is_empty())
            .map(|lines| format!("{lines}\n"))
            .unwrap_or_default();
        let message_text = format!(
            "{}\nCategory: {}\n{}Author: {}\nSize: {}\nSeeds: {}\nDownloads: {}",
            topic_title_link(title, &result.topic_url),
            category_line,
            metadata_lines,
            author,
            format_bytes(size),
            seeds,
            downloads,
        );
        let magnet = details.and_then(|details| details.magnet.as_deref());
        let mut buttons = vec![vec![url_button("RuTracker", &result.topic_url)]];
        if let Some(magnet) = magnet {
            buttons.push(vec![copy_button("Magnet", magnet)]);
        }
        if !query.is_empty()
            && let Some(category) = result.category.as_ref()
        {
            buttons.push(vec![callback_button(
                "Search in category",
                &category_search_callback(category.id, query),
            )]);
        }

        Ok(serde_json::json!({
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
        }))
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
        self.telegram.try_delete_message(progress).await;

        for result in results {
            let details = match self.cached_topic(result.topic_id).await {
                Ok(details) => {
                    ensure_same_topic(&result, &details);
                    Some(details)
                }
                Err(err) => {
                    warn!(topic_id = result.topic_id, error = %err, "failed to fetch topic details for search result");
                    None
                }
            };
            self.send_search_result(chat_id, query, &result, details.as_ref())
                .await?;
        }
        Ok(())
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
            Some(category) => format!(
                "<a href=\"{}\">{}</a>",
                html_escape(&self.rutracker.category_url(category.id)?),
                html_escape(&category.name)
            ),
            None => "unknown".to_string(),
        };
        let magnet = details
            .and_then(|details| details.magnet.as_deref())
            .map(str::to_string);
        let metadata_lines = details
            .map(format_topic_metadata_lines)
            .filter(|lines| !lines.is_empty())
            .map(|lines| format!("{lines}\n"))
            .unwrap_or_default();

        let message = format!(
            "{}\nCategory: {}\n{}Author: {}\nSize: {}\nSeeds: {}\nDownloads: {}",
            topic_title_link(title, &result.topic_url),
            category_line,
            metadata_lines,
            author,
            format_bytes(size),
            seeds,
            downloads,
        );

        let mut rows = vec![
            vec![
                callback_button("Download", &format!("dl:{}", result.topic_id)),
                callback_button("Description", &format!("desc:{}", result.topic_id)),
            ],
            vec![
                callback_button("Files", &format!("files:{}", result.topic_id)),
                magnet_button(magnet.as_deref(), result.topic_id),
            ],
        ];
        if let Some(category) = category.filter(|_| !query.is_empty()) {
            rows.push(vec![callback_button(
                &format!("Category: {}", truncate_button_text(&category.name, 50)),
                &category_search_callback(category.id, query),
            )]);
        }

        self.telegram
            .send_message(chat_id, &message, Some(inline_keyboard(rows)))
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
                            &format!("cat:{}", node.id),
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
                                &format!("cat:{}", node.id),
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
            self.send_search_result(chat_id, "", &result, details.as_ref())
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
        let text = if topic.description_text.is_empty() {
            self.topic_message_with_description(&topic, "Description is empty or unavailable.")?
        } else {
            self.topic_message_with_description(
                &topic,
                &html_escape(&truncate_for_telegram(&topic.description_text, 3000)),
            )?
        };
        let reply_markup = remove_callback_button(reply_markup, &format!("desc:{topic_id}"));
        if let Some(progress) = progress {
            self.telegram
                .edit_message(progress, &text, reply_markup)
                .await?;
        } else {
            self.telegram.send_message(chat_id, &text, None).await?;
        }
        for image in topic.first_post_images.iter().take(10) {
            if let Err(err) = self
                .telegram
                .send_photo_url(chat_id, image, None, None)
                .await
            {
                warn!(topic_id, image, error = %err, "failed to send description image");
                self.telegram
                    .send_message(
                        chat_id,
                        &format!("Image: <a href=\"{}\">open</a>", html_escape(image)),
                        None,
                    )
                    .await?;
            }
        }
        Ok(())
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
            "{}\nCategory: {}\n{}Author: {}\nSize: {}\nSeeds: {}\nDownloads: {}\n\n<b>Description</b>\n{}",
            topic_title_link(title, &self.rutracker.topic_url(topic.topic_id)?),
            category_line,
            metadata_block,
            author,
            size,
            seeds,
            downloads,
            description
        ))
    }

    fn topic_message_with_files(
        &self,
        topic: &TopicDetails,
        files: &str,
        include_description: bool,
    ) -> Result<String> {
        let description = if include_description && !topic.description_text.is_empty() {
            Some(html_escape(&truncate_for_telegram(
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
            "{}\nCategory: {}\n{}Author: {}\nSize: {}\nSeeds: {}\nDownloads: {}",
            topic_title_link(title, &self.rutracker.topic_url(topic.topic_id)?),
            category_line,
            metadata_block,
            author,
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
        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                if let Some(message_to_edit) = message_to_edit {
                    self.edit_rutracker_unavailable(message_to_edit, &err)
                        .await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
                return Ok(());
            }
        };
        let include_description = message_has_section(current_text.as_deref(), "Description");
        let reply_markup = remove_callback_button(reply_markup, &format!("files:{topic_id}"));
        if !topic.first_post_files.is_empty() {
            let files = files_from_topic_text(&topic, self.config.max_file_mb);
            let text = self.topic_message_with_files(&topic, &files, include_description)?;
            if let Some(message_to_edit) = message_to_edit {
                self.telegram
                    .edit_message(message_to_edit, &text, reply_markup)
                    .await?;
            } else {
                self.telegram.send_message(chat_id, &text, None).await?;
            }
            return Ok(());
        }

        let magnet = require_magnet(&topic)?;
        let downloader = self.downloader(topic_id);
        let progress = self
            .telegram
            .send_status_message(chat_id, "Resolving magnet metadata for file list...")
            .await?;
        match self
            .cached_metadata(topic_id, magnet, &downloader, 120)
            .await
        {
            Ok(metadata) => {
                self.telegram.try_delete_message(progress).await;
                let files = files_from_metadata_text(&metadata.files, self.config.max_file_mb);
                let text = self.topic_message_with_files(&topic, &files, include_description)?;
                if let Some(message_to_edit) = message_to_edit {
                    self.telegram
                        .edit_message(message_to_edit, &text, reply_markup)
                        .await?;
                } else {
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
    ) -> Result<()> {
        let text = "Download files smaller than 50 MB?\n\nChoose all small files, or select specific files from torrent metadata.";
        let keyboard = inline_keyboard(vec![vec![
            callback_button("All under 50 MB", &format!("dlall:{topic_id}")),
            callback_button("Select", &format!("sel:{topic_id}")),
        ]]);
        if let Some(progress) = progress {
            self.telegram
                .edit_message(progress, text, Some(keyboard))
                .await
        } else {
            self.telegram
                .send_message(chat_id, text, Some(keyboard))
                .await
        }
    }

    async fn handle_select_files(
        &self,
        chat_id: i64,
        topic_id: u64,
        progress: Option<ProgressMessage>,
    ) -> Result<()> {
        let progress = match progress {
            Some(progress) => {
                self.telegram
                    .try_edit_message(progress, "Resolving magnet metadata for file selection...")
                    .await;
                progress
            }
            None => {
                self.telegram
                    .send_status_message(chat_id, "Resolving magnet metadata for file selection...")
                    .await?
            }
        };
        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                self.edit_rutracker_unavailable(progress, &err).await?;
                return Ok(());
            }
        };
        let magnet = require_magnet(&topic)?;
        let downloader = self.downloader(topic_id);
        let metadata = match self
            .cached_metadata(topic_id, magnet, &downloader, 120)
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                self.telegram
                    .edit_message(
                        progress,
                        &format!(
                            "Cannot load torrent metadata for selection: {}",
                            html_escape(&err.to_string())
                        ),
                        None,
                    )
                    .await?;
                return Ok(());
            }
        };
        if selectable_files(&metadata, self.config.max_file_mb).is_empty() {
            self.telegram
                .edit_message(
                    progress,
                    "No files smaller than Telegram's 50 MB bot upload limit are available for selection.",
                    None,
                )
                .await?;
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
                if let Some(progress) = progress {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
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
                if let Some(progress) = progress {
                    self.edit_rutracker_unavailable(progress, &err).await?;
                } else {
                    self.send_rutracker_unavailable(chat_id, &err).await?;
                }
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
        let topic = self.cached_topic(selection.topic_id).await?;
        let magnet = require_magnet(&topic)?;
        let downloader = self.downloader(selection.topic_id);
        self.cached_metadata(selection.topic_id, magnet, &downloader, 120)
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
                .edit_message(
                    progress,
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
        let mut text = format!(
            "<b>Select files {}/{}</b>\nSelected: {} of {}\n\n",
            page + 1,
            total_pages,
            selection.selected.len(),
            files.len()
        );
        for (offset, file) in files[start..end].iter().enumerate() {
            let number = start + offset + 1;
            let mark = if selection.selected.contains(&file.index) {
                "[x]"
            } else {
                "[ ]"
            };
            text.push_str(&format!(
                "{} {}. {} - {}\n",
                mark,
                number,
                html_escape(&truncate_for_telegram(&file.path.display().to_string(), 70)),
                format_bytes(file.size_bytes)
            ));
        }
        text.push_str(
            "\nFiles over 50 MB are not selectable here. Use Files to view the full list.",
        );

        let mut rows = Vec::new();
        let mut number_row = Vec::new();
        for (offset, file) in files[start..end].iter().enumerate() {
            let number = start + offset + 1;
            let mark = if selection.selected.contains(&file.index) {
                "[x]"
            } else {
                "[ ]"
            };
            number_row.push(callback_button(
                &format!("{mark} {number}"),
                &format!("tog:{key}:{}", file.index),
            ));
            if number_row.len() == 4 {
                rows.push(number_row);
                number_row = Vec::new();
            }
        }
        if !number_row.is_empty() {
            rows.push(number_row);
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
            "Start selected",
            &format!("go:{key}"),
        )]);

        self.telegram
            .edit_message(progress, &text, Some(inline_keyboard(rows)))
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
            total_seconds,
        );

        let topic = match self.cached_topic(topic_id).await {
            Ok(topic) => topic,
            Err(err) => {
                countdown.stop();
                typing.stop();
                self.edit_rutracker_unavailable(progress, &err).await?;
                return Ok(());
            }
        };
        let magnet = match require_magnet(&topic) {
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
            .try_edit_message(progress, "Downloading selected files...\n15 minutes left")
            .await;
        let telegram = self.telegram.clone();
        let title = topic.title.clone();
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
                    async move {
                        telegram
                            .send_document(
                                chat_id,
                                &file.path,
                                &file.display_name,
                                Some(&format!(
                                    "{} ({}) - file {}/{}",
                                    title,
                                    format_bytes(file.size_bytes),
                                    completed,
                                    total
                                )),
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
                self.send_comments(chat_id, &topic).await?;
                return Ok(());
            }
        };
        countdown.stop();
        typing.stop();

        if outcome.timed_out {
            self.telegram
                .edit_message(
                    progress,
                    &format!(
                        "Downloaded and sent {} of {} files - sorry, AWS Lambda lifetime is only 15 minutes.",
                        outcome.files.len(),
                        outcome.total_files
                    ),
                    None,
                )
                .await?;
        } else if outcome.files.is_empty() {
            self.telegram
                .edit_message(
                    progress,
                    "No files smaller than Telegram's 50 MB bot upload limit were downloaded.",
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

        self.send_comments(chat_id, &topic).await?;
        Ok(())
    }

    async fn send_comments(&self, chat_id: i64, topic: &TopicDetails) -> Result<()> {
        self.send_comments_topic(chat_id, topic).await
    }

    async fn send_comments_page(&self, chat_id: i64, topic_id: u64, page: u32) -> Result<()> {
        let topic = match self.cached_topic_page(topic_id, page).await {
            Ok(topic) => topic,
            Err(err) => {
                self.send_rutracker_unavailable(chat_id, &err).await?;
                return Ok(());
            }
        };
        self.send_comments_topic(chat_id, &topic).await
    }

    async fn send_comments_topic(&self, chat_id: i64, topic: &TopicDetails) -> Result<()> {
        let page = topic.comments_page.max(1);
        let total_pages = topic.comments_total_pages.max(page).max(1);
        if topic.comments.is_empty() {
            self.telegram
                .send_message(
                    chat_id,
                    &format!("No comments found on page {page}/{total_pages}."),
                    comments_next_keyboard(topic),
                )
                .await?;
            return Ok(());
        }
        let mut text = format!("<b>Comments {page}/{total_pages}</b>");
        for comment in &topic.comments {
            text.push_str("\n\n");
            text.push_str(&comment_author_line(comment.author.as_ref()));
            text.push('\n');
            text.push_str(&html_escape(&truncate_for_telegram(&comment.text, 500)));
        }
        self.telegram
            .send_message(chat_id, &text, comments_next_keyboard(topic))
            .await
    }

    fn downloader(&self, topic_id: u64) -> TorrentDownloader {
        TorrentDownloader::new(
            self.config
                .tmp_dir
                .join(format!("rutracker-{topic_id}-{}", std::process::id())),
            self.config.peer_limit,
        )
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
        let value = self
            .rutracker
            .search(query, forum_id, self.config.search_limit)
            .await?;
        cache_set(
            SEARCH_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            key,
            value.clone(),
        );
        Ok(value)
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

    async fn cached_metadata(
        &self,
        topic_id: u64,
        magnet: &str,
        downloader: &TorrentDownloader,
        timeout_seconds: u64,
    ) -> Result<TorrentMetadata> {
        if let Some(value) = cache_get(
            METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            &topic_id,
        ) {
            return Ok(value);
        }
        let value = downloader
            .metadata_from_magnet(magnet, timeout_seconds)
            .await?;
        cache_set(
            METADATA_CACHE.get_or_init(|| Mutex::new(HashMap::new())),
            topic_id,
            value.clone(),
        );
        Ok(value)
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

fn format_author(author: &AuthorDetails) -> String {
    let mut text = author_name_link(&author.name, author.profile_url.as_deref());
    if let Some(posts) = author.posts_count {
        text.push_str(&format!(" ({} messages)", posts));
    }
    if let Some(avatar) = author.avatar_url.as_ref() {
        text.push_str(&format!(
            "; avatar: <a href=\"{}\">open</a>",
            html_escape(avatar)
        ));
    }
    text
}

fn format_search_author(result: &SearchResult) -> Option<String> {
    result
        .author
        .as_ref()
        .map(|name| author_name_link(name, result.author_profile_url.as_deref()))
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

fn comments_next_keyboard(topic: &TopicDetails) -> Option<Value> {
    let next_page = topic.comments_page.saturating_add(1);
    (next_page <= topic.comments_total_pages).then(|| {
        inline_keyboard(vec![vec![callback_button(
            &format!("Next {}/{}", next_page, topic.comments_total_pages),
            &format!("comments:{}:{next_page}", topic.topic_id),
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

fn parse_comments_callback(value: &str) -> Option<(u64, u32)> {
    let rest = value.strip_prefix("comments:")?;
    let mut parts = rest.splitn(2, ':');
    let topic_id = parts.next().and_then(parse_u64)?;
    let page = parts.next()?.parse::<u32>().ok()?;
    Some((topic_id, page.max(1)))
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

fn category_search_callback(forum_id: u64, query: &str) -> String {
    let key = store_query_cache(query);
    format!("cs:{forum_id}:{key}")
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

fn store_query_cache(query: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(query.as_bytes());
    hasher.update(now_seconds().to_be_bytes());
    let digest = hasher.finalize();
    let key = URL_SAFE_NO_PAD.encode(&digest[..9]);
    let cache = QUERY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().expect("query cache poisoned");
    let now = now_seconds();
    guard.retain(|_, entry| now.saturating_sub(entry.created_at) < 3600);
    guard.insert(
        key.clone(),
        QueryCacheEntry {
            query: query.to_string(),
            created_at: now,
        },
    );
    key
}

fn load_query_cache(key: &str) -> Option<String> {
    let cache = QUERY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().ok()?;
    let now = now_seconds();
    guard.retain(|_, entry| now.saturating_sub(entry.created_at) < 3600);
    guard.get(key).map(|entry| entry.query.clone())
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

fn topic_title_link(title: &str, topic_url: &str) -> String {
    format!(
        "<a href=\"{}\">{}</a>",
        html_escape(topic_url),
        html_escape(title)
    )
}

fn message_has_section(text: Option<&str>, section: &str) -> bool {
    text.is_some_and(|text| text.lines().any(|line| line.trim() == section))
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
        lines.push(format!("Тип: {}", html_escape(release_type)));
    }
    if let Some(publication_date) = topic.publication_date.as_ref() {
        lines.push(format!(
            "Дата публикации: {}",
            html_escape(publication_date)
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        HELP_TEXT, RUTRACKER_NEWS_URL, RUTRACKER_UNAVAILABLE_TEXT, TopicDetails, author_name_link,
        callback_button, format_bytes, format_topic_metadata_lines, inline_keyboard,
        message_has_section, remove_callback_button, telegram_command_name, topic_title_link,
    };

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
        assert_eq!(telegram_command_name("hello"), None);
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
    fn removes_used_callback_button_from_reply_markup() {
        let markup = inline_keyboard(vec![
            vec![
                callback_button("Download", "dl:42"),
                callback_button("Description", "desc:42"),
            ],
            vec![callback_button("Files", "files:42")],
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
            Some("dl:42")
        );
        assert_eq!(rows[0].as_array().unwrap().len(), 1);
        assert_eq!(
            rows[1][0]
                .get("callback_data")
                .and_then(serde_json::Value::as_str),
            Some("files:42")
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
            publication_date: Some("07-Jun-26 12:34".to_string()),
            ..TopicDetails::default()
        };
        assert_eq!(
            format_topic_metadata_lines(&topic),
            "Тип: авторская\nДата публикации: 07-Jun-26 12:34"
        );
    }

    #[test]
    fn rutracker_unavailable_message_points_to_news_channel() {
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains("@rutracker_news"));
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains(RUTRACKER_NEWS_URL));
        assert!(RUTRACKER_UNAVAILABLE_TEXT.contains("donating"));
    }

    #[test]
    fn help_mentions_limits_unofficial_status_and_donations() {
        assert!(HELP_TEXT.contains("https://github.com/ikatson/rqbit"));
        assert!(HELP_TEXT.contains(
            "<a href=\"https://core.telegram.org/bots/api#senddocument\">sendDocument</a>"
        ));
        assert!(
            HELP_TEXT.contains(
                "https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html"
            )
        );
        assert!(HELP_TEXT.contains("at most 15 minutes"));
        assert!(!HELP_TEXT.contains("900 seconds"));
        assert!(HELP_TEXT.contains("unofficial bot"));
        assert!(HELP_TEXT.contains("donating"));
        assert!(HELP_TEXT.contains("seed legal torrents"));
        assert!(HELP_TEXT.contains("favorite artists and authors"));
        assert!(HELP_TEXT.contains("culture preservation"));
        assert!(HELP_TEXT.contains("Creative Commons"));
        assert!(HELP_TEXT.contains("indie artist"));
        assert!(HELP_TEXT.contains("publish it legally on RuTracker"));
    }
}
