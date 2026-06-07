use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use encoding_rs::WINDOWS_1251;
use regex::Regex;
use reqwest::header::{COOKIE, HeaderMap, HeaderValue};
use scraper::{ElementRef, Html, Node, Selector};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

#[derive(Clone)]
pub struct RutrackerClient {
    http: reqwest::Client,
    base_urls: Vec<Url>,
    credentials: Option<RutrackerCredentials>,
    logged_in_base_urls: Arc<Mutex<HashSet<String>>>,
    max_attempts: usize,
}

#[derive(Clone)]
pub struct RutrackerCredentials {
    username: String,
    password: String,
}

impl RutrackerCredentials {
    pub fn new(username: &str, password: &str) -> Result<Self> {
        let username = username.trim();
        let password = password.trim();
        if username.is_empty() || password.is_empty() {
            bail!("RUTRACKER_USERNAME and RUTRACKER_PASSWORD must not be empty");
        }
        Ok(Self {
            username: username.to_string(),
            password: password.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub topic_id: u64,
    pub title: String,
    pub author: Option<String>,
    pub author_profile_url: Option<String>,
    pub category: Option<CategoryRef>,
    pub size_bytes: u64,
    pub seeds: i64,
    pub downloads: u64,
    pub topic_url: String,
    pub category_url: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CategoryRef {
    pub id: u64,
    pub name: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopicDetails {
    pub topic_id: u64,
    pub title: String,
    pub author: Option<AuthorDetails>,
    pub category_path: Vec<CategoryRef>,
    pub release_type: Option<String>,
    pub publication_date: Option<String>,
    pub total_size_bytes: Option<u64>,
    pub seeds: Option<i64>,
    pub downloads: Option<u64>,
    pub magnet: Option<String>,
    pub description_text: String,
    pub description_html: String,
    pub first_post_images: Vec<String>,
    pub first_post_files: Vec<TopicFile>,
    pub comments: Vec<TopicComment>,
    pub comments_page: u32,
    pub comments_total_pages: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorDetails {
    pub name: String,
    pub profile_url: Option<String>,
    pub posts_count: Option<u64>,
    pub avatar_url: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicComment {
    pub author: Option<AuthorDetails>,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicFile {
    pub path: String,
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForumNode {
    pub id: u64,
    pub name: String,
    pub parent_id: Option<u64>,
}

impl RutrackerClient {
    pub fn new(
        base_urls: &[String],
        cookie: Option<&str>,
        credentials: Option<RutrackerCredentials>,
        timeout_seconds: u64,
        max_attempts: usize,
    ) -> Result<Self> {
        let mut headers = HeaderMap::new();
        if credentials.is_some() && cookie.is_some() {
            warn!("RUTRACKER_COOKIE ignored because RuTracker credentials are configured");
        }
        if let Some(cookie) = cookie.filter(|_| credentials.is_none()) {
            headers.insert(
                COOKIE,
                HeaderValue::from_str(cookie).context("RUTRACKER_COOKIE is not a valid header")?,
            );
        }

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .default_headers(headers)
            .cookie_store(credentials.is_some())
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION"),
                " (+https://github.com/vitaly-zdanevich/bot_telegram_rutracker)"
            ))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .context("failed to build RuTracker HTTP client")?;

        let base_urls = base_urls
            .iter()
            .map(|base_url| {
                let mut base_url = base_url.to_string();
                if !base_url.ends_with('/') {
                    base_url.push('/');
                }
                Url::parse(&base_url).context("RUTRACKER_BASE_URLS must contain only URLs")
            })
            .collect::<Result<Vec<_>>>()?;
        if base_urls.is_empty() {
            bail!("RUTRACKER_BASE_URLS must contain at least one URL");
        }

        Ok(Self {
            http,
            base_urls,
            credentials,
            logged_in_base_urls: Arc::new(Mutex::new(HashSet::new())),
            max_attempts: max_attempts.max(1),
        })
    }

    pub async fn search(
        &self,
        query: &str,
        forum_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let fields = search_form_fields(query, forum_id);
        let body = cp1251_form(&fields);
        let mut last_error = None;

        for base_url in &self.base_urls {
            match self.search_base_url(base_url, body.clone(), limit).await {
                Ok(results) => return Ok(results),
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("no RuTracker base URLs configured")))
    }

    async fn search_base_url(
        &self,
        base_url: &Url,
        body: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        if let Err(err) = self.ensure_logged_in(base_url).await {
            warn!(base_url = %base_url, error = %err, "RuTracker login failed");
            return Err(err);
        }

        let url = forum_url_for(base_url, "tracker.php")?;
        let html = self.post_body_with_retries(url, body).await?;
        let mut results = parse_search_results(&html, base_url).map_err(|err| {
            warn!(
                base_url = %base_url,
                error = %err,
                "failed to parse RuTracker search results"
            );
            err
        })?;
        results.truncate(limit);
        Ok(results)
    }

    pub async fn latest_in_forum(&self, forum_id: u64, limit: usize) -> Result<Vec<SearchResult>> {
        let (html, base_url) = self
            .get_forum_html("tracker.php", |url| {
                url.query_pairs_mut()
                    .append_pair("f", &forum_id.to_string());
            })
            .await?;
        let mut results = parse_search_results(&html, &base_url)?;
        results.truncate(limit);
        Ok(results)
    }

    pub async fn topic(&self, topic_id: u64) -> Result<TopicDetails> {
        self.topic_page(topic_id, 1).await
    }

    pub async fn topic_page(&self, topic_id: u64, page: u32) -> Result<TopicDetails> {
        let page = page.max(1);
        let (html, base_url) = self
            .get_forum_html("viewtopic.php", |url| {
                url.query_pairs_mut()
                    .append_pair("t", &topic_id.to_string());
                if page > 1 {
                    url.query_pairs_mut()
                        .append_pair("start", &((page - 1) * 30).to_string());
                }
            })
            .await?;
        parse_topic_page(&html, topic_id, page, &base_url)
    }

    pub async fn categories(&self) -> Result<Vec<ForumNode>> {
        let (html, _) = self.get_forum_html("index.php", |_| {}).await?;
        Ok(parse_forum_nodes(&html))
    }

    pub async fn forum_subcategories(&self, forum_id: u64) -> Result<Vec<ForumNode>> {
        let (html, _) = self
            .get_forum_html("viewforum.php", |url| {
                url.query_pairs_mut()
                    .append_pair("f", &forum_id.to_string());
            })
            .await?;
        let mut nodes = parse_forum_nodes(&html);
        nodes.retain(|node| node.id != forum_id);
        for node in &mut nodes {
            node.parent_id = Some(forum_id);
        }
        dedupe_forums(nodes)
    }

    pub fn topic_url(&self, topic_id: u64) -> Result<String> {
        let mut url = self.forum_url("viewtopic.php")?;
        url.query_pairs_mut()
            .append_pair("t", &topic_id.to_string());
        Ok(url.to_string())
    }

    pub fn category_url(&self, forum_id: u64) -> Result<String> {
        let mut url = self.forum_url("viewforum.php")?;
        url.query_pairs_mut()
            .append_pair("f", &forum_id.to_string());
        Ok(url.to_string())
    }

    fn forum_url(&self, path: &str) -> Result<Url> {
        forum_url_for(self.primary_base_url()?, path)
    }

    fn primary_base_url(&self) -> Result<&Url> {
        self.base_urls
            .first()
            .ok_or_else(|| anyhow!("no RuTracker base URLs configured"))
    }

    async fn get_forum_html<F>(&self, path: &str, configure_url: F) -> Result<(String, Url)>
    where
        F: Fn(&mut Url),
    {
        let mut last_error = None;

        for base_url in &self.base_urls {
            if let Err(err) = self.ensure_logged_in(base_url).await {
                warn!(base_url = %base_url, error = %err, "RuTracker login failed");
                last_error = Some(err);
                continue;
            }

            let mut url = forum_url_for(base_url, path)?;
            configure_url(&mut url);

            match self.get_html_with_retries(url).await {
                Ok(html) => return Ok((html, base_url.clone())),
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("no RuTracker base URLs configured")))
    }

    async fn ensure_logged_in(&self, base_url: &Url) -> Result<()> {
        let Some(credentials) = self.credentials.clone() else {
            return Ok(());
        };
        let key = base_url.as_str().trim_end_matches('/').to_string();
        if self.logged_in_base_urls()?.contains(&key) {
            return Ok(());
        }

        self.login_to_base(base_url, &credentials).await?;
        self.logged_in_base_urls()?.insert(key);
        Ok(())
    }

    fn logged_in_base_urls(&self) -> Result<std::sync::MutexGuard<'_, HashSet<String>>> {
        self.logged_in_base_urls
            .lock()
            .map_err(|_| anyhow!("RuTracker login state lock is poisoned"))
    }

    async fn login_to_base(
        &self,
        base_url: &Url,
        credentials: &RutrackerCredentials,
    ) -> Result<()> {
        let url = forum_url_for(base_url, "login.php")?;
        let fields = login_form_fields(&credentials.username, &credentials.password);
        let html = self
            .post_body_with_retries(url, cp1251_form(&fields))
            .await
            .with_context(|| format!("RuTracker login request failed for {base_url}"))?;

        if login_succeeded(&html) {
            info!(base_url = %base_url, "RuTracker login succeeded");
            return Ok(());
        }

        if html_contains_login_form(&html) {
            bail!("RuTracker login failed; check RUTRACKER_USERNAME and RUTRACKER_PASSWORD");
        }

        bail!("RuTracker login failed; success marker was not present in the response");
    }

    async fn get_html_with_retries(&self, url: Url) -> Result<String> {
        let mut last_error = None;
        for attempt in 1..=self.max_attempts {
            match self.http.get(url.clone()).send().await {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => match decode_response(response).await {
                        Ok(html) => return Ok(html),
                        Err(err) => {
                            warn!(
                                url = %url,
                                attempt,
                                max_attempts = self.max_attempts,
                                error = %err,
                                "failed to decode RuTracker response"
                            );
                            last_error = Some(err);
                        }
                    },
                    Err(err) => {
                        warn!(
                            url = %url,
                            attempt,
                            max_attempts = self.max_attempts,
                            error = %err,
                            "RuTracker request failed"
                        );
                        last_error = Some(anyhow!(err));
                    }
                },
                Err(err) => {
                    warn!(
                        url = %url,
                        attempt,
                        max_attempts = self.max_attempts,
                        error = %err,
                        "RuTracker request failed"
                    );
                    last_error = Some(anyhow!(err));
                }
            }
            self.sleep_before_next_attempt(attempt).await;
        }
        Err(last_error.unwrap_or_else(|| anyhow!("RuTracker request failed without error")))
    }

    async fn post_body_with_retries(&self, url: Url, body: Vec<u8>) -> Result<String> {
        let mut last_error = None;
        for attempt in 1..=self.max_attempts {
            match self
                .http
                .post(url.clone())
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => match decode_response(response).await {
                        Ok(html) => return Ok(html),
                        Err(err) => {
                            warn!(
                                url = %url,
                                attempt,
                                max_attempts = self.max_attempts,
                                error = %err,
                                "failed to decode RuTracker response"
                            );
                            last_error = Some(err);
                        }
                    },
                    Err(err) => {
                        warn!(
                            url = %url,
                            attempt,
                            max_attempts = self.max_attempts,
                            error = %err,
                            "RuTracker request failed"
                        );
                        last_error = Some(anyhow!(err));
                    }
                },
                Err(err) => {
                    warn!(
                        url = %url,
                        attempt,
                        max_attempts = self.max_attempts,
                        error = %err,
                        "RuTracker request failed"
                    );
                    last_error = Some(anyhow!(err));
                }
            }
            self.sleep_before_next_attempt(attempt).await;
        }
        Err(last_error.unwrap_or_else(|| anyhow!("RuTracker request failed without error")))
    }

    async fn sleep_before_next_attempt(&self, attempt: usize) {
        if attempt < self.max_attempts {
            sleep(Duration::from_millis(250 * attempt as u64)).await;
        }
    }
}

fn forum_url_for(base_url: &Url, path: &str) -> Result<Url> {
    base_url
        .join(path)
        .with_context(|| format!("failed to build RuTracker URL for {path}"))
}

fn search_form_fields(query: &str, forum_id: Option<u64>) -> Vec<(&'static str, String)> {
    let mut fields = vec![
        ("nm", query.to_string()),
        ("max", "1".to_string()),
        ("tm", "-1".to_string()),
        ("o", "10".to_string()),
        ("s", "2".to_string()),
    ];
    if let Some(forum_id) = forum_id {
        fields.push(("f", forum_id.to_string()));
    }
    fields
}

fn login_form_fields(username: &str, password: &str) -> Vec<(&'static str, String)> {
    vec![
        ("login_username", username.to_string()),
        ("login_password", password.to_string()),
        ("login", "Вход".to_string()),
    ]
}

async fn decode_response(response: reqwest::Response) -> Result<String> {
    let bytes = response.bytes().await?;
    let (decoded, _, _) = WINDOWS_1251.decode(&bytes);
    Ok(decoded.into_owned())
}

fn selector(value: &str) -> Selector {
    Selector::parse(value).expect("static selector is valid")
}

fn html_contains_login_form(html: &str) -> bool {
    let doc = Html::parse_document(html);
    doc_contains_login_form(&doc)
}

fn doc_contains_login_form(doc: &Html) -> bool {
    doc.select(&selector(
        "form#login-form-full, form[action*=\"login.php\"], input[name=\"login_username\"]",
    ))
    .next()
    .is_some()
}

fn login_succeeded(html: &str) -> bool {
    html.contains("logged-in-username") || html.contains("logged-in-as-uname")
}

fn text(el: ElementRef<'_>) -> String {
    html_escape::decode_html_entities(
        &el.text()
            .collect::<Vec<_>>()
            .join(" ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
    )
    .trim()
    .to_string()
}

fn description_text(body: ElementRef<'_>) -> String {
    let mut out = String::new();
    for node in body.descendants() {
        match node.value() {
            Node::Text(value) => {
                let in_pre = node
                    .ancestors()
                    .filter_map(ElementRef::wrap)
                    .any(|element| matches!(element.value().name(), "pre" | "code"));
                if in_pre {
                    push_pre_description_text(&mut out, value);
                } else {
                    push_inline_description_text(&mut out, value);
                }
            }
            Node::Element(element)
                if element.name() == "br" || is_description_block(element.name()) =>
            {
                push_description_newline(&mut out);
            }
            _ => {}
        }
    }
    normalize_description_text(&out)
}

fn is_description_block(name: &str) -> bool {
    matches!(
        name,
        "address"
            | "article"
            | "aside"
            | "blockquote"
            | "dd"
            | "details"
            | "div"
            | "dl"
            | "dt"
            | "figcaption"
            | "figure"
            | "footer"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "header"
            | "hr"
            | "li"
            | "main"
            | "ol"
            | "p"
            | "pre"
            | "section"
            | "table"
            | "tbody"
            | "td"
            | "tfoot"
            | "th"
            | "thead"
            | "tr"
            | "ul"
    )
}

fn push_inline_description_text(out: &mut String, value: &str) {
    let text = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return;
    }
    if !out.is_empty() && !out.ends_with(|ch: char| ch.is_whitespace()) {
        out.push(' ');
    }
    out.push_str(&text);
}

fn push_pre_description_text(out: &mut String, value: &str) {
    for line in value.lines() {
        let line = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if line.is_empty() {
            continue;
        }
        push_description_newline(out);
        out.push_str(&line);
    }
}

fn push_description_newline(out: &mut String) {
    while out.ends_with(' ') || out.ends_with('\t') {
        out.pop();
    }
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
}

fn normalize_description_text(value: &str) -> String {
    value
        .lines()
        .map(|line| line.trim().replace(" :", ":"))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn attr_url(base_url: &Url, value: &str) -> Option<String> {
    if value.starts_with("data:") {
        return None;
    }
    base_url.join(value).ok().map(|url| url.to_string())
}

fn cp1251_form(fields: &[(&str, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            out.push(b'&');
        }
        percent_form_encode_bytes(key.as_bytes(), &mut out);
        out.push(b'=');
        let (encoded, _, _) = WINDOWS_1251.encode(value);
        percent_form_encode_bytes(&encoded, &mut out);
    }
    out
}

fn percent_form_encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    for &byte in bytes {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(byte);
            }
            b' ' => out.push(b'+'),
            other => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push(b'%');
                out.push(HEX[(other >> 4) as usize]);
                out.push(HEX[(other & 0x0f) as usize]);
            }
        }
    }
}

pub fn parse_search_results(html: &str, base_url: &Url) -> Result<Vec<SearchResult>> {
    let doc = Html::parse_document(html);
    if doc_contains_login_form(&doc) {
        bail!("RuTracker returned the login form instead of search results");
    }

    let mut results = Vec::new();

    for row in doc.select(&selector("#tor-tbl tbody tr, tr.hl-tr, tr.tCenter")) {
        let Some(title_link) = row
            .select(&selector(
                "a.tLink, a[data-topic_id], a[href*=\"viewtopic.php?t=\"]",
            ))
            .next()
        else {
            continue;
        };
        let Some(topic_id) = extract_topic_id(title_link) else {
            continue;
        };

        let title = text(title_link);
        if title.is_empty() {
            continue;
        }

        let category = row
            .select(&selector(
                ".f-name a, td.f-name-col a, a[href*=\"viewforum.php?f=\"]",
            ))
            .find_map(|link| {
                let id = link
                    .value()
                    .attr("href")
                    .and_then(extract_forum_id_from_href)?;
                let name = text(link);
                (!name.is_empty()).then_some(CategoryRef { id, name })
            });

        let topic_url = base_url
            .join(&format!("viewtopic.php?t={topic_id}"))
            .map(|url| url.to_string())?;
        let category_url = category
            .as_ref()
            .and_then(|category| {
                base_url
                    .join(&format!("viewforum.php?f={}", category.id))
                    .ok()
            })
            .map(|url| url.to_string());

        let (author, author_profile_url) = extract_search_author(row, base_url);

        results.push(SearchResult {
            topic_id,
            title,
            author,
            author_profile_url,
            category,
            size_bytes: extract_size_from_row(row),
            seeds: extract_seed_count(row),
            downloads: extract_download_count(row),
            topic_url,
            category_url,
        });
    }

    Ok(results)
}

fn extract_topic_id(link: ElementRef<'_>) -> Option<u64> {
    link.value()
        .attr("data-topic_id")
        .and_then(|id| id.parse::<u64>().ok())
        .or_else(|| {
            link.value()
                .attr("href")
                .and_then(extract_topic_id_from_href)
        })
}

fn extract_topic_id_from_href(href: &str) -> Option<u64> {
    capture_u64(href, r"[?&]t=(\d+)")
}

fn extract_forum_id_from_href(href: &str) -> Option<u64> {
    capture_u64(href, r"[?&]f=(\d+)")
}

fn capture_u64(value: &str, pattern: &str) -> Option<u64> {
    Regex::new(pattern)
        .ok()?
        .captures(value)?
        .get(1)?
        .as_str()
        .parse::<u64>()
        .ok()
}

fn extract_search_author(row: ElementRef<'_>, base_url: &Url) -> (Option<String>, Option<String>) {
    let Some(link) = row
        .select(&selector(
            ".u-name a, td.u-name a, a[href*=\"profile.php?mode=viewprofile\"]",
        ))
        .next()
    else {
        return (None, None);
    };
    let name = text(link);
    if name.is_empty() {
        return (None, None);
    }
    let profile_url = link
        .value()
        .attr("href")
        .and_then(|href| attr_url(base_url, href));
    (Some(name), profile_url)
}

fn extract_size_from_row(row: ElementRef<'_>) -> u64 {
    row.select(&selector("td.tor-size, td[data-ts_text]"))
        .find_map(|td| {
            td.value()
                .attr("data-ts_text")
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| parse_size(&text(td)))
        })
        .unwrap_or(0)
}

fn extract_seed_count(row: ElementRef<'_>) -> i64 {
    row.select(&selector("td.seedmed b, b.seedmed, .seedmed"))
        .next()
        .and_then(|el| text(el).replace(',', "").parse::<i64>().ok())
        .unwrap_or(0)
}

fn extract_download_count(row: ElementRef<'_>) -> u64 {
    row.select(&selector("td.dl-stub, td.tCenter"))
        .filter_map(|td| text(td).replace(',', "").parse::<u64>().ok())
        .last()
        .unwrap_or(0)
}

pub fn parse_topic(html: &str, topic_id: u64, base_url: &Url) -> Result<TopicDetails> {
    parse_topic_page(html, topic_id, 1, base_url)
}

pub fn parse_topic_page(
    html: &str,
    topic_id: u64,
    comments_page: u32,
    base_url: &Url,
) -> Result<TopicDetails> {
    let doc = Html::parse_document(html);
    let first_visible_post = doc
        .select(&selector(".post_wrap, .post, table.forumline tr"))
        .find(|post| {
            post.select(&selector(".post_body, td.message"))
                .next()
                .is_some()
        });
    let topic_post = if comments_page == 1 {
        first_visible_post
    } else {
        None
    };

    let title = doc
        .select(&selector("h1.maintitle, h1#topic-title, title"))
        .next()
        .map(text)
        .map(|value| {
            value
                .split("::")
                .next()
                .unwrap_or(&value)
                .trim()
                .to_string()
        })
        .unwrap_or_default();

    let category_path = doc
        .select(&selector("table.navTitle a[href*=\"viewforum.php?f=\"], td.nav a[href*=\"viewforum.php?f=\"], a[href*=\"viewforum.php?f=\"]"))
        .filter_map(|link| {
            let id = link.value().attr("href").and_then(extract_forum_id_from_href)?;
            let name = text(link);
            (!name.is_empty()).then_some(CategoryRef { id, name })
        })
        .collect::<Vec<_>>();

    let author = topic_post.and_then(|post| parse_author_from_post(post, base_url));
    let description_body =
        topic_post.and_then(|post| post.select(&selector(".post_body, td.message")).next());

    let description_html = description_body
        .map(|body| body.inner_html())
        .unwrap_or_default();
    let description_text = description_body.map(description_text).unwrap_or_default();
    let release_type = extract_release_type(&description_text);
    let first_post_images = description_body
        .map(|body| image_urls(body, base_url))
        .unwrap_or_default();
    let first_post_files = description_body
        .map(parse_first_post_files)
        .unwrap_or_default();
    let publication_date = topic_post
        .and_then(extract_publication_date_from_post)
        .or_else(|| extract_publication_date_from_description(&description_text));

    let magnet = doc
        .select(&selector("a[href^=\"magnet:\"]"))
        .find_map(|a| a.value().attr("href").map(str::to_string));
    let total_size_bytes = extract_topic_size(&doc);
    let seeds = doc
        .select(&selector("#tor-seed-count, .seedmed"))
        .next()
        .and_then(|el| text(el).parse::<i64>().ok());
    let downloads = extract_topic_downloads(&doc);
    let comments = parse_comments_from_doc(&doc, base_url, comments_page == 1);
    let comments_total_pages = extract_topic_total_pages(&doc)
        .unwrap_or(comments_page)
        .max(comments_page)
        .max(1);

    Ok(TopicDetails {
        topic_id,
        title,
        author,
        category_path,
        release_type,
        publication_date,
        total_size_bytes,
        seeds,
        downloads,
        magnet,
        description_text,
        description_html,
        first_post_images,
        first_post_files,
        comments,
        comments_page,
        comments_total_pages,
    })
}

fn parse_first_post_files(body: ElementRef<'_>) -> Vec<TopicFile> {
    let files = parse_files_from_first_post_blocks(body);
    if files.is_empty() {
        return dedupe_files(parse_files_from_text(&text(body)));
    }

    dedupe_files(files)
}

fn parse_files_from_first_post_blocks(body: ElementRef<'_>) -> Vec<TopicFile> {
    let mut files = Vec::new();
    for block in body.select(&selector("pre, code, .sp-body, .filelist, .post_body")) {
        for text_node in block.text() {
            files.extend(parse_files_from_text(text_node));
        }
    }
    files
}

fn parse_files_from_text(value: &str) -> Vec<TopicFile> {
    value.lines().filter_map(parse_file_line).collect()
}

fn parse_file_line(line: &str) -> Option<TopicFile> {
    let line = html_escape::decode_html_entities(line).trim().to_string();
    if line.is_empty() || line.len() > 500 {
        return None;
    }
    let line = line
        .trim_start_matches([' ', '\t', '-', '*', '•', '|', '+'])
        .trim()
        .to_string();
    let lowered = line.to_ascii_lowercase();
    if lowered.starts_with("http://")
        || lowered.starts_with("https://")
        || lowered.starts_with("magnet:")
        || lowered.contains("rutracker.org")
    {
        return None;
    }
    let file_ext = Regex::new(r"(?i)\.[a-z0-9]{1,8}(?:\s|$|\t| - | \(|\[)").ok()?;
    if !file_ext.is_match(&line) {
        return None;
    }
    let size = parse_size(&line);
    let path = if let Some(size_match) = Regex::new(
        r"(?i)\s*[-–—]?\s*\(?\d+(?:[\s.,]\d+)*\s*(?:b|б|kb|кб|kib|mb|мб|mib|gb|гб|gib|tb|тб|tib)\)?\s*$",
    )
    .ok()
    .and_then(|re| re.find(&line))
    {
        line[..size_match.start()].trim().to_string()
    } else {
        line.trim().to_string()
    };
    let path = path
        .trim_matches(['"', '\''])
        .trim_start_matches(|ch: char| ch.is_ascii_digit() || ch == '.' || ch == ')' || ch == ' ')
        .trim()
        .to_string();
    (!path.is_empty()).then_some(TopicFile {
        path,
        size_bytes: size,
    })
}

fn dedupe_files(files: Vec<TopicFile>) -> Vec<TopicFile> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for file in files {
        let key = file.path.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(file);
        }
    }
    out
}

fn parse_author_from_post(post: ElementRef<'_>, base_url: &Url) -> Option<AuthorDetails> {
    let linked_author = post
        .select(&selector(
            ".nick a, .poster_info a[href*=\"profile.php\"], .post-author a, p.nick a",
        ))
        .next();
    let plain_author = || post.select(&selector(".nick, .post-author, p.nick")).next();
    let author = linked_author.or_else(plain_author)?;
    let name = text(author);
    if name.is_empty() {
        return None;
    }
    let profile_url = linked_author.and_then(|author| {
        author
            .value()
            .attr("href")
            .and_then(|href| attr_url(base_url, href))
    });
    let posts_count = post
        .select(&selector(
            ".poster_info, .joined, .post-author-details, td.row1",
        ))
        .map(text)
        .find_map(|value| {
            Regex::new(r"(?i)(?:сообщ|posts?|messages?)\D+([\d\s,]+)")
                .ok()?
                .captures(&value)?
                .get(1)?
                .as_str()
                .replace([' ', ','], "")
                .parse::<u64>()
                .ok()
        });
    let avatar_url = post
        .select(&selector("img.avatar, .poster_info img, td.row1 img"))
        .find_map(|img| {
            img.value()
                .attr("src")
                .and_then(|src| attr_url(base_url, src))
        });

    Some(AuthorDetails {
        name,
        profile_url,
        posts_count,
        avatar_url,
    })
}

fn image_urls(body: ElementRef<'_>, base_url: &Url) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for img in body.select(&selector("img")) {
        let Some(src) = img.value().attr("src") else {
            continue;
        };
        let Some(url) = attr_url(base_url, src) else {
            continue;
        };
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }
    urls
}

fn parse_comments_from_doc(doc: &Html, base_url: &Url, skip_first_post: bool) -> Vec<TopicComment> {
    let mut comments = Vec::new();
    for (index, post) in doc
        .select(&selector(".post_wrap, .post, table.forumline tr"))
        .filter(|post| {
            post.select(&selector(".post_body, td.message"))
                .next()
                .is_some()
        })
        .enumerate()
    {
        if skip_first_post && index == 0 {
            continue;
        }
        let text = post
            .select(&selector(".post_body, td.message"))
            .next()
            .map(text)
            .unwrap_or_default();
        if text.is_empty() {
            continue;
        }
        comments.push(TopicComment {
            author: parse_author_from_post(post, base_url),
            text,
        });
        if comments.len() >= 10 {
            break;
        }
    }
    comments
}

fn extract_topic_total_pages(doc: &Html) -> Option<u32> {
    doc.select(&selector("a[href*=\"start=\"]"))
        .filter_map(|link| {
            let start = link
                .value()
                .attr("href")
                .and_then(|href| capture_u64(href, r"[?&]start=(\d+)"))?;
            Some((start / 30 + 1) as u32)
        })
        .max()
        .or_else(|| {
            doc.select(&selector(".pg a, .pagination a"))
                .filter_map(|link| text(link).parse::<u32>().ok())
                .max()
        })
}

fn extract_topic_size(doc: &Html) -> Option<u64> {
    doc.select(&selector("#tor-size-humn, span.tor-size, .tor-size"))
        .find_map(|el| {
            el.value()
                .attr("title")
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| parse_size(&text(el)))
        })
        .or_else(|| {
            doc.select(&selector("li, td"))
                .find_map(|el| parse_size(&text(el)))
        })
}

fn extract_topic_downloads(doc: &Html) -> Option<u64> {
    doc.select(&selector("#tor-completed, .dl-stub"))
        .find_map(|el| parse_human_u64(&text(el)))
        .or_else(|| parse_torrent_downloads_from_text(&text(doc.root_element())))
}

fn parse_torrent_downloads_from_text(value: &str) -> Option<u64> {
    let captures = Regex::new(r"(?i)(?:\.torrent\s*)?скачан\s*:\s*([\d\s,]+)\s*раз")
        .ok()?
        .captures(value)?;
    parse_human_u64(captures.get(1)?.as_str())
}

fn extract_release_type(description_text: &str) -> Option<String> {
    Regex::new(r"(?i)тип\s*:\s*авторская")
        .ok()?
        .is_match(description_text)
        .then(|| "авторская".to_string())
}

fn extract_publication_date_from_post(post: ElementRef<'_>) -> Option<String> {
    post.select(&selector(
        ".post-time, .post_time, .post-date, .post_date, .posted, .post_head, .post-head, td.catHead, .gensmall",
    ))
    .find_map(|el| parse_publication_date_text(&text(el)))
}

fn extract_publication_date_from_description(description_text: &str) -> Option<String> {
    let date = r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(?:\s+\d{1,2}:\d{2})?|\d{4}-\d{2}-\d{2}(?:\s+\d{1,2}:\d{2})?|\d{1,2}[-\s]\p{L}{3,}[-\s]\d{2,4}(?:\s+\d{1,2}:\d{2})?)";
    let labeled_date = Regex::new(&format!(
        r"(?i)(?:дата\s+(?:публикации|выхода|релиза)|опубликовано)\s*:?\s*{date}"
    ))
    .ok()?;
    if let Some(value) = labeled_date
        .captures(description_text)
        .and_then(|captures| captures.get(1))
        .map(|capture| capture.as_str().trim().to_string())
    {
        return Some(value);
    }

    Regex::new(r"(?i)год\s+выпуска\s*:?\s*(\d{4})")
        .ok()?
        .captures(description_text)
        .and_then(|captures| captures.get(1))
        .map(|capture| capture.as_str().to_string())
}

fn parse_publication_date_text(value: &str) -> Option<String> {
    let date_patterns = [
        r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(?:\s+\d{1,2}:\d{2})?",
        r"\d{4}-\d{2}-\d{2}(?:\s+\d{1,2}:\d{2})?",
        r"\d{1,2}[-\s]\p{L}{3,}[-\s]\d{2,4}(?:\s+\d{1,2}:\d{2})?",
    ];
    date_patterns.iter().find_map(|pattern| {
        Regex::new(pattern)
            .ok()?
            .find(value)
            .map(|matched| matched.as_str().trim().to_string())
    })
}

fn parse_human_u64(value: &str) -> Option<u64> {
    value.replace([' ', ','], "").parse::<u64>().ok()
}

pub fn parse_forum_nodes(html: &str) -> Vec<ForumNode> {
    let doc = Html::parse_document(html);
    let mut nodes = Vec::new();
    for link in doc.select(&selector("a[href*=\"viewforum.php?f=\"]")) {
        let Some(id) = link
            .value()
            .attr("href")
            .and_then(extract_forum_id_from_href)
        else {
            continue;
        };
        let name = text(link);
        if name.is_empty() {
            continue;
        }
        nodes.push(ForumNode {
            id,
            name,
            parent_id: None,
        });
    }
    dedupe_forums(nodes).unwrap_or_default()
}

fn dedupe_forums(nodes: Vec<ForumNode>) -> Result<Vec<ForumNode>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for node in nodes {
        if seen.insert(node.id) {
            out.push(node);
        }
    }
    Ok(out)
}

pub fn parse_size(value: &str) -> Option<u64> {
    let cleaned = value
        .replace("&nbsp;", " ")
        .replace('\u{a0}', " ")
        .trim()
        .to_string();
    let re = Regex::new(r"(?i)(\d+(?:[\s.,]\d+)*)\s*(b|б|kb|кб|kib|mb|мб|mib|gb|гб|gib|tb|тб|tib)")
        .ok()?;
    let captures = re.captures(&cleaned)?;
    let number_raw = captures.get(1)?.as_str();
    let unit = captures.get(2)?.as_str().to_ascii_uppercase();
    let decimal_separator = number_raw.chars().rev().find(|ch| *ch == '.' || *ch == ',');
    let mut number = String::new();
    for ch in number_raw.chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
        } else if Some(ch) == decimal_separator {
            number.push('.');
        }
    }
    let amount = number.parse::<f64>().ok()?;
    let multiplier = match unit.as_str() {
        "B" | "Б" => 1.0,
        "KB" | "КБ" | "KIB" => 1024.0,
        "MB" | "МБ" | "MIB" => 1024.0 * 1024.0,
        "GB" | "ГБ" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "TB" | "ТБ" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let bytes = amount * multiplier;
    if !bytes.is_finite() || bytes < 0.0 || bytes > u64::MAX as f64 {
        return None;
    }
    Some(bytes as u64)
}

pub fn require_magnet(topic: &TopicDetails) -> Result<&str> {
    topic
        .magnet
        .as_deref()
        .ok_or_else(|| anyhow!("RuTracker topic does not expose a magnet link in HTML"))
}

pub fn ensure_same_topic(result: &SearchResult, details: &TopicDetails) {
    if details.title.is_empty() {
        warn!(
            topic_id = result.topic_id,
            "topic details did not expose title"
        );
    }
}

pub fn validate_forum_query(query: &str) -> Result<()> {
    if query.trim().is_empty() {
        bail!("category search query is empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("https://rutracker.org/forum/").unwrap()
    }

    #[test]
    fn parses_size_labels() {
        assert_eq!(parse_size("49.5 MB"), Some(51_904_512));
        assert_eq!(parse_size("1,5 ГБ"), Some(1_610_612_736));
        assert_eq!(parse_size("1 234,5 KB"), Some(1_264_128));
    }

    #[test]
    fn builds_all_time_title_search_form() {
        let fields = search_form_fields("meanna", Some(123));
        let encoded = String::from_utf8(cp1251_form(&fields)).unwrap();
        assert_eq!(encoded, "nm=meanna&max=1&tm=-1&o=10&s=2&f=123");
    }

    #[test]
    fn builds_cp1251_login_form() {
        let fields = login_form_fields("alice", "secret");
        let encoded = String::from_utf8(cp1251_form(&fields)).unwrap();
        assert_eq!(
            encoded,
            "login_username=alice&login_password=secret&login=%C2%F5%EE%E4"
        );
    }

    #[test]
    fn detects_login_success_markers() {
        assert!(login_succeeded(
            r#"<span id="logged-in-username">alice</span>"#
        ));
        assert!(login_succeeded(
            r#"<a class="logged-in-as-uname">alice</a>"#
        ));
        assert!(!login_succeeded("<form action=\"login.php\"></form>"));
    }

    #[test]
    fn parses_search_fixture() {
        let html = include_str!("../tests/fixtures/rutracker/search.html");
        let results = parse_search_results(html, &base()).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].topic_id, 6849001);
        assert_eq!(results[0].title, "Legal Indie Album - 2026 [FLAC]");
        assert_eq!(results[0].author.as_deref(), Some("artist"));
        assert!(
            results[0]
                .author_profile_url
                .as_ref()
                .unwrap()
                .contains("profile.php?mode=viewprofile")
        );
        assert_eq!(results[0].category.as_ref().unwrap().id, 123);
        assert_eq!(results[0].size_bytes, 41_943_040);
        assert_eq!(results[0].seeds, 17);
        assert_eq!(results[0].downloads, 420);
    }

    #[test]
    fn rejects_search_login_form_fixture() {
        let html = include_str!("../tests/fixtures/rutracker/search_login.html");
        let err = parse_search_results(html, &base()).unwrap_err();
        assert!(
            err.to_string()
                .contains("returned the login form instead of search results")
        );
    }

    #[test]
    fn parses_topic_fixture() {
        let html = include_str!("../tests/fixtures/rutracker/topic.html");
        let topic = parse_topic(html, 6849001, &base()).unwrap();
        assert_eq!(topic.title, "Legal Indie Album - 2026 [FLAC]");
        assert_eq!(topic.author.as_ref().unwrap().name, "artist");
        assert!(
            topic
                .author
                .as_ref()
                .unwrap()
                .profile_url
                .as_ref()
                .unwrap()
                .contains("profile.php?mode=viewprofile")
        );
        assert_eq!(topic.author.as_ref().unwrap().posts_count, Some(321));
        assert_eq!(topic.release_type.as_deref(), Some("авторская"));
        assert_eq!(topic.publication_date.as_deref(), Some("07-Jun-26 12:34"));
        assert_eq!(topic.downloads, Some(295));
        assert!(topic.description_text.contains(
            "Тип: авторская\nReleased by the author under a permissive license.\n.torrent скачан: 295 раз"
        ));
        assert!(
            topic
                .description_text
                .contains("01 - First Track.flac - 30 MB\n02 - Second Track.flac - 9 MB")
        );
        assert!(
            topic
                .author
                .as_ref()
                .unwrap()
                .avatar_url
                .as_ref()
                .unwrap()
                .contains("avatar.jpg")
        );
        assert_eq!(topic.first_post_images.len(), 2);
        assert_eq!(topic.first_post_files.len(), 3);
        assert_eq!(topic.first_post_files[0].size_bytes, Some(31_457_280));
        assert!(
            topic
                .magnet
                .as_ref()
                .unwrap()
                .starts_with("magnet:?xt=urn:btih:")
        );
        assert_eq!(topic.comments.len(), 2);
        assert_eq!(topic.comments_page, 1);
        assert_eq!(topic.comments_total_pages, 6);
    }

    #[test]
    fn parses_forum_nodes_fixture() {
        let html = include_str!("../tests/fixtures/rutracker/categories.html");
        let nodes = parse_forum_nodes(html);
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].id, 123);
        assert_eq!(nodes[0].name, "Indie Music");
    }
}
