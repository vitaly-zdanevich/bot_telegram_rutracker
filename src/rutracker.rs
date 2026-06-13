use std::collections::{HashMap, HashSet};
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
    pub html: String,
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
                if let Some(spoiler_head) = node
                    .ancestors()
                    .filter_map(ElementRef::wrap)
                    .find(|element| element_has_class(element.value(), "sp-head"))
                    .map(text)
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                {
                    push_description_blank_line(&mut out);
                    push_description_newline(&mut out);
                    out.push_str("== ");
                    out.push_str(&spoiler_head);
                    out.push_str(" ==");
                    push_description_newline(&mut out);
                    continue;
                }
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
                if element.name() == "br"
                    || is_description_block(element.name())
                    || has_description_break_class(element) =>
            {
                if element_has_class(element, "sp-wrap") || element_has_class(element, "post-b") {
                    push_description_blank_line(&mut out);
                }
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

fn has_description_break_class(element: &scraper::node::Element) -> bool {
    ["post-b", "sp-head", "sp-body", "sp-wrap"]
        .iter()
        .any(|name| element_has_class(element, name))
}

fn element_has_class(element: &scraper::node::Element, class_name: &str) -> bool {
    element
        .attr("class")
        .is_some_and(|class| class.split_whitespace().any(|name| name == class_name))
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

fn push_description_blank_line(out: &mut String) {
    while out.ends_with(' ') || out.ends_with('\t') {
        out.pop();
    }
    if out.is_empty() {
        return;
    }
    if out.ends_with("\n\n") {
        return;
    }
    if out.ends_with('\n') {
        out.push('\n');
    } else {
        out.push_str("\n\n");
    }
}

fn normalize_description_text(value: &str) -> String {
    let mut normalized = Vec::new();
    let mut previous_blank = false;
    for line in value.lines().map(|line| line.trim().replace(" :", ":")) {
        if line.is_empty() {
            if !normalized.is_empty() && !previous_blank {
                normalized.push(String::new());
            }
            previous_blank = true;
            continue;
        }
        normalized.push(line);
        previous_blank = false;
    }
    while normalized.last().is_some_and(|line| line.is_empty()) {
        normalized.pop();
    }
    normalized.join("\n")
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

    Ok(dedupe_search_results(results))
}

fn dedupe_search_results(results: Vec<SearchResult>) -> Vec<SearchResult> {
    let mut output = Vec::with_capacity(results.len());
    let mut indexes = HashMap::new();

    for result in results {
        if let Some(index) = indexes.get(&result.topic_id).copied() {
            merge_search_result(&mut output[index], result);
        } else {
            indexes.insert(result.topic_id, output.len());
            output.push(result);
        }
    }

    output
}

fn merge_search_result(existing: &mut SearchResult, incoming: SearchResult) {
    if existing.title.is_empty() {
        existing.title = incoming.title;
    }
    if existing.author.is_none() {
        existing.author = incoming.author;
    }
    if existing.author_profile_url.is_none() {
        existing.author_profile_url = incoming.author_profile_url;
    }
    if existing.category.is_none() {
        existing.category = incoming.category;
    }
    if existing.size_bytes == 0 {
        existing.size_bytes = incoming.size_bytes;
    }
    if existing.seeds == 0 {
        existing.seeds = incoming.seeds;
    }
    if existing.downloads == 0 {
        existing.downloads = incoming.downloads;
    }
    if existing.category_url.is_none() {
        existing.category_url = incoming.category_url;
    }
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
    let mut post_containers = post_containers(&doc);
    let first_visible_post = post_containers.first().copied();
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
        .select(&selector(
            "table.navTitle a[href*=\"viewforum.php?f=\"], \
             .navTitle a[href*=\"viewforum.php?f=\"], \
             .breadcrumb a[href*=\"viewforum.php?f=\"], \
             td.nav a[href*=\"viewforum.php?f=\"], \
             td.t-breadcrumb-top a[href*=\"viewforum.php?f=\"]",
        ))
        .filter_map(|link| {
            let id = link
                .value()
                .attr("href")
                .and_then(extract_forum_id_from_href)?;
            let name = text(link);
            (!name.is_empty()).then_some(CategoryRef { id, name })
        })
        .collect::<Vec<_>>();

    let author = topic_post.and_then(|post| parse_author_from_post(post, base_url));
    let description_body = topic_post.and_then(topic_description_body);

    let description_html = description_body
        .map(|body| body.inner_html())
        .unwrap_or_default();
    let description_text = description_body.map(description_text).unwrap_or_default();
    let release_type = extract_release_type(topic_post, description_body, &description_text);
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
    let comments = parse_comments_from_posts(&mut post_containers, base_url, comments_page == 1);
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
    let author = post
        .select(&selector(
            ".nick a, p.nick a, .nick, p.nick, .postauthor, .post-author a, .post-author",
        ))
        .next()?;
    let name = text(author);
    if name.is_empty() {
        return None;
    }
    let profile_url = author
        .value()
        .attr("href")
        .filter(|href| href.contains("profile.php"))
        .and_then(|href| attr_url(base_url, href))
        .or_else(|| {
            author_user_id_from_post(post).and_then(|user_id| profile_url(base_url, user_id))
        })
        .or_else(|| {
            post.select(&selector(
                ".poster_btn a[href*=\"profile.php?mode=viewprofile\"], \
                 .poster_info a[href*=\"profile.php?mode=viewprofile\"], \
                 .poster-profile a[href*=\"profile.php?mode=viewprofile\"], \
                 .postdetails a[href*=\"profile.php?mode=viewprofile\"]",
            ))
            .next()
            .and_then(|author| {
                author
                    .value()
                    .attr("href")
                    .and_then(|href| attr_url(base_url, href))
            })
        });
    let posts_count = post
        .select(&selector(
            ".poster_info, .poster-profile, .postdetails, .joined, .post-author-details, td.row1",
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
        .select(&selector(
            "img.avatar, .poster_info img, .poster-profile img, .postdetails img, td.row1 img",
        ))
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

fn author_user_id_from_post(post: ElementRef<'_>) -> Option<u64> {
    post.select(&selector("[data-ext_link_data]"))
        .find_map(|element| {
            let data = element.value().attr("data-ext_link_data")?;
            Regex::new(r#""u"\s*:\s*(\d+)"#)
                .ok()?
                .captures(data)?
                .get(1)?
                .as_str()
                .parse()
                .ok()
        })
}

fn profile_url(base_url: &Url, user_id: u64) -> Option<String> {
    attr_url(
        base_url,
        &format!("profile.php?mode=viewprofile&u={user_id}"),
    )
}

fn topic_description_body(post: ElementRef<'_>) -> Option<ElementRef<'_>> {
    post.select(&selector(".post_body"))
        .next()
        .or_else(|| post.select(&selector("td.message")).next())
}

fn image_urls(body: ElementRef<'_>, base_url: &Url) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    // RuTracker renders some BBCode images as a var.postImg element whose
    // title contains the real image URL instead of using <img src=...>.
    for image in body.select(&selector("var.postImg[title]")) {
        let Some(src) = image.value().attr("title") else {
            continue;
        };
        let Some(url) = attr_url(base_url, src) else {
            continue;
        };
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }
    for img in body.select(&selector("img")) {
        let Some(src) = img.value().attr("src") else {
            continue;
        };
        let Some(url) = attr_url(base_url, src) else {
            continue;
        };
        if is_rutracker_ui_image(&url) {
            continue;
        }
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }
    urls
}

fn is_rutracker_ui_image(url: &str) -> bool {
    url.contains("static.rutracker.cc/templates/")
        || url.contains("static.rutracker.cc/smiles/")
        || url.contains("/templates/")
        || url.contains("/smiles/")
}

fn parse_author_near_body(body: ElementRef<'_>, base_url: &Url) -> Option<AuthorDetails> {
    parse_author_from_post(body, base_url).or_else(|| {
        body.ancestors()
            .filter_map(ElementRef::wrap)
            .take(12)
            .filter(|candidate| {
                candidate
                    .select(&selector(".post_body, td.message"))
                    .take(2)
                    .count()
                    <= 1
            })
            .find_map(|candidate| parse_author_from_post(candidate, base_url))
    })
}

fn post_containers(doc: &Html) -> Vec<ElementRef<'_>> {
    let modern = doc
        .select(&selector("tbody[id^=\"post_\"]"))
        .filter(|post| topic_description_body(*post).is_some())
        .collect::<Vec<_>>();
    if !modern.is_empty() {
        return modern;
    }

    let forumline_rows = doc
        .select(&selector("table.forumline tr[id]"))
        .filter(|post| topic_description_body(*post).is_some())
        .collect::<Vec<_>>();
    if !forumline_rows.is_empty() {
        return forumline_rows;
    }

    doc.select(&selector(".post_wrap, .post"))
        .filter(|post| topic_description_body(*post).is_some())
        .collect()
}

fn parse_comments_from_posts(
    posts: &mut [ElementRef<'_>],
    base_url: &Url,
    skip_first_post: bool,
) -> Vec<TopicComment> {
    let mut comments = Vec::new();
    for (index, post) in posts.iter().enumerate() {
        if skip_first_post && index == 0 {
            continue;
        }
        let Some(body) = topic_description_body(*post) else {
            continue;
        };
        let text = normalize_comment_plain_text(&text(body));
        if text.is_empty() {
            continue;
        }
        comments.push(TopicComment {
            author: parse_author_from_post(*post, base_url)
                .or_else(|| parse_author_near_body(body, base_url)),
            text,
            html: comment_html(body, base_url),
        });
    }
    comments
}

fn comment_html(body: ElementRef<'_>, base_url: &Url) -> String {
    let mut out = String::new();
    for node in body.descendants() {
        match node.value() {
            Node::Text(value) => {
                if node
                    .ancestors()
                    .filter_map(ElementRef::wrap)
                    .any(|element| matches!(element.value().name(), "script" | "style"))
                {
                    continue;
                }
                let Some(fragment) = normalize_comment_text_fragment(value) else {
                    continue;
                };
                push_comment_text_fragment(
                    &mut out,
                    &fragment,
                    comment_markup_stack(node.ancestors().filter_map(ElementRef::wrap), base_url),
                );
            }
            Node::Element(element)
                if element.name() == "br"
                    || is_description_block(element.name())
                    || has_description_break_class(element) =>
            {
                push_comment_newline(&mut out);
            }
            _ => {}
        }
    }
    normalize_comment_html(&out)
}

fn normalize_comment_plain_text(value: &str) -> String {
    value
        .replace(" .", ".")
        .replace(" ,", ",")
        .replace(" :", ":")
        .replace(" ;", ";")
        .replace(" !", "!")
        .replace(" ?", "?")
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommentMarkup {
    Bold,
    Italic,
    Underline,
    Strike,
    Spoiler,
    Code,
    Link(String),
}

fn comment_markup_stack<'a>(
    ancestors: impl Iterator<Item = ElementRef<'a>>,
    base_url: &Url,
) -> Vec<CommentMarkup> {
    let mut stack = Vec::new();
    for element in ancestors.collect::<Vec<_>>().into_iter().rev() {
        let name = element.value().name();
        if let "body" | "html" | "document" = name {
            continue;
        }
        if name == "a" {
            if let Some(url) = element
                .value()
                .attr("href")
                .and_then(|href| attr_url(base_url, href))
            {
                stack.push(CommentMarkup::Link(url));
            }
            continue;
        }
        match name {
            "b" | "strong" => stack.push(CommentMarkup::Bold),
            "i" | "em" => stack.push(CommentMarkup::Italic),
            "u" | "ins" => stack.push(CommentMarkup::Underline),
            "s" | "strike" | "del" => stack.push(CommentMarkup::Strike),
            "tg-spoiler" => stack.push(CommentMarkup::Spoiler),
            "code" | "pre" => stack.push(CommentMarkup::Code),
            _ => {
                if element_has_class(element.value(), "post-b") {
                    stack.push(CommentMarkup::Bold);
                }
                if element_has_class(element.value(), "post-i") {
                    stack.push(CommentMarkup::Italic);
                }
                if element_has_class(element.value(), "post-u") {
                    stack.push(CommentMarkup::Underline);
                }
                if element_has_class(element.value(), "post-s") {
                    stack.push(CommentMarkup::Strike);
                }
                if element_has_class(element.value(), "spoiler") {
                    stack.push(CommentMarkup::Spoiler);
                }
            }
        }
    }
    stack
}

fn push_comment_text_fragment(out: &mut String, fragment: &str, stack: Vec<CommentMarkup>) {
    if !out.is_empty() && !out.ends_with('\n') && !starts_with_no_space_punctuation(fragment) {
        out.push(' ');
    }
    let mut escaped = html_escape::encode_text(fragment).to_string();
    for markup in stack.iter().rev() {
        escaped = match markup {
            CommentMarkup::Bold => format!("<b>{escaped}</b>"),
            CommentMarkup::Italic => format!("<i>{escaped}</i>"),
            CommentMarkup::Underline => format!("<u>{escaped}</u>"),
            CommentMarkup::Strike => format!("<s>{escaped}</s>"),
            CommentMarkup::Spoiler => format!("<tg-spoiler>{escaped}</tg-spoiler>"),
            CommentMarkup::Code => format!("<code>{escaped}</code>"),
            CommentMarkup::Link(url) => format!(
                "<a href=\"{}\">{escaped}</a>",
                html_escape::encode_double_quoted_attribute(url)
            ),
        };
    }
    out.push_str(&escaped);
}

fn normalize_comment_text_fragment(value: &str) -> Option<String> {
    let fragment = value.split_whitespace().collect::<Vec<_>>().join(" ");
    (!fragment.is_empty()).then_some(fragment)
}

fn starts_with_no_space_punctuation(value: &str) -> bool {
    value
        .chars()
        .next()
        .is_some_and(|ch| matches!(ch, ':' | ',' | '.' | ';' | '!' | '?' | ')' | ']' | '}'))
}

fn push_comment_newline(out: &mut String) {
    while out.ends_with(' ') || out.ends_with('\t') {
        out.pop();
    }
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
}

fn normalize_comment_html(value: &str) -> String {
    let mut normalized = Vec::new();
    let mut previous_blank = false;
    for line in value.lines().map(|line| line.trim()) {
        if line.is_empty() {
            if !normalized.is_empty() && !previous_blank {
                normalized.push(String::new());
            }
            previous_blank = true;
            continue;
        }
        normalized.push(line.to_string());
        previous_blank = false;
    }
    while normalized.last().is_some_and(|line| line.is_empty()) {
        normalized.pop();
    }
    normalized.join("\n")
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

fn extract_release_type(
    topic_post: Option<ElementRef<'_>>,
    description_body: Option<ElementRef<'_>>,
    description_text: &str,
) -> Option<String> {
    if Regex::new(r"(?i)тип\s*:\s*авторская")
        .ok()?
        .is_match(description_text)
    {
        return Some("авторская".to_string());
    }
    if topic_post.is_some_and(|post| {
        Regex::new(r"(?i)тип\s*:\s*авторская")
            .ok()
            .is_some_and(|pattern| pattern.is_match(&text(post)))
    }) {
        return Some("авторская".to_string());
    }

    description_body
        .is_some_and(has_author_release_marker)
        .then(|| "авторская".to_string())
}

fn has_author_release_marker(body: ElementRef<'_>) -> bool {
    body.select(&selector("img[src], var[title]"))
        .any(|element| {
            ["src", "title"].iter().any(|attr| {
                element
                    .value()
                    .attr(attr)
                    .is_some_and(|value| value.to_lowercase().contains("authors_release"))
            })
        })
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
        .and_then(|capture| normalize_publication_date(capture.as_str()))
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
    let date = r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(?:\s+\d{1,2}:\d{2})?|\d{4}-\d{2}-\d{2}(?:\s+\d{1,2}:\d{2})?|\d{1,2}[-\s]\p{L}{3,}[-\s]\d{2,4}(?:\s+\d{1,2}:\d{2})?)";
    if let Some(date) = Regex::new(&format!(r"(?i)(?:ред\.?|edited)\s*{date}"))
        .ok()
        .and_then(|pattern| pattern.captures(value))
        .and_then(|captures| captures.get(1))
        .and_then(|matched| normalize_publication_date(matched.as_str()))
    {
        return Some(date);
    }

    let date_patterns = [
        r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(?:\s+\d{1,2}:\d{2})?",
        r"\d{4}-\d{2}-\d{2}(?:\s+\d{1,2}:\d{2})?",
        r"\d{1,2}[-\s]\p{L}{3,}[-\s]\d{2,4}(?:\s+\d{1,2}:\d{2})?",
    ];
    date_patterns.iter().find_map(|pattern| {
        Regex::new(pattern)
            .ok()?
            .find(value)
            .and_then(|matched| normalize_publication_date(matched.as_str()))
    })
}

fn normalize_publication_date(value: &str) -> Option<String> {
    let value = value.trim();
    if let Some(captures) = Regex::new(r"(?i)^(\d{4})-(\d{1,2})-(\d{1,2})")
        .ok()?
        .captures(value)
    {
        return format_ymd(
            captures.get(1)?.as_str(),
            captures.get(2)?.as_str(),
            captures.get(3)?.as_str(),
        );
    }
    if let Some(captures) = Regex::new(r"(?i)^(\d{1,2})[./](\d{1,2})[./](\d{2,4})")
        .ok()?
        .captures(value)
    {
        return format_ymd(
            captures.get(3)?.as_str(),
            captures.get(2)?.as_str(),
            captures.get(1)?.as_str(),
        );
    }
    if let Some(captures) = Regex::new(r"(?i)^(\d{1,2})[-\s](\p{L}{3,})[-\s](\d{2,4})")
        .ok()?
        .captures(value)
    {
        let month = month_name(captures.get(2)?.as_str())?;
        return format_date_parts(
            normalize_year(captures.get(3)?.as_str())?,
            month,
            captures.get(1)?.as_str().parse().ok()?,
        );
    }
    Regex::new(r"^\d{4}$")
        .ok()?
        .is_match(value)
        .then(|| value.to_string())
}

fn format_ymd(year: &str, month: &str, day: &str) -> Option<String> {
    let year = normalize_year(year)?;
    let month = month_name(month)?;
    let day = day.parse().ok()?;
    format_date_parts(year, month, day)
}

fn format_date_parts(year: u16, month: &'static str, day: u8) -> Option<String> {
    (1..=31)
        .contains(&day)
        .then(|| format!("{year}-{month}-{day:02}"))
}

fn normalize_year(year: &str) -> Option<u16> {
    let value: u16 = year.parse().ok()?;
    Some(if year.len() == 2 {
        if value >= 70 {
            1900 + value
        } else {
            2000 + value
        }
    } else {
        value
    })
}

fn month_name(value: &str) -> Option<&'static str> {
    let value = value.trim_matches('.').to_lowercase();
    match value.as_str() {
        "1" | "01" | "jan" | "january" | "янв" | "январь" | "января" => {
            Some("january")
        }
        "2" | "02" | "feb" | "february" | "фев" | "февраль" | "февраля" => {
            Some("february")
        }
        "3" | "03" | "mar" | "march" | "мар" | "март" | "марта" => Some("march"),
        "4" | "04" | "apr" | "april" | "апр" | "апрель" | "апреля" => Some("april"),
        "5" | "05" | "may" | "май" | "мая" => Some("may"),
        "6" | "06" | "jun" | "june" | "июн" | "июнь" | "июня" => Some("june"),
        "7" | "07" | "jul" | "july" | "июл" | "июль" | "июля" => Some("july"),
        "8" | "08" | "aug" | "august" | "авг" | "август" | "августа" => {
            Some("august")
        }
        "9" | "09" | "sep" | "sept" | "september" | "сен" | "сент" | "сентябрь" | "сентября" => {
            Some("september")
        }
        "10" | "oct" | "october" | "окт" | "октябрь" | "октября" => {
            Some("october")
        }
        "11" | "nov" | "november" | "ноя" | "ноябрь" | "ноября" => Some("november"),
        "12" | "dec" | "december" | "дек" | "декабрь" | "декабря" => {
            Some("december")
        }
        _ => None,
    }
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
        assert_eq!(topic.category_path.last().unwrap().id, 123);
        assert_eq!(topic.category_path.last().unwrap().name, "Indie Music");
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
        assert_eq!(topic.publication_date.as_deref(), Some("2026-june-07"));
        assert_eq!(topic.downloads, Some(295));
        assert!(topic.description_text.contains(
            "Тип: авторская\nReleased by the author under a permissive license.\n.torrent скачан: 295 раз"
        ));
        assert!(
            topic
                .description_text
                .contains("01 - First Track.flac - 30 MB\n02 - Second Track.flac - 9 MB")
        );
        assert!(topic.description_text.contains(
            "== Об исполнителе (группе) ==\nДепрессивно, трансцедентально.\n\n== Об альбоме (сборнике) ==\nRecorded at home and published by the author.\n\nДоп. информация: Источник: официальный сайт https://example.invalid/source"
        ));
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
        assert_eq!(topic.first_post_images.len(), 3);
        assert_eq!(
            topic.first_post_images[0],
            "https://img.example/wrapper-cover.png"
        );
        assert_eq!(topic.first_post_images[1], "https://img.example/cover.jpg");
        assert_eq!(
            topic.first_post_images[2],
            "https://rutracker.org/images/back.jpg"
        );
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
        assert_eq!(topic.comments[0].author.as_ref().unwrap().name, "listener");
        assert_eq!(
            topic.comments[0].author.as_ref().unwrap().posts_count,
            Some(12)
        );
        assert_eq!(
            topic.comments[0].text,
            "Thank you for publishing this. Please keep seeding."
        );
        assert!(topic.comments[0].html.contains(
            "Thank you for <a href=\"https://rutracker.org/forum/viewtopic.php?t=5733243\">publishing this</a>."
        ));
        assert!(topic.comments[0].html.contains("Please keep seeding."));
        assert!(!topic.comments[0].html.contains("style="));
        assert!(!topic.comments[0].html.contains("<span"));
        assert_eq!(topic.comments[1].author.as_ref().unwrap().name, "curator");
        assert_eq!(
            topic.comments[1].author.as_ref().unwrap().posts_count,
            Some(77)
        );
        assert_eq!(topic.comments_page, 1);
        assert_eq!(topic.comments_total_pages, 6);
    }

    #[test]
    fn parses_first_post_author_release_marker() {
        let html = r##"
            <html>
                <body>
                    <h1 class="maintitle">Legal Indie Album</h1>
                    <div class="post_wrap">
                        <div class="post_body">
                            <var class="postImg" title="https://static.rutracker.cc/pic/artsovet/authors_release.png"></var>
                            <span class="post-b">Genre</span>: Indie
                        </div>
                    </div>
                </body>
            </html>
        "##;

        let topic = parse_topic(html, 1, &base()).unwrap();

        assert_eq!(topic.release_type.as_deref(), Some("авторская"));
    }

    #[test]
    fn ignores_later_post_author_release_marker() {
        let html = r#"
            <html>
                <body>
                    <h1 class="maintitle">Legal Indie Album</h1>
                    <div class="post_wrap">
                        <div class="post_body">
                            <span class="post-b">Genre</span>: Indie
                        </div>
                    </div>
                    <div class="post_wrap">
                        <div class="post_body">
                            <var class="postImg" title="https://static.rutracker.cc/pic/artsovet/authors_release.png"></var>
                        </div>
                    </div>
                </body>
            </html>
        "#;

        let topic = parse_topic(html, 1, &base()).unwrap();

        assert_eq!(topic.release_type, None);
    }

    #[test]
    fn parses_modern_post_container_author_and_edited_date() {
        let html = r##"
            <html>
                <body>
                    <h1 class="maintitle">Legal Indie Album</h1>
                    <table class="topic">
                        <tbody id="post_77389736" class="row1">
                            <tr>
                                <td class="poster_info">
                                    <p class="nick nick-author"><a href="#" onclick="return false;">vnon</a></p>
                                    <p class="posts"><em>Сообщений:</em> 184</p>
                                </td>
                                <td class="message">
                                    <div class="post_head">
                                        <p class="post-time">
                                            <img src="https://static.rutracker.cc/templates/v1/images/icon_minipost.gif" alt="">
                                            <a class="p-link small" href="viewtopic.php?t=5733243">18-Май-19 00:09</a>
                                            <span class="posted_since hide-for-print">(7 лет назад, ред. 04-Июн-19 22:28)</span>
                                        </p>
                                    </div>
                                    <div class="post_wrap">
                                        <div class="post_body" data-ext_link_data='{"p":82877079,"t":6190907,"f":441,"u":12610477}'>
                                            <var class="postImg postImgAligned img-right" title="https://img.example/cover.jpg"></var>
                                            First post text.
                                        </div>
                                    </div>
                                    <table class="attach">
                                        <tr>
                                            <td>Тип:</td>
                                            <td><span><b>авторская</b></span></td>
                                        </tr>
                                        <tr>
                                            <td>Статус:</td>
                                            <td><a href="profile.php?mode=viewprofile&amp;u=43642585">DJ Stakan</a></td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </body>
            </html>
        "##;

        let topic = parse_topic(html, 5733243, &base()).unwrap();
        let author = topic.author.as_ref().unwrap();

        assert_eq!(author.name, "vnon");
        assert_eq!(author.posts_count, Some(184));
        assert!(
            author
                .profile_url
                .as_ref()
                .unwrap()
                .contains("profile.php?mode=viewprofile&u=12610477")
        );
        assert_eq!(topic.release_type.as_deref(), Some("авторская"));
        assert_eq!(topic.publication_date.as_deref(), Some("2019-june-04"));
        assert_eq!(
            topic.first_post_images,
            vec!["https://img.example/cover.jpg"]
        );
    }

    #[test]
    fn parses_modern_comment_authors() {
        let html = r##"
            <html>
                <body>
                    <h1 class="maintitle">Legal Indie Album</h1>
                    <table class="topic">
                        <tbody id="post_1" class="row1">
                            <tr>
                                <td class="poster_info">
                                    <p class="nick nick-author">topic_author</p>
                                </td>
                                <td class="message">
                                    <div class="post_wrap">
                                        <div class="post_body" data-ext_link_data='{"p":1,"t":42,"f":441,"u":11}'>First post.</div>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                        <tbody id="post_2" class="row2">
                            <tr>
                                <td class="poster_info">
                                    <p class="nick nick-author"><a href="#" onclick="return false;">listener</a></p>
                                    <p class="posts"><em>Сообщений:</em> 12</p>
                                </td>
                                <td class="message">
                                    <div class="post_wrap">
                                        <div class="post_body" data-ext_link_data='{"p":2,"t":42,"f":441,"u":22}'>Comment text.</div>
                                    </div>
                                </td>
                            </tr>
                            <tr>
                                <td class="poster_btn">
                                    <a class="txtb" href="profile.php?mode=viewprofile&amp;u=22">[Профиль]</a>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </body>
            </html>
        "##;

        let topic = parse_topic(html, 42, &base()).unwrap();

        assert_eq!(topic.comments.len(), 1);
        let author = topic.comments[0].author.as_ref().unwrap();
        assert_eq!(author.name, "listener");
        assert_eq!(author.posts_count, Some(12));
        assert!(
            author
                .profile_url
                .as_ref()
                .unwrap()
                .contains("profile.php?mode=viewprofile&u=22")
        );
        assert_eq!(topic.comments[0].html, "Comment text.");
    }

    #[test]
    fn parses_nested_forumline_comment_authors() {
        let html = r#"
        <html>
          <head><title>Nested topic :: RuTracker.org</title></head>
          <body>
            <table class="forumline">
              <tr id="p1">
                <td class="row1">
                  <span class="postdetails poster-profile">
                    <a href="profile.php?mode=viewprofile&amp;u=11"><b class="postauthor">topic_author</b></a>
                    <span>Сообщений: 10</span>
                  </span>
                </td>
                <td class="row1">
                  <table class="post"><tr><td><div class="post_body">First post text.</div></td></tr></table>
                </td>
              </tr>
              <tr id="p2">
                <td class="row1">
                  <span class="postdetails poster-profile">
                    <a href="profile.php?mode=viewprofile&amp;u=22"><b class="postauthor">listener</b></a>
                    <span>Сообщений: 12</span>
                  </span>
                </td>
                <td class="row1">
                  <table class="post"><tr><td><div class="post_body">Comment text.</div></td></tr></table>
                </td>
              </tr>
            </table>
          </body>
        </html>
        "#;

        let topic = parse_topic(html, 42, &base()).unwrap();

        assert_eq!(topic.comments.len(), 1);
        let author = topic.comments[0].author.as_ref().unwrap();
        assert_eq!(author.name, "listener");
        assert_eq!(author.posts_count, Some(12));
        assert!(
            author
                .profile_url
                .as_ref()
                .unwrap()
                .contains("profile.php?mode=viewprofile")
        );
        assert_eq!(topic.comments[0].text, "Comment text.");
        assert_eq!(topic.comments[0].html, "Comment text.");
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
