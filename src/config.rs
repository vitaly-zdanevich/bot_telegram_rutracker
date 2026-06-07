use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use crate::TELEGRAM_MAX_FILE_MB_DEFAULT;

const DEFAULT_RUTRACKER_BASE_URLS: &str =
    "https://rutracker.org/forum,https://rutracker.net/forum,https://rutracker.nl/forum";
const DEFAULT_SEARCH_LIMIT: usize = 10;
const DEFAULT_HTTP_TIMEOUT_SECONDS: u64 = 25;
const DEFAULT_LAMBDA_TIMEOUT_SECONDS: u64 = 900;
const DEFAULT_DOWNLOAD_MARGIN_SECONDS: u64 = 20;
const DEFAULT_PEER_LIMIT: usize = 120;

#[derive(Clone, Debug)]
pub struct Config {
    pub telegram_bot_token: String,
    pub telegram_webhook_secret: String,
    pub allowed_telegram_user_ids: HashSet<i64>,
    pub rutracker_base_urls: Vec<String>,
    pub rutracker_cookie: Option<String>,
    pub rutracker_username: Option<String>,
    pub rutracker_password: Option<String>,
    pub search_limit: usize,
    pub http_timeout_seconds: u64,
    pub tmp_dir: PathBuf,
    pub max_file_mb: u64,
    pub lambda_timeout_seconds: u64,
    pub download_margin_seconds: u64,
    pub peer_limit: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let (rutracker_username, rutracker_password) = parse_rutracker_credentials(
            optional_env("RUTRACKER_USERNAME"),
            optional_env("RUTRACKER_PASSWORD"),
        )?;

        Ok(Self {
            telegram_bot_token: required_env("TELEGRAM_BOT_TOKEN")?,
            telegram_webhook_secret: required_env("TELEGRAM_WEBHOOK_SECRET")?,
            allowed_telegram_user_ids: parse_allowed_telegram_user_ids()?,
            rutracker_base_urls: parse_rutracker_base_urls()?,
            rutracker_cookie: optional_env("RUTRACKER_COOKIE"),
            rutracker_username,
            rutracker_password,
            search_limit: parse_env("SEARCH_LIMIT", DEFAULT_SEARCH_LIMIT)?.clamp(1, 20),
            http_timeout_seconds: parse_env(
                "RUTRACKER_HTTP_TIMEOUT_SECONDS",
                DEFAULT_HTTP_TIMEOUT_SECONDS,
            )?,
            tmp_dir: PathBuf::from(optional_env("TMP_DIR").unwrap_or_else(|| "/tmp".to_string())),
            max_file_mb: parse_env("MAX_FILE_MB", TELEGRAM_MAX_FILE_MB_DEFAULT)?,
            lambda_timeout_seconds: parse_env(
                "LAMBDA_TIMEOUT_SECONDS",
                DEFAULT_LAMBDA_TIMEOUT_SECONDS,
            )?,
            download_margin_seconds: parse_env(
                "DOWNLOAD_MARGIN_SECONDS",
                DEFAULT_DOWNLOAD_MARGIN_SECONDS,
            )?,
            peer_limit: parse_env("TORRENT_PEER_LIMIT", DEFAULT_PEER_LIMIT)?,
        })
    }

    pub fn download_timeout_seconds(&self) -> u64 {
        self.lambda_timeout_seconds
            .saturating_sub(self.download_margin_seconds)
            .max(1)
    }
}

pub fn required_env(key: &str) -> Result<String> {
    env::var(key)
        .with_context(|| format!("{key} is required"))
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                bail!("{key} must not be empty");
            }
            Ok(trimmed.to_string())
        })
}

pub fn optional_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional_env(key) {
        Some(value) => value
            .parse::<T>()
            .with_context(|| format!("{key} has invalid value {value:?}")),
        None => Ok(default),
    }
}

fn parse_allowed_telegram_user_ids() -> Result<HashSet<i64>> {
    parse_telegram_user_id_set(optional_env("ALLOWED_TELEGRAM_USER_IDS").as_deref())
}

fn parse_rutracker_base_urls() -> Result<Vec<String>> {
    let value = optional_env("RUTRACKER_BASE_URLS")
        .or_else(|| optional_env("RUTRACKER_BASE_URL"))
        .unwrap_or_else(|| DEFAULT_RUTRACKER_BASE_URLS.to_string());

    parse_rutracker_base_url_list(Some(&value))
}

pub fn parse_rutracker_base_url_list(value: Option<&str>) -> Result<Vec<String>> {
    let Some(value) = value else {
        bail!("RUTRACKER_BASE_URLS must not be empty");
    };

    let urls = value
        .split(',')
        .map(str::trim)
        .map(|value| value.trim_end_matches('/'))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    if urls.is_empty() {
        bail!("RUTRACKER_BASE_URLS must contain at least one URL");
    }

    Ok(urls)
}

pub fn parse_rutracker_credentials(
    username: Option<String>,
    password: Option<String>,
) -> Result<(Option<String>, Option<String>)> {
    match (username, password) {
        (Some(username), Some(password)) => Ok((Some(username), Some(password))),
        (None, None) => Ok((None, None)),
        (Some(_), None) => bail!("RUTRACKER_PASSWORD is required when RUTRACKER_USERNAME is set"),
        (None, Some(_)) => bail!("RUTRACKER_USERNAME is required when RUTRACKER_PASSWORD is set"),
    }
}

pub fn parse_telegram_user_id_set(value: Option<&str>) -> Result<HashSet<i64>> {
    let Some(value) = value else {
        return Ok(HashSet::new());
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let user_id = value.parse::<i64>().with_context(|| {
                format!("ALLOWED_TELEGRAM_USER_IDS has invalid user id {value:?}")
            })?;

            if user_id <= 0 {
                bail!("ALLOWED_TELEGRAM_USER_IDS has non-positive user id {value:?}");
            }

            Ok(user_id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        parse_rutracker_base_url_list, parse_rutracker_credentials, parse_telegram_user_id_set,
    };

    #[test]
    fn parses_allowed_user_ids() {
        let parsed = parse_telegram_user_id_set(Some("1, 2,3")).unwrap();
        assert!(parsed.contains(&1));
        assert!(parsed.contains(&2));
        assert!(parsed.contains(&3));
    }

    #[test]
    fn rejects_non_positive_user_ids() {
        assert!(parse_telegram_user_id_set(Some("1,0")).is_err());
    }

    #[test]
    fn parses_rutracker_base_url_list() {
        let parsed = parse_rutracker_base_url_list(Some(
            "https://rutracker.org/forum/, https://rutracker.net/forum",
        ))
        .unwrap();
        assert_eq!(
            parsed,
            vec![
                "https://rutracker.org/forum".to_string(),
                "https://rutracker.net/forum".to_string()
            ]
        );
    }

    #[test]
    fn parses_rutracker_credentials_only_when_complete() {
        let parsed =
            parse_rutracker_credentials(Some("alice".to_string()), Some("pw".to_string())).unwrap();
        assert_eq!(parsed, (Some("alice".to_string()), Some("pw".to_string())));
        assert!(parse_rutracker_credentials(Some("alice".to_string()), None).is_err());
        assert!(parse_rutracker_credentials(None, Some("pw".to_string())).is_err());
        assert_eq!(
            parse_rutracker_credentials(None, None).unwrap(),
            (None, None)
        );
    }
}
