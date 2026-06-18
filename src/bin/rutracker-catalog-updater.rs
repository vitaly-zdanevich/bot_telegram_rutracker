use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use telegram_rutracker_bot::catalog::{
    DEFAULT_RUTRACKER_CATALOG_TOPIC_ID, rebuild_catalog_from_xml,
};
use telegram_rutracker_bot::config::{Config, optional_env, parse_env};
use telegram_rutracker_bot::downloader::TorrentDownloader;
use telegram_rutracker_bot::rutracker::{RutrackerClient, RutrackerCredentials, require_magnet};
use tracing::{info, warn};

const DEFAULT_CATALOG_DOWNLOAD_TIMEOUT_SECONDS: u64 = 12 * 60 * 60;

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
    let Some(catalog_path) = config.rutracker_catalog_path.as_ref() else {
        info!("local RuTracker catalog is disabled");
        return Ok(());
    };
    let topic_id = config.rutracker_catalog_xml_topic_id;
    let topic_id = if topic_id == 0 {
        DEFAULT_RUTRACKER_CATALOG_TOPIC_ID
    } else {
        topic_id
    };
    let download_timeout_seconds = parse_env(
        "RUTRACKER_CATALOG_DOWNLOAD_TIMEOUT_SECONDS",
        DEFAULT_CATALOG_DOWNLOAD_TIMEOUT_SECONDS,
    )?;

    let credentials = match (
        config.rutracker_username.as_deref(),
        config.rutracker_password.as_deref(),
    ) {
        (Some(username), Some(password)) => Some(RutrackerCredentials::new(username, password)?),
        _ => None,
    };
    let rutracker = RutrackerClient::new(
        &config.rutracker_base_urls,
        config.rutracker_cookie.as_deref(),
        credentials,
        config.http_timeout_seconds,
        config.http_max_attempts,
    )?;
    let topic = rutracker
        .topic(topic_id)
        .await
        .with_context(|| format!("failed to fetch RuTracker catalog topic {topic_id}"))?;
    let magnet = require_magnet(&topic)?.to_string();

    let catalog_dir = catalog_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("RUTRACKER_CATALOG_PATH must include a directory"))?;
    let download_dir = catalog_dir.join("download");
    reset_dir(&download_dir).await?;
    info!(topic_id, download_dir = %download_dir.display(), "downloading RuTracker XML catalog");
    TorrentDownloader::new(download_dir.clone(), config.peer_limit)
        .download_all_files(&magnet, download_timeout_seconds)
        .await?;

    let xml_path = find_xml_dump(&download_dir)?;
    info!(xml_path = %xml_path.display(), db_path = %catalog_path.display(), "rebuilding local catalog index");
    let stats = rebuild_catalog_from_xml(&xml_path, catalog_path)?;
    info!(
        torrents = stats.torrents,
        forums = stats.forums,
        "rebuilt local RuTracker catalog"
    );
    if optional_env("RUTRACKER_CATALOG_KEEP_DOWNLOAD").as_deref() != Some("1") {
        if let Err(err) = tokio::fs::remove_dir_all(&download_dir).await {
            warn!(error = %err, path = %download_dir.display(), "failed to remove catalog download directory");
        }
    }
    Ok(())
}

async fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        tokio::fs::remove_dir_all(path)
            .await
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    tokio::fs::create_dir_all(path)
        .await
        .with_context(|| format!("failed to create {}", path.display()))
}

fn find_xml_dump(root: &Path) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    collect_xml_dump_candidates(root, &mut candidates)?;
    candidates.sort();
    candidates.pop().ok_or_else(|| {
        anyhow::anyhow!("downloaded catalog torrent did not contain .xml or .xml.xz")
    })
}

fn collect_xml_dump_candidates(root: &Path, candidates: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_xml_dump_candidates(&path, candidates)?;
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let lower = name.to_lowercase();
        if lower.ends_with(".xml") || lower.ends_with(".xml.xz") {
            candidates.push(path);
        }
    }
    Ok(())
}
