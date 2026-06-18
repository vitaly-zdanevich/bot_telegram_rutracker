use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use telegram_rutracker_bot::catalog::{
    CatalogSourceMetadata, DEFAULT_RUTRACKER_CATALOG_TOPIC_ID, read_catalog_source_metadata,
    rebuild_catalog_from_xml_with_source,
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
    let source = CatalogSourceMetadata {
        topic_id,
        topic_title: (!topic.title.trim().is_empty()).then(|| topic.title.clone()),
        topic_date: topic.publication_date.clone(),
        info_hash: magnet_info_hash(&magnet).unwrap_or_default(),
        magnet: magnet.clone(),
    };

    let catalog_dir = catalog_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("RUTRACKER_CATALOG_PATH must include a directory"))?;
    let download_dir = catalog_dir.join("download");

    if let Some(existing_source) = read_catalog_source_metadata(catalog_path)? {
        if catalog_source_matches(&existing_source, &source) {
            info!(
                topic_id,
                info_hash = %source.info_hash,
                topic_date = ?source.topic_date,
                "local RuTracker catalog is already current; skipping XML download"
            );
            return Ok(());
        }
        info!(
            topic_id,
            existing_info_hash = %existing_source.info_hash,
            current_info_hash = %source.info_hash,
            existing_topic_date = ?existing_source.topic_date,
            current_topic_date = ?source.topic_date,
            "local RuTracker catalog source changed; rebuilding"
        );
    }

    let mut downloaded_this_run = false;
    let mut xml_path = match find_xml_dump(&download_dir) {
        Ok(xml_path) => {
            info!(xml_path = %xml_path.display(), "using existing RuTracker XML catalog download");
            xml_path
        }
        Err(_) => {
            downloaded_this_run = true;
            download_catalog_xml(
                topic_id,
                &download_dir,
                &magnet,
                download_timeout_seconds,
                config.peer_limit,
            )
            .await?
        }
    };
    info!(xml_path = %xml_path.display(), db_path = %catalog_path.display(), "rebuilding local catalog index");
    let stats = match rebuild_catalog_from_xml_with_source(&xml_path, catalog_path, &source) {
        Ok(stats) => stats,
        Err(err) if !downloaded_this_run => {
            warn!(error = %err, xml_path = %xml_path.display(), "existing XML catalog failed to index; redownloading");
            xml_path = download_catalog_xml(
                topic_id,
                &download_dir,
                &magnet,
                download_timeout_seconds,
                config.peer_limit,
            )
            .await?;
            rebuild_catalog_from_xml_with_source(&xml_path, catalog_path, &source)?
        }
        Err(err) => return Err(err),
    };
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

fn catalog_source_matches(
    existing: &CatalogSourceMetadata,
    current: &CatalogSourceMetadata,
) -> bool {
    // Prefer the torrent info-hash because it identifies the XML dump content.
    // Date comparison is only a fallback for older databases or unusual magnets.
    if !existing.info_hash.trim().is_empty() && !current.info_hash.trim().is_empty() {
        return existing.info_hash.eq_ignore_ascii_case(&current.info_hash);
    }
    if let (Some(existing_date), Some(current_date)) = (
        existing.topic_date.as_deref().map(str::trim),
        current.topic_date.as_deref().map(str::trim),
    ) {
        if !existing_date.is_empty() && existing_date == current_date {
            return true;
        }
    }
    false
}

fn magnet_info_hash(magnet: &str) -> Option<String> {
    let query = magnet.strip_prefix("magnet:?")?;
    url::form_urlencoded::parse(query.as_bytes()).find_map(|(key, value)| {
        (key == "xt")
            .then(|| {
                value
                    .strip_prefix("urn:btih:")
                    .map(|hash| hash.to_uppercase())
            })
            .flatten()
    })
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

async fn download_catalog_xml(
    topic_id: u64,
    download_dir: &Path,
    magnet: &str,
    download_timeout_seconds: u64,
    peer_limit: usize,
) -> Result<PathBuf> {
    reset_dir(download_dir).await?;
    info!(topic_id, download_dir = %download_dir.display(), "downloading RuTracker XML catalog");
    TorrentDownloader::new(download_dir.to_path_buf(), peer_limit)
        .download_all_files(magnet, download_timeout_seconds)
        .await?;
    find_xml_dump(download_dir)
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

#[cfg(test)]
mod tests {
    use super::{catalog_source_matches, magnet_info_hash};
    use telegram_rutracker_bot::catalog::CatalogSourceMetadata;

    fn source(info_hash: &str, topic_date: Option<&str>) -> CatalogSourceMetadata {
        CatalogSourceMetadata {
            topic_id: 5_591_249,
            topic_title: None,
            topic_date: topic_date.map(str::to_string),
            magnet: String::new(),
            info_hash: info_hash.to_string(),
        }
    }

    #[test]
    fn extracts_btih_from_magnet() {
        assert_eq!(
            magnet_info_hash("magnet:?xt=urn:btih:abcdef0123&dn=catalog").as_deref(),
            Some("ABCDEF0123")
        );
    }

    #[test]
    fn catalog_source_match_prefers_info_hash() {
        assert!(catalog_source_matches(
            &source("abcdef", Some("2026-june-18")),
            &source("ABCDEF", Some("different")),
        ));
        assert!(!catalog_source_matches(
            &source("abcdef", Some("2026-june-18")),
            &source("123456", Some("2026-june-18")),
        ));
    }

    #[test]
    fn catalog_source_match_uses_date_when_hash_is_missing() {
        assert!(catalog_source_matches(
            &source("", Some("2026-june-18")),
            &source("", Some("2026-june-18")),
        ));
        assert!(!catalog_source_matches(
            &source("", Some("2026-june-18")),
            &source("", Some("2026-june-19")),
        ));
    }
}
