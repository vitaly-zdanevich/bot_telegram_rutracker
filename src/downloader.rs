use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, DhtSessionConfig, ListOnlyResponse, Session,
    SessionOptions, TorrentStats,
};
use tokio::time::{sleep, timeout};
use tracing::info;

use crate::telegram_safe_file_bytes;
use crate::torrent::{TorrentFile, TorrentMetadata, build_magnet};

#[derive(Debug)]
pub struct DownloadOutcome {
    pub files: Vec<DownloadedFile>,
    pub timed_out: bool,
    pub total_files: usize,
}

#[derive(Clone, Debug)]
pub struct DownloadedFile {
    pub path: PathBuf,
    pub display_name: String,
    pub size_bytes: u64,
}

pub struct DownloadRequest<'a> {
    pub magnet: &'a str,
    pub metadata: &'a TorrentMetadata,
    pub max_file_mb: u64,
    pub selected_indexes: Option<&'a [usize]>,
    pub timeout_seconds: u64,
    pub final_window_seconds: u64,
}

pub struct TorrentDownloader {
    output_dir: PathBuf,
    peer_limit: usize,
}

impl TorrentDownloader {
    pub fn new(output_dir: PathBuf, peer_limit: usize) -> Self {
        Self {
            output_dir,
            peer_limit,
        }
    }

    pub async fn metadata_from_magnet(
        &self,
        magnet: &str,
        timeout_seconds: u64,
    ) -> Result<TorrentMetadata> {
        let session = self.session().await?;
        let response = timeout(
            Duration::from_secs(timeout_seconds),
            session.add_torrent(
                AddTorrent::from_url(magnet),
                Some(AddTorrentOptions {
                    list_only: true,
                    peer_limit: Some(self.peer_limit),
                    ..Default::default()
                }),
            ),
        )
        .await
        .context("timed out while resolving magnet metadata")?
        .context("failed to resolve magnet metadata")?;

        let AddTorrentResponse::ListOnly(list) = response else {
            bail!("magnet metadata request did not return list-only response");
        };
        Ok(metadata_from_list_only(list))
    }

    pub async fn download_small_files<F, Fut>(
        &self,
        request: DownloadRequest<'_>,
        mut on_file_completed: F,
    ) -> Result<DownloadOutcome>
    where
        F: FnMut(DownloadedFile, usize, usize) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let safe_bytes = telegram_safe_file_bytes(request.max_file_mb);
        let selected_files =
            selected_download_files(request.metadata, request.selected_indexes, safe_bytes)?;
        let selected_file_indexes = selected_files
            .iter()
            .map(|file| file.index)
            .collect::<Vec<_>>();

        tokio::fs::create_dir_all(&self.output_dir)
            .await
            .context("failed to create torrent output directory")?;

        let session = self.session().await?;
        let add = request
            .metadata
            .torrent_bytes
            .clone()
            .map(torrent_bytes_as_add)
            .unwrap_or_else(|| AddTorrent::from_url(request.magnet));
        let add_response = session
            .add_torrent(
                add,
                Some(AddTorrentOptions {
                    only_files: Some(selected_file_indexes),
                    overwrite: true,
                    output_folder: Some(self.output_dir.to_string_lossy().into_owned()),
                    peer_limit: Some(self.peer_limit),
                    ..Default::default()
                }),
            )
            .await
            .context("failed to add torrent")?;
        let Some(handle) = add_response.into_handle() else {
            bail!("torrent was not added to downloader");
        };

        let total_files = selected_files.len();
        let started_at = Instant::now();
        let hard_timeout = Duration::from_secs(request.timeout_seconds);
        let final_window = Duration::from_secs(request.final_window_seconds);
        let mut sent_indexes = HashSet::new();
        let mut sent_files = Vec::new();
        let mut timed_out = false;
        loop {
            let elapsed = started_at.elapsed();
            if download_window_expired(elapsed, hard_timeout, final_window) {
                timed_out = true;
                break;
            }

            let stats = handle.stats();
            ensure_torrent_has_no_error(&stats)?;
            send_completed_files(
                &self.output_dir,
                &stats,
                &selected_files,
                &mut sent_indexes,
                &mut sent_files,
                total_files,
                &mut on_file_completed,
            )
            .await?;
            if sent_files.len() == total_files {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        info!(
            elapsed_seconds = started_at.elapsed().as_secs(),
            sent_files = sent_files.len(),
            total_files,
            timed_out,
            "torrent download finished or timed out"
        );
        Ok(DownloadOutcome {
            files: sent_files,
            timed_out,
            total_files,
        })
    }

    async fn session(&self) -> Result<std::sync::Arc<Session>> {
        Session::new_with_opts(
            self.output_dir.clone(),
            SessionOptions {
                dht: Some(DhtSessionConfig {
                    persistence: None,
                    ..Default::default()
                }),
                persistence: None,
                peer_limit: Some(self.peer_limit),
                disable_local_service_discovery: true,
                ipv4_only: true,
                client_name_and_version: Some(format!(
                    "{}/{}",
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION")
                )),
                ..Default::default()
            },
        )
        .await
        .context("failed to create torrent session")
    }
}

fn selected_download_files(
    metadata: &TorrentMetadata,
    selected_indexes: Option<&[usize]>,
    safe_bytes: u64,
) -> Result<Vec<TorrentFile>> {
    let files = metadata
        .files
        .iter()
        .filter(|file| file.size_bytes <= safe_bytes)
        .filter(|file| {
            selected_indexes
                .map(|indexes| indexes.contains(&file.index))
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();

    if files.is_empty() {
        bail!("torrent has no files small enough for Telegram");
    }

    Ok(files)
}

fn download_window_expired(
    elapsed: Duration,
    hard_timeout: Duration,
    final_window: Duration,
) -> bool {
    elapsed >= hard_timeout || hard_timeout.saturating_sub(elapsed) <= final_window
}

fn ensure_torrent_has_no_error(stats: &TorrentStats) -> Result<()> {
    if let Some(error) = stats.error.as_ref() {
        bail!("torrent failed: {error}");
    }
    Ok(())
}

async fn send_completed_files<F, Fut>(
    output_dir: &Path,
    stats: &TorrentStats,
    selected_files: &[TorrentFile],
    sent_indexes: &mut HashSet<usize>,
    sent_files: &mut Vec<DownloadedFile>,
    total_files: usize,
    on_file_completed: &mut F,
) -> Result<()>
where
    F: FnMut(DownloadedFile, usize, usize) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    for file in selected_files {
        if sent_indexes.contains(&file.index) {
            continue;
        }
        let have_bytes = stats.file_progress.get(file.index).copied().unwrap_or(0);
        if have_bytes < file.size_bytes {
            continue;
        }
        let downloaded = DownloadedFile {
            path: output_dir.join(&file.path),
            display_name: file.path.display().to_string(),
            size_bytes: file.size_bytes,
        };
        on_file_completed(downloaded.clone(), sent_files.len() + 1, total_files).await?;
        sent_indexes.insert(file.index);
        sent_files.push(downloaded);
    }
    Ok(())
}

fn metadata_from_list_only(list: ListOnlyResponse) -> TorrentMetadata {
    let files = list
        .info
        .iter_file_details()
        .enumerate()
        .map(|(index, detail)| TorrentFile {
            index,
            path: detail.filename.to_pathbuf(),
            size_bytes: detail.len,
        })
        .collect::<Vec<_>>();
    let name = list
        .info
        .name()
        .map(|name| name.into_owned())
        .unwrap_or_else(|| list.info_hash.as_string());
    let info_hash = list.info_hash.as_string();
    TorrentMetadata {
        magnet: Some(build_magnet(&info_hash, Some(&name))),
        name,
        info_hash,
        files,
        torrent_bytes: Some(list.torrent_bytes.to_vec()),
    }
}

pub fn magnet_from_topic_or_metadata(
    topic_magnet: Option<&str>,
    metadata: &TorrentMetadata,
) -> Result<String> {
    topic_magnet
        .map(str::to_string)
        .or_else(|| metadata.magnet.clone())
        .ok_or_else(|| anyhow!("magnet link is unavailable"))
}

pub fn torrent_bytes_as_add(bytes: Vec<u8>) -> AddTorrent<'static> {
    AddTorrent::from_bytes(Bytes::from(bytes))
}
