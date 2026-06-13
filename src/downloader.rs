use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, DhtSessionConfig, ListOnlyResponse,
    ListenerMode, ListenerOptions, Session, SessionOptions, SessionPersistenceConfig, TorrentStats,
};
use nix::sys::statvfs::statvfs;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tracing::{info, warn};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeedTorrentStats {
    pub id: usize,
    pub name: String,
    pub state: String,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub uploaded_bytes: u64,
    pub upload_speed: String,
}

pub struct DownloadRequest<'a> {
    pub magnet: &'a str,
    pub metadata: &'a TorrentMetadata,
    pub max_file_mb: u64,
    pub selected_indexes: Option<&'a [usize]>,
    pub timeout_seconds: u64,
    pub final_window_seconds: u64,
}

#[derive(Clone, Debug)]
pub struct SeedConfig {
    /// Root directory containing persistent rqbit state and per-topic data dirs.
    pub root_dir: PathBuf,
    /// TCP/uTP port announced for incoming peers when the VM seeds torrents.
    pub listen_port: u16,
    /// Extra free space to keep after fitting the selected download bytes.
    pub disk_reserve_bytes: u64,
}

pub struct TorrentDownloader {
    output_dir: PathBuf,
    peer_limit: usize,
    seed_config: Option<SeedConfig>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SeedSessionKey {
    root_dir: PathBuf,
    listen_port: u16,
    peer_limit: usize,
}

static SEED_SESSIONS: OnceLock<Mutex<HashMap<SeedSessionKey, Arc<Session>>>> = OnceLock::new();
static SEED_DOWNLOAD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

impl TorrentDownloader {
    pub fn new(output_dir: PathBuf, peer_limit: usize) -> Self {
        Self {
            output_dir,
            peer_limit,
            seed_config: None,
        }
    }

    pub fn with_seed_config(mut self, seed_config: SeedConfig) -> Self {
        self.seed_config = Some(seed_config);
        self
    }

    /// Starts the shared persistent seed session without adding a new torrent.
    ///
    /// VM workers call this on startup so the listener opens immediately and
    /// rqbit restores previously persisted torrents before the next Telegram
    /// update arrives.
    pub async fn initialize_seed_session(
        seed_config: SeedConfig,
        peer_limit: usize,
    ) -> Result<Arc<Session>> {
        seed_session(&seed_config, peer_limit).await
    }

    /// Returns a snapshot of torrents currently managed by the persistent VM
    /// seed session.
    pub async fn seed_stats(
        seed_config: SeedConfig,
        peer_limit: usize,
    ) -> Result<Vec<SeedTorrentStats>> {
        let session = seed_session(&seed_config, peer_limit).await?;
        let mut stats = session.with_torrents(|torrents| {
            torrents
                .map(|(id, handle)| {
                    let torrent_stats = handle.stats();
                    SeedTorrentStats {
                        id,
                        name: handle
                            .name()
                            .unwrap_or_else(|| handle.info_hash().as_string()),
                        state: torrent_stats.state.to_string(),
                        progress_bytes: torrent_stats.progress_bytes,
                        total_bytes: torrent_stats.total_bytes,
                        uploaded_bytes: torrent_stats.uploaded_bytes,
                        upload_speed: torrent_stats
                            .live
                            .as_ref()
                            .map(|live| live.upload_speed.to_string())
                            .unwrap_or_else(|| "0.00 MiB/s".to_string()),
                    }
                })
                .collect::<Vec<_>>()
        });
        stats.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(stats)
    }

    /// Resolves torrent metadata from a magnet link without downloading files.
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
        // Seeding mode mutates shared rqbit state and may evict old torrents,
        // so downloads are serialized to avoid deleting files still being sent.
        let _seed_guard = if self.seed_config.is_some() {
            Some(seed_download_lock().lock().await)
        } else {
            None
        };
        let safe_bytes = telegram_safe_file_bytes(request.max_file_mb);
        let selected_files =
            selected_download_files(request.metadata, request.selected_indexes, safe_bytes)?;
        let selected_file_indexes = selected_files
            .iter()
            .map(|file| file.index)
            .collect::<Vec<_>>();

        self.prepare_output_dir(&selected_files).await?;
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

    async fn prepare_output_dir(&self, selected_files: &[TorrentFile]) -> Result<()> {
        let Some(seed_config) = self.seed_config.as_ref() else {
            cleanup_stale_download_dirs(&self.output_dir).await;
            return Ok(());
        };
        tokio::fs::create_dir_all(&seed_config.root_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to create seed cache directory {}",
                    seed_config.root_dir.display()
                )
            })?;
        let session = self.session().await?;
        let selected_bytes = selected_files
            .iter()
            .map(|file| file.size_bytes)
            .sum::<u64>();
        ensure_seed_capacity(&session, seed_config, &self.output_dir, selected_bytes).await
    }

    async fn session(&self) -> Result<Arc<Session>> {
        if let Some(seed_config) = self.seed_config.as_ref() {
            return seed_session(seed_config, self.peer_limit).await;
        }
        self.new_session(self.output_dir.clone(), None).await
    }

    async fn new_session(
        &self,
        output_dir: PathBuf,
        seed_config: Option<&SeedConfig>,
    ) -> Result<Arc<Session>> {
        Session::new_with_opts(
            output_dir,
            SessionOptions {
                dht: Some(DhtSessionConfig {
                    persistence: None,
                    ..Default::default()
                }),
                fastresume: seed_config.is_some(),
                persistence: seed_config.map(|seed_config| SessionPersistenceConfig::Json {
                    folder: Some(seed_config.root_dir.join(".rqbit-session")),
                }),
                listen: seed_config.map(|seed_config| ListenerOptions {
                    mode: ListenerMode::TcpAndUtp,
                    listen_addr: SocketAddr::from((Ipv4Addr::UNSPECIFIED, seed_config.listen_port)),
                    announce_port: Some(seed_config.listen_port),
                    ipv4_only: true,
                    ..Default::default()
                }),
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

fn seed_download_lock() -> &'static Mutex<()> {
    SEED_DOWNLOAD_LOCK.get_or_init(|| Mutex::new(()))
}

async fn seed_session(seed_config: &SeedConfig, peer_limit: usize) -> Result<Arc<Session>> {
    // rqbit sessions own listener sockets and restored torrent handles, so the
    // process keeps one shared session per seed configuration.
    let key = SeedSessionKey {
        root_dir: seed_config.root_dir.clone(),
        listen_port: seed_config.listen_port,
        peer_limit,
    };
    let sessions = SEED_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = sessions.lock().await;
    if let Some(session) = guard.get(&key) {
        return Ok(session.clone());
    }
    tokio::fs::create_dir_all(&seed_config.root_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create seed cache directory {}",
                seed_config.root_dir.display()
            )
        })?;
    let downloader = TorrentDownloader::new(seed_config.root_dir.clone(), peer_limit);
    let session = downloader
        .new_session(seed_config.root_dir.clone(), Some(seed_config))
        .await?;
    if let Some(addr) = session.listen_addr() {
        info!(%addr, "torrent seed listener is active");
    }
    guard.insert(key, session.clone());
    Ok(session)
}

async fn ensure_seed_capacity(
    session: &Arc<Session>,
    seed_config: &SeedConfig,
    current_output_dir: &Path,
    selected_bytes: u64,
) -> Result<()> {
    // We already know the selected files size before adding the torrent. Evict
    // only when that known size plus the optional reserve will not fit.
    let required_free_bytes = selected_bytes.saturating_add(seed_config.disk_reserve_bytes);
    if has_required_space(&seed_config.root_dir, required_free_bytes)? {
        return Ok(());
    }

    let mut ids = session.with_torrents(|torrents| {
        let mut ids = torrents.map(|(id, _)| id).collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    });
    for id in ids.drain(..) {
        warn!(id, "evicting seeded torrent to free disk space");
        if let Err(err) = session.delete(id.into(), true).await {
            warn!(id, error = %err, "failed to delete seeded torrent during eviction");
        }
        if has_required_space(&seed_config.root_dir, required_free_bytes)? {
            return Ok(());
        }
    }

    remove_old_seed_dirs_until_enough(
        &seed_config.root_dir,
        current_output_dir,
        required_free_bytes,
    )
    .await?;

    if has_required_space(&seed_config.root_dir, required_free_bytes)? {
        return Ok(());
    }
    let available = available_bytes(&seed_config.root_dir)?;
    bail!(
        "not enough disk space for torrent: need {} bytes available including reserve, have {}",
        required_free_bytes,
        available
    )
}

fn has_required_space(path: &Path, required_free_bytes: u64) -> Result<bool> {
    Ok(available_bytes(path)? >= required_free_bytes)
}

fn available_bytes(path: &Path) -> Result<u64> {
    let stat = statvfs(path).with_context(|| {
        format!(
            "failed to read available filesystem space for {}",
            path.display()
        )
    })?;
    let bytes = (stat.blocks_available() as u128).saturating_mul(stat.fragment_size() as u128);
    Ok(bytes.min(u64::MAX as u128) as u64)
}

async fn remove_old_seed_dirs_until_enough(
    root_dir: &Path,
    current_output_dir: &Path,
    required_free_bytes: u64,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(root_dir)
        .await
        .with_context(|| format!("failed to read seed cache directory {}", root_dir.display()))?;
    let mut dirs = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path == current_output_dir {
            continue;
        }
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("rutracker-") {
            continue;
        }
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() {
            continue;
        }
        let modified = entry
            .metadata()
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        dirs.push((modified, path));
    }
    dirs.sort_by_key(|(modified, path)| (*modified, path.clone()));

    for (_, path) in dirs {
        warn!(path = %path.display(), "removing old seed directory to free disk space");
        if let Err(err) = tokio::fs::remove_dir_all(&path).await {
            warn!(path = %path.display(), error = %err, "failed to remove old seed directory");
        }
        if has_required_space(root_dir, required_free_bytes)? {
            break;
        }
    }
    Ok(())
}

async fn cleanup_stale_download_dirs(current_output_dir: &Path) {
    let Some(parent) = current_output_dir.parent() else {
        return;
    };
    let Some(current_name) = current_output_dir.file_name() else {
        return;
    };
    let Ok(mut entries) = tokio::fs::read_dir(parent).await else {
        return;
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        if name == current_name || !name.to_string_lossy().starts_with("rutracker-") {
            continue;
        }
        let path = entry.path();
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if file_type.is_dir() {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
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

#[cfg(test)]
mod tests {
    use super::cleanup_stale_download_dirs;

    #[tokio::test]
    async fn removes_stale_rutracker_download_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let stale = tmp.path().join("rutracker-1-100");
        let current = tmp.path().join("rutracker-2-200");
        let unrelated = tmp.path().join("other");
        tokio::fs::create_dir_all(&stale).await.unwrap();
        tokio::fs::create_dir_all(&current).await.unwrap();
        tokio::fs::create_dir_all(&unrelated).await.unwrap();

        cleanup_stale_download_dirs(&current).await;

        assert!(!stale.exists());
        assert!(current.exists());
        assert!(unrelated.exists());
    }
}
