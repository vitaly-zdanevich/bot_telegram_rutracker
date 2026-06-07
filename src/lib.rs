pub mod app;
pub mod config;
pub mod downloader;
pub mod rutracker;
pub mod telegram;
pub mod torrent;

pub const TELEGRAM_MAX_FILE_MB_DEFAULT: u64 = 50;
pub const TELEGRAM_UPLOAD_MARGIN_BYTES: u64 = 100 * 1024;

pub fn telegram_safe_file_bytes(max_mb: u64) -> u64 {
    max_mb
        .saturating_mul(1024)
        .saturating_mul(1024)
        .saturating_sub(TELEGRAM_UPLOAD_MARGIN_BYTES)
}
