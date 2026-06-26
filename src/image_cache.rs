use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use sha2::{Digest, Sha256};
use url::Url;

const IMAGE_CACHE_MAX_BYTES: u64 = 10 * 1024 * 1024;
const IMAGE_CACHE_USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/vitaly-zdanevich/bot_telegram_rutracker)"
);

pub struct CachedImage {
    pub bytes: Vec<u8>,
    pub content_type: String,
}

pub fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent(IMAGE_CACHE_USER_AGENT)
        .build()
        .context("failed to build image cache HTTP client")
}

pub async fn cache_remote_image(
    client: &reqwest::Client,
    cache_dir: &Path,
    public_base_url: &str,
    remote_url: &str,
) -> Result<String> {
    let parsed = Url::parse(remote_url)
        .with_context(|| format!("image cache URL is invalid: {remote_url:?}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => bail!("image cache URL must use http or https, got {scheme:?}"),
    }

    let guessed_file_name = cached_file_name(remote_url, None);
    let guessed_path = cache_dir.join(&guessed_file_name);
    if tokio::fs::metadata(&guessed_path).await.is_ok() {
        return public_cached_image_url(public_base_url, &guessed_file_name);
    }

    let response = client
        .get(parsed.clone())
        .send()
        .await
        .map_err(|err| anyhow!("failed to download image for cache: {}", err.without_url()))?;
    let status = response.status();
    if !status.is_success() {
        bail!("image cache download returned HTTP {status}");
    }
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > IMAGE_CACHE_MAX_BYTES
    {
        bail!("image is too large for cache: {length} bytes");
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let mime = image_mime(content_type.as_deref(), parsed.path())?;
    let bytes = response
        .bytes()
        .await
        .map_err(|err| anyhow!("failed to read image cache bytes: {}", err.without_url()))?;
    if bytes.len() as u64 > IMAGE_CACHE_MAX_BYTES {
        bail!("image is too large for cache: {} bytes", bytes.len());
    }

    let file_name = cached_file_name(remote_url, Some(&mime));
    let path = cache_dir.join(&file_name);
    if tokio::fs::metadata(&path).await.is_err() {
        tokio::fs::create_dir_all(cache_dir)
            .await
            .with_context(|| format!("failed to create image cache dir {}", cache_dir.display()))?;
        let temp_path = temp_cache_path(cache_dir, &file_name);
        tokio::fs::write(&temp_path, &bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to write cached image temp file {}",
                    temp_path.display()
                )
            })?;
        tokio::fs::rename(&temp_path, &path)
            .await
            .with_context(|| {
                format!(
                    "failed to move cached image {} to {}",
                    temp_path.display(),
                    path.display()
                )
            })?;
    }

    public_cached_image_url(public_base_url, &file_name)
}

pub async fn read_cached_image(cache_dir: &Path, file_name: &str) -> Result<CachedImage> {
    validate_cache_file_name(file_name)?;
    let path = cache_dir.join(file_name);
    let content_type = mime_guess::from_path(&path)
        .first_raw()
        .filter(|mime| mime.starts_with("image/"))
        .map(str::to_string)
        .ok_or_else(|| anyhow!("cached file has no image content type"))?;
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("failed to read cached image {}", path.display()))?;
    if bytes.len() as u64 > IMAGE_CACHE_MAX_BYTES {
        bail!("cached image is too large: {} bytes", bytes.len());
    }
    Ok(CachedImage {
        bytes,
        content_type,
    })
}

fn cached_file_name(remote_url: &str, mime: Option<&str>) -> String {
    let digest = Sha256::digest(remote_url.as_bytes());
    let extension = mime
        .and_then(extension_for_mime)
        .or_else(|| extension_from_url(remote_url))
        .unwrap_or_else(|| "jpg".to_string());
    format!("{}.{}", hex_lower(&digest), extension)
}

fn public_cached_image_url(public_base_url: &str, file_name: &str) -> Result<String> {
    let mut base = public_base_url.trim().trim_end_matches('/').to_string();
    if base.is_empty() {
        bail!("IMAGE_CACHE_PUBLIC_BASE_URL must not be empty");
    }
    base.push('/');
    let base = Url::parse(&base)
        .with_context(|| format!("IMAGE_CACHE_PUBLIC_BASE_URL has invalid URL {base:?}"))?;
    let url = base
        .join(&format!("image-cache/{file_name}"))
        .context("failed to build public cached image URL")?;
    Ok(url.to_string())
}

pub fn validate_cache_file_name(file_name: &str) -> Result<()> {
    let Some((stem, extension)) = file_name.rsplit_once('.') else {
        bail!("cached image file name has no extension");
    };
    if stem.len() != 64 || !stem.chars().all(|ch| ch.is_ascii_hexdigit()) {
        bail!("cached image file name has invalid hash");
    }
    if extension.is_empty()
        || extension.len() > 8
        || !extension.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        bail!("cached image file name has invalid extension");
    }
    Ok(())
}

fn image_mime(content_type: Option<&str>, path: &str) -> Result<String> {
    if let Some(content_type) = content_type.and_then(|value| value.split(';').next()) {
        let content_type = content_type.trim();
        if content_type.starts_with("image/") {
            return Ok(content_type.to_string());
        }
        if content_type == "application/octet-stream"
            && let Some(mime) = mime_guess::from_path(path).first_raw()
            && mime.starts_with("image/")
        {
            return Ok(mime.to_string());
        }
        bail!("downloaded cache URL is not an image: {content_type}");
    }
    mime_guess::from_path(path)
        .first_raw()
        .filter(|mime| mime.starts_with("image/"))
        .map(str::to_string)
        .ok_or_else(|| anyhow!("downloaded cache URL has no image content type"))
}

fn extension_for_mime(mime: &str) -> Option<String> {
    match mime {
        "image/jpeg" => Some("jpg".to_string()),
        "image/png" => Some("png".to_string()),
        "image/gif" => Some("gif".to_string()),
        "image/webp" => Some("webp".to_string()),
        _ => mime_guess::get_mime_extensions_str(mime)
            .and_then(|extensions| extensions.first().copied())
            .map(str::to_string),
    }
}

fn extension_from_url(remote_url: &str) -> Option<String> {
    Url::parse(remote_url)
        .ok()
        .and_then(|url| {
            Path::new(url.path())
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| value.to_ascii_lowercase())
        })
        .filter(|extension| {
            mime_guess::from_ext(extension)
                .first_raw()
                .map(|mime| mime.starts_with("image/"))
                .unwrap_or(false)
        })
}

fn temp_cache_path(cache_dir: &Path, file_name: &str) -> PathBuf {
    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    cache_dir.join(format!(".{file_name}.{now}.tmp"))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{cached_file_name, public_cached_image_url, validate_cache_file_name};

    #[test]
    fn cached_file_name_uses_stable_hash_and_image_extension() {
        let file_name = cached_file_name("https://img.example/cover.jpg?x=1", None);

        assert!(file_name.ends_with(".jpg"));
        assert_eq!(file_name.len(), 68);
        validate_cache_file_name(&file_name).unwrap();
    }

    #[test]
    fn public_cached_image_url_uses_vm_route() {
        assert_eq!(
            public_cached_image_url("http://203.0.113.10:8080/", &("a".repeat(64) + ".jpg"))
                .unwrap(),
            format!(
                "http://203.0.113.10:8080/image-cache/{}.jpg",
                "a".repeat(64)
            )
        );
    }

    #[test]
    fn rejects_path_like_cache_file_names() {
        assert!(validate_cache_file_name("../cover.jpg").is_err());
        assert!(validate_cache_file_name("abc.jpg").is_err());
        assert!(validate_cache_file_name(&("a".repeat(64) + ".tar.gz")).is_err());
    }
}
