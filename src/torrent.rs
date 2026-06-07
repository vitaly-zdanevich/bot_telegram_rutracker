use std::path::PathBuf;

use anyhow::{Context, Result};
use lava_torrent::torrent::v1::Torrent;
use sha1::{Digest, Sha1};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TorrentFile {
    pub index: usize,
    pub path: PathBuf,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TorrentMetadata {
    pub name: String,
    pub info_hash: String,
    pub magnet: Option<String>,
    pub files: Vec<TorrentFile>,
    pub torrent_bytes: Option<Vec<u8>>,
}

pub fn parse_torrent_bytes(bytes: Vec<u8>) -> Result<TorrentMetadata> {
    let torrent = Torrent::read_from_bytes(bytes).context("failed to parse torrent metadata")?;
    Ok(metadata_from_lava(torrent))
}

fn metadata_from_lava(torrent: Torrent) -> TorrentMetadata {
    let files = match torrent.files.as_ref() {
        Some(files) => files
            .iter()
            .enumerate()
            .map(|(index, file)| TorrentFile {
                index,
                path: file.path.clone(),
                size_bytes: file.length.max(0) as u64,
            })
            .collect(),
        None => vec![TorrentFile {
            index: 0,
            path: PathBuf::from(&torrent.name),
            size_bytes: torrent.length.max(0) as u64,
        }],
    };

    TorrentMetadata {
        name: torrent.name.clone(),
        info_hash: torrent.info_hash(),
        magnet: torrent.magnet_link().ok(),
        files,
        torrent_bytes: None,
    }
}

pub fn build_magnet(info_hash_hex: &str, name: Option<&str>) -> String {
    let mut magnet = format!("magnet:?xt=urn:btih:{info_hash_hex}");
    if let Some(name) = name.filter(|value| !value.trim().is_empty()) {
        magnet.push_str("&dn=");
        magnet.push_str(&urlencoding::encode(name));
    }
    magnet
}

pub fn info_hash_hex_from_torrent(bytes: &[u8]) -> Result<String> {
    let info =
        extract_info_dictionary(bytes).context("failed to extract torrent info dictionary")?;
    let digest = Sha1::digest(info);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn extract_info_dictionary(bytes: &[u8]) -> Option<&[u8]> {
    let key = b"4:info";
    let pos = bytes.windows(key.len()).position(|window| window == key)?;
    let start = pos + key.len();
    let end = bencode_value_end(bytes, start)?;
    Some(&bytes[start..end])
}

fn bencode_value_end(bytes: &[u8], start: usize) -> Option<usize> {
    match *bytes.get(start)? {
        b'i' => bytes[start..]
            .iter()
            .position(|byte| *byte == b'e')
            .map(|offset| start + offset + 1),
        b'l' | b'd' => {
            let mut pos = start + 1;
            while *bytes.get(pos)? != b'e' {
                pos = bencode_value_end(bytes, pos)?;
            }
            Some(pos + 1)
        }
        b'0'..=b'9' => {
            let colon = bytes[start..].iter().position(|byte| *byte == b':')? + start;
            let len = std::str::from_utf8(&bytes[start..colon])
                .ok()?
                .parse::<usize>()
                .ok()?;
            Some(colon + 1 + len)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::build_magnet;

    #[test]
    fn builds_magnet_with_encoded_name() {
        assert_eq!(
            build_magnet("abcdef", Some("hello world")),
            "magnet:?xt=urn:btih:abcdef&dn=hello%20world"
        );
    }
}
