use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use rusqlite::{Connection, OptionalExtension, params};
use tracing::info;
use xz2::read::XzDecoder;

use crate::rutracker::{CategoryRef, SearchResult};

pub const DEFAULT_RUTRACKER_CATALOG_TOPIC_ID: u64 = 5_591_249;
const DEFAULT_TRACKER_ID: u16 = 4;
const INSERT_BATCH_SIZE: u64 = 10_000;

#[derive(Clone, Debug)]
pub struct OfflineCatalog {
    db_path: PathBuf,
    forum_base_url: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogBuildStats {
    pub torrents: u64,
    pub forums: u64,
}

#[derive(Default)]
struct XmlTorrentRecord {
    topic_id: u64,
    title: String,
    info_hash: String,
    tracker_id: u16,
    size_bytes: u64,
    published: String,
    published_unix: Option<i64>,
    forum_id: u64,
    forum_name: String,
}

#[derive(Default)]
struct XmlState {
    record: Option<XmlTorrentRecord>,
    torrent_depth: usize,
    text_target: TextTarget,
}

#[derive(Copy, Clone, Default, Eq, PartialEq)]
enum TextTarget {
    #[default]
    None,
    Title,
    Forum,
}

impl OfflineCatalog {
    pub fn new(db_path: PathBuf, forum_base_url: String) -> Self {
        Self {
            db_path,
            forum_base_url: forum_base_url.trim_end_matches('/').to_string(),
        }
    }

    pub fn exists(&self) -> bool {
        self.db_path.is_file()
    }

    pub fn search(
        &self,
        query: &str,
        forum_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        if query.trim().is_empty() || !self.exists() {
            return Ok(Vec::new());
        }
        let conn =
            Connection::open_with_flags(&self.db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .with_context(|| {
                    format!("failed to open catalog SQLite {}", self.db_path.display())
                })?;

        let schema = detect_schema(&conn)?;
        match schema {
            CatalogSchema::Current => self.search_current_schema(&conn, query, forum_id, limit),
            CatalogSchema::Legacy => self.search_legacy_schema(&conn, query, forum_id, limit),
        }
    }

    fn search_current_schema(
        &self,
        conn: &Connection,
        query: &str,
        forum_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        if table_exists(conn, "torrent_fts") {
            let fts_query = fts_query(query);
            if let Some(fts_query) = fts_query {
                let results = self.search_current_fts(conn, &fts_query, forum_id, limit)?;
                if !results.is_empty() {
                    return Ok(results);
                }
            }
        }
        self.search_current_like(conn, query, forum_id, limit)
    }

    fn search_current_fts(
        &self,
        conn: &Connection,
        fts_query: &str,
        forum_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let limit = limit_to_i64(limit);
        let mut sql = current_select_sql(
            "JOIN torrent_fts ON torrent_fts.rowid = torrent.topic_id WHERE torrent_fts MATCH ?",
            forum_id,
        );
        sql.push_str(" ORDER BY torrent.published_unix DESC, torrent.topic_id DESC LIMIT ?");
        let mut stmt = conn.prepare(&sql)?;
        let rows = if let Some(forum_id) = forum_id {
            stmt.query_map(
                params![fts_query, forum_id_to_i64(forum_id), limit],
                |row| self.current_row(row),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            stmt.query_map(params![fts_query, limit], |row| self.current_row(row))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(rows)
    }

    fn search_current_like(
        &self,
        conn: &Connection,
        query: &str,
        forum_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let limit = limit_to_i64(limit);
        let pattern = like_pattern(query);
        let mut sql = current_select_sql("WHERE torrent.title LIKE ? ESCAPE '\\'", forum_id);
        sql.push_str(" ORDER BY torrent.published_unix DESC, torrent.topic_id DESC LIMIT ?");
        let mut stmt = conn.prepare(&sql)?;
        let rows = if let Some(forum_id) = forum_id {
            stmt.query_map(params![pattern, forum_id_to_i64(forum_id), limit], |row| {
                self.current_row(row)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            stmt.query_map(params![pattern, limit], |row| self.current_row(row))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(rows)
    }

    fn current_row(&self, row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchResult> {
        let topic_id: u64 = row.get(0)?;
        let title: String = row.get(1)?;
        let info_hash: String = row.get(2)?;
        let tracker_id: u16 = row.get(3)?;
        let size_bytes: u64 = row.get(4)?;
        let published: Option<String> = row.get(5)?;
        let forum_id: u64 = row.get(6)?;
        let forum_name: String = row.get(7)?;
        Ok(SearchResult {
            topic_id,
            title,
            author: None,
            author_profile_url: None,
            category: Some(CategoryRef {
                id: forum_id,
                name: forum_name,
            }),
            size_bytes,
            seeds: 0,
            downloads: 0,
            topic_url: self.topic_url(topic_id),
            category_url: Some(self.category_url(forum_id)),
            magnet: Some(magnet_from_hash(&info_hash, tracker_id)),
            published,
            local_catalog: true,
        })
    }

    fn search_legacy_schema(
        &self,
        conn: &Connection,
        query: &str,
        forum_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let limit = limit_to_i64(limit);
        let pattern = like_pattern(query);
        let mut sql = String::from(
            "SELECT data.ID, data.NAME, data.HASH, data.SIZE, data.REG_DATE, \
             data.FORUM_ID, data.FORUM_NAME FROM data WHERE data.NAME LIKE ? ESCAPE '\\'",
        );
        if forum_id.is_some() {
            sql.push_str(" AND data.FORUM_ID = ?");
        }
        sql.push_str(" ORDER BY data.REG_DATE DESC, data.ID DESC LIMIT ?");
        let mut stmt = conn.prepare(&sql)?;
        let rows = if let Some(forum_id) = forum_id {
            stmt.query_map(params![pattern, forum_id_to_i64(forum_id), limit], |row| {
                self.legacy_row(row)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            stmt.query_map(params![pattern, limit], |row| self.legacy_row(row))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(rows)
    }

    fn legacy_row(&self, row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchResult> {
        let topic_id: u64 = row.get(0)?;
        let title: String = row.get(1)?;
        let hash_or_magnet: String = row.get(2)?;
        let size_bytes: u64 = row.get(3)?;
        let published: Option<String> = row.get(4)?;
        let forum_id: u64 = row.get(5)?;
        let forum_name: String = row.get(6)?;
        let magnet = if hash_or_magnet.starts_with("magnet:") {
            hash_or_magnet
        } else {
            magnet_from_hash(&hash_or_magnet, DEFAULT_TRACKER_ID)
        };
        Ok(SearchResult {
            topic_id,
            title,
            author: None,
            author_profile_url: None,
            category: Some(CategoryRef {
                id: forum_id,
                name: forum_name,
            }),
            size_bytes,
            seeds: 0,
            downloads: 0,
            topic_url: self.topic_url(topic_id),
            category_url: Some(self.category_url(forum_id)),
            magnet: Some(magnet),
            published,
            local_catalog: true,
        })
    }

    fn topic_url(&self, topic_id: u64) -> String {
        format!("{}/viewtopic.php?t={topic_id}", self.forum_base_url)
    }

    fn category_url(&self, forum_id: u64) -> String {
        format!("{}/viewforum.php?f={forum_id}", self.forum_base_url)
    }
}

/// Rebuilds the local SQLite catalog from a RuTracker XML dump and swaps it in
/// atomically after the new database passes SQLite integrity checks.
pub fn rebuild_catalog_from_xml(xml_path: &Path, db_path: &Path) -> Result<CatalogBuildStats> {
    let parent = db_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("catalog DB path must have a parent directory"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create catalog directory {}", parent.display()))?;
    let tmp_path = db_path.with_extension("tmp");
    if tmp_path.exists() {
        fs::remove_file(&tmp_path).with_context(|| {
            format!(
                "failed to remove stale temporary catalog {}",
                tmp_path.display()
            )
        })?;
    }

    let stats = build_catalog_database(xml_path, &tmp_path)?;
    validate_sqlite_integrity(&tmp_path)?;
    fs::rename(&tmp_path, db_path).with_context(|| {
        format!(
            "failed to replace catalog {} with {}",
            db_path.display(),
            tmp_path.display()
        )
    })?;
    Ok(stats)
}

fn build_catalog_database(xml_path: &Path, db_path: &Path) -> Result<CatalogBuildStats> {
    let mut conn = Connection::open(db_path)
        .with_context(|| format!("failed to create catalog SQLite {}", db_path.display()))?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        CREATE TABLE catalog_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE forum(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE torrent(
            topic_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            info_hash TEXT NOT NULL,
            tracker_id INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL,
            published TEXT,
            published_unix INTEGER,
            forum_id INTEGER NOT NULL REFERENCES forum(id)
        );
        ",
    )?;

    let tx = conn.transaction()?;
    let mut state = XmlState::default();
    let input = open_xml_input(xml_path)?;
    let mut reader = Reader::from_reader(input);
    reader.config_mut().trim_text(false);
    let mut buf = Vec::new();
    let mut torrents = 0;
    let mut forums = 0;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) => {
                handle_xml_start(&reader, &mut state, &event)?;
            }
            Ok(Event::Empty(event)) => {
                handle_xml_empty(&reader, &mut state, &event)?;
            }
            Ok(Event::Text(event)) => {
                let text = event.decode()?.into_owned();
                match state.text_target {
                    TextTarget::Title => {
                        if let Some(record) = state.record.as_mut() {
                            record.title.push_str(&text);
                        }
                    }
                    TextTarget::Forum => {
                        if let Some(record) = state.record.as_mut() {
                            record.forum_name.push_str(&text);
                        }
                    }
                    TextTarget::None => {}
                }
            }
            Ok(Event::End(event)) => {
                if event.name().as_ref() == b"title" || event.name().as_ref() == b"forum" {
                    state.text_target = TextTarget::None;
                } else if event.name().as_ref() == b"torrent" && state.torrent_depth == 1 {
                    if let Some(record) = state.record.take() {
                        let (inserted_torrent, inserted_forum) = insert_record(&tx, &record)?;
                        if inserted_torrent {
                            torrents += 1;
                        }
                        if inserted_forum {
                            forums += 1;
                        }
                        if torrents % INSERT_BATCH_SIZE == 0 {
                            info!(torrents, "indexed local RuTracker catalog records");
                        }
                    }
                    state.torrent_depth = 0;
                } else if event.name().as_ref() == b"torrent" && state.torrent_depth > 1 {
                    state.torrent_depth -= 1;
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(err).context("failed to parse XML catalog"),
            _ => {}
        }
        buf.clear();
    }

    tx.commit()?;
    conn.execute_batch(
        "
        CREATE INDEX torrent_forum_id_idx ON torrent(forum_id);
        CREATE INDEX torrent_published_unix_idx ON torrent(published_unix);
        CREATE VIRTUAL TABLE torrent_fts USING fts5(
            title,
            content='torrent',
            content_rowid='topic_id',
            tokenize='unicode61'
        );
        INSERT INTO torrent_fts(rowid, title)
            SELECT topic_id, title FROM torrent;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        ",
    )?;
    Ok(CatalogBuildStats { torrents, forums })
}

fn open_xml_input(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = fs::File::open(path)
        .with_context(|| format!("failed to open XML dump {}", path.display()))?;
    let reader: Box<dyn BufRead> = if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("xz"))
    {
        Box::new(BufReader::new(XzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    Ok(reader)
}

fn handle_xml_start<R: BufRead>(
    reader: &Reader<R>,
    state: &mut XmlState,
    event: &BytesStart<'_>,
) -> Result<()> {
    match event.name().as_ref() {
        b"torrent" if state.torrent_depth == 0 => {
            state.record = Some(XmlTorrentRecord {
                topic_id: attr_value(reader, event, b"id")?
                    .context("catalog torrent is missing id")?
                    .parse()
                    .context("catalog torrent id is invalid")?,
                size_bytes: attr_value(reader, event, b"size")?
                    .context("catalog torrent is missing size")?
                    .parse()
                    .context("catalog torrent size is invalid")?,
                published: attr_value(reader, event, b"registred_at")?.unwrap_or_default(),
                published_unix: attr_value(reader, event, b"unixts")?
                    .and_then(|value| value.parse().ok()),
                tracker_id: DEFAULT_TRACKER_ID,
                ..Default::default()
            });
            state.torrent_depth = 1;
        }
        b"torrent" => {
            state.torrent_depth += 1;
            apply_inner_torrent_attrs(reader, state, event)?;
        }
        b"title" if state.record.is_some() => state.text_target = TextTarget::Title,
        b"forum" if state.record.is_some() => {
            if let Some(record) = state.record.as_mut() {
                record.forum_id = attr_value(reader, event, b"id")?
                    .context("catalog forum is missing id")?
                    .parse()
                    .context("catalog forum id is invalid")?;
            }
            state.text_target = TextTarget::Forum;
        }
        _ => {}
    }
    Ok(())
}

fn handle_xml_empty<R: BufRead>(
    reader: &Reader<R>,
    state: &mut XmlState,
    event: &BytesStart<'_>,
) -> Result<()> {
    if event.name().as_ref() == b"torrent" && state.torrent_depth > 0 {
        apply_inner_torrent_attrs(reader, state, event)?;
    }
    Ok(())
}

fn apply_inner_torrent_attrs<R: BufRead>(
    reader: &Reader<R>,
    state: &mut XmlState,
    event: &BytesStart<'_>,
) -> Result<()> {
    let Some(record) = state.record.as_mut() else {
        return Ok(());
    };
    if let Some(hash) = attr_value(reader, event, b"hash")? {
        record.info_hash = hash;
    }
    if let Some(tracker_id) = attr_value(reader, event, b"tracker_id")? {
        record.tracker_id = tracker_id.parse().unwrap_or(DEFAULT_TRACKER_ID);
    }
    Ok(())
}

fn attr_value<R: BufRead>(
    reader: &Reader<R>,
    event: &BytesStart<'_>,
    name: &[u8],
) -> Result<Option<String>> {
    for attr in event.attributes().with_checks(false) {
        let attr = attr?;
        if attr.key.as_ref() == name {
            return Ok(Some(
                attr.decode_and_unescape_value(reader.decoder())?
                    .into_owned(),
            ));
        }
    }
    Ok(None)
}

fn insert_record(conn: &Connection, record: &XmlTorrentRecord) -> Result<(bool, bool)> {
    if record.topic_id == 0
        || record.title.trim().is_empty()
        || record.info_hash.trim().is_empty()
        || record.forum_id == 0
    {
        return Ok((false, false));
    }
    let forum_name = display_forum_name(&record.forum_name);
    let inserted_forum = conn.execute(
        "INSERT OR IGNORE INTO forum(id, name) VALUES (?, ?)",
        params![u64_to_i64(record.forum_id), forum_name],
    )? > 0;
    let inserted_torrent = conn.execute(
        "INSERT OR REPLACE INTO torrent(
            topic_id, title, info_hash, tracker_id, size_bytes, published, published_unix, forum_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            u64_to_i64(record.topic_id),
            record.title.trim(),
            record.info_hash.trim(),
            i64::from(record.tracker_id),
            u64_to_i64(record.size_bytes),
            empty_to_null(&record.published),
            record.published_unix,
            u64_to_i64(record.forum_id),
        ],
    )? > 0;
    Ok((inserted_torrent, inserted_forum))
}

fn validate_sqlite_integrity(path: &Path) -> Result<()> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let result: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if result != "ok" {
        bail!("catalog SQLite integrity check failed: {result}");
    }
    Ok(())
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum CatalogSchema {
    Current,
    Legacy,
}

fn detect_schema(conn: &Connection) -> Result<CatalogSchema> {
    if table_exists(conn, "torrent") && table_exists(conn, "forum") {
        return Ok(CatalogSchema::Current);
    }
    if table_exists(conn, "data") {
        return Ok(CatalogSchema::Legacy);
    }
    bail!("catalog SQLite has unsupported schema");
}

fn table_exists(conn: &Connection, table: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1",
        [table],
        |_| Ok(()),
    )
    .optional()
    .ok()
    .flatten()
    .is_some()
}

fn current_select_sql(where_sql: &str, forum_id: Option<u64>) -> String {
    let mut sql = format!(
        "SELECT torrent.topic_id, torrent.title, torrent.info_hash, torrent.tracker_id, \
         torrent.size_bytes, torrent.published, forum.id, forum.name \
         FROM torrent JOIN forum ON forum.id = torrent.forum_id {where_sql}"
    );
    if forum_id.is_some() {
        sql.push_str(" AND torrent.forum_id = ?");
    }
    sql
}

fn fts_query(query: &str) -> Option<String> {
    let terms = query
        .split(|ch: char| !ch.is_alphanumeric())
        .map(str::trim)
        .filter(|term| term.chars().count() >= 2)
        .map(|term| format!("{}*", term.to_lowercase()))
        .collect::<Vec<_>>();
    (!terms.is_empty()).then(|| terms.join(" AND "))
}

fn like_pattern(query: &str) -> String {
    let mut out = String::from("%");
    for ch in query.trim().chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('%');
    out
}

fn magnet_from_hash(hash: &str, tracker_id: u16) -> String {
    if hash.starts_with("magnet:") {
        return hash.to_string();
    }
    let tracker_host = if tracker_id == 1 {
        "bt.t-ru.org".to_string()
    } else {
        format!("bt{tracker_id}.t-ru.org")
    };
    format!(
        "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2F{}%2Fann%3Fmagnet",
        hash.trim(),
        tracker_host
    )
}

fn display_forum_name(value: &str) -> String {
    value
        .rsplit(" - ")
        .next()
        .unwrap_or(value)
        .trim()
        .to_string()
}

fn empty_to_null(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn forum_id_to_i64(value: u64) -> i64 {
    u64_to_i64(value)
}

fn limit_to_i64(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

#[cfg(test)]
mod tests {
    use super::{OfflineCatalog, fts_query, like_pattern, rebuild_catalog_from_xml};
    use std::fs;

    #[test]
    fn escapes_like_patterns() {
        assert_eq!(like_pattern(r"a%b_c\d"), r"%a\%b\_c\\d%");
    }

    #[test]
    fn builds_fts_prefix_query() {
        assert_eq!(
            fts_query("Meanna Mine").as_deref(),
            Some("meanna* AND mine*")
        );
    }

    #[test]
    fn rebuilds_catalog_and_searches_it() {
        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("backup.20260530.xml");
        let db_path = dir.path().join("rutracker.sqlite");
        fs::write(
            &xml_path,
            r#"<root>
<torrent id="5733243" registred_at="2019.06.04 12:00:00" unixts="1559649600" size="110624768">
  <title>(Rap, Trip-hop) Meanna - Внутренняя жизнь - 2019, MP3</title>
  <torrent hash="0123456789ABCDEF0123456789ABCDEF01234567" tracker_id="4"/>
  <forum id="441">Музыка - Отечественный Рэп, Хип-Хоп (lossy)</forum>
</torrent>
</root>"#,
        )
        .unwrap();

        let stats = rebuild_catalog_from_xml(&xml_path, &db_path).unwrap();
        assert_eq!(stats.torrents, 1);

        let catalog = OfflineCatalog::new(db_path, "https://rutracker.org/forum".to_string());
        let results = catalog.search("meanna", None, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].topic_id, 5733243);
        assert_eq!(results[0].category.as_ref().unwrap().id, 441);
        assert!(results[0].magnet.as_ref().unwrap().contains("bt4.t-ru.org"));
        assert!(results[0].local_catalog);
    }
}
