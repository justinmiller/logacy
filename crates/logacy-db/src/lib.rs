use anyhow::{Context, Result};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::Connection as _;
use rusqlite::Connection;
use std::path::Path;

pub mod functions;
pub mod models;
pub mod schema;

const SCHEMA_VERSION: &str = "1";

pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open database at {}", path.display()))?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL;")?;
    Ok(conn)
}

pub fn open_diesel(path: &Path) -> Result<SqliteConnection> {
    let url = path.to_string_lossy().to_string();
    let mut conn = SqliteConnection::establish(&url)
        .with_context(|| format!("failed to open database at {}", path.display()))?;
    diesel::sql_query("PRAGMA journal_mode=WAL").execute(&mut conn)?;
    diesel::sql_query("PRAGMA foreign_keys=ON").execute(&mut conn)?;
    diesel::sql_query("PRAGMA synchronous=NORMAL").execute(&mut conn)?;
    Ok(conn)
}

/// Open the database and run any pending schema migrations.
/// This is the standard entry point for all commands that read/write an existing database.
pub fn open_and_migrate(path: &Path) -> Result<Connection> {
    let conn = open(path).context("database not found — run `logacy init` first")?;
    migrate(&conn)?;
    Ok(conn)
}

pub fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA_SQL)?;
    conn.execute(
        "INSERT OR REPLACE INTO logacy_meta (key, value) VALUES ('schema_version', ?1)",
        [SCHEMA_VERSION],
    )?;
    Ok(())
}

pub fn migrate(conn: &Connection) -> Result<()> {
    let version = get_meta(conn, "schema_version")?
        .unwrap_or_else(|| "0".to_string());

    if version != SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported schema version {version}; expected {SCHEMA_VERSION}. \
             Delete the database and re-run `logacy init`."
        );
    }

    // Validate required tables exist
    let required = [
        "identities",
        "identity_aliases",
        "identity_emails",
        "organizations",
        "org_domain_rules",
        "identity_affiliations",
        "commit_org_attribution",
        "trailer_org_attribution",
        "commits",
        "trailers",
        "tags",
        "commit_hunks",
    ];
    for table in &required {
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |r| r.get(0),
        )?;
        if !exists {
            anyhow::bail!(
                "required table `{table}` is missing. Delete the database and re-run `logacy init`."
            );
        }
    }

    Ok(())
}

pub fn get_meta(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT value FROM logacy_meta WHERE key = ?1")?;
    let result = stmt.query_row([key], |row| row.get::<_, String>(0));
    match result {
        Ok(v) => Ok(Some(v)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn set_meta(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO logacy_meta (key, value) VALUES (?1, ?2)",
        [key, value],
    )?;
    Ok(())
}

// ── SQL helpers ─────────────────────────────────────────────────────────────

/// Build a comma-separated list of `?` placeholders for use in SQL IN clauses.
pub fn sql_placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(",")
}

// ── Identity resolution API ──────────────────────────────────────────────────
// All identity lookups go through these functions. Do not reimplement elsewhere.

/// Parse "Name \<email\>" into (name, email) components.
/// Handles trailer values like "Andreas Dilger \<adilger@whamcloud.com\>".
pub fn parse_identity_value(value: &str) -> Option<(String, String)> {
    let value = value.trim();
    let lt = value.find('<')?;
    let gt = value.find('>')?;
    if gt <= lt + 1 {
        return None;
    }
    let name = value[..lt].trim().to_string();
    let email = value[lt + 1..gt].trim().to_string();
    Some((name, email))
}

/// Resolve a (name, email) pair to an identity_id.
///
/// Lookup order:
/// 1. Exact email match in identity_aliases
/// 2. Exact canonical_name match in identities (for cases where
///    the email changed but the name is known, e.g. MAINTAINERS entries)
pub fn resolve_identity(conn: &Connection, name: &str, email: &str) -> Option<i64> {
    resolve_identity_by_email(conn, email).or_else(|| {
        conn.query_row(
            "SELECT id FROM identities WHERE canonical_name = ?1 LIMIT 1",
            [name],
            |r| r.get(0),
        )
        .ok()
    })
}

/// Resolve an email address to an identity_id via the identity_aliases table.
pub fn resolve_identity_by_email(conn: &Connection, email: &str) -> Option<i64> {
    conn.query_row(
        "SELECT identity_id FROM identity_aliases WHERE email = ?1",
        [email],
        |r| r.get(0),
    )
    .ok()
}

/// Extract the domain from an email address.
/// Returns None for bare identifiers (no @).
pub fn email_domain(email: &str) -> Option<&str> {
    email.rsplit_once('@').map(|(_, domain)| domain)
}

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS logacy_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identities (
    id              INTEGER PRIMARY KEY,
    canonical_name  TEXT NOT NULL,
    canonical_email TEXT NOT NULL,
    is_bot          INTEGER NOT NULL DEFAULT 0,
    UNIQUE(canonical_name, canonical_email)
);

CREATE TABLE IF NOT EXISTS identity_aliases (
    identity_id INTEGER NOT NULL REFERENCES identities(id),
    name        TEXT,
    email       TEXT NOT NULL,
    PRIMARY KEY (email)
);

CREATE TABLE IF NOT EXISTS identity_emails (
    identity_id   INTEGER NOT NULL REFERENCES identities(id),
    email         TEXT NOT NULL,
    first_seen_at TEXT,
    last_seen_at  TEXT,
    commit_count  INTEGER NOT NULL DEFAULT 0,
    trailer_count INTEGER NOT NULL DEFAULT 0,
    source        TEXT NOT NULL DEFAULT 'commit',
    is_preferred  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (identity_id, email)
);

CREATE TABLE IF NOT EXISTS organizations (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS org_domain_rules (
    id          INTEGER PRIMARY KEY,
    org_id      INTEGER NOT NULL REFERENCES organizations(id),
    domain      TEXT NOT NULL,
    valid_from  TEXT,
    valid_until TEXT
);

CREATE TABLE IF NOT EXISTS identity_affiliations (
    id          INTEGER PRIMARY KEY,
    identity_id INTEGER NOT NULL REFERENCES identities(id),
    org_id      INTEGER NOT NULL REFERENCES organizations(id),
    valid_from  TEXT,
    valid_until TEXT,
    source      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commits (
    hash           TEXT PRIMARY KEY,
    author_name    TEXT NOT NULL DEFAULT '',
    author_email   TEXT NOT NULL DEFAULT '',
    committer_name TEXT NOT NULL DEFAULT '',
    committer_email TEXT NOT NULL DEFAULT '',
    author_id      INTEGER REFERENCES identities(id),
    committer_id   INTEGER REFERENCES identities(id),
    author_date    TEXT NOT NULL,
    commit_date    TEXT NOT NULL,
    subject        TEXT NOT NULL,
    body           TEXT,
    ticket         TEXT,
    component      TEXT,
    is_merge       INTEGER NOT NULL DEFAULT 0,
    first_parent   INTEGER NOT NULL DEFAULT 1,
    insertions     INTEGER,
    deletions      INTEGER
);

CREATE TABLE IF NOT EXISTS trailers (
    commit_hash  TEXT NOT NULL REFERENCES commits(hash),
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    identity_id  INTEGER REFERENCES identities(id),
    seq          INTEGER NOT NULL,
    parsed_name  TEXT,
    parsed_email TEXT,
    PRIMARY KEY (commit_hash, key, seq)
);

CREATE TABLE IF NOT EXISTS commit_files (
    commit_hash TEXT NOT NULL REFERENCES commits(hash),
    path        TEXT NOT NULL,
    status      TEXT NOT NULL,
    insertions  INTEGER,
    deletions   INTEGER,
    language    TEXT NOT NULL DEFAULT 'Other',
    category    TEXT NOT NULL DEFAULT 'source',
    PRIMARY KEY (commit_hash, path)
);

CREATE TABLE IF NOT EXISTS commit_hunks (
    commit_hash TEXT NOT NULL REFERENCES commits(hash),
    path        TEXT NOT NULL,
    old_start   INTEGER NOT NULL,
    old_lines   INTEGER NOT NULL,
    new_start   INTEGER NOT NULL,
    new_lines   INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    PRIMARY KEY (commit_hash, path, seq)
);

CREATE TABLE IF NOT EXISTS subsystems (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    status     TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subsystem_reviewers (
    subsystem_id INTEGER NOT NULL REFERENCES subsystems(id),
    identity_id  INTEGER NOT NULL REFERENCES identities(id),
    PRIMARY KEY (subsystem_id, identity_id)
);

CREATE TABLE IF NOT EXISTS subsystem_paths (
    subsystem_id INTEGER NOT NULL REFERENCES subsystems(id),
    pattern      TEXT NOT NULL,
    is_exclude   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (subsystem_id, pattern)
);

CREATE TABLE IF NOT EXISTS file_subsystems (
    path         TEXT NOT NULL,
    subsystem_id INTEGER NOT NULL REFERENCES subsystems(id),
    PRIMARY KEY (path, subsystem_id)
);

CREATE TABLE IF NOT EXISTS blame_snapshots (
    id          INTEGER PRIMARY KEY,
    commit_hash TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE(commit_hash)
);

CREATE TABLE IF NOT EXISTS blame_hunks (
    snapshot_id  INTEGER NOT NULL REFERENCES blame_snapshots(id),
    path         TEXT NOT NULL,
    start_line   INTEGER NOT NULL,
    line_count   INTEGER NOT NULL,
    orig_commit  TEXT NOT NULL,
    identity_id  INTEGER NOT NULL REFERENCES identities(id),
    PRIMARY KEY (snapshot_id, path, start_line)
);

CREATE TABLE IF NOT EXISTS file_ownership (
    snapshot_id  INTEGER NOT NULL REFERENCES blame_snapshots(id),
    path         TEXT NOT NULL,
    identity_id  INTEGER NOT NULL REFERENCES identities(id),
    lines_owned  INTEGER NOT NULL,
    fraction     REAL NOT NULL,
    PRIMARY KEY (snapshot_id, path, identity_id)
);

CREATE TABLE IF NOT EXISTS commit_org_attribution (
    commit_hash     TEXT PRIMARY KEY REFERENCES commits(hash),
    org_id          INTEGER REFERENCES organizations(id),
    org_name        TEXT,
    source          TEXT NOT NULL,
    matched_email   TEXT,
    matched_domain  TEXT,
    matched_rule_id INTEGER REFERENCES org_domain_rules(id)
);

CREATE TABLE IF NOT EXISTS trailer_org_attribution (
    commit_hash     TEXT NOT NULL REFERENCES commits(hash),
    key             TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    org_id          INTEGER REFERENCES organizations(id),
    org_name        TEXT,
    source          TEXT NOT NULL,
    matched_email   TEXT,
    matched_domain  TEXT,
    matched_rule_id INTEGER REFERENCES org_domain_rules(id),
    PRIMARY KEY (commit_hash, key, seq)
);

CREATE TABLE IF NOT EXISTS tags (
    name            TEXT PRIMARY KEY,
    target_commit   TEXT NOT NULL,
    tag_object_hash TEXT,
    is_annotated    INTEGER NOT NULL DEFAULT 0,
    tagger_name     TEXT,
    tagger_email    TEXT,
    tagger_date     TEXT,
    annotation      TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commit_releases (
    commit_hash     TEXT PRIMARY KEY REFERENCES commits(hash),
    release_tag     TEXT NOT NULL REFERENCES tags(name),
    release_date    TEXT NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_commits_date      ON commits(author_date);
CREATE INDEX IF NOT EXISTS idx_commits_ticket    ON commits(ticket);
CREATE INDEX IF NOT EXISTS idx_commits_component ON commits(component);
CREATE INDEX IF NOT EXISTS idx_commits_author    ON commits(author_id);
CREATE INDEX IF NOT EXISTS idx_trailers_key      ON trailers(key);
CREATE INDEX IF NOT EXISTS idx_trailers_identity ON trailers(identity_id);
CREATE INDEX IF NOT EXISTS idx_commit_files_path ON commit_files(path);
CREATE INDEX IF NOT EXISTS idx_commit_files_language ON commit_files(language);
CREATE INDEX IF NOT EXISTS idx_blame_hunks_identity ON blame_hunks(identity_id);
CREATE INDEX IF NOT EXISTS idx_blame_hunks_commit   ON blame_hunks(orig_commit);
CREATE INDEX IF NOT EXISTS idx_org_domain_rules_domain ON org_domain_rules(domain);
CREATE INDEX IF NOT EXISTS idx_identity_affiliations_identity ON identity_affiliations(identity_id);
CREATE INDEX IF NOT EXISTS idx_identity_affiliations_org ON identity_affiliations(org_id);
CREATE INDEX IF NOT EXISTS idx_identity_emails_email ON identity_emails(email);
CREATE INDEX IF NOT EXISTS idx_commit_hunks_path ON commit_hunks(path);
CREATE INDEX IF NOT EXISTS idx_commit_hunks_path_range ON commit_hunks(path, new_start);
CREATE INDEX IF NOT EXISTS idx_tags_target ON tags(target_commit);
CREATE INDEX IF NOT EXISTS idx_tags_date ON tags(created_at);
CREATE INDEX IF NOT EXISTS idx_commit_releases_tag ON commit_releases(release_tag);

-- Views

-- Per-identity current org: picks the best affiliation.
-- Priority: alias_override > org_override > domain_rule, then most recent valid_from.
CREATE VIEW IF NOT EXISTS v_identity_org AS
SELECT ia.identity_id, o.name AS org
FROM identity_affiliations ia
JOIN organizations o ON o.id = ia.org_id
WHERE ia.id = (
    SELECT ia2.id FROM identity_affiliations ia2
    WHERE ia2.identity_id = ia.identity_id
    ORDER BY
      CASE ia2.source WHEN 'alias_override' THEN 0 WHEN 'org_override' THEN 1 ELSE 2 END,
      COALESCE(ia2.valid_from, '9999') DESC
    LIMIT 1
);

CREATE VIEW IF NOT EXISTS v_commits AS
SELECT c.*,
       COALESCE(i.canonical_name, c.author_name) AS resolved_author_name,
       COALESCE(i.canonical_email, c.author_email) AS resolved_author_email,
       coa.org_name AS author_org,
       i.is_bot AS author_is_bot
FROM commits c
LEFT JOIN identities i ON c.author_id = i.id
LEFT JOIN commit_org_attribution coa ON coa.commit_hash = c.hash;

CREATE VIEW IF NOT EXISTS v_reviews AS
SELECT c.hash, c.ticket, c.component,
       author.canonical_name AS author,
       reviewer.canonical_name AS reviewer,
       c.author_date,
       coa.org_name AS author_org,
       toa.org_name AS reviewer_org
FROM commits c
JOIN identities author ON c.author_id = author.id
JOIN trailers t ON t.commit_hash = c.hash AND t.key = 'Reviewed-by'
JOIN identities reviewer ON t.identity_id = reviewer.id
LEFT JOIN commit_org_attribution coa ON coa.commit_hash = c.hash
LEFT JOIN trailer_org_attribution toa
    ON toa.commit_hash = t.commit_hash AND toa.key = t.key AND toa.seq = t.seq
WHERE author.is_bot = 0 AND reviewer.is_bot = 0;

CREATE VIEW IF NOT EXISTS v_subsystem_activity AS
SELECT s.name AS subsystem, c.ticket, c.component, c.author_date,
       i.canonical_name AS author,
       coa.org_name AS org
FROM commits c
JOIN commit_files cf ON cf.commit_hash = c.hash
JOIN file_subsystems fs ON fs.path = cf.path
JOIN subsystems s ON s.id = fs.subsystem_id
JOIN identities i ON c.author_id = i.id
LEFT JOIN commit_org_attribution coa ON coa.commit_hash = c.hash;

CREATE VIEW IF NOT EXISTS v_subsystem_contributors AS
SELECT s.id AS subsystem_id,
       s.name AS subsystem,
       i.id AS identity_id,
       i.canonical_name,
       count(DISTINCT c.hash) AS commits,
       COALESCE(sum(cf.insertions), 0) AS lines_added,
       COALESCE(sum(cf.deletions), 0) AS lines_removed,
       max(c.author_date) AS last_commit,
       min(c.author_date) AS first_commit,
       CASE WHEN sr.identity_id IS NOT NULL THEN 1 ELSE 0 END AS is_reviewer,
       COALESCE(MAX(own.lines_owned), 0) AS lines_owned
FROM commits c
JOIN commit_files cf ON cf.commit_hash = c.hash
JOIN file_subsystems fs ON fs.path = cf.path
JOIN subsystems s ON s.id = fs.subsystem_id
JOIN identities i ON c.author_id = i.id
LEFT JOIN subsystem_reviewers sr ON sr.subsystem_id = s.id AND sr.identity_id = i.id
LEFT JOIN (
    SELECT fs2.subsystem_id, fo.identity_id, SUM(fo.lines_owned) AS lines_owned
    FROM file_ownership fo
    JOIN file_subsystems fs2 ON fs2.path = fo.path
    JOIN (SELECT id FROM blame_snapshots ORDER BY id DESC LIMIT 1) bs ON bs.id = fo.snapshot_id
    GROUP BY fs2.subsystem_id, fo.identity_id
) own ON own.subsystem_id = s.id AND own.identity_id = i.id
WHERE i.is_bot = 0
GROUP BY s.id, i.id;

CREATE VIEW IF NOT EXISTS v_commit_releases AS
SELECT c.*, cr.release_tag, cr.release_date,
       t.is_annotated, t.annotation
FROM commits c
LEFT JOIN commit_releases cr ON cr.commit_hash = c.hash
LEFT JOIN tags t ON t.name = cr.release_tag;
"#;
