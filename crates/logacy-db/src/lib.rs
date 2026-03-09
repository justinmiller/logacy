use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

const SCHEMA_VERSION: &str = "1";

pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open database at {}", path.display()))?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL;")?;
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
        .unwrap_or_else(|| "1".to_string());

    if version != SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported schema version {version}; expected {SCHEMA_VERSION}. \
             Delete the database and re-run `logacy init`."
        );
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
/// 2. Exact canonical_name match in identities
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
    org             TEXT,
    UNIQUE(canonical_name, canonical_email)
);

CREATE TABLE IF NOT EXISTS identity_aliases (
    identity_id INTEGER NOT NULL REFERENCES identities(id),
    name        TEXT,
    email       TEXT NOT NULL,
    PRIMARY KEY (email)
);

CREATE TABLE IF NOT EXISTS org_domains (
    domain      TEXT NOT NULL,
    org         TEXT NOT NULL,
    valid_from  TEXT,
    valid_until TEXT,
    PRIMARY KEY (domain, org, valid_from)
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
    commit_hash TEXT NOT NULL REFERENCES commits(hash),
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,
    identity_id INTEGER REFERENCES identities(id),
    seq         INTEGER NOT NULL,
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

CREATE TABLE IF NOT EXISTS blame_lines (
    snapshot_id  INTEGER NOT NULL REFERENCES blame_snapshots(id),
    path         TEXT NOT NULL,
    line_number  INTEGER NOT NULL,
    orig_commit  TEXT NOT NULL,
    identity_id  INTEGER NOT NULL REFERENCES identities(id),
    PRIMARY KEY (snapshot_id, path, line_number)
);

CREATE TABLE IF NOT EXISTS file_ownership (
    snapshot_id  INTEGER NOT NULL REFERENCES blame_snapshots(id),
    path         TEXT NOT NULL,
    identity_id  INTEGER NOT NULL REFERENCES identities(id),
    lines_owned  INTEGER NOT NULL,
    fraction     REAL NOT NULL,
    PRIMARY KEY (snapshot_id, path, identity_id)
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
CREATE INDEX IF NOT EXISTS idx_blame_lines_identity ON blame_lines(identity_id);
CREATE INDEX IF NOT EXISTS idx_blame_lines_commit   ON blame_lines(orig_commit);

-- Views

-- Resolve org temporally: check org_domains with date bounds, fall back to identities.org
CREATE VIEW IF NOT EXISTS v_commits AS
SELECT c.*,
       COALESCE(i.canonical_name, c.author_name) AS resolved_author_name,
       COALESCE(i.canonical_email, c.author_email) AS resolved_author_email,
       COALESCE(
           (SELECT od.org FROM org_domains od
            WHERE COALESCE(i.canonical_email, c.author_email) LIKE '%@' || od.domain
              AND (od.valid_from IS NULL OR c.author_date >= od.valid_from)
              AND (od.valid_until IS NULL OR c.author_date < od.valid_until)
            ORDER BY od.valid_from DESC
            LIMIT 1),
           i.org
       ) AS author_org,
       i.is_bot AS author_is_bot
FROM commits c
LEFT JOIN identities i ON c.author_id = i.id;

CREATE VIEW IF NOT EXISTS v_reviews AS
SELECT c.hash, c.ticket, c.component,
       author.canonical_name AS author,
       reviewer.canonical_name AS reviewer,
       c.author_date,
       COALESCE(
           (SELECT od.org FROM org_domains od
            WHERE author.canonical_email LIKE '%@' || od.domain
              AND (od.valid_from IS NULL OR c.author_date >= od.valid_from)
              AND (od.valid_until IS NULL OR c.author_date < od.valid_until)
            ORDER BY od.valid_from DESC LIMIT 1),
           author.org
       ) AS author_org,
       COALESCE(
           (SELECT od.org FROM org_domains od
            WHERE reviewer.canonical_email LIKE '%@' || od.domain
              AND (od.valid_from IS NULL OR c.author_date >= od.valid_from)
              AND (od.valid_until IS NULL OR c.author_date < od.valid_until)
            ORDER BY od.valid_from DESC LIMIT 1),
           reviewer.org
       ) AS reviewer_org
FROM commits c
JOIN identities author ON c.author_id = author.id
JOIN trailers t ON t.commit_hash = c.hash AND t.key = 'Reviewed-by'
JOIN identities reviewer ON t.identity_id = reviewer.id
WHERE author.is_bot = 0 AND reviewer.is_bot = 0;

CREATE VIEW IF NOT EXISTS v_subsystem_activity AS
SELECT s.name AS subsystem, c.ticket, c.component, c.author_date,
       i.canonical_name AS author,
       COALESCE(
           (SELECT od.org FROM org_domains od
            WHERE i.canonical_email LIKE '%@' || od.domain
              AND (od.valid_from IS NULL OR c.author_date >= od.valid_from)
              AND (od.valid_until IS NULL OR c.author_date < od.valid_until)
            ORDER BY od.valid_from DESC LIMIT 1),
           i.org
       ) AS org
FROM commits c
JOIN commit_files cf ON cf.commit_hash = c.hash
JOIN file_subsystems fs ON fs.path = cf.path
JOIN subsystems s ON s.id = fs.subsystem_id
JOIN identities i ON c.author_id = i.id;

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
"#;
