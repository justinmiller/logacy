use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;

use logacy_core::config::Config;

struct Subsystem {
    name: String,
    status: Option<String>,
    reviewers: Vec<(String, String)>, // (name, email)
    file_patterns: Vec<String>,
    exclude_patterns: Vec<String>,
}

pub fn run_maintain(repo_path: &Path, conn: &Connection, config: &Config) -> Result<()> {
    let maintainers_path = repo_path.join(&config.maintainers.file);
    if !maintainers_path.exists() {
        anyhow::bail!(
            "MAINTAINERS file not found at {}",
            maintainers_path.display()
        );
    }

    let content = std::fs::read_to_string(&maintainers_path)
        .with_context(|| format!("failed to read {}", maintainers_path.display()))?;
    let subsystems = parse_maintainers(&content);

    tracing::info!(
        "parsed {} subsystems from {}",
        subsystems.len(),
        config.maintainers.file
    );

    // Clear existing subsystem data
    conn.execute_batch(
        "DELETE FROM file_subsystems;
         DELETE FROM subsystem_paths;
         DELETE FROM subsystem_reviewers;
         DELETE FROM subsystems;",
    )?;

    let now = chrono::Utc::now().to_rfc3339();

    // Insert subsystems, reviewers, paths
    for sub in &subsystems {
        conn.execute(
            "INSERT INTO subsystems (name, status, updated_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![sub.name, sub.status, now],
        )?;
        let sub_id = conn.last_insert_rowid();

        // Resolve reviewers to identity_ids
        for (name, email) in &sub.reviewers {
            if let Some(id) = logacy_db::resolve_identity(conn, name, email) {
                conn.execute(
                    "INSERT OR IGNORE INTO subsystem_reviewers (subsystem_id, identity_id) VALUES (?1, ?2)",
                    rusqlite::params![sub_id, id],
                )?;
            } else {
                tracing::warn!("reviewer not found in identities: {} <{}>", name, email);
            }
        }

        // Insert file patterns
        for pattern in &sub.file_patterns {
            conn.execute(
                "INSERT OR IGNORE INTO subsystem_paths (subsystem_id, pattern, is_exclude) VALUES (?1, ?2, 0)",
                rusqlite::params![sub_id, pattern],
            )?;
        }
        for pattern in &sub.exclude_patterns {
            conn.execute(
                "INSERT OR IGNORE INTO subsystem_paths (subsystem_id, pattern, is_exclude) VALUES (?1, ?2, 1)",
                rusqlite::params![sub_id, pattern],
            )?;
        }
    }

    // Map files to subsystems
    map_files_to_subsystems(conn)?;

    // Ensure the subsystem contributors view exists (with blame ownership)
    conn.execute_batch(
        "DROP VIEW IF EXISTS v_subsystem_contributors;
         CREATE VIEW v_subsystem_contributors AS
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
         GROUP BY s.id, i.id;",
    )?;

    // Print summary
    let sub_count: i64 =
        conn.query_row("SELECT count(*) FROM subsystems", [], |r| r.get(0))?;
    let reviewer_count: i64 =
        conn.query_row("SELECT count(*) FROM subsystem_reviewers", [], |r| r.get(0))?;
    let mapping_count: i64 =
        conn.query_row("SELECT count(*) FROM file_subsystems", [], |r| r.get(0))?;
    let unmapped: i64 = conn.query_row(
        "SELECT count(DISTINCT path) FROM commit_files WHERE path NOT IN (SELECT path FROM file_subsystems)",
        [],
        |r| r.get(0),
    )?;

    println!("Subsystems:        {}", sub_count);
    println!("Reviewer mappings: {}", reviewer_count);
    println!("File mappings:     {}", mapping_count);
    println!("Unmapped files:    {}", unmapped);

    print_maintainer_insights(conn)?;

    Ok(())
}

/// Parse a Linux-format MAINTAINERS file into subsystem entries.
fn parse_maintainers(content: &str) -> Vec<Subsystem> {
    let mut subsystems = Vec::new();
    let mut in_body = false;
    let mut current: Option<Subsystem> = None;

    for line in content.lines() {
        // Skip preamble — body starts after the "---" separator
        if !in_body {
            if line.trim().starts_with("---") {
                in_body = true;
            }
            continue;
        }

        let trimmed = line.trim();

        // Skip blank lines (they may separate subsystems, but we detect subsystem
        // boundaries by the name line itself)
        if trimmed.is_empty() {
            continue;
        }

        // Check for tag lines: "X:\t..."
        if trimmed.len() >= 2 && trimmed.as_bytes()[1] == b':' {
            let tag = trimmed.as_bytes()[0];
            let value = trimmed[2..].trim();

            match tag {
                b'R' => {
                    if let Some(ref mut sub) = current {
                        if let Some((name, email)) = logacy_db::parse_identity_value(value) {
                            sub.reviewers.push((name, email));
                        }
                    }
                }
                b'S' => {
                    if let Some(ref mut sub) = current {
                        sub.status = Some(value.to_string());
                    }
                }
                b'F' => {
                    if let Some(ref mut sub) = current {
                        sub.file_patterns.push(value.to_string());
                    }
                }
                b'X' => {
                    if let Some(ref mut sub) = current {
                        sub.exclude_patterns.push(value.to_string());
                    }
                }
                // W:, K:, N: — skip
                _ => {}
            }
        } else {
            // This is a subsystem name line
            if let Some(sub) = current.take() {
                subsystems.push(sub);
            }
            current = Some(Subsystem {
                name: trimmed.to_string(),
                status: None,
                reviewers: Vec::new(),
                file_patterns: Vec::new(),
                exclude_patterns: Vec::new(),
            });
        }
    }

    // Don't forget the last subsystem
    if let Some(sub) = current {
        subsystems.push(sub);
    }

    subsystems
}

/// Print per-subsystem insights comparing designated reviewers against actual
/// commit activity and code ownership from blame.
fn print_maintainer_insights(conn: &Connection) -> Result<()> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(730))
        .format("%Y-%m-%dT00:00:00")
        .to_string();

    // Get subsystems that have designated reviewers
    let mut sub_stmt = conn.prepare(
        "SELECT s.id, s.name FROM subsystems s
         WHERE EXISTS (SELECT 1 FROM subsystem_reviewers sr WHERE sr.subsystem_id = s.id)
         ORDER BY s.name",
    )?;
    let subsystems: Vec<(i64, String)> = sub_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    if subsystems.is_empty() {
        return Ok(());
    }

    // For each reviewer: their commit/ownership stats in the subsystem
    let mut rev_stmt = conn.prepare(
        "SELECT i.canonical_name,
                COALESCE(vc.commits, 0),
                COALESCE(vc.lines_owned, 0),
                COALESCE(vc.last_commit, '')
         FROM subsystem_reviewers sr
         JOIN identities i ON sr.identity_id = i.id
         LEFT JOIN v_subsystem_contributors vc
           ON vc.subsystem_id = sr.subsystem_id AND vc.identity_id = i.id
         WHERE sr.subsystem_id = ?1
         ORDER BY COALESCE(vc.commits, 0) DESC",
    )?;

    // Top non-reviewer contributors with recent activity
    let mut contrib_stmt = conn.prepare(
        "SELECT canonical_name, commits, lines_owned, last_commit
         FROM v_subsystem_contributors
         WHERE subsystem_id = ?1 AND is_reviewer = 0
           AND last_commit >= ?2
         ORDER BY commits DESC
         LIMIT 5",
    )?;

    println!();
    println!("Maintainer Insights");
    println!("{}", "-".repeat(70));

    let mut any_printed = false;

    for (sub_id, sub_name) in &subsystems {
        let reviewers: Vec<(String, i64, i64, String)> = rev_stmt
            .query_map([sub_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let inactive: Vec<&(String, i64, i64, String)> = reviewers
            .iter()
            .filter(|(_, _commits, _lines, last)| {
                last.is_empty() || last.as_str() < cutoff.as_str()
            })
            .collect();

        let top_contribs: Vec<(String, i64, i64, String)> = contrib_stmt
            .query_map(rusqlite::params![sub_id, cutoff], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        if inactive.is_empty() && top_contribs.is_empty() {
            continue;
        }

        any_printed = true;
        println!();
        println!("  {}", sub_name);

        // Show all reviewers with their status
        for (name, commits, lines, last) in &reviewers {
            let last_str = if last.is_empty() {
                "never"
            } else {
                &last[..10.min(last.len())]
            };
            let marker = if last.is_empty() || last.as_str() < cutoff.as_str() {
                "INACTIVE"
            } else {
                "active  "
            };
            println!(
                "    [{}] {:<30} {:>5} commits  {:>7} lines  last: {}",
                marker, name, commits, lines, last_str
            );
        }

        // Show active non-reviewers who might be good candidates
        if !top_contribs.is_empty() {
            let max_reviewer_commits = reviewers.iter().map(|(_, c, _, _)| *c).max().unwrap_or(0);
            let notable: Vec<_> = top_contribs
                .iter()
                .filter(|(_, commits, _, _)| {
                    // Show if they have meaningful activity (>10% of top reviewer, or >10 commits)
                    *commits > max_reviewer_commits / 10 || *commits > 10
                })
                .collect();

            if !notable.is_empty() {
                println!("    Active contributors (not listed as reviewer):");
                for (name, commits, lines, last) in notable {
                    let last_str = &last[..10.min(last.len())];
                    println!(
                        "      {:<30} {:>5} commits  {:>7} lines  last: {}",
                        name, commits, lines, last_str
                    );
                }
            }
        }
    }

    if !any_printed {
        println!("  All designated reviewers have recent activity.");
    }

    Ok(())
}

/// Match a file path against a MAINTAINERS pattern.
///
/// Pattern conventions:
/// - Trailing `/` means all files in and below that directory
/// - `*` matches anything except `/`
/// - `?` matches any single character except `/`
/// - `[...]` matches character classes
fn file_matches_pattern(file: &str, pattern: &str) -> bool {
    if pattern.ends_with('/') {
        // Directory pattern: matches all files under this directory
        file.starts_with(pattern)
    } else {
        let opts = glob::MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: false,
        };
        match glob::Pattern::new(pattern) {
            Ok(pat) => pat.matches_with(file, opts),
            Err(_) => file == pattern,
        }
    }
}

/// Map all file paths from commit_files to subsystems via pattern matching.
fn map_files_to_subsystems(conn: &Connection) -> Result<()> {
    // Load all subsystem patterns
    let mut stmt = conn.prepare(
        "SELECT s.id, sp.pattern, sp.is_exclude
         FROM subsystem_paths sp
         JOIN subsystems s ON s.id = sp.subsystem_id",
    )?;
    let patterns: Vec<(i64, String, bool)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2).map(|v| v != 0)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Group patterns by subsystem
    let mut subsystem_patterns: std::collections::HashMap<i64, (Vec<String>, Vec<String>)> =
        std::collections::HashMap::new();
    for (sub_id, pattern, is_exclude) in &patterns {
        let entry = subsystem_patterns
            .entry(*sub_id)
            .or_insert_with(|| (Vec::new(), Vec::new()));
        if *is_exclude {
            entry.1.push(pattern.clone());
        } else {
            entry.0.push(pattern.clone());
        }
    }

    // Get all distinct file paths from commit_files
    let mut file_stmt = conn.prepare("SELECT DISTINCT path FROM commit_files")?;
    let files: Vec<String> = file_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    tracing::info!("matching {} files against {} subsystems", files.len(), subsystem_patterns.len());

    // Match each file against each subsystem's patterns
    let mut insert_stmt = conn.prepare(
        "INSERT OR IGNORE INTO file_subsystems (path, subsystem_id) VALUES (?1, ?2)",
    )?;

    let mut total_mappings = 0u64;
    for file in &files {
        for (sub_id, (includes, excludes)) in &subsystem_patterns {
            // Check excludes first
            let excluded = excludes.iter().any(|p| file_matches_pattern(file, p));
            if excluded {
                continue;
            }
            let included = includes.iter().any(|p| file_matches_pattern(file, p));
            if included {
                insert_stmt.execute(rusqlite::params![file, sub_id])?;
                total_mappings += 1;
            }
        }
    }

    tracing::info!("created {} file-to-subsystem mappings", total_mappings);
    Ok(())
}
