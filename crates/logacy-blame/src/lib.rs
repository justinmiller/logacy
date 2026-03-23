use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rusqlite::Connection;

use logacy_core::config::Config;

/// A contiguous range of lines attributed to one author/commit.
struct BlameHunk {
    path: String,
    start_line: usize,
    line_count: usize,
    orig_commit: String,
    author_name: String,
    author_email: String,
}

/// Pre-loaded identity lookup tables for O(1) resolution.
struct IdentityCache {
    by_email: HashMap<String, i64>,
    by_name: HashMap<String, i64>,
}

impl IdentityCache {
    fn load(conn: &Connection) -> Result<Self> {
        let mut by_email = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT email, identity_id FROM identity_aliases")?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (email, id) = row?;
                by_email.insert(email, id);
            }
        }

        let mut by_name = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT canonical_name, id FROM identities")?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (name, id) = row?;
                by_name.entry(name).or_insert(id);
            }
        }

        Ok(Self { by_email, by_name })
    }

    fn resolve(&self, name: &str, email: &str) -> Option<i64> {
        self.by_email
            .get(email)
            .or_else(|| self.by_name.get(name))
            .copied()
    }
}

pub fn run_blame(repo_path: &Path, conn: &Connection, config: &Config) -> Result<()> {
    // Resolve HEAD via git
    let head_output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_path)
        .output()
        .context("failed to run git rev-parse HEAD")?;
    if !head_output.status.success() {
        anyhow::bail!("git rev-parse HEAD failed");
    }
    let head_hash = String::from_utf8_lossy(&head_output.stdout)
        .trim()
        .to_string();

    // Check if we already have a snapshot for this commit
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM blame_snapshots WHERE commit_hash = ?1",
            [&head_hash],
            |r| r.get(0),
        )
        .ok();
    if let Some(id) = existing {
        println!(
            "Blame snapshot already exists for {} (id={})",
            &head_hash[..12],
            id
        );
        return Ok(());
    }

    // List all files at HEAD using git ls-tree
    let ls_output = std::process::Command::new("git")
        .args(["ls-tree", "-r", "--name-only", "-z", &head_hash])
        .current_dir(repo_path)
        .output()
        .context("failed to run git ls-tree")?;
    let mut file_paths: Vec<String> = String::from_utf8_lossy(&ls_output.stdout)
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Filter out excluded paths and binary extensions
    let exclude_patterns: Vec<glob::Pattern> = config
        .blame
        .exclude_paths
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();
    let glob_opts = glob::MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    };

    let binary_exts: Vec<String> = config
        .blame
        .binary_extensions
        .iter()
        .map(|e| e.to_lowercase())
        .collect();

    file_paths.retain(|path| {
        if exclude_patterns
            .iter()
            .any(|p| p.matches_with(path, glob_opts))
        {
            return false;
        }
        let lower = path.to_lowercase();
        if binary_exts.iter().any(|ext| lower.ends_with(ext)) {
            return false;
        }
        true
    });

    // Filter by file size using git ls-tree with sizes
    let max_size = config.blame.max_file_size;
    if max_size > 0 {
        let size_output = std::process::Command::new("git")
            .args(["ls-tree", "-r", "-l", &head_hash])
            .current_dir(repo_path)
            .output()
            .context("failed to run git ls-tree -l")?;
        let oversized: HashSet<String> = String::from_utf8_lossy(&size_output.stdout)
            .lines()
            .filter_map(|line| {
                // Format: <mode> <type> <hash> <size>\t<path>
                let tab_pos = line.find('\t')?;
                let path = line[tab_pos + 1..].to_string();
                let meta = &line[..tab_pos];
                let size_str = meta.split_whitespace().nth(3)?;
                let size: u64 = size_str.trim().parse().ok()?;
                if size > max_size {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        if !oversized.is_empty() {
            file_paths.retain(|p| !oversized.contains(p));
        }
    }

    println!(
        "Running blame on {} files at {}...",
        file_paths.len(),
        &head_hash[..12]
    );

    let pb = ProgressBar::new(file_paths.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let skipped_files = AtomicU64::new(0);
    let repo_path_owned = repo_path.to_path_buf();
    let head_hash_owned = head_hash.clone();

    // Parallel blame using `git blame --porcelain`.
    // Native git blame is significantly faster than libgit2.
    let results: Vec<Vec<BlameHunk>> = file_paths
        .par_iter()
        .filter_map(|path| {
            let hunks =
                run_git_blame_porcelain(&repo_path_owned, &head_hash_owned, path);

            match hunks {
                Some(h) => {
                    pb.inc(1);
                    Some(h)
                }
                None => {
                    tracing::debug!("blame failed for {}", path);
                    skipped_files.fetch_add(1, Ordering::Relaxed);
                    pb.inc(1);
                    None
                }
            }
        })
        .collect();

    pb.finish_and_clear();

    let skipped = skipped_files.load(Ordering::Relaxed);
    let total_lines: usize = results
        .iter()
        .flat_map(|v| v.iter())
        .map(|h| h.line_count)
        .sum();
    let total_hunks: usize = results.iter().map(|v| v.len()).sum();
    println!(
        "Blamed {} files ({} skipped), {} lines in {} hunks. Inserting...",
        file_paths.len() - skipped as usize,
        skipped,
        total_lines,
        total_hunks,
    );

    // Pre-load identity cache
    let id_cache = IdentityCache::load(conn).context("failed to load identity cache")?;

    let mut unique_authors: HashSet<(String, String)> = HashSet::new();
    for hunks in &results {
        for hunk in hunks {
            unique_authors.insert((hunk.author_name.clone(), hunk.author_email.clone()));
        }
    }
    let unresolved_authors: usize = unique_authors
        .iter()
        .filter(|(name, email)| id_cache.resolve(name, email).is_none())
        .count();

    // Insert hunks directly — no per-line expansion needed
    let now = chrono::Utc::now().to_rfc3339();
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "INSERT INTO blame_snapshots (commit_hash, created_at) VALUES (?1, ?2)",
        rusqlite::params![head_hash, now],
    )?;
    let snapshot_id = tx.last_insert_rowid();

    let mut insert_hunk = tx.prepare(
        "INSERT INTO blame_hunks (snapshot_id, path, start_line, line_count, orig_commit, identity_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;

    let mut unresolved_lines = 0u64;
    let mut inserted_hunks = 0u64;
    for hunks in &results {
        for hunk in hunks {
            let identity_id = id_cache.resolve(&hunk.author_name, &hunk.author_email);
            if let Some(id) = identity_id {
                insert_hunk.execute(rusqlite::params![
                    snapshot_id,
                    hunk.path,
                    hunk.start_line as i64,
                    hunk.line_count as i64,
                    hunk.orig_commit,
                    id,
                ])?;
                inserted_hunks += 1;
            } else {
                unresolved_lines += hunk.line_count as u64;
            }
        }
    }

    drop(insert_hunk);

    // Compute file_ownership aggregates from hunks
    println!("Computing file ownership...");
    tx.execute(
        "INSERT INTO file_ownership (snapshot_id, path, identity_id, lines_owned, fraction)
         SELECT ?1, bh.path, bh.identity_id,
                sum(bh.line_count) AS lines_owned,
                CAST(sum(bh.line_count) AS REAL) / total.total_lines AS fraction
         FROM blame_hunks bh
         JOIN (SELECT path AS p, sum(line_count) AS total_lines
               FROM blame_hunks WHERE snapshot_id = ?1 GROUP BY path) total
           ON total.p = bh.path
         WHERE bh.snapshot_id = ?1
         GROUP BY bh.path, bh.identity_id",
        [snapshot_id],
    )?;

    tx.commit()?;

    let ownership_rows: i64 = conn.query_row(
        "SELECT count(*) FROM file_ownership WHERE snapshot_id = ?1",
        [snapshot_id],
        |r| r.get(0),
    )?;

    println!("Blame snapshot complete:");
    println!("  Snapshot ID:      {}", snapshot_id);
    println!("  Commit:           {}", &head_hash[..12]);
    println!(
        "  Files blamed:     {}",
        file_paths.len() - skipped as usize
    );
    println!("  Files skipped:    {}", skipped);
    println!("  Total lines:      {}", total_lines);
    println!("  Hunks inserted:   {}", inserted_hunks);
    println!(
        "  Unresolved:       {} authors, {} lines",
        unresolved_authors, unresolved_lines
    );
    println!("  Ownership rows:   {}", ownership_rows);

    Ok(())
}

/// Run `git blame --porcelain` on a single file and parse the output into hunks.
///
/// Porcelain format emits one group per contiguous blame range:
///   <40-char SHA> <orig-line> <final-line> <num-lines>    ← group header (4 fields)
///   author <name>                                         ← only on first occurrence of commit
///   author-mail <<email>>
///   ... other headers ...
///   \t<line content>
///   <40-char SHA> <orig-line> <final-line>                ← continuation (3 fields, no count)
///   \t<line content>
///   ...
///
/// We only create a BlameHunk for group headers (4-field lines with num-lines).
fn run_git_blame_porcelain(
    repo_path: &Path,
    commit: &str,
    file_path: &str,
) -> Option<Vec<BlameHunk>> {
    let output = std::process::Command::new("git")
        .args(["blame", "--porcelain", commit, "--", file_path])
        .current_dir(repo_path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let mut hunks: Vec<BlameHunk> = Vec::new();
    // commit hash → (author_name, author_email)
    let mut commit_authors: HashMap<String, (String, String)> = HashMap::new();

    let reader = std::io::BufReader::new(&output.stdout[..]);
    let mut lines_iter = reader.lines();

    // Pending hunk being built (waiting for author info to be resolved)
    let mut pending_hash: Option<String> = None;
    let mut pending_start: usize = 0;
    let mut pending_count: usize = 0;

    // Author info being read for the current commit
    let mut cur_author_name: Option<String> = None;
    let mut cur_author_email: Option<String> = None;
    let mut cur_commit_hash: Option<String> = None;

    while let Some(Ok(line)) = lines_iter.next() {
        // Content line — skip
        if line.starts_with('\t') {
            continue;
        }

        // Try to parse as a commit line: 40 hex chars + numbers
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3
            && parts[0].len() == 40
            && parts[0].chars().all(|c| c.is_ascii_hexdigit())
        {
            if parts.len() >= 4 {
                // Group header: <hash> <orig-line> <final-line> <num-lines>
                // Flush previous pending hunk
                if let Some(ref hash) = pending_hash {
                    if pending_count > 0 {
                        if let Some((name, email)) = commit_authors.get(hash) {
                            hunks.push(BlameHunk {
                                path: file_path.to_string(),
                                start_line: pending_start,
                                line_count: pending_count,
                                orig_commit: hash.clone(),
                                author_name: name.clone(),
                                author_email: email.clone(),
                            });
                        }
                    }
                }

                let hash = parts[0].to_string();
                let final_line: usize = parts[2].parse().unwrap_or(1);
                let num_lines: usize = parts[3].parse().unwrap_or(1);

                pending_hash = Some(hash.clone());
                pending_start = final_line;
                pending_count = num_lines;

                // Track which commit we're reading headers for
                if !commit_authors.contains_key(&hash) {
                    cur_commit_hash = Some(hash);
                    cur_author_name = None;
                    cur_author_email = None;
                }
            }
            // 3-field lines are continuations within the same group — skip
            continue;
        }

        // Header lines
        if let Some(name) = line.strip_prefix("author ") {
            cur_author_name = Some(name.to_string());
        } else if let Some(email) = line.strip_prefix("author-mail ") {
            let email = email.trim_start_matches('<').trim_end_matches('>');
            cur_author_email = Some(email.to_string());
        }

        // If we have both name and email, store for this commit
        if let (Some(ref name), Some(ref email), Some(ref hash)) =
            (&cur_author_name, &cur_author_email, &cur_commit_hash)
        {
            commit_authors
                .entry(hash.clone())
                .or_insert_with(|| (name.clone(), email.clone()));
            cur_author_name = None;
            cur_author_email = None;
            cur_commit_hash = None;
        }
    }

    // Flush last pending hunk
    if let Some(ref hash) = pending_hash {
        if pending_count > 0 {
            if let Some((name, email)) = commit_authors.get(hash) {
                hunks.push(BlameHunk {
                    path: file_path.to_string(),
                    start_line: pending_start,
                    line_count: pending_count,
                    orig_commit: hash.clone(),
                    author_name: name.clone(),
                    author_email: email.clone(),
                });
            }
        }
    }

    Some(hunks)
}
