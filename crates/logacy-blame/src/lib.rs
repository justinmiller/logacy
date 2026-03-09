use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rusqlite::Connection;

use logacy_core::config::Config;

struct BlameLine {
    path: String,
    line_number: usize,
    orig_commit: String,
    author_name: String,
    author_email: String,
}

pub fn run_blame(repo_path: &Path, conn: &Connection, config: &Config) -> Result<()> {
    let repo = git2::Repository::open(repo_path).context("failed to open git repository")?;
    let head = repo.head().context("failed to resolve HEAD")?;
    let head_commit = head.peel_to_commit().context("HEAD is not a commit")?;
    let head_hash = head_commit.id().to_string();
    let head_oid = head_commit.id();
    let head_tree = head_commit.tree().context("failed to get HEAD tree")?;

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

    // Collect all blobs (file paths) from the HEAD tree
    let mut file_paths: Vec<String> = Vec::new();
    head_tree.walk(git2::TreeWalkMode::PreOrder, |dir, entry| {
        if entry.kind() == Some(git2::ObjectType::Blob) {
            let path = if dir.is_empty() {
                entry.name().unwrap_or("").to_string()
            } else {
                format!("{}{}", dir, entry.name().unwrap_or(""))
            };
            file_paths.push(path);
        }
        git2::TreeWalkResult::Ok
    })?;

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

    // Filter by file size
    let max_size = config.blame.max_file_size;
    if max_size > 0 {
        file_paths.retain(|path| match head_tree.get_path(Path::new(path)) {
            Ok(entry) => match repo.find_blob(entry.id()) {
                Ok(blob) => (blob.size() as u64) <= max_size,
                Err(_) => false,
            },
            Err(_) => false,
        });
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

    // Parallel blame: each thread opens its own git2::Repository
    let results: Vec<Vec<BlameLine>> = file_paths
        .par_iter()
        .filter_map(|path| {
            let repo = match git2::Repository::open(&repo_path_owned) {
                Ok(r) => r,
                Err(_) => {
                    skipped_files.fetch_add(1, Ordering::Relaxed);
                    pb.inc(1);
                    return None;
                }
            };

            let mut blame_opts = git2::BlameOptions::new();
            blame_opts.newest_commit(head_oid);

            let blame = match repo.blame_file(Path::new(path), Some(&mut blame_opts)) {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!("blame failed for {}: {}", path, e);
                    skipped_files.fetch_add(1, Ordering::Relaxed);
                    pb.inc(1);
                    return None;
                }
            };

            let mut lines = Vec::new();
            for hunk_idx in 0..blame.len() {
                let hunk = blame.get_index(hunk_idx).unwrap();
                let orig_commit = hunk.orig_commit_id().to_string();
                let sig = hunk.final_signature();
                let name = sig.name().unwrap_or("").to_string();
                let email = sig.email().unwrap_or("").to_string();
                let start = hunk.final_start_line();
                let count = hunk.lines_in_hunk();

                for offset in 0..count {
                    lines.push(BlameLine {
                        path: path.clone(),
                        line_number: start + offset,
                        orig_commit: orig_commit.clone(),
                        author_name: name.clone(),
                        author_email: email.clone(),
                    });
                }
            }
            pb.inc(1);
            Some(lines)
        })
        .collect();

    pb.finish_and_clear();

    let skipped = skipped_files.load(Ordering::Relaxed);
    let total_lines: usize = results.iter().map(|v| v.len()).sum();
    println!(
        "Blamed {} files ({} skipped), {} total lines. Inserting...",
        file_paths.len() - skipped as usize,
        skipped,
        total_lines
    );

    // Insert into database (single-threaded, SQLite)
    let now = chrono::Utc::now().to_rfc3339();
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "INSERT INTO blame_snapshots (commit_hash, created_at) VALUES (?1, ?2)",
        rusqlite::params![head_hash, now],
    )?;
    let snapshot_id = tx.last_insert_rowid();

    let mut insert_line = tx.prepare(
        "INSERT INTO blame_lines (snapshot_id, path, line_number, orig_commit, identity_id)
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )?;

    let mut unresolved_lines = 0u64;
    for lines in &results {
        for line in lines {
            let identity_id =
                logacy_db::resolve_identity(conn, &line.author_name, &line.author_email);
            if let Some(id) = identity_id {
                insert_line.execute(rusqlite::params![
                    snapshot_id,
                    line.path,
                    line.line_number as i64,
                    line.orig_commit,
                    id,
                ])?;
            } else {
                unresolved_lines += 1;
            }
        }
    }

    drop(insert_line);

    // Compute file_ownership aggregates
    println!("Computing file ownership...");
    tx.execute(
        "INSERT INTO file_ownership (snapshot_id, path, identity_id, lines_owned, fraction)
         SELECT ?1, path, identity_id, count(*) AS lines_owned,
                CAST(count(*) AS REAL) / total.total_lines AS fraction
         FROM blame_lines bl
         JOIN (SELECT path AS p, count(*) AS total_lines
               FROM blame_lines WHERE snapshot_id = ?1 GROUP BY path) total
           ON total.p = bl.path
         WHERE bl.snapshot_id = ?1
         GROUP BY bl.path, bl.identity_id",
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
    println!("  Unresolved lines: {}", unresolved_lines);
    println!("  Ownership rows:   {}", ownership_rows);

    Ok(())
}
