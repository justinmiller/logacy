pub mod identity;
pub mod tags;

use std::ops::Range;
use std::path::Path;

use anyhow::{Context, Result};
use gix::bstr::ByteSlice;
use gix::revision::walk::Sorting;
use gix::diff::blob::platform::prepare_diff::Operation;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use rusqlite::Connection;

use logacy_core::classify;
use logacy_core::config::Config;

pub struct IndexOptions {
    pub full: bool,
    pub first_parent: bool,
    pub include_diff_stats: bool,
    pub include_file_list: bool,
}

struct CommitRow {
    hash: String,
    author_name: String,
    author_email: String,
    committer_name: String,
    committer_email: String,
    author_date: String,
    commit_date: String,
    subject: String,
    body: Option<String>,
    ticket: Option<String>,
    component: Option<String>,
    is_merge: bool,
}

struct TrailerRow {
    key: String,
    value: String,
    seq: i32,
}

struct HunkRange {
    old_start: u32,
    old_lines: u32,
    new_start: u32,
    new_lines: u32,
}

struct FileChangeRow {
    path: String,
    status: String,
    insertions: Option<i32>,
    deletions: Option<i32>,
    language: &'static str,
    category: &'static str,
    hunks: Vec<HunkRange>,
}

pub fn run_index(
    repo_path: &Path,
    conn: &Connection,
    config: &Config,
    opts: &IndexOptions,
) -> Result<()> {
    let repo = gix::open(repo_path).context("failed to open git repository")?;

    let head = repo
        .head_commit()
        .context("failed to resolve HEAD — is the repository empty?")?;
    let head_id = head.id;

    // Check for incremental
    let last_indexed = if opts.full {
        tracing::info!("full reindex requested, clearing commit tables");
        conn.execute_batch(
            "DELETE FROM commit_hunks; DELETE FROM commit_files; DELETE FROM trailers; DELETE FROM commits;",
        )?;
        logacy_db::set_meta(conn, "last_indexed_commit", "")?;
        None
    } else {
        logacy_db::get_meta(conn, "last_indexed_commit")?
            .filter(|s| !s.is_empty())
    };

    if let Some(ref last) = last_indexed {
        if last == &head_id.to_string() {
            tracing::info!("already up to date at {}", &last[..12]);
            println!("Already up to date.");
            return Ok(());
        }
    }

    // Compile regexes
    let ticket_re = config
        .repository
        .ticket_pattern
        .as_ref()
        .map(|p| Regex::new(p))
        .transpose()
        .context("invalid ticket_pattern regex")?;
    let component_re = config
        .repository
        .component_pattern
        .as_ref()
        .map(|p| Regex::new(p))
        .transpose()
        .context("invalid component_pattern regex")?;

    // Collect all trailer keys we care about
    let all_trailer_keys: Vec<String> = config
        .trailers
        .identity_keys
        .iter()
        .chain(config.trailers.metadata_keys.iter())
        .cloned()
        .collect();

    // Walk commits
    tracing::info!("walking commits from HEAD {}", head_id);
    let mut walk_builder = head.ancestors();
    walk_builder = walk_builder.sorting(Sorting::ByCommitTime(Default::default()));
    if opts.first_parent {
        walk_builder = walk_builder.first_parent_only();
    }
    let walk = walk_builder.all().context("failed to start commit walk")?;

    let mut commits: Vec<(CommitRow, Vec<TrailerRow>)> = Vec::new();
    let stop_at = last_indexed.as_deref();

    for info in walk {
        let info = info.context("failed during commit walk")?;
        let hash = info.id.to_string();

        if stop_at == Some(hash.as_str()) {
            break;
        }

        let commit = repo.find_commit(info.id).context("failed to find commit")?;
        let message_raw = commit.message_raw_sloppy().to_string();
        let (subject, body) = split_message(&message_raw);

        let ticket = ticket_re
            .as_ref()
            .and_then(|re| re.captures(&subject))
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string());

        let component = component_re
            .as_ref()
            .and_then(|re| re.captures(&subject))
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().trim().to_string());

        let author = commit.author().context("failed to read author")?;
        let committer = commit.committer().context("failed to read committer")?;

        let author_name = author.name.to_str_lossy().to_string();
        let author_email = author.email.to_str_lossy().to_string();
        let committer_name = committer.name.to_str_lossy().to_string();
        let committer_email = committer.email.to_str_lossy().to_string();

        let author_date = parse_git_time_str(author.time);
        let commit_date = parse_git_time_str(committer.time);

        let is_merge = commit.parent_ids().count() > 1;

        // Parse trailers from body
        let trailers = parse_trailers(body.as_deref(), &all_trailer_keys);

        let row = CommitRow {
            hash,
            author_name,
            author_email,
            committer_name,
            committer_email,
            author_date,
            commit_date,
            subject,
            body,
            ticket,
            component,
            is_merge,
        };

        commits.push((row, trailers));
    }

    if commits.is_empty() {
        println!("No new commits to index.");
        return Ok(());
    }

    // Reverse so we insert oldest first
    commits.reverse();

    let total = commits.len();
    println!("Indexing {} commits...", total);

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Batch insert in transaction
    let tx = conn.unchecked_transaction()?;
    {
        let mut insert_commit = tx.prepare(
            "INSERT OR IGNORE INTO commits (hash, author_name, author_email, committer_name, committer_email, author_date, commit_date, subject, body, ticket, component, is_merge, first_parent, insertions, deletions)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        )?;

        let mut insert_trailer = tx.prepare(
            "INSERT OR IGNORE INTO trailers (commit_hash, key, value, seq, parsed_name, parsed_email)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;

        let mut insert_file = tx.prepare(
            "INSERT OR IGNORE INTO commit_files (commit_hash, path, status, insertions, deletions, language, category)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;

        let mut insert_hunk = tx.prepare(
            "INSERT OR IGNORE INTO commit_hunks (commit_hash, path, old_start, old_lines, new_start, new_lines, seq)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;

        // Prepare diff resource cache if we need file stats
        let compute_diffs = opts.include_diff_stats || opts.include_file_list;
        let mut resource_cache = if compute_diffs {
            Some(
                repo.diff_resource_cache_for_tree_diff()
                    .context("failed to create diff resource cache")?,
            )
        } else {
            None
        };

        for (commit, trailers) in &commits {
            // Compute per-file diff stats if requested
            let (file_changes, total_ins, total_del) = if compute_diffs {
                compute_commit_diff_stats(&repo, &commit.hash, resource_cache.as_mut().unwrap())?
            } else {
                (Vec::new(), None, None)
            };

            insert_commit.execute(rusqlite::params![
                commit.hash,
                commit.author_name,
                commit.author_email,
                commit.committer_name,
                commit.committer_email,
                commit.author_date,
                commit.commit_date,
                commit.subject,
                commit.body,
                commit.ticket,
                commit.component,
                commit.is_merge as i32,
                1i32,
                total_ins,
                total_del,
            ])?;

            for trailer in trailers {
                let (parsed_name, parsed_email) =
                    match logacy_db::parse_identity_value(&trailer.value) {
                        Some((n, e)) => (Some(n), Some(e)),
                        None => (None, None),
                    };
                insert_trailer.execute(rusqlite::params![
                    commit.hash,
                    trailer.key,
                    trailer.value,
                    trailer.seq,
                    parsed_name,
                    parsed_email,
                ])?;
            }

            for file in &file_changes {
                insert_file.execute(rusqlite::params![
                    commit.hash,
                    file.path,
                    file.status,
                    file.insertions,
                    file.deletions,
                    file.language,
                    file.category,
                ])?;

                for (seq, hunk) in file.hunks.iter().enumerate() {
                    insert_hunk.execute(rusqlite::params![
                        commit.hash,
                        file.path,
                        hunk.old_start,
                        hunk.old_lines,
                        hunk.new_start,
                        hunk.new_lines,
                        seq as i32,
                    ])?;
                }
            }

            pb.inc(1);
        }
    }

    logacy_db::set_meta(&tx, "last_indexed_commit", &head_id.to_string())?;
    tx.commit()?;

    pb.finish_and_clear();
    let head_short = &head_id.to_string()[..12];
    println!("Indexed {} commits. HEAD is now {}", total, head_short);

    Ok(())
}

fn compute_commit_diff_stats(
    repo: &gix::Repository,
    hash: &str,
    resource_cache: &mut gix::diff::blob::Platform,
) -> Result<(Vec<FileChangeRow>, Option<i32>, Option<i32>)> {
    let oid = gix::ObjectId::from_hex(hash.as_bytes()).context("invalid commit hash")?;
    let commit = repo.find_commit(oid).context("failed to find commit for diff")?;
    let tree = commit.tree().context("failed to get commit tree")?;

    // Get parent tree (empty tree for root commits, first parent for others)
    let parent_tree = match commit.parent_ids().next() {
        Some(parent_id) => {
            let parent = repo.find_commit(parent_id.detach()).context("failed to find parent commit")?;
            parent.tree().context("failed to get parent tree")?
        }
        None => repo.empty_tree(),
    };

    let mut file_changes: Vec<FileChangeRow> = Vec::new();
    let mut total_insertions: i64 = 0;
    let mut total_deletions: i64 = 0;

    let mut platform = parent_tree.changes().context("failed to create diff platform")?;
    platform.options(|opts: &mut gix::diff::Options| {
        opts.track_rewrites(None); // disable rename tracking for speed
    });

    platform
        .for_each_to_obtain_tree(
            &tree,
            |change: gix::object::tree::diff::Change<'_, '_, '_>| {
                let location = change.location().to_str_lossy().to_string();
                let status = match &change {
                    gix::object::tree::diff::Change::Addition { .. } => "A",
                    gix::object::tree::diff::Change::Deletion { .. } => "D",
                    gix::object::tree::diff::Change::Modification { .. } => "M",
                    gix::object::tree::diff::Change::Rewrite { .. } => "R",
                };

                // Only count blobs (files), not trees
                if !change.entry_mode().is_blob() {
                    return Ok(std::ops::ControlFlow::Continue(()));
                }

                let mut file_hunks: Vec<HunkRange> = Vec::new();
                let (ins, del) = change
                    .diff(resource_cache)
                    .ok()
                    .and_then(|p| {
                        p.resource_cache.options.skip_internal_diff_if_external_is_configured = false;
                        let prep = p.resource_cache.prepare_diff().ok()?;
                        match prep.operation {
                            Operation::InternalDiff { algorithm } => {
                                let input = prep.interned_input();
                                let counter = gix::diff::blob::diff(
                                    algorithm,
                                    &input,
                                    gix::diff::blob::sink::Counter::new(
                                        |before: Range<u32>, after: Range<u32>| {
                                            file_hunks.push(HunkRange {
                                                old_start: before.start + 1,
                                                old_lines: before.end - before.start,
                                                new_start: after.start + 1,
                                                new_lines: after.end - after.start,
                                            });
                                        },
                                    ),
                                );
                                Some(counter)
                            }
                            _ => None,
                        }
                    })
                    .map(|counter| {
                        let i = counter.insertions as i32;
                        let d = counter.removals as i32;
                        total_insertions += i as i64;
                        total_deletions += d as i64;
                        (Some(i), Some(d))
                    })
                    .unwrap_or((None, None));

                resource_cache.clear_resource_cache_keep_allocation();

                let lang = classify::language_from_path(&location);
                let cat = classify::category_from_path(&location);

                file_changes.push(FileChangeRow {
                    path: location,
                    status: status.to_string(),
                    insertions: ins,
                    deletions: del,
                    language: lang,
                    category: cat,
                    hunks: file_hunks,
                });

                Ok::<_, std::convert::Infallible>(std::ops::ControlFlow::Continue(()))
            },
        )
        .context("failed during tree diff")?;

    let total_ins = if total_insertions > 0 || total_deletions > 0 {
        Some(total_insertions as i32)
    } else if file_changes.is_empty() {
        None
    } else {
        Some(0)
    };
    let total_del = if total_insertions > 0 || total_deletions > 0 {
        Some(total_deletions as i32)
    } else if file_changes.is_empty() {
        None
    } else {
        Some(0)
    };

    Ok((file_changes, total_ins, total_del))
}

fn split_message(message: &str) -> (String, Option<String>) {
    match message.find('\n') {
        Some(pos) => {
            let subject = message[..pos].trim().to_string();
            let body = message[pos + 1..].trim();
            let body = if body.is_empty() {
                None
            } else {
                Some(body.to_string())
            };
            (subject, body)
        }
        None => (message.trim().to_string(), None),
    }
}

fn parse_trailers(body: Option<&str>, all_keys: &[String]) -> Vec<TrailerRow> {
    let body_text = match body {
        Some(b) => b,
        None => return Vec::new(),
    };

    let mut trailers = Vec::new();
    for line in body_text.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((key, value)) = parse_trailer_line(line) {
            let key_str = key.to_string();
            if all_keys.iter().any(|k| k.eq_ignore_ascii_case(&key_str)) {
                trailers.push(TrailerRow {
                    key: key_str,
                    value: value.to_string(),
                    seq: 0,
                });
            }
        } else {
            break;
        }
    }
    trailers.reverse();
    for (i, t) in trailers.iter_mut().enumerate() {
        t.seq = i as i32;
    }
    trailers
}

fn parse_trailer_line(line: &str) -> Option<(&str, &str)> {
    let colon = line.find(':')?;
    let key = &line[..colon];
    if key.is_empty() || key.contains(' ') {
        return None;
    }
    if !key.chars().next()?.is_ascii_uppercase() {
        return None;
    }
    let value = line[colon + 1..].trim();
    if value.is_empty() {
        return None;
    }
    Some((key, value))
}

/// Parse a raw git timestamp string like "1234567890 +0000" into ISO 8601.
fn parse_git_time_str(raw: &str) -> String {
    let parts: Vec<&str> = raw.trim().splitn(2, ' ').collect();
    let secs: i64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let dt = chrono::DateTime::from_timestamp(secs, 0)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
