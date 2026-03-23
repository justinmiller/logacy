use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use gix::bstr::ByteSlice;
use gix::revision::walk::Sorting;
use rusqlite::Connection;

use logacy_core::config::Config;

/// Index git tags and map commits to their containing release.
pub fn run_tag_index(
    repo_path: &Path,
    conn: &Connection,
    config: &Config,
    full: bool,
) -> Result<()> {
    let repo = gix::open(repo_path).context("failed to open git repository")?;

    if full {
        tracing::info!("full reindex: clearing tags and commit_releases");
        conn.execute_batch("DELETE FROM commit_releases; DELETE FROM tags;")?;
    }

    // Compile tag glob pattern if configured
    let tag_glob = config
        .releases
        .tag_pattern
        .as_ref()
        .map(|p| glob::Pattern::new(p))
        .transpose()
        .context("invalid releases.tag_pattern glob")?;

    // Collect existing tags so we can skip them in incremental mode
    let existing_tags: std::collections::HashSet<String> = if full {
        std::collections::HashSet::new()
    } else {
        let mut stmt = conn.prepare("SELECT name FROM tags")?;
        let result = stmt.query_map([], |r| r.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        result
    };

    // Iterate all tag references
    let references = repo.references().context("failed to list references")?;
    let tag_iter = references.tags().context("failed to list tag references")?;

    struct TagInfo {
        name: String,
        target_commit: String,
        tag_object_hash: Option<String>,
        is_annotated: bool,
        tagger_name: Option<String>,
        tagger_email: Option<String>,
        tagger_date: Option<String>,
        annotation: Option<String>,
        created_at: String,
    }

    let mut tags: Vec<TagInfo> = Vec::new();

    for reference in tag_iter {
        let mut reference = match reference {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("skipping unreadable tag ref: {}", e);
                continue;
            }
        };

        let full_name = reference.name().as_bstr().to_str_lossy().to_string();
        let short_name = full_name
            .strip_prefix("refs/tags/")
            .unwrap_or(&full_name)
            .to_string();

        // Filter by glob pattern
        if let Some(ref pat) = tag_glob {
            if !pat.matches(&short_name) {
                continue;
            }
        }

        // Skip already-indexed tags in incremental mode
        if existing_tags.contains(&short_name) {
            continue;
        }

        // Try to peel to an annotated tag first
        let mut is_annotated = false;
        let mut tag_object_hash: Option<String> = None;
        let mut tagger_name: Option<String> = None;
        let mut tagger_email: Option<String> = None;
        let mut tagger_date: Option<String> = None;
        let mut annotation: Option<String> = None;
        let mut target_commit: Option<String> = None;

        if let Ok(tag) = reference.peel_to_tag() {
            is_annotated = true;
            tag_object_hash = Some(tag.id.to_string());
            if let Ok(tag_ref) = tag.decode() {
                if let Ok(Some(sig)) = tag_ref.tagger() {
                    tagger_name = Some(sig.name.to_str_lossy().to_string());
                    tagger_email = Some(sig.email.to_str_lossy().to_string());
                    tagger_date = Some(parse_git_time_secs(sig.seconds()));
                }
                let msg = tag_ref.message.to_str_lossy().trim().to_string();
                if !msg.is_empty() {
                    annotation = Some(msg);
                }
                target_commit = Some(tag_ref.target().to_string());
            }
        }

        // Get the commit ID: for annotated tags from tag object target,
        // for lightweight tags from the reference directly
        let commit_hash = match target_commit {
            Some(h) => h,
            None => match reference.try_id() {
                Some(id) => id.to_string(),
                None => {
                    tracing::debug!("tag {} has no direct id, skipping", short_name);
                    continue;
                }
            },
        };

        // Verify target is a commit
        let commit_oid = match gix::ObjectId::from_hex(commit_hash.as_bytes()) {
            Ok(oid) => oid,
            Err(_) => continue,
        };
        let commit = match repo.find_commit(commit_oid) {
            Ok(c) => c,
            Err(_) => {
                tracing::debug!("tag {} does not point to a commit, skipping", short_name);
                continue;
            }
        };

        // created_at: tagger date for annotated, commit date for lightweight
        let created_at = tagger_date.clone().unwrap_or_else(|| {
            commit
                .committer()
                .ok()
                .map(|sig| parse_git_time_str(sig.time))
                .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
        });

        tags.push(TagInfo {
            name: short_name,
            target_commit: commit_hash,
            tag_object_hash,
            is_annotated,
            tagger_name,
            tagger_email,
            tagger_date,
            annotation,
            created_at,
        });
    }

    if tags.is_empty() && existing_tags.is_empty() {
        println!("No tags found.");
        return Ok(());
    }

    // Insert tags
    let new_count = tags.len();
    if new_count > 0 {
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO tags (name, target_commit, tag_object_hash, is_annotated, \
                 tagger_name, tagger_email, tagger_date, annotation, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for tag in &tags {
                stmt.execute(rusqlite::params![
                    tag.name,
                    tag.target_commit,
                    tag.tag_object_hash,
                    tag.is_annotated as i32,
                    tag.tagger_name,
                    tag.tagger_email,
                    tag.tagger_date,
                    tag.annotation,
                    tag.created_at,
                ])?;
            }
        }
        tx.commit()?;
        println!("Indexed {} new tags.", new_count);
    }

    // Map commits to releases
    if config.releases.map_commits {
        map_commits_to_releases(&repo, conn, full)?;
    }

    Ok(())
}

/// Walk first-parent history from HEAD and assign each commit to its nearest
/// release tag (the most recent tag encountered walking backwards).
fn map_commits_to_releases(
    repo: &gix::Repository,
    conn: &Connection,
    full: bool,
) -> Result<()> {
    if full {
        conn.execute_batch("DELETE FROM commit_releases;")?;
    }

    // Load all tags sorted by created_at DESC
    let mut tag_stmt =
        conn.prepare("SELECT name, target_commit, created_at FROM tags ORDER BY created_at DESC")?;
    let tag_targets: HashMap<String, (String, String)> = tag_stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(1)?, // target_commit
                (r.get::<_, String>(0)?, r.get::<_, String>(2)?), // (name, created_at)
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if tag_targets.is_empty() {
        return Ok(());
    }

    let head = repo.head_commit().context("failed to resolve HEAD")?;

    let mut walk_builder = head.ancestors();
    walk_builder = walk_builder.sorting(Sorting::ByCommitTime(Default::default()));
    walk_builder = walk_builder.first_parent_only();
    let walk = walk_builder.all().context("failed to start commit walk")?;

    let mut current_release: Option<(String, String)> = None; // (tag_name, release_date)
    let mut mappings: Vec<(String, String, String)> = Vec::new(); // (commit_hash, tag, date)

    for info in walk {
        let info = info.context("failed during commit walk")?;
        let hash = info.id.to_string();

        // Check if this commit is a tag target
        if let Some((tag_name, tag_date)) = tag_targets.get(&hash) {
            current_release = Some((tag_name.clone(), tag_date.clone()));
        }

        if let Some((ref tag, ref date)) = current_release {
            mappings.push((hash, tag.clone(), date.clone()));
        }
    }

    if mappings.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO commit_releases (commit_hash, release_tag, release_date) \
             VALUES (?1, ?2, ?3)",
        )?;
        for (hash, tag, date) in &mappings {
            stmt.execute(rusqlite::params![hash, tag, date])?;
        }
    }
    tx.commit()?;

    println!("Mapped {} commits to releases.", mappings.len());
    Ok(())
}

/// Parse a raw git timestamp string like "1234567890 +0000" into ISO 8601.
fn parse_git_time_str(raw: &str) -> String {
    let parts: Vec<&str> = raw.trim().splitn(2, ' ').collect();
    let secs: i64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    parse_git_time_secs(secs)
}

fn parse_git_time_secs(secs: i64) -> String {
    let dt =
        chrono::DateTime::from_timestamp(secs, 0).unwrap_or(chrono::DateTime::UNIX_EPOCH);
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
