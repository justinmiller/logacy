use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use gix::bstr::{BStr, ByteSlice};
use rusqlite::Connection;

use logacy_core::config::Config;

pub fn run_identity(repo_path: &Path, conn: &Connection, config: &Config) -> Result<()> {
    let repo = gix::open(repo_path).context("failed to open git repository")?;

    // Load mailmap
    let mailmap = if config.identity.mailmap {
        let mm = repo.open_mailmap();
        println!("Loaded .mailmap");
        mm
    } else {
        gix::mailmap::Snapshot::default()
    };

    // Collect all distinct (name, email) pairs from commits and identity trailers
    let mut raw_pairs_set: HashMap<(String, String), bool> = HashMap::new();
    {
        // From commit authors and committers
        let mut stmt = conn.prepare(
            "SELECT DISTINCT author_name, author_email FROM commits
             UNION
             SELECT DISTINCT committer_name, committer_email FROM commits",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            raw_pairs_set.insert(row?, true);
        }

        // From identity trailers (Signed-off-by, Reviewed-by, Tested-by, Acked-by)
        let identity_keys = &config.trailers.identity_keys;
        let sql = format!(
            "SELECT DISTINCT value FROM trailers WHERE key IN ({})",
            logacy_db::sql_placeholders(identity_keys.len())
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::types::ToSql> = identity_keys
            .iter()
            .map(|k| k as &dyn rusqlite::types::ToSql)
            .collect();
        let rows = stmt.query_map(params.as_slice(), |row| row.get::<_, String>(0))?;
        for row in rows {
            let value = row?;
            if let Some((name, email)) = logacy_db::parse_identity_value(&value) {
                raw_pairs_set.insert((name, email), true);
            }
        }
    }

    let raw_pairs: Vec<(String, String)> = raw_pairs_set.into_keys().collect();
    println!("Found {} distinct name/email pairs", raw_pairs.len());

    // Resolve each pair through mailmap → canonical (name, email)
    let mut resolved: HashMap<(String, String), (String, String)> = HashMap::new();

    for (name, email) in &raw_pairs {
        let (canonical_name, canonical_email) = resolve_via_mailmap(name, email, &mailmap);
        resolved.insert(
            (name.clone(), email.clone()),
            (canonical_name, canonical_email),
        );
    }

    // Group by canonical (name, email) → collect raw aliases
    let mut identity_groups: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
    for (raw, canonical) in &resolved {
        identity_groups
            .entry(canonical.clone())
            .or_default()
            .push(raw.clone());
    }

    println!(
        "Mailmap resolved to {} unique (name, email) pairs",
        identity_groups.len()
    );

    // Merge identities that share the same canonical_name but different emails.
    // Pick the most recently used email as canonical (query from commits).
    // This handles people who changed companies (CFS → Sun → Oracle → Intel → Whamcloud → DDN).
    let mut name_groups: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (canonical_name, canonical_email) in identity_groups.keys() {
        name_groups
            .entry(canonical_name.clone())
            .or_default()
            .push((canonical_name.clone(), canonical_email.clone()));
    }

    // For names with multiple emails, find the most recently used email
    struct MergedIdentity {
        canonical_name: String,
        canonical_email: String,
        all_aliases: Vec<(String, String)>,
    }

    let mut merged: Vec<MergedIdentity> = Vec::new();

    for (name, keys) in &name_groups {
        if keys.len() == 1 {
            let key = &keys[0];
            let aliases = identity_groups.get(key).cloned().unwrap_or_default();
            merged.push(MergedIdentity {
                canonical_name: name.clone(),
                canonical_email: key.1.clone(),
                all_aliases: aliases,
            });
        } else {
            // Multiple emails for same name — find most recent
            let emails: Vec<&str> = keys.iter().map(|(_, e)| e.as_str()).collect();
            let best_email = find_most_recent_email(conn, name, &emails)?
                .unwrap_or_else(|| keys[0].1.clone());

            // Merge all aliases from all groups
            let mut all_aliases: Vec<(String, String)> = Vec::new();
            for key in keys {
                if let Some(aliases) = identity_groups.get(key) {
                    all_aliases.extend(aliases.iter().cloned());
                }
                // Also add the canonical email itself as an alias
                all_aliases.push(key.clone());
            }
            // Dedup
            all_aliases.sort();
            all_aliases.dedup();

            merged.push(MergedIdentity {
                canonical_name: name.clone(),
                canonical_email: best_email,
                all_aliases,
            });
        }
    }

    let merge_savings = identity_groups.len() - merged.len();
    if merge_savings > 0 {
        println!(
            "Merged {} identities by name → {} unique identities",
            merge_savings,
            merged.len()
        );
    }

    // Clear and rebuild identity tables
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        "DELETE FROM file_ownership;
         DELETE FROM blame_lines;
         DELETE FROM blame_snapshots;
         DELETE FROM subsystem_reviewers;
         DELETE FROM identity_aliases;
         UPDATE trailers SET identity_id = NULL;
         UPDATE commits SET author_id = NULL, committer_id = NULL;
         DELETE FROM identities;",
    )?;

    // Determine bots
    let bot_emails: Vec<String> = config
        .identity
        .bot_emails
        .iter()
        .map(|e| e.to_lowercase())
        .collect();
    let bot_names: Vec<String> = config
        .identity
        .bot_names
        .iter()
        .map(|n| n.to_lowercase())
        .collect();

    let mut insert_identity = tx.prepare(
        "INSERT INTO identities (canonical_name, canonical_email, is_bot)
         VALUES (?1, ?2, ?3)",
    )?;
    let mut insert_alias = tx.prepare(
        "INSERT OR IGNORE INTO identity_aliases (identity_id, name, email)
         VALUES (?1, ?2, ?3)",
    )?;

    let mut identity_count = 0;
    let mut bot_count = 0;

    // Map: (raw_name, raw_email) → identity_id (for backfill)
    let mut pair_to_id: HashMap<(String, String), i64> = HashMap::new();
    // Map: raw_email → identity_id (for trailer resolution)
    let mut email_to_id: HashMap<String, i64> = HashMap::new();

    for mi in &merged {
        let is_bot = bot_emails.contains(&mi.canonical_email.to_lowercase())
            || bot_names.contains(&mi.canonical_name.to_lowercase());

        insert_identity.execute(rusqlite::params![
            mi.canonical_name,
            mi.canonical_email,
            is_bot as i32,
        ])?;
        let identity_id = tx.last_insert_rowid();
        identity_count += 1;
        if is_bot {
            bot_count += 1;
        }

        // Insert canonical as an alias
        insert_alias.execute(rusqlite::params![
            identity_id,
            mi.canonical_name,
            mi.canonical_email,
        ])?;
        email_to_id.insert(mi.canonical_email.clone(), identity_id);

        for (raw_name, raw_email) in &mi.all_aliases {
            insert_alias
                .execute(rusqlite::params![identity_id, raw_name, raw_email])
                .ok(); // ignore dupes
            email_to_id.insert(raw_email.clone(), identity_id);
            pair_to_id.insert((raw_name.clone(), raw_email.clone()), identity_id);
        }
    }

    drop(insert_identity);
    drop(insert_alias);

    // Backfill author_id and committer_id on commits
    println!("Backfilling author_id / committer_id...");
    {
        let mut update_author = tx.prepare(
            "UPDATE commits SET author_id = ?1 WHERE author_name = ?2 AND author_email = ?3",
        )?;
        let mut update_committer = tx.prepare(
            "UPDATE commits SET committer_id = ?1 WHERE committer_name = ?2 AND committer_email = ?3",
        )?;

        for ((raw_name, raw_email), &id) in &pair_to_id {
            update_author.execute(rusqlite::params![id, raw_name, raw_email])?;
            update_committer.execute(rusqlite::params![id, raw_name, raw_email])?;
        }
    }

    // Backfill identity_id on identity trailers (Signed-off-by, Reviewed-by, etc.)
    println!("Resolving trailer identities...");
    {
        let identity_keys = &config.trailers.identity_keys;
        let mut trailer_rows: Vec<(String, String, i32, String)> = Vec::new();
        {
            let sql = format!(
                "SELECT commit_hash, key, seq, value FROM trailers WHERE key IN ({})",
                logacy_db::sql_placeholders(identity_keys.len())
            );
            let mut stmt = tx.prepare(&sql)?;
            let params: Vec<&dyn rusqlite::types::ToSql> = identity_keys
                .iter()
                .map(|k| k as &dyn rusqlite::types::ToSql)
                .collect();
            let rows = stmt.query_map(params.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?;
            for row in rows {
                trailer_rows.push(row?);
            }
        }

        let mut update_trailer = tx.prepare(
            "UPDATE trailers SET identity_id = ?1 WHERE commit_hash = ?2 AND key = ?3 AND seq = ?4",
        )?;

        let mut resolved_count = 0;
        for (commit_hash, key, seq, value) in &trailer_rows {
            if let Some((t_name, t_email)) = logacy_db::parse_identity_value(value) {
                let (canonical_name, canonical_email) =
                    resolve_via_mailmap(&t_name, &t_email, &mailmap);

                // Try email first, then name-based lookup
                let id = email_to_id.get(&canonical_email).copied().or_else(|| {
                    pair_to_id
                        .get(&(canonical_name, canonical_email))
                        .copied()
                });

                if let Some(id) = id {
                    update_trailer.execute(rusqlite::params![id, commit_hash, key, seq])?;
                    resolved_count += 1;
                }
            }
        }

        println!(
            "Resolved {}/{} identity trailers",
            resolved_count,
            trailer_rows.len()
        );
    }

    // Populate org_domains from config
    if !config.identity.orgs.is_empty() {
        let mut insert_org = tx.prepare(
            "INSERT OR REPLACE INTO org_domains (domain, org, valid_from, valid_until)
             VALUES (?1, ?2, ?3, ?4)",
        )?;
        let mut domain_count = 0usize;
        for org in &config.identity.orgs {
            for domain in org.all_domains() {
                insert_org.execute(rusqlite::params![
                    domain,
                    org.org,
                    org.from,
                    org.until,
                ])?;
                domain_count += 1;
            }
        }
        println!("Loaded {} org domain mappings", domain_count);
    }

    // Backfill identities.org from org_domains by matching email domain.
    // For each identity, find the org whose domain matches the canonical_email.
    // If multiple orgs match (temporal mappings), pick the one with the latest valid_from.
    let org_updated = tx.execute(
        "UPDATE identities SET org = (
            SELECT od.org FROM org_domains od
            WHERE identities.canonical_email LIKE '%@' || od.domain
            ORDER BY od.valid_from DESC
            LIMIT 1
         )
         WHERE EXISTS (
            SELECT 1 FROM org_domains od
            WHERE identities.canonical_email LIKE '%@' || od.domain
         )",
        [],
    )?;
    if org_updated > 0 {
        println!("Assigned org to {} identities from domain mappings", org_updated);
    }

    // Apply explicit org overrides (for contributors using personal emails)
    let mut override_count = 0u64;
    for ovr in &config.identity.org_overrides {
        let updated = match (&ovr.name, &ovr.email) {
            (Some(name), _) => tx.execute(
                "UPDATE identities SET org = ?1 WHERE canonical_name = ?2 AND org IS NULL",
                rusqlite::params![ovr.org, name],
            )?,
            (None, Some(email)) => tx.execute(
                "UPDATE identities SET org = ?1 WHERE id IN (
                    SELECT identity_id FROM identity_aliases WHERE email = ?2
                ) AND org IS NULL",
                rusqlite::params![ovr.org, email],
            )?,
            _ => 0,
        };
        override_count += updated as u64;
    }
    if override_count > 0 {
        println!("Applied {} org overrides", override_count);
    }

    tx.commit()?;

    println!(
        "Identity resolution complete: {} identities ({} bots)",
        identity_count, bot_count
    );

    Ok(())
}

/// Resolve a (name, email) pair through the mailmap to canonical form.
fn resolve_via_mailmap(
    name: &str,
    email: &str,
    mailmap: &gix::mailmap::Snapshot,
) -> (String, String) {
    let name_bstr: &BStr = name.as_bytes().as_bstr();
    let email_bstr: &BStr = email.as_bytes().as_bstr();
    let sig_ref = gix::actor::SignatureRef {
        name: name_bstr,
        email: email_bstr,
        time: "0 +0000",
    };
    let canonical = mailmap.resolve(sig_ref);
    (
        canonical.name.to_str_lossy().to_string(),
        canonical.email.to_str_lossy().to_string(),
    )
}

/// For a canonical name with multiple emails, find which email was used most recently
/// across both commit authorship and trailer values.
fn find_most_recent_email(conn: &Connection, name: &str, emails: &[&str]) -> Result<Option<String>> {
    if emails.is_empty() {
        return Ok(None);
    }
    let placeholders = logacy_db::sql_placeholders(emails.len());
    let sql = format!(
        "SELECT email FROM (
             SELECT author_email AS email, author_date AS used_date
             FROM commits
             WHERE author_name = ?1 AND author_email IN ({placeholders})
             UNION ALL
             SELECT SUBSTR(t.value, INSTR(t.value, '<') + 1,
                           INSTR(t.value, '>') - INSTR(t.value, '<') - 1) AS email,
                    c.author_date AS used_date
             FROM trailers t
             JOIN commits c ON c.hash = t.commit_hash
             WHERE INSTR(t.value, '<') > 0
               AND INSTR(t.value, '>') > INSTR(t.value, '<')
               AND SUBSTR(t.value, INSTR(t.value, '<') + 1,
                           INSTR(t.value, '>') - INSTR(t.value, '<') - 1) IN ({placeholders})
         )
         ORDER BY used_date DESC LIMIT 1"
    );
    let mut stmt = conn.prepare(&sql)?;

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params.push(Box::new(name.to_string()));
    for email in emails {
        params.push(Box::new(email.to_string()));
    }
    for email in emails {
        params.push(Box::new(email.to_string()));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let result = stmt.query_row(param_refs.as_slice(), |row| row.get::<_, String>(0));
    match result {
        Ok(email) => Ok(Some(email)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(Some(emails[0].to_string())),
        Err(e) => Err(e.into()),
    }
}

