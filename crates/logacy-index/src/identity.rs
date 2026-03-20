use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use gix::bstr::{BStr, ByteSlice};
use rusqlite::Connection;

use logacy_core::config::Config;

// ── Domain types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CanonicalIdentity {
    name: String,
    email: String,
}

struct PersistedIdentity {
    #[allow(dead_code)]
    id: i64,
    #[allow(dead_code)]
    canonical: CanonicalIdentity,
    #[allow(dead_code)]
    aliases: Vec<(String, String)>,
    #[allow(dead_code)]
    is_bot: bool,
}

// ── Config helpers ──────────────────────────────────────────────────────────

/// Build the config alias override map: raw email (lowercase) → (canonical_name, canonical_email).
fn build_alias_overrides(config: &Config) -> HashMap<String, (String, String)> {
    let mut overrides = HashMap::new();
    for alias in &config.identity.aliases {
        let canonical_email = alias.emails.first().cloned().unwrap_or_default();
        for email in &alias.emails {
            overrides.insert(
                email.to_lowercase(),
                (alias.name.clone(), canonical_email.clone()),
            );
        }
    }
    overrides
}

// ── Resolution pipeline ─────────────────────────────────────────────────────

/// Resolve a (name, email) pair through: mailmap → config alias → passthrough.
fn resolve_canonical(
    name: &str,
    email: &str,
    mailmap: &gix::mailmap::Snapshot,
    alias_overrides: &HashMap<String, (String, String)>,
) -> CanonicalIdentity {
    let (mm_name, mm_email) = resolve_via_mailmap(name, email, mailmap);

    if let Some(overridden) = alias_overrides
        .get(&email.to_lowercase())
        .or_else(|| alias_overrides.get(&mm_email.to_lowercase()))
    {
        return CanonicalIdentity {
            name: overridden.0.clone(),
            email: overridden.1.clone(),
        };
    }

    CanonicalIdentity {
        name: mm_name,
        email: mm_email,
    }
}

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

/// Look up the identity_id for a (name, email) pair.
fn lookup_identity(
    name: &str,
    email: &str,
    mailmap: &gix::mailmap::Snapshot,
    alias_overrides: &HashMap<String, (String, String)>,
    email_to_id: &HashMap<String, i64>,
    pair_to_id: &HashMap<(String, String), i64>,
) -> Option<i64> {
    let canonical = resolve_canonical(name, email, mailmap, alias_overrides);

    email_to_id
        .get(&canonical.email)
        .copied()
        .or_else(|| email_to_id.get(&email.to_lowercase()).copied())
        .or_else(|| {
            pair_to_id
                .get(&(canonical.name, canonical.email))
                .copied()
        })
}

// ── Pass 1: Collect observed identities ─────────────────────────────────────

fn collect_observed_identities(
    conn: &Connection,
    config: &Config,
) -> Result<HashSet<(String, String)>> {
    let mut pairs = HashSet::new();

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
        pairs.insert(row?);
    }

    // From identity trailers
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
            pairs.insert((name, email));
        }
    }

    println!("Found {} distinct name/email pairs", pairs.len());
    Ok(pairs)
}

// ── Pass 2: Resolve canonical identities ────────────────────────────────────

fn resolve_canonical_identities(
    raw_pairs: &HashSet<(String, String)>,
    mailmap: &gix::mailmap::Snapshot,
    alias_overrides: &HashMap<String, (String, String)>,
) -> HashMap<(String, String), CanonicalIdentity> {
    let mut resolved = HashMap::new();
    for (name, email) in raw_pairs {
        let canonical = resolve_canonical(name, email, mailmap, alias_overrides);
        resolved.insert((name.clone(), email.clone()), canonical);
    }
    resolved
}

// ── Pass 3: Persist identities and aliases ──────────────────────────────────
// No same-name auto-merge. Only mailmap and config aliases merge identities.

fn persist_identities_and_aliases(
    tx: &Connection,
    resolved: &HashMap<(String, String), CanonicalIdentity>,
    config: &Config,
) -> Result<(Vec<PersistedIdentity>, HashMap<String, i64>, HashMap<(String, String), i64>)> {
    // Group by canonical identity → collect raw aliases
    let mut identity_groups: HashMap<CanonicalIdentity, Vec<(String, String)>> = HashMap::new();
    for (raw, canonical) in resolved {
        identity_groups
            .entry(canonical.clone())
            .or_default()
            .push(raw.clone());
    }

    println!(
        "Resolved to {} unique canonical identities",
        identity_groups.len()
    );

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

    let mut persisted = Vec::new();
    let mut email_to_id: HashMap<String, i64> = HashMap::new();
    let mut pair_to_id: HashMap<(String, String), i64> = HashMap::new();
    let mut bot_count = 0;

    for (canonical, aliases) in &identity_groups {
        let is_bot = bot_emails.contains(&canonical.email.to_lowercase())
            || bot_names.contains(&canonical.name.to_lowercase());

        insert_identity.execute(rusqlite::params![
            canonical.name,
            canonical.email,
            is_bot as i32,
        ])?;
        let identity_id = tx.last_insert_rowid();
        if is_bot {
            bot_count += 1;
        }

        // Insert canonical as an alias
        insert_alias.execute(rusqlite::params![
            identity_id,
            canonical.name,
            canonical.email,
        ])?;

        for (raw_name, raw_email) in aliases {
            insert_alias
                .execute(rusqlite::params![identity_id, raw_name, raw_email])
                .ok(); // ignore dupes
        }

        // Register for backfill lookups
        email_to_id.insert(canonical.email.clone(), identity_id);
        for (raw_name, raw_email) in aliases {
            email_to_id.insert(raw_email.clone(), identity_id);
            pair_to_id.insert((raw_name.clone(), raw_email.clone()), identity_id);
        }

        persisted.push(PersistedIdentity {
            id: identity_id,
            canonical: canonical.clone(),
            aliases: aliases.clone(),
            is_bot,
        });
    }

    drop(insert_identity);
    drop(insert_alias);

    println!(
        "Identity resolution complete: {} identities ({} bots)",
        persisted.len(),
        bot_count
    );

    Ok((persisted, email_to_id, pair_to_id))
}

// ── Pass 3b: Backfill author_id / committer_id / trailer identity_id ────────

fn backfill_identity_ids(
    tx: &Connection,
    config: &Config,
    mailmap: &gix::mailmap::Snapshot,
    alias_overrides: &HashMap<String, (String, String)>,
    email_to_id: &HashMap<String, i64>,
    pair_to_id: &HashMap<(String, String), i64>,
) -> Result<()> {
    println!("Backfilling author_id / committer_id...");
    {
        let mut update_author = tx.prepare(
            "UPDATE commits SET author_id = ?1 WHERE author_name = ?2 AND author_email = ?3",
        )?;
        let mut update_committer = tx.prepare(
            "UPDATE commits SET committer_id = ?1 WHERE committer_name = ?2 AND committer_email = ?3",
        )?;

        for ((raw_name, raw_email), &id) in pair_to_id {
            update_author.execute(rusqlite::params![id, raw_name, raw_email])?;
            update_committer.execute(rusqlite::params![id, raw_name, raw_email])?;
        }
    }

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
                if let Some(id) = lookup_identity(
                    &t_name,
                    &t_email,
                    mailmap,
                    alias_overrides,
                    email_to_id,
                    pair_to_id,
                ) {
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

    Ok(())
}

// ── Pass 4: Populate identity_emails ────────────────────────────────────────

fn populate_identity_emails(tx: &Connection) -> Result<()> {
    // Collect email usage from commits (author)
    tx.execute_batch(
        "INSERT OR IGNORE INTO identity_emails (identity_id, email, first_seen_at, last_seen_at, commit_count, trailer_count, source, is_preferred)
         SELECT c.author_id, c.author_email,
                MIN(c.author_date), MAX(c.author_date),
                COUNT(*), 0, 'commit', 0
         FROM commits c
         WHERE c.author_id IS NOT NULL
         GROUP BY c.author_id, c.author_email;",
    )?;

    // Upsert committer emails — merge counts and extend date range if already present.
    tx.execute_batch(
        "INSERT INTO identity_emails (identity_id, email, first_seen_at, last_seen_at, commit_count, trailer_count, source, is_preferred)
         SELECT c.committer_id, c.committer_email,
                MIN(c.commit_date), MAX(c.commit_date),
                COUNT(*), 0, 'commit', 0
         FROM commits c
         WHERE c.committer_id IS NOT NULL
         GROUP BY c.committer_id, c.committer_email
         ON CONFLICT(identity_id, email) DO UPDATE SET
           commit_count = commit_count + excluded.commit_count,
           first_seen_at = MIN(first_seen_at, excluded.first_seen_at),
           last_seen_at = MAX(last_seen_at, excluded.last_seen_at);",
    )?;

    // Update with trailer emails
    tx.execute_batch(
        "INSERT INTO identity_emails (identity_id, email, first_seen_at, last_seen_at, commit_count, trailer_count, source, is_preferred)
         SELECT t.identity_id, t.parsed_email,
                MIN(c.author_date), MAX(c.author_date),
                0, COUNT(*), 'trailer', 0
         FROM trailers t
         JOIN commits c ON c.hash = t.commit_hash
         WHERE t.identity_id IS NOT NULL AND t.parsed_email IS NOT NULL
         GROUP BY t.identity_id, t.parsed_email
         ON CONFLICT(identity_id, email) DO UPDATE SET
           trailer_count = trailer_count + excluded.trailer_count,
           first_seen_at = MIN(first_seen_at, excluded.first_seen_at),
           last_seen_at = MAX(last_seen_at, excluded.last_seen_at);",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM identity_emails", [], |r| r.get(0))?;
    println!("Populated {} identity email records", count);

    Ok(())
}

// ── Pass 5: Select preferred email by recency ───────────────────────────────

fn select_preferred_email_by_recency(tx: &Connection) -> Result<()> {
    // Mark the most recently used email as preferred for each identity.
    // Ties broken by email text for determinism.
    // Do NOT update identities.canonical_email — that is set by the resolution
    // pipeline and is part of the UNIQUE constraint. The preferred email lives
    // in identity_emails.is_preferred and is used for downstream org attribution.
    tx.execute_batch(
        "UPDATE identity_emails SET is_preferred = 1
         WHERE rowid IN (
             SELECT ie.rowid FROM identity_emails ie
             WHERE ie.rowid = (
                 SELECT ie2.rowid FROM identity_emails ie2
                 WHERE ie2.identity_id = ie.identity_id
                 ORDER BY ie2.last_seen_at DESC, ie2.email ASC
                 LIMIT 1
             )
         );",
    )?;

    Ok(())
}

// ── Pass 6: Load organizations and domain rules ─────────────────────────────

fn load_organizations_and_rules(tx: &Connection, config: &Config) -> Result<()> {
    // Always insert org names from all config sources, not just [[identity.orgs]].
    // org_overrides and alias-level orgs need organization rows to exist too.
    for ovr in &config.identity.org_overrides {
        tx.execute(
            "INSERT OR IGNORE INTO organizations (name) VALUES (?1)",
            rusqlite::params![ovr.org],
        )?;
    }
    for alias in &config.identity.aliases {
        if let Some(ref org) = alias.org {
            tx.execute(
                "INSERT OR IGNORE INTO organizations (name) VALUES (?1)",
                rusqlite::params![org],
            )?;
        }
    }

    if config.identity.orgs.is_empty() {
        return Ok(());
    }

    let mut insert_org = tx.prepare(
        "INSERT OR IGNORE INTO organizations (name) VALUES (?1)",
    )?;
    let mut insert_rule = tx.prepare(
        "INSERT INTO org_domain_rules (org_id, domain, valid_from, valid_until)
         VALUES ((SELECT id FROM organizations WHERE name = ?1), ?2, ?3, ?4)",
    )?;

    let mut domain_count = 0usize;
    for org in &config.identity.orgs {
        insert_org.execute(rusqlite::params![org.org])?;
        for domain in org.all_domains() {
            insert_rule.execute(rusqlite::params![
                org.org,
                domain,
                org.from,
                org.until,
            ])?;
            domain_count += 1;
        }
    }

    println!("Loaded {} org domain rules", domain_count);
    Ok(())
}

// ── Pass 7: Populate identity affiliations ──────────────────────────────────

fn populate_identity_affiliations(tx: &Connection, config: &Config) -> Result<()> {
    // From org_domain_rules: match each identity's emails against domain rules.
    // Domain matching covers: exact @domain, subdomains (.domain), and bare identifiers.
    tx.execute_batch(
        "INSERT INTO identity_affiliations (identity_id, org_id, valid_from, valid_until, source)
         SELECT DISTINCT ie.identity_id, odr.org_id, odr.valid_from, odr.valid_until, 'domain_rule'
         FROM identity_emails ie
         JOIN org_domain_rules odr
           ON (ie.email LIKE '%@' || odr.domain
               OR ie.email LIKE '%.' || odr.domain
               OR (INSTR(ie.email, '@') = 0 AND odr.domain = '(bare)'))
         WHERE NOT EXISTS (
             SELECT 1 FROM identity_affiliations ia
             WHERE ia.identity_id = ie.identity_id AND ia.org_id = odr.org_id
               AND COALESCE(ia.valid_from, '') = COALESCE(odr.valid_from, '')
               AND COALESCE(ia.valid_until, '') = COALESCE(odr.valid_until, '')
         );",
    )?;

    // From config org_overrides — these always apply, even when a domain-derived
    // affiliation already exists. Explicit overrides are authoritative.
    let mut override_count = 0u64;
    for ovr in &config.identity.org_overrides {
        let updated = match (&ovr.name, &ovr.email) {
            (Some(name), _) => tx.execute(
                "INSERT INTO identity_affiliations (identity_id, org_id, source)
                 SELECT i.id, o.id, 'org_override'
                 FROM identities i, organizations o
                 WHERE i.canonical_name = ?1 AND o.name = ?2",
                rusqlite::params![name, ovr.org],
            )?,
            (None, Some(email)) => tx.execute(
                "INSERT INTO identity_affiliations (identity_id, org_id, source)
                 SELECT ia.identity_id, o.id, 'org_override'
                 FROM identity_aliases ia, organizations o
                 WHERE ia.email = ?1 AND o.name = ?2",
                rusqlite::params![email, ovr.org],
            )?,
            _ => 0,
        };
        override_count += updated as u64;
    }
    if override_count > 0 {
        println!("Applied {} org overrides", override_count);
    }

    // From config aliases with org — alias_override is authoritative too.
    for alias in &config.identity.aliases {
        if let Some(ref org) = alias.org {
            tx.execute(
                "INSERT INTO identity_affiliations (identity_id, org_id, source)
                 SELECT i.id, o.id, 'alias_override'
                 FROM identities i, organizations o
                 WHERE i.canonical_name = ?1 AND o.name = ?2",
                rusqlite::params![alias.name, org],
            )?;
        }
    }

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM identity_affiliations", [], |r| r.get(0))?;
    println!("Populated {} identity affiliations", count);

    Ok(())
}

// ── Pass 8: Materialize commit org attribution ──────────────────────────────

fn materialize_commit_org_attribution(tx: &Connection) -> Result<()> {
    // Domain match condition used throughout: @domain, subdomains (.domain), bare identifiers.
    // When multiple rules match, pick the one with the most specific (latest) valid_from.

    // Pass 1: raw commit email domain → org_domain_rules with temporal bounds
    // Use a correlated subquery to pick exactly one rule per commit deterministically.
    tx.execute_batch(
        "INSERT OR IGNORE INTO commit_org_attribution (commit_hash, org_id, org_name, source, matched_email, matched_domain, matched_rule_id)
         SELECT c.hash, odr.org_id, o.name, 'raw_email_domain', c.author_email, odr.domain, odr.id
         FROM commits c
         JOIN org_domain_rules odr ON odr.id = (
             SELECT odr2.id FROM org_domain_rules odr2
             WHERE (c.author_email LIKE '%@' || odr2.domain
                    OR c.author_email LIKE '%.' || odr2.domain
                    OR (INSTR(c.author_email, '@') = 0 AND odr2.domain = '(bare)'))
               AND (odr2.valid_from IS NULL OR c.author_date >= odr2.valid_from)
               AND (odr2.valid_until IS NULL OR c.author_date < odr2.valid_until)
             ORDER BY odr2.valid_from DESC
             LIMIT 1
         )
         JOIN organizations o ON o.id = odr.org_id;",
    )?;

    // Pass 2: dated identity affiliation (prefer overrides, then most specific date)
    tx.execute_batch(
        "INSERT OR IGNORE INTO commit_org_attribution (commit_hash, org_id, org_name, source, matched_email, matched_domain)
         SELECT c.hash, ia.org_id, o.name, 'affiliation', NULL, NULL
         FROM commits c
         JOIN identity_affiliations ia ON ia.id = (
             SELECT ia2.id FROM identity_affiliations ia2
             WHERE ia2.identity_id = c.author_id
               AND (ia2.valid_from IS NULL OR c.author_date >= ia2.valid_from)
               AND (ia2.valid_until IS NULL OR c.author_date < ia2.valid_until)
             ORDER BY
               CASE ia2.source WHEN 'alias_override' THEN 0 WHEN 'org_override' THEN 1 ELSE 2 END,
               ia2.valid_from DESC
             LIMIT 1
         )
         JOIN organizations o ON o.id = ia.org_id
         WHERE c.author_id IS NOT NULL
           AND c.hash NOT IN (SELECT commit_hash FROM commit_org_attribution);",
    )?;

    // Pass 3: preferred email domain
    tx.execute_batch(
        "INSERT OR IGNORE INTO commit_org_attribution (commit_hash, org_id, org_name, source, matched_email, matched_domain, matched_rule_id)
         SELECT c.hash, odr.org_id, o.name, 'preferred_email_domain', ie.email, odr.domain, odr.id
         FROM commits c
         JOIN identity_emails ie ON ie.identity_id = c.author_id AND ie.is_preferred = 1
         JOIN org_domain_rules odr ON odr.id = (
             SELECT odr2.id FROM org_domain_rules odr2
             WHERE (ie.email LIKE '%@' || odr2.domain
                    OR ie.email LIKE '%.' || odr2.domain)
               AND (odr2.valid_from IS NULL OR c.author_date >= odr2.valid_from)
               AND (odr2.valid_until IS NULL OR c.author_date < odr2.valid_until)
             ORDER BY odr2.valid_from DESC
             LIMIT 1
         )
         JOIN organizations o ON o.id = odr.org_id
         WHERE c.author_id IS NOT NULL
           AND c.hash NOT IN (SELECT commit_hash FROM commit_org_attribution);",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM commit_org_attribution", [], |r| r.get(0))?;
    println!("Materialized org attribution for {} commits", count);

    Ok(())
}

// ── Pass 9: Materialize trailer org attribution ─────────────────────────────

fn materialize_trailer_org_attribution(tx: &Connection, config: &Config) -> Result<()> {
    let identity_keys = &config.trailers.identity_keys;
    if identity_keys.is_empty() {
        return Ok(());
    }

    let placeholders = logacy_db::sql_placeholders(identity_keys.len());
    let params: Vec<&dyn rusqlite::types::ToSql> = identity_keys
        .iter()
        .map(|k| k as &dyn rusqlite::types::ToSql)
        .collect();

    // Pass 1: parsed trailer email domain → org_domain_rules with temporal bounds
    // Deterministic: correlated subquery picks most-specific rule per trailer.
    let sql1 = format!(
        "INSERT OR IGNORE INTO trailer_org_attribution (commit_hash, key, seq, org_id, org_name, source, matched_email, matched_domain, matched_rule_id)
         SELECT t.commit_hash, t.key, t.seq, odr.org_id, o.name, 'parsed_email_domain', t.parsed_email, odr.domain, odr.id
         FROM trailers t
         JOIN commits c ON c.hash = t.commit_hash
         JOIN org_domain_rules odr ON odr.id = (
             SELECT odr2.id FROM org_domain_rules odr2
             WHERE (t.parsed_email LIKE '%@' || odr2.domain
                    OR t.parsed_email LIKE '%.' || odr2.domain
                    OR (INSTR(t.parsed_email, '@') = 0 AND odr2.domain = '(bare)'))
               AND (odr2.valid_from IS NULL OR c.author_date >= odr2.valid_from)
               AND (odr2.valid_until IS NULL OR c.author_date < odr2.valid_until)
             ORDER BY odr2.valid_from DESC
             LIMIT 1
         )
         JOIN organizations o ON o.id = odr.org_id
         WHERE t.key IN ({placeholders})
           AND t.parsed_email IS NOT NULL;"
    );
    let mut stmt = tx.prepare(&sql1)?;
    stmt.execute(params.as_slice())?;
    drop(stmt);

    // Pass 2: dated identity affiliation for resolved trailer identities
    let sql2 = format!(
        "INSERT OR IGNORE INTO trailer_org_attribution (commit_hash, key, seq, org_id, org_name, source, matched_email, matched_domain)
         SELECT t.commit_hash, t.key, t.seq, ia.org_id, o.name, 'affiliation', NULL, NULL
         FROM trailers t
         JOIN commits c ON c.hash = t.commit_hash
         JOIN identity_affiliations ia ON ia.id = (
             SELECT ia2.id FROM identity_affiliations ia2
             WHERE ia2.identity_id = t.identity_id
               AND (ia2.valid_from IS NULL OR c.author_date >= ia2.valid_from)
               AND (ia2.valid_until IS NULL OR c.author_date < ia2.valid_until)
             ORDER BY
               CASE ia2.source WHEN 'alias_override' THEN 0 WHEN 'org_override' THEN 1 ELSE 2 END,
               ia2.valid_from DESC
             LIMIT 1
         )
         JOIN organizations o ON o.id = ia.org_id
         WHERE t.key IN ({placeholders})
           AND t.identity_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM trailer_org_attribution toa
               WHERE toa.commit_hash = t.commit_hash AND toa.key = t.key AND toa.seq = t.seq
           );"
    );
    let mut stmt = tx.prepare(&sql2)?;
    stmt.execute(params.as_slice())?;
    drop(stmt);

    // Pass 3: preferred email domain for resolved trailer identities
    let sql3 = format!(
        "INSERT OR IGNORE INTO trailer_org_attribution (commit_hash, key, seq, org_id, org_name, source, matched_email, matched_domain, matched_rule_id)
         SELECT t.commit_hash, t.key, t.seq, odr.org_id, o.name, 'preferred_email_domain', ie.email, odr.domain, odr.id
         FROM trailers t
         JOIN commits c ON c.hash = t.commit_hash
         JOIN identity_emails ie ON ie.identity_id = t.identity_id AND ie.is_preferred = 1
         JOIN org_domain_rules odr ON odr.id = (
             SELECT odr2.id FROM org_domain_rules odr2
             WHERE (ie.email LIKE '%@' || odr2.domain
                    OR ie.email LIKE '%.' || odr2.domain)
               AND (odr2.valid_from IS NULL OR c.author_date >= odr2.valid_from)
               AND (odr2.valid_until IS NULL OR c.author_date < odr2.valid_until)
             ORDER BY odr2.valid_from DESC
             LIMIT 1
         )
         JOIN organizations o ON o.id = odr.org_id
         WHERE t.key IN ({placeholders})
           AND t.identity_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM trailer_org_attribution toa
               WHERE toa.commit_hash = t.commit_hash AND toa.key = t.key AND toa.seq = t.seq
           );"
    );
    let mut stmt = tx.prepare(&sql3)?;
    stmt.execute(params.as_slice())?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM trailer_org_attribution", [], |r| r.get(0))?;
    println!("Materialized org attribution for {} trailers", count);

    Ok(())
}

// ── Main entry point ────────────────────────────────────────────────────────

pub fn run_identity(repo_path: &Path, conn: &Connection, config: &Config) -> Result<()> {
    let repo = gix::open(repo_path).context("failed to open git repository")?;

    let mailmap = if config.identity.mailmap {
        let mm = repo.open_mailmap();
        println!("Loaded .mailmap");
        mm
    } else {
        gix::mailmap::Snapshot::default()
    };

    let alias_overrides = build_alias_overrides(config);

    // Pass 1: Collect observed identities
    let raw_pairs = collect_observed_identities(conn, config)?;

    // Pass 2: Resolve canonical identities
    let resolved = resolve_canonical_identities(&raw_pairs, &mailmap, &alias_overrides);

    if !config.identity.aliases.is_empty() {
        println!(
            "Applied {} identity alias rules from config",
            config.identity.aliases.len()
        );
    }

    // Clear and rebuild identity-related tables
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        "DELETE FROM trailer_org_attribution;
         DELETE FROM commit_org_attribution;
         DELETE FROM identity_affiliations;
         DELETE FROM org_domain_rules;
         DELETE FROM organizations;
         DELETE FROM identity_emails;
         DELETE FROM file_ownership;
         DELETE FROM blame_hunks;
         DELETE FROM blame_snapshots;
         DELETE FROM subsystem_reviewers;
         DELETE FROM identity_aliases;
         UPDATE trailers SET identity_id = NULL;
         UPDATE commits SET author_id = NULL, committer_id = NULL;
         DELETE FROM identities;",
    )?;

    // Pass 3: Persist identities and aliases
    let (_persisted, email_to_id, pair_to_id) =
        persist_identities_and_aliases(&tx, &resolved, config)?;

    // Pass 3b: Backfill identity IDs on commits and trailers
    backfill_identity_ids(&tx, config, &mailmap, &alias_overrides, &email_to_id, &pair_to_id)?;

    // Pass 4: Populate identity_emails
    populate_identity_emails(&tx)?;

    // Pass 5: Select preferred email by recency
    select_preferred_email_by_recency(&tx)?;

    // Pass 6: Load organizations and domain rules
    load_organizations_and_rules(&tx, config)?;

    // Pass 7: Populate identity affiliations
    populate_identity_affiliations(&tx, config)?;

    // Pass 8: Materialize commit org attribution
    materialize_commit_org_attribution(&tx)?;

    // Pass 9: Materialize trailer org attribution
    materialize_trailer_org_attribution(&tx, config)?;

    tx.commit()?;

    Ok(())
}
