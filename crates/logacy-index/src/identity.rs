use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use gix::bstr::{BStr, ByteSlice};
use rusqlite::Connection;

use logacy_core::config::Config;

// ── Union-Find ───────────────────────────────────────────────────────────────

struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]);
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }
}

// ── Domain types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct MergedIdentity {
    canonical_name: String,
    canonical_email: String,
    aliases: Vec<(String, String)>,
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
) -> (String, String) {
    let (mm_name, mm_email) = resolve_via_mailmap(name, email, mailmap);

    if let Some(overridden) = alias_overrides
        .get(&email.to_lowercase())
        .or_else(|| alias_overrides.get(&mm_email.to_lowercase()))
    {
        return (overridden.0.clone(), overridden.1.clone());
    }

    (mm_name, mm_email)
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
    let (cn, ce) = resolve_canonical(name, email, mailmap, alias_overrides);

    email_to_id
        .get(&ce)
        .copied()
        .or_else(|| email_to_id.get(&email.to_lowercase()).copied())
        .or_else(|| pair_to_id.get(&(cn, ce)).copied())
}

// ── Loading resolution sources ──────────────────────────────────────────────

fn load_resolution_sources(
    repo_path: &Path,
    config: &Config,
) -> Result<(gix::mailmap::Snapshot, HashMap<String, (String, String)>)> {
    let repo = gix::open(repo_path).context("failed to open git repository")?;

    let mailmap = if config.identity.mailmap {
        let mm = repo.open_mailmap();
        println!("Loaded .mailmap");
        mm
    } else {
        gix::mailmap::Snapshot::default()
    };

    let alias_overrides = build_alias_overrides(config);
    if !config.identity.aliases.is_empty() {
        println!(
            "Applied {} identity alias rules from config",
            config.identity.aliases.len()
        );
    }

    Ok((mailmap, alias_overrides))
}

// ── Phase 1: Resolve & merge identities (in-memory) ─────────────────────────

fn resolve_and_merge_identities(
    conn: &Connection,
    config: &Config,
    mailmap: &gix::mailmap::Snapshot,
    alias_overrides: &HashMap<String, (String, String)>,
) -> Result<Vec<MergedIdentity>> {
    // Step 1: Collect all raw (name, email) pairs from commits + trailers
    let raw_pairs = collect_observed_identities(conn, config)?;

    // Step 2: Resolve each pair through mailmap + config aliases
    let mut resolved_pairs: Vec<((String, String), (String, String))> = Vec::new();
    for (name, email) in &raw_pairs {
        let (cn, ce) = resolve_canonical(name, email, mailmap, alias_overrides);
        resolved_pairs.push(((name.clone(), email.clone()), (cn, ce)));
    }

    // Step 3: Union-Find merge — union all pairs sharing the same canonical name
    let n = resolved_pairs.len();
    let mut uf = UnionFind::new(n);
    let mut name_to_first_idx: HashMap<String, usize> = HashMap::new();

    for (i, (_, (cn, _))) in resolved_pairs.iter().enumerate() {
        let cn_lower = cn.to_lowercase();
        if let Some(&first) = name_to_first_idx.get(&cn_lower) {
            uf.union(i, first);
        } else {
            name_to_first_idx.insert(cn_lower, i);
        }
    }

    // Step 4: Group by find(i) → clusters
    let mut clusters: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        clusters.entry(uf.find(i)).or_default().push(i);
    }

    println!(
        "Found {} distinct name/email pairs, merged to {} identities",
        raw_pairs.len(),
        clusters.len()
    );

    // Step 5: For each cluster, pick canonical name/email and collect aliases
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

    let mut merged = Vec::with_capacity(clusters.len());
    for (_, indices) in clusters {
        // Collect all raw aliases and all resolved (name, email) pairs
        let mut all_raw: Vec<(String, String)> = Vec::new();
        let mut all_resolved: Vec<(String, String)> = Vec::new();
        for &i in &indices {
            all_raw.push(resolved_pairs[i].0.clone());
            all_resolved.push(resolved_pairs[i].1.clone());
        }

        // Pick canonical: prefer config alias override, else first resolved pair
        // (config alias canonical email is already set correctly by resolve_canonical)
        let (canonical_name, canonical_email) = {
            // Check if any resolved email is in alias_overrides
            let alias_pick = all_resolved.iter().find(|(_, ce)| {
                alias_overrides.contains_key(&ce.to_lowercase())
            });
            if let Some(pick) = alias_pick {
                pick.clone()
            } else {
                all_resolved[0].clone()
            }
        };

        let is_bot = bot_emails.contains(&canonical_email.to_lowercase())
            || bot_names.contains(&canonical_name.to_lowercase());

        // Deduplicate raw aliases
        let mut alias_set = HashSet::new();
        let mut aliases = Vec::new();
        for raw in all_raw {
            if alias_set.insert(raw.clone()) {
                aliases.push(raw);
            }
        }
        // Also add all resolved pairs as aliases (canonical email variants)
        for resolved in &all_resolved {
            if alias_set.insert(resolved.clone()) {
                aliases.push(resolved.clone());
            }
        }

        merged.push(MergedIdentity {
            canonical_name,
            canonical_email,
            aliases,
            is_bot,
        });
    }

    let bot_count = merged.iter().filter(|m| m.is_bot).count();
    println!(
        "Identity resolution complete: {} identities ({} bots)",
        merged.len(),
        bot_count
    );

    Ok(merged)
}

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

    Ok(pairs)
}

// ── Phase 2: Persist identities ──────────────────────────────────────────────

fn persist_identities(
    tx: &Connection,
    merged: &[MergedIdentity],
    config: &Config,
    mailmap: &gix::mailmap::Snapshot,
    alias_overrides: &HashMap<String, (String, String)>,
) -> Result<()> {
    // Insert identities and aliases
    let mut insert_identity = tx.prepare(
        "INSERT INTO identities (canonical_name, canonical_email, is_bot)
         VALUES (?1, ?2, ?3)",
    )?;
    let mut insert_alias = tx.prepare(
        "INSERT OR IGNORE INTO identity_aliases (identity_id, name, email)
         VALUES (?1, ?2, ?3)",
    )?;

    let mut email_to_id: HashMap<String, i64> = HashMap::new();
    let mut pair_to_id: HashMap<(String, String), i64> = HashMap::new();

    for identity in merged {
        insert_identity.execute(rusqlite::params![
            identity.canonical_name,
            identity.canonical_email,
            identity.is_bot as i32,
        ])?;
        let identity_id = tx.last_insert_rowid();

        // Insert canonical as an alias
        insert_alias.execute(rusqlite::params![
            identity_id,
            identity.canonical_name,
            identity.canonical_email,
        ])?;

        for (raw_name, raw_email) in &identity.aliases {
            insert_alias
                .execute(rusqlite::params![identity_id, raw_name, raw_email])
                .ok(); // ignore dupes
        }

        // Register for backfill lookups
        email_to_id.insert(identity.canonical_email.clone(), identity_id);
        for (raw_name, raw_email) in &identity.aliases {
            email_to_id.insert(raw_email.clone(), identity_id);
            pair_to_id.insert((raw_name.clone(), raw_email.clone()), identity_id);
        }
    }

    drop(insert_identity);
    drop(insert_alias);

    // Backfill author_id / committer_id
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

    // Resolve trailer identities
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
                    &email_to_id,
                    &pair_to_id,
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

    // Populate identity_emails
    populate_identity_emails(tx)?;

    // Select preferred email by recency
    select_preferred_email_by_recency(tx)?;

    Ok(())
}

fn populate_identity_emails(tx: &Connection) -> Result<()> {
    // Collect email usage from commits (author)
    tx.execute_batch(
        "INSERT OR IGNORE INTO identity_emails (identity_id, email, first_seen_at, last_seen_at, commit_count, trailer_count, source, is_preferred)
         SELECT c.author_id, c.author_email,
                MIN(c.commit_date), MAX(c.commit_date),
                COUNT(*), 0, 'commit', 0
         FROM commits c
         WHERE c.author_id IS NOT NULL
         GROUP BY c.author_id, c.author_email;",
    )?;

    // Upsert committer emails
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
                MIN(c.commit_date), MAX(c.commit_date),
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

fn select_preferred_email_by_recency(tx: &Connection) -> Result<()> {
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

// ── Phase 3: Org attribution ─────────────────────────────────────────────────

fn materialize_org_attribution(tx: &Connection, config: &Config) -> Result<()> {
    // Load organizations and domain rules
    load_organizations_and_rules(tx, config)?;

    // Populate identity affiliations
    populate_identity_affiliations(tx, config)?;

    // Materialize commit org attribution
    materialize_commit_org_attribution(tx)?;

    // Materialize trailer org attribution
    materialize_trailer_org_attribution(tx, config)?;

    Ok(())
}

fn load_organizations_and_rules(tx: &Connection, config: &Config) -> Result<()> {
    // Insert org names from all config sources
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
        "INSERT INTO org_domain_rules (org_id, domain)
         VALUES ((SELECT id FROM organizations WHERE name = ?1), ?2)",
    )?;

    let mut domain_count = 0usize;
    for org in &config.identity.orgs {
        insert_org.execute(rusqlite::params![org.org])?;
        for domain in org.all_domains() {
            insert_rule.execute(rusqlite::params![org.org, domain])?;
            domain_count += 1;
        }
    }

    println!("Loaded {} org domain rules", domain_count);
    Ok(())
}

fn populate_identity_affiliations(tx: &Connection, config: &Config) -> Result<()> {
    // From org_domain_rules: match each identity's emails against domain rules
    tx.execute_batch(
        "INSERT INTO identity_affiliations (identity_id, org_id, source)
         SELECT DISTINCT ie.identity_id, odr.org_id, 'domain_rule'
         FROM identity_emails ie
         JOIN org_domain_rules odr
           ON (ie.email LIKE '%@' || odr.domain
               OR ie.email LIKE '%.' || odr.domain
               OR (INSTR(ie.email, '@') = 0 AND odr.domain = '(bare)'));",
    )?;

    // From config org_overrides
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

    // From config aliases with org
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

fn materialize_commit_org_attribution(tx: &Connection) -> Result<()> {
    // Simple domain match: raw commit email → org_domain_rules (no temporal bounds)
    tx.execute_batch(
        "INSERT OR IGNORE INTO commit_org_attribution (commit_hash, org_id, org_name, source, matched_email, matched_domain)
         SELECT c.hash, odr.org_id, o.name, 'email_domain', c.author_email, odr.domain
         FROM commits c
         JOIN org_domain_rules odr
           ON (c.author_email LIKE '%@' || odr.domain
               OR (INSTR(c.author_email, '@') = 0 AND odr.domain = '(bare)'))
         JOIN organizations o ON o.id = odr.org_id;",
    )?;

    // Fallback: identity affiliation for commits not yet attributed
    tx.execute_batch(
        "INSERT OR IGNORE INTO commit_org_attribution (commit_hash, org_id, org_name, source, matched_email, matched_domain)
         SELECT c.hash, ia.org_id, o.name, 'affiliation', NULL, NULL
         FROM commits c
         JOIN identity_affiliations ia ON ia.id = (
             SELECT ia2.id FROM identity_affiliations ia2
             WHERE ia2.identity_id = c.author_id
             ORDER BY
               CASE ia2.source WHEN 'alias_override' THEN 0 WHEN 'org_override' THEN 1 ELSE 2 END
             LIMIT 1
         )
         JOIN organizations o ON o.id = ia.org_id
         WHERE c.author_id IS NOT NULL
           AND c.hash NOT IN (SELECT commit_hash FROM commit_org_attribution);",
    )?;

    // Fallback: preferred email domain for remaining unattributed commits
    tx.execute_batch(
        "INSERT OR IGNORE INTO commit_org_attribution (commit_hash, org_id, org_name, source, matched_email, matched_domain)
         SELECT c.hash, odr.org_id, o.name, 'preferred_email_domain', ie.email, odr.domain
         FROM commits c
         JOIN identity_emails ie ON ie.identity_id = c.author_id AND ie.is_preferred = 1
         JOIN org_domain_rules odr
           ON (ie.email LIKE '%@' || odr.domain
               OR ie.email LIKE '%.' || odr.domain)
         JOIN organizations o ON o.id = odr.org_id
         WHERE c.author_id IS NOT NULL
           AND c.hash NOT IN (SELECT commit_hash FROM commit_org_attribution);",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM commit_org_attribution", [], |r| r.get(0))?;
    println!("Materialized org attribution for {} commits", count);

    Ok(())
}

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

    // Simple domain match: parsed trailer email → org_domain_rules
    let sql1 = format!(
        "INSERT OR IGNORE INTO trailer_org_attribution (commit_hash, key, seq, org_id, org_name, source, matched_email, matched_domain)
         SELECT t.commit_hash, t.key, t.seq, odr.org_id, o.name, 'parsed_email_domain', t.parsed_email, odr.domain
         FROM trailers t
         JOIN org_domain_rules odr
           ON (t.parsed_email LIKE '%@' || odr.domain
               OR t.parsed_email LIKE '%.' || odr.domain
               OR (INSTR(t.parsed_email, '@') = 0 AND odr.domain = '(bare)'))
         JOIN organizations o ON o.id = odr.org_id
         WHERE t.key IN ({placeholders})
           AND t.parsed_email IS NOT NULL;"
    );
    let mut stmt = tx.prepare(&sql1)?;
    stmt.execute(params.as_slice())?;
    drop(stmt);

    // Fallback: identity affiliation for resolved trailer identities
    let sql2 = format!(
        "INSERT OR IGNORE INTO trailer_org_attribution (commit_hash, key, seq, org_id, org_name, source, matched_email, matched_domain)
         SELECT t.commit_hash, t.key, t.seq, ia.org_id, o.name, 'affiliation', NULL, NULL
         FROM trailers t
         JOIN identity_affiliations ia ON ia.id = (
             SELECT ia2.id FROM identity_affiliations ia2
             WHERE ia2.identity_id = t.identity_id
             ORDER BY
               CASE ia2.source WHEN 'alias_override' THEN 0 WHEN 'org_override' THEN 1 ELSE 2 END
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

    // Fallback: preferred email domain for remaining unattributed trailers
    let sql3 = format!(
        "INSERT OR IGNORE INTO trailer_org_attribution (commit_hash, key, seq, org_id, org_name, source, matched_email, matched_domain)
         SELECT t.commit_hash, t.key, t.seq, odr.org_id, o.name, 'preferred_email_domain', ie.email, odr.domain
         FROM trailers t
         JOIN identity_emails ie ON ie.identity_id = t.identity_id AND ie.is_preferred = 1
         JOIN org_domain_rules odr
           ON (ie.email LIKE '%@' || odr.domain
               OR ie.email LIKE '%.' || odr.domain)
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

// ── Clear tables ─────────────────────────────────────────────────────────────

fn clear_identity_tables(tx: &Connection) -> Result<()> {
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
    Ok(())
}

// ── Main entry point ────────────────────────────────────────────────────────

pub fn run_identity(repo_path: &Path, conn: &Connection, config: &Config) -> Result<()> {
    let (mailmap, alias_overrides) = load_resolution_sources(repo_path, config)?;

    // Phase 1: Resolve & merge (in-memory)
    let merged = resolve_and_merge_identities(conn, config, &mailmap, &alias_overrides)?;

    let tx = conn.unchecked_transaction()?;
    clear_identity_tables(&tx)?;

    // Phase 2: Persist
    persist_identities(&tx, &merged, config, &mailmap, &alias_overrides)?;

    // Phase 3: Org attribution
    materialize_org_attribution(&tx, config)?;

    tx.commit()?;

    Ok(())
}
