use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use axum::http::StatusCode;
use chrono::{Datelike, Timelike};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde_json::{json, Value};

use logacy_db::schema::{
    blame_hunks, commit_files, commit_org_attribution, commit_releases, commits, file_ownership,
    file_subsystems, identities, identity_affiliations, identity_aliases, identity_emails,
    organizations, subsystems, tags, trailers,
    v_commits, v_identity_org, v_reviews,
};

use crate::Params;

fn db_error<E: std::fmt::Display>(e: E) -> StatusCode {
    tracing::error!("diesel: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn month_key(date: &str) -> String {
    date.get(..7).unwrap_or(date).to_string()
}

fn year_key(date: &str) -> String {
    date.get(..4).unwrap_or(date).to_string()
}

fn top_dir(path: &str) -> String {
    path.split_once('/')
        .map(|(dir, _)| dir.to_string())
        .unwrap_or_else(|| "(root)".to_string())
}

fn lower(s: &str) -> String {
    s.to_ascii_lowercase()
}

fn contains_ci(haystack: &str, needle: &str) -> bool {
    haystack.to_ascii_lowercase().contains(needle)
}

fn parse_dow_hour(date: &str) -> Option<(u32, u32)> {
    let dt = chrono::DateTime::parse_from_rfc3339(date).ok()?;
    Some((dt.weekday().num_days_from_sunday(), dt.hour()))
}

fn parse_i32_id(id: i64) -> Result<i32, StatusCode> {
    i32::try_from(id).map_err(|_| StatusCode::BAD_REQUEST)
}

fn latest_snapshot(conn: &mut SqliteConnection) -> Result<Option<i32>, StatusCode> {
    use logacy_db::schema::blame_snapshots::dsl::*;

    blame_snapshots
        .select(id)
        .order(id.desc())
        .first::<i32>(conn)
        .optional()
        .map_err(db_error)
}

/// Consolidated identity display info: preferred (most recent) email and latest org per identity.
/// All code that displays an identity's email or org should use this instead of
/// canonical_email or the v_identity_org view directly.
struct IdentityDisplayInfo {
    preferred_emails: HashMap<i32, String>,
    latest_orgs: HashMap<i32, String>,
}

impl IdentityDisplayInfo {
    fn load(conn: &mut SqliteConnection) -> Result<Self, StatusCode> {
        let preferred_emails = identity_emails::table
            .filter(identity_emails::is_preferred.eq(1))
            .select((identity_emails::identity_id, identity_emails::email))
            .load::<(i32, String)>(conn)
            .map_err(db_error)?
            .into_iter()
            .collect();

        // Latest org: from most recent commit's org attribution per identity,
        // falling back to v_identity_org view.
        let mut latest_orgs = HashMap::<i32, String>::new();

        // First, populate fallback from v_identity_org
        for (id, org) in v_identity_org::table
            .select((v_identity_org::identity_id, v_identity_org::org))
            .load::<(i32, Option<String>)>(conn)
            .map_err(db_error)?
        {
            if let Some(org) = org {
                latest_orgs.insert(id, org);
            }
        }

        // Then override with most-recent-commit org attribution
        let rows = commit_org_attribution::table
            .inner_join(commits::table.on(commits::hash.eq(commit_org_attribution::commit_hash)))
            .filter(commits::author_id.is_not_null())
            .filter(commit_org_attribution::org_name.is_not_null())
            .select((commits::author_id, commit_org_attribution::org_name, commits::author_date))
            .load::<(Option<i32>, Option<String>, String)>(conn)
            .map_err(db_error)?;
        let mut best_dates: HashMap<i32, String> = HashMap::new();
        for (author_id, org_name, date) in rows {
            if let (Some(id), Some(org)) = (author_id, org_name) {
                let is_newer = best_dates.get(&id).map_or(true, |d| date > *d);
                if is_newer {
                    best_dates.insert(id, date);
                    latest_orgs.insert(id, org);
                }
            }
        }

        Ok(Self { preferred_emails, latest_orgs })
    }

    /// Get the display email for an identity (preferred/most-recent, or fallback).
    fn email(&self, id: i32, fallback: &str) -> String {
        self.preferred_emails.get(&id).cloned().unwrap_or_else(|| fallback.to_string())
    }

    /// Get the display org for an identity.
    fn org(&self, id: i32) -> String {
        self.latest_orgs.get(&id).cloned().unwrap_or_default()
    }

    /// Get the display org with a custom default.
    fn org_or(&self, id: i32, default: &str) -> String {
        self.latest_orgs.get(&id).cloned().unwrap_or_else(|| default.to_string())
    }
}

/// Load identity IDs belonging to a given org.
fn load_org_member_ids(
    conn: &mut SqliteConnection,
    org: &str,
) -> Result<Vec<i32>, StatusCode> {
    let org_id: Option<i32> = organizations::table
        .filter(organizations::name.eq(org))
        .select(organizations::id)
        .first(conn)
        .optional()
        .map_err(db_error)?;

    let Some(org_id) = org_id else {
        return Ok(Vec::new());
    };

    identity_affiliations::table
        .filter(identity_affiliations::org_id.eq(org_id))
        .select(identity_affiliations::identity_id)
        .distinct()
        .load::<i32>(conn)
        .map_err(db_error)
}

pub fn status(
    conn: &mut SqliteConnection,
    github_base: Option<&str>,
    ticket_url: Option<&str>,
) -> Result<Value, StatusCode> {
    let commits_count = commits::table
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;
    let ids = identities::table
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;
    let subs = subsystems::table
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;

    let min_date = if commits_count > 0 {
        commits::table
            .select(commits::author_date)
            .order(commits::author_date.asc())
            .first::<String>(conn)
            .optional()
            .map_err(db_error)?
            .unwrap_or_default()
    } else {
        String::new()
    };
    let max_date = if commits_count > 0 {
        commits::table
            .select(commits::author_date)
            .order(commits::author_date.desc())
            .first::<String>(conn)
            .optional()
            .map_err(db_error)?
            .unwrap_or_default()
    } else {
        String::new()
    };

    let tag_count = tags::table
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;

    Ok(json!({
        "commits": commits_count,
        "identities": ids,
        "subsystems": subs,
        "tags": tag_count,
        "min_date": min_date,
        "max_date": max_date,
        "github_base": github_base,
        "ticket_url": ticket_url,
    }))
}

pub fn timeline(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = v_commits::table
        .filter(v_commits::author_is_bot.eq(Some(0)))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(v_commits::author_date.lt(until));
    }

    let rows = query
        .select(v_commits::author_date)
        .load::<String>(conn)
        .map_err(db_error)?;
    let mut grouped = BTreeMap::<String, i64>::new();
    for date in rows {
        *grouped.entry(month_key(&date)).or_default() += 1;
    }
    Ok(json!(
        grouped
            .into_iter()
            .map(|(month, commits)| json!({ "month": month, "commits": commits }))
            .collect::<Vec<_>>()
    ))
}

pub fn contributors(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = v_commits::table
        .filter(v_commits::author_is_bot.eq(Some(0)))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(v_commits::author_date.lt(until));
    }

    let rows = query
        .select((
            v_commits::resolved_author_name,
            v_commits::author_org,
            v_commits::insertions,
            v_commits::deletions,
            v_commits::author_date,
        ))
        .load::<(String, Option<String>, Option<i32>, Option<i32>, String)>(conn)
        .map_err(db_error)?;

    #[derive(Default)]
    struct Agg {
        commits: i64,
        insertions: i64,
        deletions: i64,
        org: String,
        org_date: String,
        first_commit: String,
        last_commit: String,
    }

    let mut grouped = HashMap::<String, Agg>::new();
    for (author, org, insertions, deletions, author_date) in rows {
        let entry = grouped.entry(author).or_default();
        entry.commits += 1;
        entry.insertions += i64::from(insertions.unwrap_or(0));
        entry.deletions += i64::from(deletions.unwrap_or(0));
        if entry.first_commit.is_empty() || author_date < entry.first_commit {
            entry.first_commit = author_date.clone();
        }
        if entry.last_commit.is_empty() || author_date > entry.last_commit {
            entry.last_commit = author_date.clone();
        }
        if author_date >= entry.org_date {
            entry.org_date = author_date;
            entry.org = org.unwrap_or_else(|| "Unknown".to_string());
        }
    }

    let mut rows = grouped
        .into_iter()
        .map(|(author, agg)| {
            json!({
                "author": author,
                "commits": agg.commits,
                "insertions": agg.insertions,
                "deletions": agg.deletions,
                "org": agg.org,
                "first_commit": agg.first_commit,
                "last_commit": agg.last_commit,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_commits = a["commits"].as_i64().unwrap_or_default();
        let b_commits = b["commits"].as_i64().unwrap_or_default();
        b_commits
            .cmp(&a_commits)
            .then_with(|| a["author"].as_str().cmp(&b["author"].as_str()))
    });
    rows.truncate(params.limit.unwrap_or(25) as usize);
    Ok(json!(rows))
}

pub fn orgs(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = v_commits::table
        .filter(v_commits::author_is_bot.eq(Some(0)))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(v_commits::author_date.lt(until));
    }
    let rows = query
        .select((v_commits::author_date, v_commits::author_org))
        .load::<(String, Option<String>)>(conn)
        .map_err(db_error)?;

    let mut grouped = BTreeMap::<(String, String), i64>::new();
    for (author_date, org) in rows {
        *grouped
            .entry((month_key(&author_date), org.unwrap_or_else(|| "Unknown".to_string())))
            .or_default() += 1;
    }

    Ok(json!(
        grouped
            .into_iter()
            .map(|((month, org), commits)| json!({ "month": month, "org": org, "commits": commits }))
            .collect::<Vec<_>>()
    ))
}

pub fn languages(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = commit_files::table.inner_join(commits::table).into_boxed();
    query = query.filter(commit_files::language.ne("Other"));
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }

    let rows = query
        .select((
            commit_files::language,
            commit_files::insertions,
            commit_files::deletions,
        ))
        .load::<(String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?;

    let mut grouped = HashMap::<String, (i64, i64, i64)>::new();
    for (language, insertions, deletions) in rows {
        let entry = grouped.entry(language).or_default();
        entry.0 += 1;
        entry.1 += i64::from(insertions.unwrap_or(0));
        entry.2 += i64::from(deletions.unwrap_or(0));
    }
    let mut rows = grouped
        .into_iter()
        .map(|(language, (file_changes, insertions, deletions))| {
            json!({
                "language": language,
                "file_changes": file_changes,
                "insertions": insertions,
                "deletions": deletions,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_changes = a["file_changes"].as_i64().unwrap_or_default();
        let b_changes = b["file_changes"].as_i64().unwrap_or_default();
        b_changes
            .cmp(&a_changes)
            .then_with(|| a["language"].as_str().cmp(&b["language"].as_str()))
    });
    rows.truncate(20);
    Ok(json!(rows))
}

pub fn heatmap(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = v_commits::table
        .filter(v_commits::author_is_bot.eq(Some(0)))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(v_commits::author_date.lt(until));
    }
    let rows = query
        .select(v_commits::author_date)
        .load::<String>(conn)
        .map_err(db_error)?;

    let mut grouped = BTreeMap::<(u32, u32), i64>::new();
    for date in rows {
        if let Some((dow, hour)) = parse_dow_hour(&date) {
            *grouped.entry((dow, hour)).or_default() += 1;
        }
    }
    Ok(json!(
        grouped
            .into_iter()
            .map(|((dow, hour), commits)| json!({ "dow": dow, "hour": hour, "commits": commits }))
            .collect::<Vec<_>>()
    ))
}

pub fn subsystems(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = subsystems::table
        .inner_join(file_subsystems::table.on(file_subsystems::subsystem_id.eq(subsystems::id)))
        .inner_join(commit_files::table.on(commit_files::path.eq(file_subsystems::path)))
        .inner_join(commits::table.on(commits::hash.eq(commit_files::commit_hash)))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }

    let rows = query
        .select((subsystems::name, commit_files::commit_hash, commits::author_id))
        .load::<(String, String, Option<i32>)>(conn)
        .map_err(db_error)?;

    let mut grouped = HashMap::<String, (HashSet<String>, HashSet<i32>)>::new();
    for (subsystem, commit_hash, author_id) in rows {
        let entry = grouped.entry(subsystem).or_default();
        entry.0.insert(commit_hash);
        if let Some(author_id) = author_id {
            entry.1.insert(author_id);
        }
    }
    let mut rows = grouped
        .into_iter()
        .map(|(subsystem, (commits, contributors))| {
            json!({
                "subsystem": subsystem,
                "commits": commits.len() as i64,
                "contributors": contributors.len() as i64,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_commits = a["commits"].as_i64().unwrap_or_default();
        let b_commits = b["commits"].as_i64().unwrap_or_default();
        b_commits
            .cmp(&a_commits)
            .then_with(|| a["subsystem"].as_str().cmp(&b["subsystem"].as_str()))
    });
    Ok(json!(rows))
}

pub fn reviews(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = v_reviews::table.into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(v_reviews::author_date.lt(until));
    }
    let rows = query
        .select((
            v_reviews::author,
            v_reviews::reviewer,
            v_reviews::author_org,
            v_reviews::reviewer_org,
            v_reviews::author_date,
        ))
        .load::<(String, String, Option<String>, Option<String>, String)>(conn)
        .map_err(db_error)?;

    #[derive(Default)]
    struct Agg {
        reviews: i64,
        author_org: String,
        reviewer_org: String,
        last_date: String,
    }
    let mut grouped = HashMap::<(String, String), Agg>::new();
    for (author, reviewer, author_org, reviewer_org, author_date) in rows {
        let entry = grouped.entry((author, reviewer)).or_default();
        entry.reviews += 1;
        if author_date >= entry.last_date {
            entry.last_date = author_date;
            entry.author_org = author_org.unwrap_or_default();
            entry.reviewer_org = reviewer_org.unwrap_or_default();
        }
    }
    let mut rows = grouped
        .into_iter()
        .map(|((author, reviewer), agg)| {
            json!({
                "author": author,
                "reviewer": reviewer,
                "reviews": agg.reviews,
                "author_org": agg.author_org,
                "reviewer_org": agg.reviewer_org,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_reviews = a["reviews"].as_i64().unwrap_or_default();
        let b_reviews = b["reviews"].as_i64().unwrap_or_default();
        b_reviews
            .cmp(&a_reviews)
            .then_with(|| a["author"].as_str().cmp(&b["author"].as_str()))
    });
    rows.truncate(50);
    Ok(json!(rows))
}

pub fn ownership(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let display = IdentityDisplayInfo::load(conn)?;

    let rows = file_ownership::table
        .inner_join(identities::table)
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .filter(identities::is_bot.eq(0))
        .select((
            identities::id,
            identities::canonical_name,
            file_ownership::lines_owned,
        ))
        .load::<(i32, String, i32)>(conn)
        .map_err(db_error)?;

    let mut grouped = HashMap::<i32, (String, String, i64)>::new();
    for (id, author, lines_owned) in rows {
        let org = display.org_or(id, "Unknown");
        let entry = grouped.entry(id).or_insert_with(|| (author, org, 0));
        entry.2 += i64::from(lines_owned);
    }
    let mut rows = grouped
        .into_values()
        .map(|(author, org, lines)| json!({ "author": author, "org": org, "lines": lines }))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_lines = a["lines"].as_i64().unwrap_or_default();
        let b_lines = b["lines"].as_i64().unwrap_or_default();
        b_lines
            .cmp(&a_lines)
            .then_with(|| a["author"].as_str().cmp(&b["author"].as_str()))
    });
    rows.truncate(30);
    Ok(json!(rows))
}

pub fn commits_list(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = v_commits::table
        .filter(v_commits::author_is_bot.eq(Some(0)))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(v_commits::author_date.lt(until));
    }
    let search_pattern = params.search.as_ref().map(|q| format!("%{}%", q));
    if let Some(ref pattern) = search_pattern {
        query = query.filter(
            v_commits::subject
                .like(pattern.clone())
                .or(v_commits::resolved_author_name.like(pattern.clone()))
                .or(v_commits::ticket.like(pattern.clone())),
        );
    }
    let rows = query
        .order(v_commits::author_date.desc())
        .offset(params.offset.unwrap_or(0) as i64)
        .limit(params.limit.unwrap_or(50).min(200) as i64)
        .select((
            v_commits::hash,
            v_commits::resolved_author_name,
            v_commits::author_org,
            v_commits::author_date,
            v_commits::subject,
            v_commits::ticket,
            v_commits::component,
            v_commits::insertions,
            v_commits::deletions,
        ))
        .load::<(
            String,
            String,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<i32>,
            Option<i32>,
        )>(conn)
        .map_err(db_error)?;
    let filtered = rows
        .into_iter()
        .map(
            |(hash, author, org, author_date, subject, ticket, component, insertions, deletions)| {
                json!({
                    "hash": hash,
                    "author": author,
                    "org": org.unwrap_or_default(),
                    "author_date": author_date,
                    "subject": subject,
                    "ticket": ticket,
                    "component": component,
                    "insertions": insertions.unwrap_or(0),
                    "deletions": deletions.unwrap_or(0),
                })
            },
        )
        .collect::<Vec<_>>();
    Ok(json!(filtered))
}

pub fn contributor_detail(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let name = params.name.as_deref().ok_or(StatusCode::BAD_REQUEST)?;

    let mut commit_query = v_commits::table
        .filter(v_commits::resolved_author_name.eq(name))
        .into_boxed();
    if let Some(ref since) = params.since {
        commit_query = commit_query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        commit_query = commit_query.filter(v_commits::author_date.lt(until));
    }
    let commit_dates = commit_query
        .select(v_commits::author_date)
        .load::<String>(conn)
        .map_err(db_error)?;

    let mut timeline = BTreeMap::<String, i64>::new();
    for date in commit_dates {
        *timeline.entry(month_key(&date)).or_default() += 1;
    }

    let mut lang_query = commit_files::table
        .inner_join(v_commits::table.on(v_commits::hash.eq(commit_files::commit_hash)))
        .filter(v_commits::resolved_author_name.eq(name))
        .filter(commit_files::language.ne("Other"))
        .into_boxed();
    if let Some(ref since) = params.since {
        lang_query = lang_query.filter(v_commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        lang_query = lang_query.filter(v_commits::author_date.lt(until));
    }
    let mut languages = HashMap::<String, i64>::new();
    for (language,) in lang_query
        .select((commit_files::language,))
        .load::<(String,)>(conn)
        .map_err(db_error)?
    {
        *languages.entry(language).or_default() += 1;
    }
    let mut languages = languages
        .into_iter()
        .map(|(language, file_changes)| json!({ "language": language, "file_changes": file_changes }))
        .collect::<Vec<_>>();
    languages.sort_by(|a, b| {
        let a_count = a["file_changes"].as_i64().unwrap_or_default();
        let b_count = b["file_changes"].as_i64().unwrap_or_default();
        b_count.cmp(&a_count)
    });
    languages.truncate(10);

    let mut rg_query = v_reviews::table.filter(v_reviews::reviewer.eq(name)).into_boxed();
    if let Some(ref since) = params.since {
        rg_query = rg_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        rg_query = rg_query.filter(v_reviews::author_date.lt(until));
    }
    let mut reviews_given_map = HashMap::<String, i64>::new();
    for (author,) in rg_query
        .select((v_reviews::author,))
        .load::<(String,)>(conn)
        .map_err(db_error)?
    {
        *reviews_given_map.entry(author).or_default() += 1;
    }
    let mut reviews_given = reviews_given_map
        .into_iter()
        .map(|(author, reviews)| json!({ "author": author, "reviews": reviews }))
        .collect::<Vec<_>>();
    reviews_given.sort_by(|a, b| b["reviews"].as_i64().cmp(&a["reviews"].as_i64()));
    reviews_given.truncate(10);

    let mut rr_query = v_reviews::table.filter(v_reviews::author.eq(name)).into_boxed();
    if let Some(ref since) = params.since {
        rr_query = rr_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        rr_query = rr_query.filter(v_reviews::author_date.lt(until));
    }
    let mut reviews_received_map = HashMap::<String, i64>::new();
    for (reviewer,) in rr_query
        .select((v_reviews::reviewer,))
        .load::<(String,)>(conn)
        .map_err(db_error)?
    {
        *reviews_received_map.entry(reviewer).or_default() += 1;
    }
    let mut reviews_received = reviews_received_map
        .into_iter()
        .map(|(reviewer, reviews)| json!({ "reviewer": reviewer, "reviews": reviews }))
        .collect::<Vec<_>>();
    reviews_received.sort_by(|a, b| b["reviews"].as_i64().cmp(&a["reviews"].as_i64()));
    reviews_received.truncate(10);

    Ok(json!({
        "timeline": timeline.into_iter().map(|(month, commits)| json!({ "month": month, "commits": commits })).collect::<Vec<_>>(),
        "languages": languages,
        "reviews_given": reviews_given,
        "reviews_received": reviews_received,
    }))
}

pub fn identities_summary(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let raw_aliases = identity_aliases::table
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;

    let display = IdentityDisplayInfo::load(conn)?;

    let mut commit_query = commits::table.inner_join(identities::table).into_boxed();
    if let Some(ref since) = params.since {
        commit_query = commit_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        commit_query = commit_query.filter(commits::author_date.lt(until));
    }
    let author_rows = commit_query
        .select((commits::author_id, identities::is_bot))
        .load::<(Option<i32>, i32)>(conn)
        .map_err(db_error)?;

    let mut identities_set = HashSet::new();
    let mut bots_set = HashSet::new();
    let mut with_org_set = HashSet::new();
    for (author_id, is_bot) in author_rows {
        if let Some(author_id) = author_id {
            identities_set.insert(author_id);
            if is_bot == 1 {
                bots_set.insert(author_id);
            } else if display.latest_orgs.contains_key(&author_id) {
                with_org_set.insert(author_id);
            }
        }
    }

    let mut trailer_query = trailers::table
        .inner_join(commits::table.on(commits::hash.eq(trailers::commit_hash)))
        .filter(trailers::key.eq_any(vec![
            "Signed-off-by",
            "Reviewed-by",
            "Tested-by",
            "Acked-by",
        ]))
        .into_boxed();
    if let Some(ref since) = params.since {
        trailer_query = trailer_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        trailer_query = trailer_query.filter(commits::author_date.lt(until));
    }
    let trailer_rows = trailer_query
        .select(trailers::identity_id)
        .load::<Option<i32>>(conn)
        .map_err(db_error)?;
    let total_trailers = trailer_rows.len() as i64;
    let resolved_trailers = trailer_rows.into_iter().filter(|id| id.is_some()).count() as i64;

    Ok(json!({
        "raw_aliases": raw_aliases,
        "identities": identities_set.len() as i64,
        "bots": bots_set.len() as i64,
        "with_org": with_org_set.len() as i64,
        "without_org": identities_set.len() as i64 - bots_set.len() as i64 - with_org_set.len() as i64,
        "resolved_trailers": resolved_trailers,
        "total_trailers": total_trailers,
        "trailer_resolution_pct": if total_trailers > 0 {
            (resolved_trailers as f64 / total_trailers as f64 * 100.0).round()
        } else {
            0.0
        }
    }))
}

pub fn identities_list(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let search = params.search.as_deref().map(lower);
    let display = IdentityDisplayInfo::load(conn)?;

    let identities_rows = identities::table
        .select((
            identities::id,
            identities::canonical_name,
            identities::canonical_email,
            identities::is_bot,
        ))
        .load::<(i32, String, String, i32)>(conn)
        .map_err(db_error)?;

    let alias_rows = identity_aliases::table
        .select((identity_aliases::identity_id, identity_aliases::email))
        .load::<(i32, String)>(conn)
        .map_err(db_error)?;
    let mut aliases = HashMap::<i32, Vec<String>>::new();
    for (identity_id, email) in alias_rows {
        aliases.entry(identity_id).or_default().push(email);
    }
    for values in aliases.values_mut() {
        values.sort();
    }

    let mut commit_query = commits::table.into_boxed();
    if let Some(ref since) = params.since {
        commit_query = commit_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        commit_query = commit_query.filter(commits::author_date.lt(until));
    }
    let mut commit_counts = HashMap::<i32, i64>::new();
    for author_id in commit_query
        .select(commits::author_id)
        .load::<Option<i32>>(conn)
        .map_err(db_error)?
        .into_iter()
        .flatten()
    {
        *commit_counts.entry(author_id).or_default() += 1;
    }

    let mut review_query = trailers::table
        .inner_join(commits::table.on(commits::hash.eq(trailers::commit_hash)))
        .filter(trailers::key.eq("Reviewed-by"))
        .into_boxed();
    if let Some(ref since) = params.since {
        review_query = review_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        review_query = review_query.filter(commits::author_date.lt(until));
    }
    let mut review_counts = HashMap::<i32, i64>::new();
    for identity_id in review_query
        .select(trailers::identity_id)
        .load::<Option<i32>>(conn)
        .map_err(db_error)?
        .into_iter()
        .flatten()
    {
        *review_counts.entry(identity_id).or_default() += 1;
    }

    let mut rows = identities_rows
        .into_iter()
        .filter(|(id, name, email, _)| {
            if let Some(ref search) = search {
                let org = display.org(*id);
                contains_ci(name, search)
                    || contains_ci(email, search)
                    || contains_ci(&org, search)
            } else {
                true
            }
        })
        .map(|(id, name, email, bot)| {
            let display_email = display.email(id, &email);
            let org = display.org(id);
            let alias_emails = aliases.get(&id).cloned().unwrap_or_default();
            json!({
                "id": id,
                "name": name,
                "email": display_email,
                "org": org,
                "bot": bot,
                "aliases": alias_emails.len(),
                "alias_emails": alias_emails.join(", "),
                "commits": commit_counts.get(&id).copied().unwrap_or_default(),
                "reviews": review_counts.get(&id).copied().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_commits = a["commits"].as_i64().unwrap_or_default();
        let b_commits = b["commits"].as_i64().unwrap_or_default();
        let a_reviews = a["reviews"].as_i64().unwrap_or_default();
        let b_reviews = b["reviews"].as_i64().unwrap_or_default();
        b_commits
            .cmp(&a_commits)
            .then_with(|| b_reviews.cmp(&a_reviews))
            .then_with(|| a["name"].as_str().cmp(&b["name"].as_str()))
    });
    rows.truncate(params.limit.unwrap_or(200) as usize);
    Ok(json!(rows))
}

pub fn identities_orgs(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let display = IdentityDisplayInfo::load(conn)?;

    let mut query = commits::table.inner_join(identities::table).into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }
    let rows = query
        .select((identities::id, identities::is_bot))
        .load::<(i32, i32)>(conn)
        .map_err(db_error)?;

    let mut grouped = HashMap::<String, (HashSet<i32>, HashSet<i32>)>::new();
    for (id, is_bot) in rows {
        let org = display.org_or(id, "Unaffiliated");
        let entry = grouped.entry(org).or_default();
        entry.0.insert(id);
        if is_bot == 0 {
            entry.1.insert(id);
        }
    }
    let mut rows = grouped
        .into_iter()
        .map(|(org, (identities, humans))| {
            json!({
                "org": org,
                "identities": identities.len() as i64,
                "humans": humans.len() as i64,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["identities"].as_i64().cmp(&a["identities"].as_i64()));
    Ok(json!(rows))
}

pub fn identities_alias_dist(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let rows = identity_aliases::table
        .select(identity_aliases::identity_id)
        .load::<i32>(conn)
        .map_err(db_error)?;
    let mut alias_counts = HashMap::<i32, i64>::new();
    for identity_id in rows {
        *alias_counts.entry(identity_id).or_default() += 1;
    }
    let mut grouped = BTreeMap::<i64, i64>::new();
    for alias_count in alias_counts.into_values() {
        *grouped.entry(alias_count).or_default() += 1;
    }
    Ok(json!(
        grouped
            .into_iter()
            .map(|(alias_count, identities)| json!({ "alias_count": alias_count, "identities": identities }))
            .collect::<Vec<_>>()
    ))
}

pub fn identities_unresolved(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = trailers::table
        .inner_join(commits::table.on(commits::hash.eq(trailers::commit_hash)))
        .filter(trailers::key.eq_any(vec![
            "Signed-off-by",
            "Reviewed-by",
            "Tested-by",
            "Acked-by",
        ]))
        .filter(trailers::identity_id.is_null())
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }
    let rows = query
        .select((trailers::key, trailers::value))
        .load::<(String, String)>(conn)
        .map_err(db_error)?;

    let mut grouped = HashMap::<(String, String), i64>::new();
    for row in rows {
        *grouped.entry(row).or_default() += 1;
    }
    let mut rows = grouped
        .into_iter()
        .map(|((key, value), occurrences)| {
            json!({ "key": key, "value": value, "occurrences": occurrences })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["occurrences"].as_i64().cmp(&a["occurrences"].as_i64()));
    rows.truncate(50);
    Ok(json!(rows))
}

pub fn identities_bots(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let display = IdentityDisplayInfo::load(conn)?;
    let bots = identities::table
        .filter(identities::is_bot.eq(1))
        .select((identities::id, identities::canonical_name, identities::canonical_email))
        .load::<(i32, String, String)>(conn)
        .map_err(db_error)?;
    let bot_ids = bots.iter().map(|(id, _, _)| Some(*id)).collect::<Vec<_>>();

    let mut commit_query = commits::table.into_boxed();
    if !bot_ids.is_empty() {
        commit_query = commit_query.filter(commits::author_id.eq_any(bot_ids.clone()));
    }
    if let Some(ref since) = params.since {
        commit_query = commit_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        commit_query = commit_query.filter(commits::author_date.lt(until));
    }
    let mut commit_counts = HashMap::<i32, i64>::new();
    for author_id in commit_query
        .select(commits::author_id)
        .load::<Option<i32>>(conn)
        .map_err(db_error)?
        .into_iter()
        .flatten()
    {
        *commit_counts.entry(author_id).or_default() += 1;
    }

    let mut trailer_query = trailers::table
        .inner_join(commits::table.on(commits::hash.eq(trailers::commit_hash)))
        .into_boxed();
    if !bot_ids.is_empty() {
        trailer_query = trailer_query.filter(trailers::identity_id.eq_any(bot_ids));
    }
    if let Some(ref since) = params.since {
        trailer_query = trailer_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        trailer_query = trailer_query.filter(commits::author_date.lt(until));
    }
    let mut trailer_counts = HashMap::<i32, i64>::new();
    for identity_id in trailer_query
        .select(trailers::identity_id)
        .load::<Option<i32>>(conn)
        .map_err(db_error)?
        .into_iter()
        .flatten()
    {
        *trailer_counts.entry(identity_id).or_default() += 1;
    }

    let mut rows = bots
        .into_iter()
        .map(|(id, name, email)| {
            json!({
                "name": name,
                "email": display.email(id, &email),
                "commits": commit_counts.get(&id).copied().unwrap_or_default(),
                "trailer_mentions": trailer_counts.get(&id).copied().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["commits"].as_i64().cmp(&a["commits"].as_i64()));
    Ok(json!(rows))
}

pub fn identities_multi_alias(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let display = IdentityDisplayInfo::load(conn)?;

    let humans = identities::table
        .filter(identities::is_bot.eq(0))
        .select((
            identities::id,
            identities::canonical_name,
            identities::canonical_email,
        ))
        .load::<(i32, String, String)>(conn)
        .map_err(db_error)?;
    let alias_rows = identity_aliases::table
        .select((identity_aliases::identity_id, identity_aliases::email))
        .load::<(i32, String)>(conn)
        .map_err(db_error)?;
    let mut aliases = HashMap::<i32, Vec<String>>::new();
    for (identity_id, email) in alias_rows {
        aliases.entry(identity_id).or_default().push(email);
    }
    for emails in aliases.values_mut() {
        emails.sort();
    }

    let mut rows = humans
        .into_iter()
        .filter_map(|(id, name, email)| {
            let alias_emails = aliases.remove(&id).unwrap_or_default();
            if alias_emails.len() <= 1 {
                return None;
            }
            let org = display.org(id);
            Some(json!({
                "name": name,
                "email": display.email(id, &email),
                "org": org,
                "alias_count": alias_emails.len() as i64,
                "all_emails": alias_emails.join(", "),
            }))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["alias_count"].as_i64().cmp(&a["alias_count"].as_i64()));
    rows.truncate(50);
    Ok(json!(rows))
}

pub fn identity_profile(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let raw_id = params.id.ok_or(StatusCode::BAD_REQUEST)?;
    let id = parse_i32_id(raw_id)?;

    let info = identities::table
        .filter(identities::id.eq(id))
        .select((
            identities::id,
            identities::canonical_name,
            identities::canonical_email,
            identities::is_bot,
        ))
        .first::<(i32, String, String, i32)>(conn)
        .optional()
        .map_err(db_error)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let canonical_name = info.1.clone();
    let display = IdentityDisplayInfo::load(conn)?;
    let display_email = display.email(id, &info.2);
    let identity_org = display.org(id);

    let aliases = identity_aliases::table
        .filter(identity_aliases::identity_id.eq(id))
        .order(identity_aliases::email.asc())
        .select((identity_aliases::name, identity_aliases::email))
        .load::<(Option<String>, String)>(conn)
        .map_err(db_error)?;

    let emails = identity_emails::table
        .filter(identity_emails::identity_id.eq(id))
        .order(identity_emails::last_seen_at.desc())
        .select((
            identity_emails::email,
            identity_emails::first_seen_at,
            identity_emails::last_seen_at,
            identity_emails::commit_count,
            identity_emails::trailer_count,
        ))
        .load::<(String, Option<String>, Option<String>, i32, i32)>(conn)
        .map_err(db_error)?;

    // Org history: all orgs this identity has been associated with, with date ranges
    let org_history = commit_org_attribution::table
        .inner_join(commits::table.on(commits::hash.eq(commit_org_attribution::commit_hash)))
        .filter(commits::author_id.eq(Some(id)))
        .filter(commit_org_attribution::org_name.is_not_null())
        .select((
            commit_org_attribution::org_name,
            commits::commit_date,
        ))
        .load::<(Option<String>, String)>(conn)
        .map_err(db_error)?;
    let mut org_history_map: HashMap<String, (String, String, i64)> = HashMap::new();
    for (org_name, date) in org_history {
        if let Some(org) = org_name {
            let entry = org_history_map
                .entry(org)
                .or_insert_with(|| (date.clone(), date.clone(), 0));
            if date < entry.0 {
                entry.0 = date.clone();
            }
            if date > entry.1 {
                entry.1 = date.clone();
            }
            entry.2 += 1;
        }
    }
    let mut org_history_rows: Vec<_> = org_history_map
        .into_iter()
        .map(|(org, (first, last, commits))| json!({
            "org": org,
            "first_seen": first,
            "last_seen": last,
            "commits": commits,
        }))
        .collect();
    org_history_rows.sort_by(|a, b| b["last_seen"].as_str().cmp(&a["last_seen"].as_str()));

    let mut commit_query = commits::table.filter(commits::author_id.eq(Some(id))).into_boxed();
    if let Some(ref since) = params.since {
        commit_query = commit_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        commit_query = commit_query.filter(commits::author_date.lt(until));
    }
    let commit_rows = commit_query
        .select((
            commits::hash,
            commits::author_date,
            commits::subject,
            commits::ticket,
            commits::component,
            commits::insertions,
            commits::deletions,
        ))
        .load::<(
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<i32>,
            Option<i32>,
        )>(conn)
        .map_err(db_error)?;

    let mut stats = json!({
        "commits": commit_rows.len() as i64,
        "insertions": 0i64,
        "deletions": 0i64,
        "first_commit": "",
        "last_commit": "",
        "reviews_given": 0i64,
        "reviews_received": 0i64,
        "signoffs": 0i64,
        "tested_by": 0i64,
        "acked_by": 0i64,
        "lines_owned": 0i64,
    });
    let mut timeline = BTreeMap::<String, i64>::new();
    let mut yearly = BTreeMap::<String, (i64, i64, i64)>::new();
    let mut recent_commits = commit_rows
        .iter()
        .map(|(hash, author_date, subject, ticket, component, insertions, deletions)| {
            json!({
                "hash": hash,
                "author_date": author_date,
                "subject": subject,
                "ticket": ticket,
                "component": component,
                "insertions": insertions.unwrap_or(0),
                "deletions": deletions.unwrap_or(0),
            })
        })
        .collect::<Vec<_>>();
    recent_commits.sort_by(|a, b| b["author_date"].as_str().cmp(&a["author_date"].as_str()));
    recent_commits.truncate(20);

    let mut heatmap = BTreeMap::<(u32, u32), i64>::new();
    for (_, author_date, _, _, _, insertions, deletions) in &commit_rows {
        *timeline.entry(month_key(author_date)).or_default() += 1;
        let entry = yearly.entry(year_key(author_date)).or_default();
        entry.0 += 1;
        entry.1 += i64::from(insertions.unwrap_or(0));
        entry.2 += i64::from(deletions.unwrap_or(0));
        if let Some((dow, hour)) = parse_dow_hour(author_date) {
            *heatmap.entry((dow, hour)).or_default() += 1;
        }
        stats["insertions"] = json!(stats["insertions"].as_i64().unwrap_or_default() + i64::from(insertions.unwrap_or(0)));
        stats["deletions"] = json!(stats["deletions"].as_i64().unwrap_or_default() + i64::from(deletions.unwrap_or(0)));
        if stats["first_commit"].as_str().unwrap_or("").is_empty()
            || author_date.as_str() < stats["first_commit"].as_str().unwrap_or("")
        {
            stats["first_commit"] = json!(author_date);
        }
        if stats["last_commit"].as_str().unwrap_or("").is_empty()
            || author_date.as_str() > stats["last_commit"].as_str().unwrap_or("")
        {
            stats["last_commit"] = json!(author_date);
        }
    }

    let mut lang_query = commit_files::table
        .inner_join(commits::table)
        .filter(commits::author_id.eq(Some(id)))
        .filter(commit_files::language.ne("Other"))
        .into_boxed();
    if let Some(ref since) = params.since {
        lang_query = lang_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        lang_query = lang_query.filter(commits::author_date.lt(until));
    }
    let mut languages_map = HashMap::<String, (i64, i64, i64)>::new();
    for (language, insertions, deletions) in lang_query
        .select((
            commit_files::language,
            commit_files::insertions,
            commit_files::deletions,
        ))
        .load::<(String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?
    {
        let entry = languages_map.entry(language).or_default();
        entry.0 += 1;
        entry.1 += i64::from(insertions.unwrap_or(0));
        entry.2 += i64::from(deletions.unwrap_or(0));
    }
    let mut languages = languages_map
        .into_iter()
        .map(|(language, (file_changes, insertions, deletions))| {
            json!({
                "language": language,
                "file_changes": file_changes,
                "insertions": insertions,
                "deletions": deletions,
            })
        })
        .collect::<Vec<_>>();
    languages.sort_by(|a, b| b["file_changes"].as_i64().cmp(&a["file_changes"].as_i64()));
    languages.truncate(15);

    let mut cat_query = commit_files::table
        .inner_join(commits::table)
        .filter(commits::author_id.eq(Some(id)))
        .into_boxed();
    if let Some(ref since) = params.since {
        cat_query = cat_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        cat_query = cat_query.filter(commits::author_date.lt(until));
    }
    let mut categories_map = HashMap::<String, (i64, i64)>::new();
    for (category, insertions, deletions) in cat_query
        .select((
            commit_files::category,
            commit_files::insertions,
            commit_files::deletions,
        ))
        .load::<(String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?
    {
        let entry = categories_map.entry(category).or_default();
        entry.0 += 1;
        entry.1 += i64::from(insertions.unwrap_or(0)) + i64::from(deletions.unwrap_or(0));
    }
    let mut categories = categories_map
        .into_iter()
        .map(|(category, (file_changes, lines_changed))| {
            json!({ "category": category, "file_changes": file_changes, "lines_changed": lines_changed })
        })
        .collect::<Vec<_>>();
    categories.sort_by(|a, b| b["lines_changed"].as_i64().cmp(&a["lines_changed"].as_i64()));

    let mut subsystem_query = commits::table
        .inner_join(commit_files::table)
        .inner_join(file_subsystems::table.on(file_subsystems::path.eq(commit_files::path)))
        .inner_join(subsystems::table.on(subsystems::id.eq(file_subsystems::subsystem_id)))
        .filter(commits::author_id.eq(Some(id)))
        .into_boxed();
    if let Some(ref since) = params.since {
        subsystem_query = subsystem_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        subsystem_query = subsystem_query.filter(commits::author_date.lt(until));
    }
    let subsystem_rows = subsystem_query
        .select((
            subsystems::name,
            commit_files::commit_hash,
            commit_files::insertions,
            commit_files::deletions,
        ))
        .load::<(String, String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?;
    let mut subsystems_map = HashMap::<String, (HashSet<String>, i64, i64)>::new();
    for (subsystem, commit_hash, insertions, deletions) in subsystem_rows {
        let entry = subsystems_map.entry(subsystem).or_default();
        entry.0.insert(commit_hash);
        entry.1 += i64::from(insertions.unwrap_or(0));
        entry.2 += i64::from(deletions.unwrap_or(0));
    }
    let mut subsystems = subsystems_map
        .into_iter()
        .map(|(subsystem, (commit_hashes, insertions, deletions))| {
            json!({
                "subsystem": subsystem,
                "commits": commit_hashes.len() as i64,
                "insertions": insertions,
                "deletions": deletions,
            })
        })
        .collect::<Vec<_>>();
    subsystems.sort_by(|a, b| b["commits"].as_i64().cmp(&a["commits"].as_i64()));

    let files_owned = if let Some(snapshot_id) = latest_snapshot(conn)? {
        let mut rows = file_ownership::table
            .filter(file_ownership::snapshot_id.eq(snapshot_id))
            .filter(file_ownership::identity_id.eq(id))
            .select((
                file_ownership::path,
                file_ownership::lines_owned,
                file_ownership::fraction,
            ))
            .load::<(String, i32, f64)>(conn)
            .map_err(db_error)?
            .into_iter()
            .map(|(path, lines_owned, fraction)| {
                json!({ "path": path, "lines_owned": lines_owned, "fraction": fraction })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| b["lines_owned"].as_i64().cmp(&a["lines_owned"].as_i64()));
        rows.truncate(25);
        let lines_owned: i64 = rows
            .iter()
            .map(|row| row["lines_owned"].as_i64().unwrap_or_default())
            .sum();
        stats["lines_owned"] = json!(lines_owned);
        rows
    } else {
        Vec::new()
    };

    let mut reviewed_by_query = trailers::table
        .inner_join(commits::table)
        .filter(trailers::key.eq("Reviewed-by"))
        .filter(trailers::identity_id.eq(Some(id)))
        .into_boxed();
    if let Some(ref since) = params.since {
        reviewed_by_query = reviewed_by_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        reviewed_by_query = reviewed_by_query.filter(commits::author_date.lt(until));
    }
    let reviews_given_count = reviewed_by_query
        .select(trailers::commit_hash)
        .load::<String>(conn)
        .map_err(db_error)?
        .len() as i64;
    stats["reviews_given"] = json!(reviews_given_count);

    let mut reviews_received_query = trailers::table
        .inner_join(commits::table)
        .filter(trailers::key.eq("Reviewed-by"))
        .filter(commits::author_id.eq(Some(id)))
        .into_boxed();
    if let Some(ref since) = params.since {
        reviews_received_query = reviews_received_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        reviews_received_query = reviews_received_query.filter(commits::author_date.lt(until));
    }
    stats["reviews_received"] = json!(
        reviews_received_query
            .select(trailers::commit_hash)
            .load::<String>(conn)
            .map_err(db_error)?
            .len() as i64
    );

    for (key_name, out_field) in [
        ("Signed-off-by", "signoffs"),
        ("Tested-by", "tested_by"),
        ("Acked-by", "acked_by"),
    ] {
        let mut query = trailers::table
            .inner_join(commits::table)
            .filter(trailers::key.eq(key_name))
            .filter(trailers::identity_id.eq(Some(id)))
            .into_boxed();
        if let Some(ref since) = params.since {
            query = query.filter(commits::author_date.ge(since));
        }
        if let Some(ref until) = params.until {
            query = query.filter(commits::author_date.lt(until));
        }
        let count = query
            .select(trailers::commit_hash)
            .load::<String>(conn)
            .map_err(db_error)?
            .len() as i64;
        stats[out_field] = json!(count);
    }

    let mut review_given_query = v_reviews::table
        .filter(v_reviews::reviewer.eq(&canonical_name))
        .into_boxed();
    if let Some(ref since) = params.since {
        review_given_query = review_given_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        review_given_query = review_given_query.filter(v_reviews::author_date.lt(until));
    }
    let mut reviews_given_counts = HashMap::<String, i64>::new();
    for author in review_given_query
        .select(v_reviews::author)
        .load::<String>(conn)
        .map_err(db_error)?
    {
        *reviews_given_counts.entry(author).or_default() += 1;
    }
    let mut reviews_given = reviews_given_counts
        .into_iter()
        .map(|(author, reviews)| {
            json!({ "author": author, "reviews": reviews })
        })
        .collect::<Vec<_>>();
    reviews_given.sort_by(|a, b| b["reviews"].as_i64().cmp(&a["reviews"].as_i64()));
    reviews_given.truncate(15);

    let mut review_received_query = v_reviews::table
        .filter(v_reviews::author.eq(&canonical_name))
        .into_boxed();
    if let Some(ref since) = params.since {
        review_received_query = review_received_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        review_received_query = review_received_query.filter(v_reviews::author_date.lt(until));
    }
    let mut reviews_received_counts = HashMap::<String, i64>::new();
    for reviewer in review_received_query
        .select(v_reviews::reviewer)
        .load::<String>(conn)
        .map_err(db_error)?
    {
        *reviews_received_counts.entry(reviewer).or_default() += 1;
    }
    let mut reviews_received = reviews_received_counts
        .into_iter()
        .map(|(reviewer, reviews)| {
            json!({ "reviewer": reviewer, "reviews": reviews })
        })
        .collect::<Vec<_>>();
    reviews_received.sort_by(|a, b| b["reviews"].as_i64().cmp(&a["reviews"].as_i64()));
    reviews_received.truncate(15);

    let own_paths = commit_files::table
        .inner_join(commits::table)
        .filter(commits::author_id.eq(Some(id)))
        .into_boxed();
    let mut own_paths = if let Some(ref since) = params.since {
        own_paths.filter(commits::author_date.ge(since))
    } else {
        own_paths
    };
    if let Some(ref until) = params.until {
        own_paths = own_paths.filter(commits::author_date.lt(until));
    }
    let own_paths = own_paths
        .select(commit_files::path)
        .load::<String>(conn)
        .map_err(db_error)?
        .into_iter()
        .collect::<HashSet<_>>();
    let collaborators = if own_paths.is_empty() {
        Vec::new()
    } else {
        let rows = commit_files::table
            .inner_join(commits::table)
            .inner_join(identities::table.on(identities::id.nullable().eq(commits::author_id)))
            .filter(commit_files::path.eq_any(own_paths.into_iter().collect::<Vec<_>>()))
            .filter(commits::author_id.ne(Some(id)))
            .filter(identities::is_bot.eq(0))
            .select((
                identities::id,
                identities::canonical_name,
                commit_files::commit_hash,
            ))
            .load::<(i32, String, String)>(conn)
            .map_err(db_error)?;
        let mut grouped = HashMap::<i32, (String, String, HashSet<String>)>::new();
        for (other_id, collaborator, commit_hash) in rows {
            let org = display.org(other_id);
            let entry = grouped
                .entry(other_id)
                .or_insert_with(|| (collaborator, org, HashSet::new()));
            entry.2.insert(commit_hash);
        }
        let mut rows = grouped
            .into_values()
            .map(|(collaborator, org, commits)| {
                json!({
                    "collaborator": collaborator,
                    "org": org,
                    "shared_file_commits": commits.len() as i64,
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b["shared_file_commits"]
                .as_i64()
                .cmp(&a["shared_file_commits"].as_i64())
        });
        rows.truncate(15);
        rows
    };

    Ok(json!({
        "info": {
            "id": info.0,
            "name": info.1,
            "email": display_email,
            "org": identity_org,
            "bot": info.3,
        },
        "aliases": aliases.into_iter().map(|(name, email)| json!({ "name": name, "email": email })).collect::<Vec<_>>(),
        "emails": emails.into_iter().map(|(email, first_seen, last_seen, commit_count, trailer_count)| json!({
            "email": email,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "commit_count": commit_count,
            "trailer_count": trailer_count,
        })).collect::<Vec<_>>(),
        "org_history": org_history_rows,
        "stats": stats,
        "timeline": timeline.into_iter().map(|(month, commits)| json!({ "month": month, "commits": commits })).collect::<Vec<_>>(),
        "languages": languages,
        "categories": categories,
        "subsystems": subsystems,
        "files_owned": files_owned,
        "reviews_given": reviews_given,
        "reviews_received": reviews_received,
        "collaborators": collaborators,
        "recent_commits": recent_commits,
        "heatmap": heatmap.into_iter().map(|((dow, hour), commits)| json!({ "dow": dow, "hour": hour, "commits": commits })).collect::<Vec<_>>(),
        "yearly": yearly.into_iter().map(|(year, (commits, insertions, deletions))| json!({ "year": year, "commits": commits, "insertions": insertions, "deletions": deletions })).collect::<Vec<_>>(),
    }))
}

fn language_map_for_paths(
    conn: &mut SqliteConnection,
    paths: &[String],
) -> Result<HashMap<String, String>, StatusCode> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = commit_files::table
        .filter(commit_files::path.eq_any(paths))
        .select((commit_files::path, commit_files::language))
        .load::<(String, String)>(conn)
        .map_err(db_error)?;
    let mut map = HashMap::new();
    for (path, language) in rows {
        map.insert(path, language);
    }
    Ok(map)
}

pub fn files_age(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    // Load hunks with their commit dates, then aggregate line_count by year in Rust
    let mut query = blame_hunks::table
        .inner_join(commits::table.on(commits::hash.eq(blame_hunks::orig_commit)))
        .filter(blame_hunks::snapshot_id.eq(snapshot_id))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }
    let rows = query
        .select((commits::author_date, blame_hunks::line_count))
        .load::<(String, i32)>(conn)
        .map_err(db_error)?;

    let mut grouped = BTreeMap::<String, i64>::new();
    for (author_date, line_count) in rows {
        *grouped.entry(year_key(&author_date)).or_default() += line_count as i64;
    }
    Ok(json!(
        grouped.into_iter()
            .map(|(year, lines)| json!({ "year": year, "lines": lines }))
            .collect::<Vec<_>>()
    ))
}

pub fn files_concentration(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let rows = file_ownership::table
        .inner_join(identities::table)
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .filter(identities::is_bot.eq(0))
        .select((
            identities::id,
            identities::canonical_name,
            file_ownership::lines_owned,
        ))
        .load::<(i32, String, i32)>(conn)
        .map_err(db_error)?;
    let mut grouped = HashMap::<i32, (String, i64)>::new();
    for (id, author, lines_owned) in rows {
        let entry = grouped.entry(id).or_insert_with(|| (author, 0));
        entry.1 += i64::from(lines_owned);
    }
    let mut rows = grouped
        .into_values()
        .map(|(author, lines)| json!({ "author": author, "lines": lines }))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["lines"].as_i64().cmp(&a["lines"].as_i64()));
    Ok(json!(rows))
}

pub fn files_hotspots(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = commit_files::table
        .inner_join(commits::table)
        .filter(commits::first_parent.eq(1))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }
    let rows = query
        .select((commit_files::path, commit_files::language))
        .load::<(String, String)>(conn)
        .map_err(db_error)?;
    let mut grouped = HashMap::<String, (String, i64)>::new();
    for (path, language) in rows {
        let entry = grouped.entry(path).or_insert_with(|| (language, 0));
        entry.1 += 1;
    }
    let mut rows = grouped
        .into_iter()
        .map(|(path, (language, commits))| json!({ "path": path, "language": language, "commits": commits }))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["commits"].as_i64().cmp(&a["commits"].as_i64()));
    rows.truncate(30);
    Ok(json!(rows))
}

pub fn files_largest(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let rows = file_ownership::table
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .select((file_ownership::path, file_ownership::lines_owned))
        .load::<(String, i32)>(conn)
        .map_err(db_error)?;
    let mut grouped = HashMap::<String, i64>::new();
    for (path, lines_owned) in rows {
        *grouped.entry(path).or_default() += i64::from(lines_owned);
    }
    let mut paths = grouped.keys().cloned().collect::<Vec<_>>();
    paths.sort();
    let language_map = language_map_for_paths(conn, &paths)?;
    let mut rows = grouped
        .into_iter()
        .map(|(path, lines)| {
            json!({
                "path": path,
                "lines": lines,
                "language": language_map.get(&path).cloned().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["lines"].as_i64().cmp(&a["lines"].as_i64()));
    rows.truncate(30);
    Ok(json!(rows))
}

pub fn files_silos(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let raw_rows = file_ownership::table
        .inner_join(identities::table)
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .filter(file_ownership::fraction.gt(0.8))
        .filter(file_ownership::lines_owned.ge(50))
        .select((
            file_ownership::path,
            identities::canonical_name,
            file_ownership::lines_owned,
            file_ownership::fraction,
        ))
        .load::<(String, String, i32, f64)>(conn)
        .map_err(db_error)?;
    let mut paths = raw_rows
        .iter()
        .map(|(path, _, _, _)| path.clone())
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    let language_map = language_map_for_paths(conn, &paths)?;
    let mut rows = raw_rows
        .into_iter()
        .map(|(path, owner, lines, fraction)| {
            json!({
                "path": path,
                "owner": owner,
                "lines": lines,
                "pct": (fraction * 1000.0).round() / 10.0,
                "language": language_map.get(&path).cloned().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["lines"].as_i64().cmp(&a["lines"].as_i64()));
    rows.truncate(50);
    Ok(json!(rows))
}

pub fn files_detail(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let rows = file_ownership::table
        .inner_join(identities::table)
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .select((
            file_ownership::path,
            identities::canonical_name,
            file_ownership::lines_owned,
            file_ownership::fraction,
        ))
        .load::<(String, String, i32, f64)>(conn)
        .map_err(db_error)?;

    #[derive(Default)]
    struct Agg {
        total_lines: i64,
        primary_owner: String,
        primary_lines: i64,
        top_pct: f64,
        bus_factor: i64,
    }
    let mut grouped = HashMap::<String, Agg>::new();
    for (path, owner, lines_owned, fraction) in rows {
        let entry = grouped.entry(path).or_default();
        entry.total_lines += i64::from(lines_owned);
        if i64::from(lines_owned) > entry.primary_lines {
            entry.primary_lines = i64::from(lines_owned);
            entry.primary_owner = owner;
        }
        if fraction > entry.top_pct {
            entry.top_pct = fraction;
        }
        if fraction >= 0.05 {
            entry.bus_factor += 1;
        }
    }
    let mut paths = grouped.keys().cloned().collect::<Vec<_>>();
    paths.sort();
    let language_map = language_map_for_paths(conn, &paths)?;
    let mut rows = grouped
        .into_iter()
        .map(|(path, agg)| {
            json!({
                "path": path,
                "primary_owner": agg.primary_owner,
                "top_pct": (agg.top_pct * 1000.0).round() / 10.0,
                "bus_factor": agg.bus_factor,
                "lines": agg.total_lines,
                "language": language_map.get(&path).cloned().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["lines"].as_i64().cmp(&a["lines"].as_i64()));
    rows.truncate(50);
    Ok(json!(rows))
}

pub fn files_dir_ownership(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let rows = file_ownership::table
        .inner_join(identities::table)
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .filter(identities::is_bot.eq(0))
        .select((
            file_ownership::path,
            identities::canonical_name,
            file_ownership::lines_owned,
        ))
        .load::<(String, String, i32)>(conn)
        .map_err(db_error)?;
    let mut grouped = HashMap::<(String, String), i64>::new();
    for (path, author, lines_owned) in rows {
        *grouped.entry((top_dir(&path), author)).or_default() += i64::from(lines_owned);
    }
    let mut rows = grouped
        .into_iter()
        .map(|((directory, author), lines)| {
            json!({ "directory": directory, "author": author, "lines": lines })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        a["directory"]
            .as_str()
            .cmp(&b["directory"].as_str())
            .then_with(|| b["lines"].as_i64().cmp(&a["lines"].as_i64()))
    });
    Ok(json!(rows))
}

pub fn files_dir_churn(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let mut query = commit_files::table
        .inner_join(commits::table)
        .filter(commits::first_parent.eq(1))
        .into_boxed();
    if let Some(ref since) = params.since {
        query = query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        query = query.filter(commits::author_date.lt(until));
    }
    let rows = query
        .select(commit_files::path)
        .load::<String>(conn)
        .map_err(db_error)?;
    let mut grouped = HashMap::<String, i64>::new();
    for path in rows {
        *grouped.entry(top_dir(&path)).or_default() += 1;
    }
    let mut rows = grouped
        .into_iter()
        .map(|(directory, commits)| json!({ "directory": directory, "commits": commits }))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["commits"].as_i64().cmp(&a["commits"].as_i64()));
    Ok(json!(rows))
}

pub fn files_dir_busfactor(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let Some(snapshot_id) = latest_snapshot(conn)? else {
        return Err(StatusCode::NOT_FOUND);
    };
    let rows = file_ownership::table
        .inner_join(identities::table)
        .filter(file_ownership::snapshot_id.eq(snapshot_id))
        .filter(identities::is_bot.eq(0))
        .select((
            file_ownership::path,
            file_ownership::identity_id,
            identities::canonical_name,
            file_ownership::lines_owned,
        ))
        .load::<(String, i32, String, i32)>(conn)
        .map_err(db_error)?;
    let mut per_author = HashMap::<(String, i32), (String, i64)>::new();
    for (path, identity_id, canonical_name, lines_owned) in rows {
        let directory = top_dir(&path);
        let entry = per_author
            .entry((directory, identity_id))
            .or_insert_with(|| (canonical_name, 0));
        entry.1 += i64::from(lines_owned);
    }
    #[derive(Default)]
    struct Agg {
        total_lines: i64,
        contributors: i64,
        primary_owner: String,
        primary_lines: i64,
    }
    let mut grouped = HashMap::<String, Agg>::new();
    for ((directory, _), (canonical_name, author_lines)) in per_author {
        let entry = grouped.entry(directory).or_default();
        entry.total_lines += author_lines;
        entry.contributors += 1;
        if author_lines > entry.primary_lines {
            entry.primary_lines = author_lines;
            entry.primary_owner = canonical_name;
        }
    }
    let mut rows = grouped
        .into_iter()
        .map(|(directory, agg)| {
            let top_pct = if agg.total_lines > 0 {
                (agg.primary_lines as f64 * 1000.0 / agg.total_lines as f64).round() / 10.0
            } else {
                0.0
            };
            json!({
                "directory": directory,
                "total_lines": agg.total_lines,
                "contributors": agg.contributors,
                "primary_owner": agg.primary_owner,
                "top_pct": top_pct,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b["total_lines"].as_i64().cmp(&a["total_lines"].as_i64()));
    Ok(json!(rows))
}

pub fn org_profile(conn: &mut SqliteConnection, params: &Params) -> Result<Value, StatusCode> {
    let org = params.org.as_deref().ok_or(StatusCode::BAD_REQUEST)?;
    let org_value = org.to_string();

    let display = IdentityDisplayInfo::load(conn)?;
    let member_ids = load_org_member_ids(conn, org)?;
    if member_ids.is_empty() {
        return Ok(json!({
            "org": org,
            "members": [],
            "stats": { "commits": 0, "insertions": 0, "deletions": 0, "contributors": 0, "first_commit": "", "last_commit": "" },
            "timeline": [],
            "languages": [],
            "subsystems": [],
            "ownership": [],
            "total_lines_owned": 0,
            "reviews_given": [],
            "reviews_received": [],
            "internal_reviews": 0,
            "recent_commits": [],
        }));
    }

    let members = identities::table
        .filter(identities::id.eq_any(&member_ids))
        .select((
            identities::id,
            identities::canonical_name,
            identities::canonical_email,
            identities::is_bot,
        ))
        .load::<(i32, String, String, i32)>(conn)
        .map_err(db_error)?;
    let member_opt_ids = member_ids.iter().map(|id| Some(*id)).collect::<Vec<_>>();

    let mut member_commit_query = commits::table
        .filter(commits::author_id.eq_any(&member_opt_ids))
        .into_boxed();
    if let Some(ref since) = params.since {
        member_commit_query = member_commit_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        member_commit_query = member_commit_query.filter(commits::author_date.lt(until));
    }
    let commit_rows = member_commit_query
        .select((
            commits::author_id,
            commits::hash,
            commits::author_date,
            commits::insertions,
            commits::deletions,
        ))
        .load::<(Option<i32>, String, String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?;

    let mut per_member = HashMap::<i32, (i64, i64, i64)>::new();
    let mut timeline = BTreeMap::<String, i64>::new();
    let mut stats_commits = BTreeSet::<String>::new();
    let mut contributors = HashSet::<i32>::new();
    let mut stats_insertions = 0i64;
    let mut stats_deletions = 0i64;
    let mut first_commit = String::new();
    let mut last_commit = String::new();
    for (author_id, hash, author_date, insertions, deletions) in &commit_rows {
        if let Some(member_id) = author_id {
            let entry = per_member.entry(*member_id).or_default();
            entry.0 += 1;
            entry.1 += i64::from(insertions.unwrap_or(0));
            entry.2 += i64::from(deletions.unwrap_or(0));
            contributors.insert(*member_id);
        }
        *timeline.entry(month_key(author_date)).or_default() += 1;
        stats_commits.insert(hash.clone());
        stats_insertions += i64::from(insertions.unwrap_or(0));
        stats_deletions += i64::from(deletions.unwrap_or(0));
        if first_commit.is_empty() || author_date < &first_commit {
            first_commit = author_date.clone();
        }
        if last_commit.is_empty() || author_date > &last_commit {
            last_commit = author_date.clone();
        }
    }

    let mut review_query = trailers::table
        .inner_join(commits::table.on(commits::hash.eq(trailers::commit_hash)))
        .filter(trailers::key.eq("Reviewed-by"))
        .filter(trailers::identity_id.eq_any(&member_opt_ids))
        .into_boxed();
    if let Some(ref since) = params.since {
        review_query = review_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        review_query = review_query.filter(commits::author_date.lt(until));
    }
    let mut review_counts = HashMap::<i32, i64>::new();
    for identity_id in review_query
        .select(trailers::identity_id)
        .load::<Option<i32>>(conn)
        .map_err(db_error)?
        .into_iter()
        .flatten()
    {
        *review_counts.entry(identity_id).or_default() += 1;
    }

    let mut members_json = members
        .into_iter()
        .map(|(id, name, email, bot)| {
            let stats = per_member.get(&id).copied().unwrap_or_default();
            json!({
                "id": id,
                "name": name,
                "email": display.email(id, &email),
                "bot": bot,
                "commits": stats.0,
                "insertions": stats.1,
                "deletions": stats.2,
                "reviews": review_counts.get(&id).copied().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    members_json.sort_by(|a, b| b["commits"].as_i64().cmp(&a["commits"].as_i64()));

    let mut languages_query = commit_files::table
        .inner_join(commits::table)
        .filter(commits::author_id.eq_any(&member_opt_ids))
        .filter(commit_files::language.ne("Other"))
        .into_boxed();
    if let Some(ref since) = params.since {
        languages_query = languages_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        languages_query = languages_query.filter(commits::author_date.lt(until));
    }
    let mut languages_map = HashMap::<String, (i64, i64, i64)>::new();
    for (language, insertions, deletions) in languages_query
        .select((
            commit_files::language,
            commit_files::insertions,
            commit_files::deletions,
        ))
        .load::<(String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?
    {
        let entry = languages_map.entry(language).or_default();
        entry.0 += 1;
        entry.1 += i64::from(insertions.unwrap_or(0));
        entry.2 += i64::from(deletions.unwrap_or(0));
    }
    let mut languages = languages_map
        .into_iter()
        .map(|(language, (file_changes, insertions, deletions))| {
            json!({ "language": language, "file_changes": file_changes, "insertions": insertions, "deletions": deletions })
        })
        .collect::<Vec<_>>();
    languages.sort_by(|a, b| b["file_changes"].as_i64().cmp(&a["file_changes"].as_i64()));
    languages.truncate(15);

    let mut subsystem_query = commits::table
        .inner_join(commit_files::table)
        .inner_join(file_subsystems::table.on(file_subsystems::path.eq(commit_files::path)))
        .inner_join(subsystems::table.on(subsystems::id.eq(file_subsystems::subsystem_id)))
        .filter(commits::author_id.eq_any(&member_opt_ids))
        .into_boxed();
    if let Some(ref since) = params.since {
        subsystem_query = subsystem_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        subsystem_query = subsystem_query.filter(commits::author_date.lt(until));
    }
    let subsystem_rows = subsystem_query
        .select((subsystems::name, commit_files::commit_hash, commits::author_id))
        .load::<(String, String, Option<i32>)>(conn)
        .map_err(db_error)?;
    let mut subsystem_map = HashMap::<String, (HashSet<String>, HashSet<i32>)>::new();
    for (subsystem, commit_hash, author_id) in subsystem_rows {
        let entry = subsystem_map.entry(subsystem).or_default();
        entry.0.insert(commit_hash);
        if let Some(author_id) = author_id {
            entry.1.insert(author_id);
        }
    }
    let mut subsystems_json = subsystem_map
        .into_iter()
        .map(|(subsystem, (commits, contributors))| {
            json!({ "subsystem": subsystem, "commits": commits.len() as i64, "contributors": contributors.len() as i64 })
        })
        .collect::<Vec<_>>();
    subsystems_json.sort_by(|a, b| b["commits"].as_i64().cmp(&a["commits"].as_i64()));

    let ownership = if let Some(snapshot_id) = latest_snapshot(conn)? {
        let rows = file_ownership::table
            .inner_join(identities::table)
            .filter(file_ownership::snapshot_id.eq(snapshot_id))
            .filter(identities::id.eq_any(&member_ids))
            .filter(identities::is_bot.eq(0))
            .select((identities::id, identities::canonical_name, file_ownership::lines_owned))
            .load::<(i32, String, i32)>(conn)
            .map_err(db_error)?;
        let mut grouped = HashMap::<i32, (String, i64)>::new();
        for (id, author, lines_owned) in rows {
            let entry = grouped.entry(id).or_insert_with(|| (author, 0));
            entry.1 += i64::from(lines_owned);
        }
        let mut rows = grouped
            .into_values()
            .map(|(author, lines)| json!({ "author": author, "lines": lines }))
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| b["lines"].as_i64().cmp(&a["lines"].as_i64()));
        rows
    } else {
        Vec::new()
    };
    let total_lines_owned: i64 = ownership
        .iter()
        .map(|row| row["lines"].as_i64().unwrap_or_default())
        .sum();

    let mut review_given_query = v_reviews::table
        .filter(v_reviews::reviewer_org.eq(Some(org_value.clone())))
        .filter(v_reviews::author_org.ne(Some(org_value.clone())).or(v_reviews::author_org.is_null()))
        .into_boxed();
    if let Some(ref since) = params.since {
        review_given_query = review_given_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        review_given_query = review_given_query.filter(v_reviews::author_date.lt(until));
    }
    let review_given_rows = review_given_query
        .select(v_reviews::author_org)
        .load::<Option<String>>(conn)
        .map_err(db_error)?;
    let mut reviews_given_map = HashMap::<String, i64>::new();
    for author_org in review_given_rows {
        *reviews_given_map
            .entry(author_org.unwrap_or_else(|| "Unknown".to_string()))
            .or_default() += 1;
    }

    let mut review_received_query = v_reviews::table
        .filter(v_reviews::author_org.eq(Some(org_value.clone())))
        .filter(v_reviews::reviewer_org.ne(Some(org_value.clone())).or(v_reviews::reviewer_org.is_null()))
        .into_boxed();
    if let Some(ref since) = params.since {
        review_received_query = review_received_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        review_received_query = review_received_query.filter(v_reviews::author_date.lt(until));
    }
    let review_received_rows = review_received_query
        .select(v_reviews::reviewer_org)
        .load::<Option<String>>(conn)
        .map_err(db_error)?;
    let mut reviews_received_map = HashMap::<String, i64>::new();
    for reviewer_org in review_received_rows {
        *reviews_received_map
            .entry(reviewer_org.unwrap_or_else(|| "Unknown".to_string()))
            .or_default() += 1;
    }
    let mut internal_reviews_query = v_reviews::table
        .filter(v_reviews::author_org.eq(Some(org_value.clone())))
        .filter(v_reviews::reviewer_org.eq(Some(org_value)))
        .into_boxed();
    if let Some(ref since) = params.since {
        internal_reviews_query = internal_reviews_query.filter(v_reviews::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        internal_reviews_query = internal_reviews_query.filter(v_reviews::author_date.lt(until));
    }
    let internal_reviews = internal_reviews_query
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;
    let mut reviews_given = reviews_given_map
        .into_iter()
        .map(|(to_org, reviews)| json!({ "to_org": to_org, "reviews": reviews }))
        .collect::<Vec<_>>();
    reviews_given.sort_by(|a, b| b["reviews"].as_i64().cmp(&a["reviews"].as_i64()));
    reviews_given.truncate(15);
    let mut reviews_received = reviews_received_map
        .into_iter()
        .map(|(from_org, reviews)| json!({ "from_org": from_org, "reviews": reviews }))
        .collect::<Vec<_>>();
    reviews_received.sort_by(|a, b| b["reviews"].as_i64().cmp(&a["reviews"].as_i64()));
    reviews_received.truncate(15);

    let mut recent_query = commits::table
        .inner_join(identities::table.on(identities::id.nullable().eq(commits::author_id)))
        .filter(identities::id.eq_any(&member_ids))
        .into_boxed();
    if let Some(ref since) = params.since {
        recent_query = recent_query.filter(commits::author_date.ge(since));
    }
    if let Some(ref until) = params.until {
        recent_query = recent_query.filter(commits::author_date.lt(until));
    }
    let mut recent_commits = recent_query
        .select((
            commits::hash,
            identities::canonical_name,
            commits::author_date,
            commits::subject,
            commits::ticket,
            commits::insertions,
            commits::deletions,
        ))
        .load::<(
            String,
            String,
            String,
            String,
            Option<String>,
            Option<i32>,
            Option<i32>,
        )>(conn)
        .map_err(db_error)?
        .into_iter()
        .map(|(hash, author, author_date, subject, ticket, insertions, deletions)| {
            json!({
                "hash": hash,
                "author": author,
                "author_date": author_date,
                "subject": subject,
                "ticket": ticket,
                "insertions": insertions.unwrap_or(0),
                "deletions": deletions.unwrap_or(0),
            })
        })
        .collect::<Vec<_>>();
    recent_commits.sort_by(|a, b| b["author_date"].as_str().cmp(&a["author_date"].as_str()));
    recent_commits.truncate(30);

    Ok(json!({
        "org": org,
        "members": members_json,
        "stats": {
            "commits": stats_commits.len() as i64,
            "insertions": stats_insertions,
            "deletions": stats_deletions,
            "contributors": contributors.len() as i64,
            "first_commit": first_commit,
            "last_commit": last_commit,
        },
        "timeline": timeline.into_iter().map(|(month, commits)| json!({ "month": month, "commits": commits })).collect::<Vec<_>>(),
        "languages": languages,
        "subsystems": subsystems_json,
        "ownership": ownership,
        "total_lines_owned": total_lines_owned,
        "reviews_given": reviews_given,
        "reviews_received": reviews_received,
        "internal_reviews": internal_reviews,
        "recent_commits": recent_commits,
    }))
}

// ── Releases ─────────────────────────────────────────────────────────────────

pub fn releases_summary(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    let tag_count = tags::table.count().get_result::<i64>(conn).map_err(db_error)?;
    let annotated = tags::table
        .filter(tags::is_annotated.eq(1))
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;
    let mapped = commit_releases::table
        .count()
        .get_result::<i64>(conn)
        .map_err(db_error)?;

    Ok(json!({
        "tags": tag_count,
        "annotated": annotated,
        "lightweight": tag_count - annotated,
        "mapped_commits": mapped,
    }))
}

pub fn releases_list(conn: &mut SqliteConnection, _p: &Params) -> Result<Value, StatusCode> {
    // Load all tags
    let all_tags = tags::table
        .select((
            tags::name,
            tags::target_commit,
            tags::is_annotated,
            tags::tagger_name,
            tags::annotation,
            tags::created_at,
        ))
        .order(tags::created_at.desc())
        .load::<(String, String, i32, Option<String>, Option<String>, String)>(conn)
        .map_err(db_error)?;

    // Load commit counts per release (aggregate in Rust to avoid Diesel mixed-aggregate issues)
    let release_commits_raw = commit_releases::table
        .select(commit_releases::release_tag)
        .load::<String>(conn)
        .map_err(db_error)?;
    let mut commit_counts: HashMap<String, i64> = HashMap::new();
    for tag in release_commits_raw {
        *commit_counts.entry(tag).or_default() += 1;
    }

    // Load distinct contributor count per release
    let release_contribs = commit_releases::table
        .inner_join(commits::table.on(commits::hash.eq(commit_releases::commit_hash)))
        .select((commit_releases::release_tag, commits::author_email))
        .load::<(String, String)>(conn)
        .map_err(db_error)?;
    let mut contrib_counts: HashMap<String, HashSet<String>> = HashMap::new();
    for (tag, email) in release_contribs {
        contrib_counts.entry(tag).or_default().insert(email);
    }

    // Load insertions/deletions per release
    let release_churn = commit_releases::table
        .inner_join(commits::table.on(commits::hash.eq(commit_releases::commit_hash)))
        .select((
            commit_releases::release_tag,
            commits::insertions,
            commits::deletions,
        ))
        .load::<(String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?;
    let mut churn: HashMap<String, (i64, i64)> = HashMap::new();
    for (tag, ins, del) in release_churn {
        let e = churn.entry(tag).or_default();
        e.0 += ins.unwrap_or(0) as i64;
        e.1 += del.unwrap_or(0) as i64;
    }

    let result: Vec<Value> = all_tags
        .into_iter()
        .map(|(name, target, annotated, tagger, annotation, date)| {
            let commits = commit_counts.get(&name).copied().unwrap_or(0);
            let contributors = contrib_counts.get(&name).map(|s| s.len()).unwrap_or(0);
            let (ins, del) = churn.get(&name).copied().unwrap_or((0, 0));
            json!({
                "name": name,
                "target_commit": target,
                "is_annotated": annotated != 0,
                "tagger": tagger,
                "annotation": annotation,
                "date": date,
                "commits": commits,
                "contributors": contributors,
                "insertions": ins,
                "deletions": del,
            })
        })
        .collect();

    Ok(json!(result))
}

pub fn releases_timeline(conn: &mut SqliteConnection, _p: &Params) -> Result<Value, StatusCode> {
    // Load raw data and aggregate in Rust
    let data = commit_releases::table
        .select((commit_releases::release_tag, commit_releases::release_date))
        .order(commit_releases::release_date.asc())
        .load::<(String, String)>(conn)
        .map_err(db_error)?;

    let mut counts: BTreeMap<String, (String, i64)> = BTreeMap::new();
    for (tag, date) in data {
        let e = counts.entry(tag).or_insert_with(|| (date, 0));
        e.1 += 1;
    }

    // Sort by date
    let mut result: Vec<(String, String, i64)> = counts
        .into_iter()
        .map(|(tag, (date, count))| (date.clone(), tag, count))
        .collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));

    let json_result: Vec<Value> = result
        .into_iter()
        .map(|(date, tag, commits)| json!({"release": tag, "date": date, "commits": commits}))
        .collect();
    Ok(json!(json_result))
}

pub fn releases_cadence(conn: &mut SqliteConnection) -> Result<Value, StatusCode> {
    // Tags sorted by date — compute days between consecutive releases
    let all_tags = tags::table
        .select((tags::name, tags::created_at))
        .order(tags::created_at.asc())
        .load::<(String, String)>(conn)
        .map_err(db_error)?;

    let mut result: Vec<Value> = Vec::new();
    let mut prev_date: Option<chrono::NaiveDate> = None;
    for (name, date_str) in &all_tags {
        if let Ok(d) = chrono::NaiveDate::parse_from_str(&date_str.get(..10).unwrap_or(date_str), "%Y-%m-%d") {
            if let Some(prev) = prev_date {
                let days = (d - prev).num_days();
                if days > 0 {
                    result.push(json!({"release": name, "days": days}));
                }
            }
            prev_date = Some(d);
        }
    }
    Ok(json!(result))
}

pub fn releases_contributors(conn: &mut SqliteConnection, _p: &Params) -> Result<Value, StatusCode> {
    // Distinct contributors per release
    let data = commit_releases::table
        .inner_join(commits::table.on(commits::hash.eq(commit_releases::commit_hash)))
        .select((commit_releases::release_tag, commit_releases::release_date, commits::author_email))
        .load::<(String, String, String)>(conn)
        .map_err(db_error)?;

    let mut per_release: BTreeMap<String, (String, HashSet<String>)> = BTreeMap::new();
    for (tag, date, email) in data {
        per_release.entry(tag).or_insert_with(|| (date, HashSet::new())).1.insert(email);
    }

    let result: Vec<Value> = per_release
        .into_iter()
        .map(|(tag, (date, emails))| json!({"release": tag, "date": date, "contributors": emails.len()}))
        .collect();
    Ok(json!(result))
}

pub fn release_detail(conn: &mut SqliteConnection, p: &Params) -> Result<Value, StatusCode> {
    let tag_name = p.name.as_deref().ok_or(StatusCode::BAD_REQUEST)?;

    // Tag info
    let tag_info = tags::table
        .filter(tags::name.eq(tag_name))
        .select((
            tags::name,
            tags::target_commit,
            tags::is_annotated,
            tags::tagger_name,
            tags::tagger_email,
            tags::tagger_date,
            tags::annotation,
            tags::created_at,
        ))
        .first::<(String, String, i32, Option<String>, Option<String>, Option<String>, Option<String>, String)>(conn)
        .optional()
        .map_err(db_error)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Commits in this release
    let release_commits = commit_releases::table
        .filter(commit_releases::release_tag.eq(tag_name))
        .inner_join(commits::table.on(commits::hash.eq(commit_releases::commit_hash)))
        .select((
            commits::hash,
            commits::subject,
            commits::author_name,
            commits::author_email,
            commits::author_date,
            commits::insertions,
            commits::deletions,
        ))
        .order(commits::author_date.desc())
        .load::<(String, String, String, String, String, Option<i32>, Option<i32>)>(conn)
        .map_err(db_error)?;

    let total_commits = release_commits.len();
    let total_ins: i64 = release_commits.iter().map(|c| c.5.unwrap_or(0) as i64).sum();
    let total_del: i64 = release_commits.iter().map(|c| c.6.unwrap_or(0) as i64).sum();
    let contributor_count = {
        let contributors: HashSet<&str> = release_commits.iter().map(|c| c.3.as_str()).collect();
        contributors.len()
    };

    // Top contributors in this release
    let mut author_commits: HashMap<String, (String, i64, i64, i64)> = HashMap::new();
    for c in &release_commits {
        let e = author_commits
            .entry(c.3.clone())
            .or_insert_with(|| (c.2.clone(), 0, 0, 0));
        e.1 += 1;
        e.2 += c.5.unwrap_or(0) as i64;
        e.3 += c.6.unwrap_or(0) as i64;
    }
    let mut top_authors: Vec<_> = author_commits.into_iter().collect();
    top_authors.sort_by(|a, b| b.1 .1.cmp(&a.1 .1));
    let top_authors: Vec<Value> = top_authors
        .into_iter()
        .take(30)
        .map(|(email, (name, commits, ins, del))| {
            json!({"name": name, "email": email, "commits": commits, "insertions": ins, "deletions": del})
        })
        .collect();

    // Language breakdown for this release (aggregate in Rust)
    let lang_raw = commit_releases::table
        .filter(commit_releases::release_tag.eq(tag_name))
        .inner_join(commit_files::table.on(commit_files::commit_hash.eq(commit_releases::commit_hash)))
        .select(commit_files::language)
        .load::<String>(conn)
        .map_err(db_error)?;
    let mut lang_counts: HashMap<String, i64> = HashMap::new();
    for lang in lang_raw {
        *lang_counts.entry(lang).or_default() += 1;
    }
    let mut lang_sorted: Vec<_> = lang_counts.into_iter().collect();
    lang_sorted.sort_by(|a, b| b.1.cmp(&a.1));
    let languages: Vec<Value> = lang_sorted
        .into_iter()
        .map(|(lang, count)| json!({"language": lang, "file_changes": count}))
        .collect();

    // Recent commits (limited)
    let recent: Vec<Value> = release_commits
        .into_iter()
        .take(100)
        .map(|(hash, subject, author, email, date, ins, del)| {
            json!({
                "hash": hash,
                "subject": subject,
                "author": author,
                "email": email,
                "date": date,
                "insertions": ins,
                "deletions": del,
            })
        })
        .collect();

    Ok(json!({
        "tag": {
            "name": tag_info.0,
            "target_commit": tag_info.1,
            "is_annotated": tag_info.2 != 0,
            "tagger_name": tag_info.3,
            "tagger_email": tag_info.4,
            "tagger_date": tag_info.5,
            "annotation": tag_info.6,
            "created_at": tag_info.7,
        },
        "stats": {
            "commits": total_commits,
            "contributors": contributor_count,
            "insertions": total_ins,
            "deletions": total_del,
        },
        "top_authors": top_authors,
        "languages": languages,
        "commits": recent,
    }))
}
