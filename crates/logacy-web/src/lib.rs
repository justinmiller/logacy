mod queries;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Json, Router,
};
use diesel::sqlite::SqliteConnection;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

struct AppState {
    conn: Mutex<Connection>,
    diesel_conn: Mutex<SqliteConnection>,
    github_base: Option<String>,
    ticket_url: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct Params {
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub search: Option<String>,
    pub name: Option<String>,
    pub sql: Option<String>,
    pub id: Option<i64>,
    pub org: Option<String>,
}

fn qj(conn: &Connection, sql: &str) -> Result<Vec<Value>, StatusCode> {
    let mut stmt = conn.prepare(sql).map_err(|e| { tracing::error!("SQL: {e}"); StatusCode::BAD_REQUEST })?;
    let n = stmt.column_count();
    let names: Vec<String> = (0..n).map(|i| stmt.column_name(i).unwrap().to_string()).collect();
    let rows = stmt.query_map([], |row| {
        let mut map = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let val = match row.get_ref(i) {
                Ok(rusqlite::types::ValueRef::Null) => Value::Null,
                Ok(rusqlite::types::ValueRef::Integer(n)) => json!(n),
                Ok(rusqlite::types::ValueRef::Real(f)) => json!(f),
                Ok(rusqlite::types::ValueRef::Text(s)) => Value::String(String::from_utf8_lossy(s).to_string()),
                _ => Value::Null,
            };
            map.insert(name.clone(), val);
        }
        Ok(Value::Object(map))
    }).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// ── Internal JSON endpoints (plumbing for the UI) ────────────────────────────

async fn j_status(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::status(
        &mut c,
        s.github_base.as_deref(),
        s.ticket_url.as_deref(),
    )?))
}

async fn j_timeline(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::timeline(&mut c, &p)?))
}

async fn j_contributors(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::contributors(&mut c, &p)?))
}

async fn j_orgs(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::orgs(&mut c, &p)?))
}

async fn j_languages(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::languages(&mut c, &p)?))
}

async fn j_heatmap(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::heatmap(&mut c, &p)?))
}

async fn j_subsystems(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::subsystems(&mut c, &p)?))
}

async fn j_reviews(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::reviews(&mut c, &p)?))
}

async fn j_ownership(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::ownership(&mut c)?))
}

async fn j_commits(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::commits_list(&mut c, &p)?))
}

async fn j_contributor_detail(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::contributor_detail(&mut c, &p)?))
}

// ── Identities endpoints ────────────────────────────────────────────────────

async fn j_identities_summary(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_summary(&mut c, &p)?))
}

async fn j_identities_list(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_list(&mut c, &p)?))
}

async fn j_identities_orgs(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_orgs(&mut c, &p)?))
}

async fn j_identities_alias_dist(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_alias_dist(&mut c)?))
}

async fn j_identities_unresolved(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_unresolved(&mut c, &p)?))
}

async fn j_identities_bots(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_bots(&mut c, &p)?))
}

async fn j_identities_multi_alias(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identities_multi_alias(&mut c)?))
}

async fn j_identity_profile(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::identity_profile(&mut c, &p)?))
}

// ── Files endpoints ──────────────────────────────────────────────────────────

async fn j_files_age(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_age(&mut c, &p)?))
}

async fn j_files_concentration(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_concentration(&mut c)?))
}

async fn j_files_hotspots(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_hotspots(&mut c, &p)?))
}

async fn j_files_largest(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_largest(&mut c)?))
}

async fn j_files_silos(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_silos(&mut c)?))
}

async fn j_files_detail(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_detail(&mut c)?))
}

async fn j_files_dir_ownership(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_dir_ownership(&mut c)?))
}

async fn j_files_dir_churn(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_dir_churn(&mut c, &p)?))
}

async fn j_files_dir_busfactor(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::files_dir_busfactor(&mut c)?))
}

// ── Releases endpoints ───────────────────────────────────────────────────────

async fn j_releases_summary(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::releases_summary(&mut c)?))
}

async fn j_releases_list(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::releases_list(&mut c, &p)?))
}

async fn j_releases_timeline(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::releases_timeline(&mut c, &p)?))
}

async fn j_releases_cadence(State(s): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::releases_cadence(&mut c)?))
}

async fn j_releases_contributors(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::releases_contributors(&mut c, &p)?))
}

async fn j_release_detail(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::release_detail(&mut c, &p)?))
}

// ── Hotspots endpoints ───────────────────────────────────────────────────────

async fn j_hotspots_fragmented(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT ch.path, count(*) AS hunks, count(DISTINCT ch.commit_hash) AS commits,
                round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         WHERE 1=1{df}
         GROUP BY ch.path ORDER BY hunks DESC LIMIT 30"
    ))?;
    Ok(Json(json!(data)))
}

async fn j_hotspots_scatter(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT ch.path, count(DISTINCT ch.commit_hash) AS commits,
                round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks,
                sum(ch.new_lines + ch.old_lines) AS total_churn
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         WHERE 1=1{df}
         GROUP BY ch.path HAVING commits >= 5
         ORDER BY total_churn DESC LIMIT 200"
    ))?;
    Ok(Json(json!(data)))
}

async fn j_hotspots_regions(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT ch.path AS file, ch.new_start AS line, ch.new_lines AS span,
                count(DISTINCT ch.commit_hash) AS touches,
                min(c.author_date) AS first_touch, max(c.author_date) AS last_touch
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         WHERE ch.new_lines > 0{df}
         GROUP BY ch.path, ch.new_start
         ORDER BY touches DESC LIMIT 50"
    ))?;
    Ok(Json(json!(data)))
}

async fn j_hotspots_size_dist(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT CASE
                  WHEN new_lines + old_lines <= 1 THEN '1'
                  WHEN new_lines + old_lines <= 5 THEN '2-5'
                  WHEN new_lines + old_lines <= 20 THEN '6-20'
                  WHEN new_lines + old_lines <= 50 THEN '21-50'
                  WHEN new_lines + old_lines <= 100 THEN '51-100'
                  ELSE '100+'
                END AS size_bucket,
                count(*) AS hunks
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         WHERE 1=1{df}
         GROUP BY size_bucket
         ORDER BY min(new_lines + old_lines)"
    ))?;
    Ok(Json(json!(data)))
}

async fn j_hotspots_trend(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT strftime('%Y-%m', c.author_date) AS month,
                round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks,
                count(*) AS total_hunks
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         WHERE 1=1{df}
         GROUP BY month ORDER BY month"
    ))?;
    Ok(Json(json!(data)))
}

async fn j_hotspots_scattered(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT ch.path AS file, count(DISTINCT ch.commit_hash) AS commits,
                count(*) AS total_hunks,
                round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks_per_commit,
                round(avg(ch.new_lines + ch.old_lines), 1) AS avg_hunk_size
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         WHERE 1=1{df}
         GROUP BY ch.path HAVING count(DISTINCT ch.commit_hash) >= 10
         ORDER BY avg_hunks_per_commit DESC LIMIT 30"
    ))?;
    Ok(Json(json!(data)))
}

async fn j_hotspots_contributors(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let c = s.conn.lock().await;
    let df = date_filter(&p, "c.author_date");
    let data = qj(&c, &format!(
        "SELECT i.canonical_name AS author,
                count(DISTINCT ch.commit_hash) AS commits,
                count(*) AS total_hunks,
                round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks_per_commit,
                round(avg(ch.new_lines + ch.old_lines), 1) AS avg_hunk_size,
                sum(ch.new_lines + ch.old_lines) AS total_churn
         FROM commit_hunks ch
         JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
         JOIN identities i ON c.author_id = i.id
         WHERE i.is_bot = 0{df}
         GROUP BY i.id
         HAVING count(DISTINCT ch.commit_hash) >= 10
         ORDER BY avg_hunks_per_commit DESC"
    ))?;
    Ok(Json(json!(data)))
}

fn date_filter(p: &Params, col: &str) -> String {
    let mut parts = Vec::new();
    if let Some(ref s) = p.since {
        parts.push(format!(" AND {} >= '{}'", col, s.replace('\'', "")));
    }
    if let Some(ref u) = p.until {
        parts.push(format!(" AND {} < '{}'", col, u.replace('\'', "")));
    }
    parts.join("")
}

// ── Org profile endpoint ─────────────────────────────────────────────────────

async fn j_org_profile(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let mut c = s.diesel_conn.lock().await;
    Ok(Json(queries::org_profile(&mut c, &p)?))
}

async fn j_sql(State(s): State<Arc<AppState>>, Query(p): Query<Params>) -> Result<Json<Value>, StatusCode> {
    let sql = p.sql.as_deref().ok_or(StatusCode::BAD_REQUEST)?.trim();
    let upper = sql.to_uppercase();
    if !upper.starts_with("SELECT") && !upper.starts_with("WITH") { return Err(StatusCode::FORBIDDEN); }
    for kw in ["INSERT","UPDATE","DELETE","DROP","ALTER","CREATE","ATTACH"] {
        if upper.contains(kw) { return Err(StatusCode::FORBIDDEN); }
    }
    let c = s.conn.lock().await;
    let data = qj(&c, sql)?;
    let count = data.len();
    Ok(Json(json!({"rows":data,"count":count})))
}

// ── Page ─────────────────────────────────────────────────────────────────────

async fn index_page() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

// ── GitHub URL detection ─────────────────────────────────────────────────────

fn detect_forge_base(repo_path: &Path) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(repo_path)
        .output()
        .ok()?;
    let url = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if url.is_empty() { return None; }

    // git@github.com:owner/repo.git → https://github.com/owner/repo
    if let Some(rest) = url.strip_prefix("git@github.com:") {
        let rest = rest.strip_suffix(".git").unwrap_or(rest);
        return Some(format!("https://github.com/{rest}"));
    }
    // git@host:owner/repo.git → https://host/owner/repo
    if url.starts_with("git@") {
        let rest = url.strip_prefix("git@").unwrap();
        if let Some((host, path)) = rest.split_once(':') {
            let path = path.strip_suffix(".git").unwrap_or(path);
            return Some(format!("https://{host}/{path}"));
        }
    }
    // https://host/path.git or https://host/path
    if url.starts_with("https://") || url.starts_with("http://") {
        let u = url.strip_suffix(".git").unwrap_or(&url);
        return Some(u.to_string());
    }
    None
}

// ── Server ───────────────────────────────────────────────────────────────────

pub async fn serve(db_path: &Path, repo_path: &Path, bind: &str, url_override: Option<&str>, ticket_url: Option<&str>) -> Result<()> {
    let conn = logacy_db::open_and_migrate(db_path)?;
    let diesel_conn = logacy_db::open_diesel(db_path)?;
    let github_base = url_override.map(|s| s.trim_end_matches('/').to_string())
        .or_else(|| detect_forge_base(repo_path));
    if let Some(ref gb) = github_base {
        println!("Forge: {gb}");
    }
    let state = Arc::new(AppState {
        conn: Mutex::new(conn),
        diesel_conn: Mutex::new(diesel_conn),
        github_base,
        ticket_url: ticket_url.map(|s| s.to_string()),
    });

    let app = Router::new()
        .route("/", get(index_page))
        .route("/api/status", get(j_status))
        .route("/api/timeline", get(j_timeline))
        .route("/api/contributors", get(j_contributors))
        .route("/api/contributor", get(j_contributor_detail))
        .route("/api/orgs", get(j_orgs))
        .route("/api/languages", get(j_languages))
        .route("/api/heatmap", get(j_heatmap))
        .route("/api/subsystems", get(j_subsystems))
        .route("/api/reviews", get(j_reviews))
        .route("/api/ownership", get(j_ownership))
        .route("/api/commits", get(j_commits))
        .route("/api/query", get(j_sql))
        .route("/api/files/age", get(j_files_age))
        .route("/api/files/concentration", get(j_files_concentration))
        .route("/api/files/hotspots", get(j_files_hotspots))
        .route("/api/files/largest", get(j_files_largest))
        .route("/api/files/silos", get(j_files_silos))
        .route("/api/files/detail", get(j_files_detail))
        .route("/api/files/dir-ownership", get(j_files_dir_ownership))
        .route("/api/files/dir-churn", get(j_files_dir_churn))
        .route("/api/files/dir-busfactor", get(j_files_dir_busfactor))
        .route("/api/identities/summary", get(j_identities_summary))
        .route("/api/identities/list", get(j_identities_list))
        .route("/api/identities/orgs", get(j_identities_orgs))
        .route("/api/identities/aliases", get(j_identities_alias_dist))
        .route("/api/identities/unresolved", get(j_identities_unresolved))
        .route("/api/identities/bots", get(j_identities_bots))
        .route("/api/identities/multi-alias", get(j_identities_multi_alias))
        .route("/api/identity/profile", get(j_identity_profile))
        .route("/api/org/profile", get(j_org_profile))
        .route("/api/releases/summary", get(j_releases_summary))
        .route("/api/releases/list", get(j_releases_list))
        .route("/api/releases/timeline", get(j_releases_timeline))
        .route("/api/releases/cadence", get(j_releases_cadence))
        .route("/api/releases/contributors", get(j_releases_contributors))
        .route("/api/release/detail", get(j_release_detail))
        .route("/api/hotspots/fragmented", get(j_hotspots_fragmented))
        .route("/api/hotspots/scatter", get(j_hotspots_scatter))
        .route("/api/hotspots/regions", get(j_hotspots_regions))
        .route("/api/hotspots/size-distribution", get(j_hotspots_size_dist))
        .route("/api/hotspots/trend", get(j_hotspots_trend))
        .route("/api/hotspots/scattered", get(j_hotspots_scattered))
        .route("/api/hotspots/contributors", get(j_hotspots_contributors))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("logacy web → http://{bind}");
    println!("logacy web → http://{bind}");
    axum::serve(listener, app).await?;
    Ok(())
}
