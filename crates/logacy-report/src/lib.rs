use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{json, Value};

const VEGA_JS: &str = include_str!("../vendor/vega.min.js");
const VEGA_LITE_JS: &str = include_str!("../vendor/vega-lite.min.js");
const VEGA_EMBED_JS: &str = include_str!("../vendor/vega-embed.min.js");

const TEMPLATE: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         color: #1a1a2e; background: #f8f9fa; padding: 2rem; max-width: 1400px; margin: 0 auto; }
  h1 { font-size: 1.8rem; margin-bottom: 0.5rem; }
  .meta { color: #666; font-size: 0.9rem; margin-bottom: 2rem; }
  .chart-section { background: #fff; border-radius: 8px; padding: 1.5rem;
                   margin-bottom: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  .chart-section h2 { font-size: 1.2rem; margin-bottom: 1rem; color: #2d3436; }
  .chart-container { width: 100%; }
  table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; font-size: 0.85rem; }
  th, td { text-align: left; padding: 6px 12px; border-bottom: 1px solid #eee; }
  th { background: #f1f3f5; font-weight: 600; }
  tr:hover td { background: #f8f9fa; }
  .number { text-align: right; font-variant-numeric: tabular-nums; }
</style>
<script>{{ vega_js }}</script>
<script>{{ vega_lite_js }}</script>
<script>{{ vega_embed_js }}</script>
</head>
<body>
<h1>{{ title }}</h1>
<p class="meta">{{ subtitle }}Generated {{ generated_at }}</p>
{{ content }}
<script>
document.querySelectorAll('[data-vegalite]').forEach(el => {
  const spec = JSON.parse(el.getAttribute('data-vegalite'));
  vegaEmbed(el, spec, {actions: false, renderer: 'svg'}).catch(console.error);
});
</script>
</body>
</html>
"#;

pub const TEMPLATES: &[&str] = &[
    "overview",
    "contributors",
    "subsystems",
    "reviews",
    "ownership",
];

// ── DateRange ────────────────────────────────────────────────────────────────

/// Date range filter applied to all report queries.
#[derive(Clone, Default)]
pub struct DateRange {
    pub since: Option<String>,
    pub until: Option<String>,
}

impl DateRange {
    /// Returns a SQL AND-clause fragment filtering on the given date column.
    /// Returns empty string if no range is set.
    /// E.g. ` AND c.author_date >= '2024-01-01' AND c.author_date < '2025-01-01'`
    fn sql(&self, date_col: &str) -> String {
        let mut parts = Vec::new();
        if let Some(ref s) = self.since {
            parts.push(format!("{} >= '{}'", date_col, sql_escape_date(s)));
        }
        if let Some(ref u) = self.until {
            parts.push(format!("{} < '{}'", date_col, sql_escape_date(u)));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" AND {}", parts.join(" AND "))
        }
    }

    /// Returns a human-readable label for the date range, or empty string.
    pub fn label(&self) -> String {
        match (&self.since, &self.until) {
            (Some(s), Some(u)) => format!("{} to {} | ", s, u),
            (Some(s), None) => format!("Since {} | ", s),
            (None, Some(u)) => format!("Until {} | ", u),
            (None, None) => String::new(),
        }
    }
}

/// Minimal safety: strip any single quotes from date strings.
fn sql_escape_date(s: &str) -> String {
    s.replace('\'', "")
}

// ── Public API ───────────────────────────────────────────────────────────────

pub fn run_report(
    conn: &Connection,
    template: &str,
    output_dir: &Path,
    range: &DateRange,
) -> Result<std::path::PathBuf> {
    let (title, content) = match template {
        "overview" => report_overview(conn, range)?,
        "contributors" => report_contributors(conn, range)?,
        "subsystems" => report_subsystems(conn, range)?,
        "reviews" => report_reviews(conn, range)?,
        "ownership" => report_ownership(conn, range)?,
        _ => anyhow::bail!("unknown template: {}", template),
    };

    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string();

    let mut env = minijinja::Environment::new();
    env.add_template("report", TEMPLATE)?;
    let tmpl = env.get_template("report")?;
    let html = tmpl.render(minijinja::context! {
        title => title,
        subtitle => range.label(),
        generated_at => now,
        vega_js => VEGA_JS,
        vega_lite_js => VEGA_LITE_JS,
        vega_embed_js => VEGA_EMBED_JS,
        content => content,
    })?;

    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let out_path = output_dir.join(format!("{}.html", template));
    std::fs::write(&out_path, html)
        .with_context(|| format!("failed to write {}", out_path.display()))?;

    Ok(out_path)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn vegalite_div(id: &str, title: &str, spec: &Value) -> String {
    format!(
        r#"<div class="chart-section"><h2>{}</h2><div class="chart-container" id="{}" data-vegalite='{}'></div></div>"#,
        html_escape(title),
        id,
        spec.to_string().replace('\'', "&#39;"),
    )
}

fn html_table_section(title: &str, headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut s = format!(
        r#"<div class="chart-section"><h2>{}</h2><table><thead><tr>"#,
        html_escape(title)
    );
    for h in headers {
        s.push_str(&format!("<th>{}</th>", html_escape(h)));
    }
    s.push_str("</tr></thead><tbody>");
    for row in rows {
        s.push_str("<tr>");
        for (i, cell) in row.iter().enumerate() {
            let cls = if i > 0
                && cell
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == ',')
            {
                " class=\"number\""
            } else {
                ""
            };
            s.push_str(&format!("<td{}>{}</td>", cls, html_escape(cell)));
        }
        s.push_str("</tr>");
    }
    s.push_str("</tbody></table></div>");
    s
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn query_json_array(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(sql)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap().to_string())
        .collect();

    let rows = stmt.query_map(params, |row| {
        let mut map = serde_json::Map::new();
        for (i, name) in col_names.iter().enumerate() {
            let val = match row.get_ref(i) {
                Ok(rusqlite::types::ValueRef::Null) => Value::Null,
                Ok(rusqlite::types::ValueRef::Integer(n)) => json!(n),
                Ok(rusqlite::types::ValueRef::Real(f)) => json!(f),
                Ok(rusqlite::types::ValueRef::Text(s)) => {
                    Value::String(String::from_utf8_lossy(s).to_string())
                }
                Ok(rusqlite::types::ValueRef::Blob(_)) => Value::Null,
                Err(_) => Value::Null,
            };
            map.insert(name.clone(), val);
        }
        Ok(Value::Object(map))
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn query_table(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut stmt = conn.prepare(sql)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap().to_string())
        .collect();

    let rows: Vec<Vec<String>> = stmt
        .query_map(params, |row| {
            let mut vals = Vec::new();
            for i in 0..col_count {
                let val = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => String::new(),
                    Ok(rusqlite::types::ValueRef::Integer(n)) => n.to_string(),
                    Ok(rusqlite::types::ValueRef::Real(f)) => format!("{:.2}", f),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        String::from_utf8_lossy(s).to_string()
                    }
                    Ok(rusqlite::types::ValueRef::Blob(_)) => String::new(),
                    Err(_) => String::new(),
                };
                vals.push(val);
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok((col_names, rows))
}

// ── Report: Overview ─────────────────────────────────────────────────────────

fn report_overview(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("author_date");

    // Commits over time (monthly area chart)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT strftime('%Y-%m', author_date) AS month, count(*) AS commits
             FROM v_commits WHERE author_is_bot = 0{df}
             GROUP BY month ORDER BY month"
        ),
        &[],
    )?;
    let spec = json!({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": 250,
        "data": {"values": data},
        "mark": {"type": "area", "line": true, "point": false, "opacity": 0.7},
        "encoding": {
            "x": {"field": "month", "type": "temporal", "title": "Month"},
            "y": {"field": "commits", "type": "quantitative", "title": "Commits"},
            "tooltip": [
                {"field": "month", "type": "temporal", "title": "Month"},
                {"field": "commits", "type": "quantitative", "title": "Commits"}
            ]
        }
    });
    content.push_str(&vegalite_div("commits-time", "Commits Over Time", &spec));

    // Top 20 contributors (bar chart)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT resolved_author_name AS author, count(*) AS commits
             FROM v_commits WHERE author_is_bot = 0{df}
             GROUP BY resolved_author_name ORDER BY commits DESC LIMIT 20"
        ),
        &[],
    )?;
    let spec = json!({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": 400,
        "data": {"values": data},
        "mark": "bar",
        "encoding": {
            "y": {"field": "author", "type": "nominal", "sort": "-x", "title": null},
            "x": {"field": "commits", "type": "quantitative", "title": "Commits"},
            "color": {"value": "#4c72b0"},
            "tooltip": [
                {"field": "author", "type": "nominal"},
                {"field": "commits", "type": "quantitative"}
            ]
        }
    });
    content.push_str(&vegalite_div("top-contributors", "Top 20 Contributors", &spec));

    // Organizational contribution share (stacked area)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT strftime('%Y-%m', author_date) AS month,
                    COALESCE(author_org, 'Unknown') AS org, count(*) AS commits
             FROM v_commits WHERE author_is_bot = 0{df}
             GROUP BY month, org ORDER BY month"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": {"type": "area", "opacity": 0.8},
            "encoding": {
                "x": {"field": "month", "type": "temporal", "title": "Month"},
                "y": {"field": "commits", "type": "quantitative", "stack": "normalize",
                       "title": "Share of Commits"},
                "color": {"field": "org", "type": "nominal", "title": "Organization"},
                "tooltip": [
                    {"field": "month", "type": "temporal"},
                    {"field": "org", "type": "nominal"},
                    {"field": "commits", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "org-share",
            "Organization Contribution Share",
            &spec,
        ));
    }

    // Subsystem breakdown
    let cf_date = dr.sql("c.author_date");
    let data = query_json_array(
        conn,
        &format!(
            "SELECT s.name AS subsystem, count(DISTINCT cf.commit_hash) AS commits
             FROM subsystems s
             JOIN file_subsystems fs ON fs.subsystem_id = s.id
             JOIN commit_files cf ON cf.path = fs.path
             JOIN commits c ON c.hash = cf.commit_hash
             WHERE 1=1{cf_date}
             GROUP BY s.name ORDER BY commits DESC"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "subsystem", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "commits", "type": "quantitative", "title": "Commits"},
                "color": {"field": "commits", "type": "quantitative", "scale": {"scheme": "blues"}, "legend": null},
                "tooltip": [
                    {"field": "subsystem", "type": "nominal"},
                    {"field": "commits", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("subsystems", "Subsystem Breakdown", &spec));
    }

    Ok(("Overview".to_string(), content))
}

// ── Report: Contributors ─────────────────────────────────────────────────────

fn report_contributors(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("c.author_date");

    // Also filter reviews by the commit's author_date
    let review_df = dr.sql("rc.author_date");

    // Top contributors table with commit count, review count, tenure
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT
                i.canonical_name AS name,
                COALESCE(i.org, '') AS org,
                count(DISTINCT c.hash) AS commits,
                COALESCE(reviews.review_count, 0) AS reviews,
                COALESCE(subs.subsystem_count, 0) AS subsystems,
                min(c.author_date) AS first_commit,
                max(c.author_date) AS last_commit,
                CAST(julianday(max(c.author_date)) - julianday(min(c.author_date)) AS INTEGER) AS tenure_days
             FROM identities i
             JOIN commits c ON c.author_id = i.id
             LEFT JOIN (
                 SELECT t.identity_id, count(*) AS review_count
                 FROM trailers t
                 JOIN commits rc ON rc.hash = t.commit_hash
                 WHERE t.key = 'Reviewed-by'{review_df}
                 GROUP BY t.identity_id
             ) reviews ON reviews.identity_id = i.id
             LEFT JOIN (
                 SELECT vsc.identity_id, count(DISTINCT vsc.subsystem_id) AS subsystem_count
                 FROM v_subsystem_contributors vsc
                 GROUP BY vsc.identity_id
             ) subs ON subs.identity_id = i.id
             WHERE i.is_bot = 0{df}
             GROUP BY i.id
             ORDER BY commits DESC
             LIMIT 50"
        ),
        &[],
    )?;
    let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
    content.push_str(&html_table_section(
        "Top 50 Contributors",
        &header_refs,
        &rows,
    ));

    // Commits per contributor (bar chart, top 30)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT i.canonical_name AS author, count(*) AS commits
             FROM identities i
             JOIN commits c ON c.author_id = i.id
             WHERE i.is_bot = 0{df}
             GROUP BY i.id ORDER BY commits DESC LIMIT 30"
        ),
        &[],
    )?;
    let spec = json!({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": 500,
        "data": {"values": data},
        "mark": "bar",
        "encoding": {
            "y": {"field": "author", "type": "nominal", "sort": "-x", "title": null},
            "x": {"field": "commits", "type": "quantitative", "title": "Commits"},
            "color": {"value": "#4c72b0"},
            "tooltip": [
                {"field": "author", "type": "nominal"},
                {"field": "commits", "type": "quantitative"}
            ]
        }
    });
    content.push_str(&vegalite_div(
        "contributor-commits",
        "Commits by Contributor",
        &spec,
    ));

    // Reviews per reviewer (bar chart, top 20)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT i.canonical_name AS reviewer, count(*) AS reviews
             FROM trailers t
             JOIN identities i ON t.identity_id = i.id
             JOIN commits c ON c.hash = t.commit_hash
             WHERE t.key = 'Reviewed-by' AND i.is_bot = 0{df}
             GROUP BY i.id ORDER BY reviews DESC LIMIT 20"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "reviewer", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "reviews", "type": "quantitative", "title": "Reviews"},
                "color": {"value": "#e07b39"},
                "tooltip": [
                    {"field": "reviewer", "type": "nominal"},
                    {"field": "reviews", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("reviewer-reviews", "Top Reviewers", &spec));
    }

    Ok(("Contributors".to_string(), content))
}

// ── Report: Subsystems ───────────────────────────────────────────────────────

fn report_subsystems(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("c.author_date");

    // Subsystem health table — need to inline date filter into the view-like joins
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT
                s.name AS subsystem,
                count(DISTINCT i.id) AS contributors,
                count(DISTINCT c.hash) AS total_commits,
                COALESCE(listed.reviewer_count, 0) AS listed_reviewers,
                COALESCE(active_rev.active_reviewer_count, 0) AS active_reviewers,
                max(c.author_date) AS last_activity
             FROM subsystems s
             JOIN file_subsystems fs ON fs.subsystem_id = s.id
             JOIN commit_files cf ON cf.path = fs.path
             JOIN commits c ON c.hash = cf.commit_hash
             JOIN identities i ON c.author_id = i.id
             LEFT JOIN (
                 SELECT subsystem_id, count(*) AS reviewer_count
                 FROM subsystem_reviewers GROUP BY subsystem_id
             ) listed ON listed.subsystem_id = s.id
             LEFT JOIN (
                 SELECT sr.subsystem_id, count(DISTINCT sr.identity_id) AS active_reviewer_count
                 FROM subsystem_reviewers sr
                 JOIN commits c2 ON c2.author_id = sr.identity_id
                 JOIN commit_files cf2 ON cf2.commit_hash = c2.hash
                 JOIN file_subsystems fs2 ON fs2.path = cf2.path AND fs2.subsystem_id = sr.subsystem_id
                 WHERE 1=1{df_c2}
                 GROUP BY sr.subsystem_id
             ) active_rev ON active_rev.subsystem_id = s.id
             WHERE i.is_bot = 0{df}
             GROUP BY s.id
             ORDER BY total_commits DESC",
            df_c2 = dr.sql("c2.author_date"),
        ),
        &[],
    )?;
    let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
    content.push_str(&html_table_section(
        "Subsystem Health",
        &header_refs,
        &rows,
    ));

    // Activity over time per subsystem (top 10 subsystems)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT s.name AS subsystem,
                    strftime('%Y', c.author_date) AS year,
                    count(DISTINCT c.hash) AS commits
             FROM subsystems s
             JOIN file_subsystems fs ON fs.subsystem_id = s.id
             JOIN commit_files cf ON cf.path = fs.path
             JOIN commits c ON c.hash = cf.commit_hash
             WHERE s.id IN (
                 SELECT subsystem_id FROM (
                     SELECT fs2.subsystem_id, count(DISTINCT cf2.commit_hash) AS cnt
                     FROM file_subsystems fs2
                     JOIN commit_files cf2 ON cf2.path = fs2.path
                     JOIN commits c3 ON c3.hash = cf2.commit_hash
                     WHERE 1=1{df_c3}
                     GROUP BY fs2.subsystem_id ORDER BY cnt DESC LIMIT 10
                 )
             ){df}
             GROUP BY s.name, year ORDER BY year",
            df_c3 = dr.sql("c3.author_date"),
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": {"type": "line", "point": true},
            "encoding": {
                "x": {"field": "year", "type": "ordinal", "title": "Year"},
                "y": {"field": "commits", "type": "quantitative", "title": "Commits"},
                "color": {"field": "subsystem", "type": "nominal", "title": "Subsystem"},
                "tooltip": [
                    {"field": "subsystem", "type": "nominal"},
                    {"field": "year", "type": "ordinal"},
                    {"field": "commits", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "subsystem-activity",
            "Subsystem Activity Over Time (Top 10)",
            &spec,
        ));
    }

    // Bus factor per subsystem (blame-based, not date-filtered)
    let data = query_json_array(
        conn,
        "WITH ownership AS (
            SELECT s.name AS subsystem, i.canonical_name AS author,
                   SUM(fo.lines_owned) AS lines
            FROM file_ownership fo
            JOIN (SELECT id, commit_hash FROM blame_snapshots ORDER BY id DESC LIMIT 1) bs ON bs.id = fo.snapshot_id
            JOIN identities i ON fo.identity_id = i.id
            JOIN file_subsystems fs ON fs.path = fo.path
            JOIN subsystems s ON s.id = fs.subsystem_id
            WHERE i.is_bot = 0
            GROUP BY s.name, i.canonical_name
        ),
        ranked AS (
            SELECT subsystem, author, lines,
                   SUM(lines) OVER (PARTITION BY subsystem) AS total,
                   SUM(lines) OVER (PARTITION BY subsystem ORDER BY lines DESC
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative,
                   ROW_NUMBER() OVER (PARTITION BY subsystem ORDER BY lines DESC) AS rk
            FROM ownership
        )
        SELECT subsystem,
               MAX(CASE WHEN cumulative <= 0.8 * total THEN rk ELSE 0 END) + 1 AS bus_factor
        FROM ranked
        GROUP BY subsystem
        ORDER BY bus_factor",
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "subsystem", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "bus_factor", "type": "quantitative", "title": "Bus Factor (contributors for 80% of code)"},
                "color": {
                    "field": "bus_factor", "type": "quantitative",
                    "scale": {"scheme": "redyellowgreen", "domain": [1, 10]},
                    "legend": null
                },
                "tooltip": [
                    {"field": "subsystem", "type": "nominal"},
                    {"field": "bus_factor", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("bus-factor", "Bus Factor by Subsystem", &spec));
    }

    Ok(("Subsystems".to_string(), content))
}

// ── Report: Reviews ──────────────────────────────────────────────────────────

fn report_reviews(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("author_date");

    // Review counts per reviewer (bar chart)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT reviewer, count(*) AS reviews
             FROM v_reviews WHERE 1=1{df}
             GROUP BY reviewer ORDER BY reviews DESC LIMIT 20"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "reviewer", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "reviews", "type": "quantitative", "title": "Reviews"},
                "color": {"value": "#e07b39"},
                "tooltip": [
                    {"field": "reviewer", "type": "nominal"},
                    {"field": "reviews", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("review-counts", "Review Counts by Reviewer", &spec));
    }

    // Review network heatmap (author × reviewer, top 15 each)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT author, reviewer, count(*) AS reviews
             FROM v_reviews
             WHERE author IN (
                 SELECT author FROM v_reviews WHERE 1=1{df} GROUP BY author ORDER BY count(*) DESC LIMIT 15
             ) AND reviewer IN (
                 SELECT reviewer FROM v_reviews WHERE 1=1{df} GROUP BY reviewer ORDER BY count(*) DESC LIMIT 15
             ){df}
             GROUP BY author, reviewer"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": data},
            "mark": "rect",
            "encoding": {
                "x": {"field": "reviewer", "type": "nominal", "title": "Reviewer",
                       "axis": {"labelAngle": -45}},
                "y": {"field": "author", "type": "nominal", "title": "Author"},
                "color": {"field": "reviews", "type": "quantitative",
                          "scale": {"scheme": "orangered"}, "title": "Reviews"},
                "tooltip": [
                    {"field": "author", "type": "nominal"},
                    {"field": "reviewer", "type": "nominal"},
                    {"field": "reviews", "type": "quantitative"}
                ]
            },
            "config": {"axis": {"grid": true, "tickBand": "extent"}}
        });
        content.push_str(&vegalite_div(
            "review-heatmap",
            "Review Network (Author \u{00d7} Reviewer)",
            &spec,
        ));
    }

    // Review latency proxy
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT ticket, subject,
                    resolved_author_name AS author,
                    CAST(julianday(commit_date) - julianday(author_date) AS INTEGER) AS days_in_review
             FROM v_commits
             WHERE ticket IS NOT NULL
               AND julianday(commit_date) - julianday(author_date) > 0{df}
             ORDER BY days_in_review DESC
             LIMIT 20",
            df = dr.sql("author_date"),
        ),
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Longest Review Latency (author\u{2192}commit date gap)",
            &header_refs,
            &rows,
        ));
    }

    // Cross-org review patterns (uses temporal org from v_reviews)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT author_org, reviewer_org, count(*) AS reviews
             FROM v_reviews
             WHERE author_org IS NOT NULL AND reviewer_org IS NOT NULL{df}
             GROUP BY author_org, reviewer_org
             ORDER BY reviews DESC"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": "rect",
            "encoding": {
                "x": {"field": "reviewer_org", "type": "nominal", "title": "Reviewer Org"},
                "y": {"field": "author_org", "type": "nominal", "title": "Author Org"},
                "color": {"field": "reviews", "type": "quantitative",
                          "scale": {"scheme": "blues"}, "title": "Reviews"},
                "tooltip": [
                    {"field": "author_org", "type": "nominal"},
                    {"field": "reviewer_org", "type": "nominal"},
                    {"field": "reviews", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "cross-org",
            "Cross-Organization Review Patterns",
            &spec,
        ));
    }

    Ok(("Reviews".to_string(), content))
}

// ── Report: Ownership ────────────────────────────────────────────────────────

fn report_ownership(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();

    // Get latest snapshot
    let snapshot_id: Option<i64> = conn
        .query_row(
            "SELECT id FROM blame_snapshots ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .ok();

    let snapshot_id = match snapshot_id {
        Some(id) => id,
        None => {
            return Ok((
                "Ownership".to_string(),
                "<div class=\"chart-section\"><h2>No blame snapshots</h2>\
                 <p>Run <code>logacy blame</code> first.</p></div>"
                    .to_string(),
            ));
        }
    };

    // Ownership charts are point-in-time (blame snapshot), not date-filtered.
    // Lines owned by author (top 30, bar chart)
    let data = query_json_array(
        conn,
        "SELECT i.canonical_name AS author, SUM(fo.lines_owned) AS lines
         FROM file_ownership fo
         JOIN identities i ON fo.identity_id = i.id
         WHERE fo.snapshot_id = ?1 AND i.is_bot = 0
         GROUP BY i.id ORDER BY lines DESC LIMIT 30",
        &[&snapshot_id],
    )?;
    let spec = json!({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": 500,
        "data": {"values": data},
        "mark": "bar",
        "encoding": {
            "y": {"field": "author", "type": "nominal", "sort": "-x", "title": null},
            "x": {"field": "lines", "type": "quantitative", "title": "Lines Owned"},
            "color": {"value": "#55a868"},
            "tooltip": [
                {"field": "author", "type": "nominal"},
                {"field": "lines", "type": "quantitative"}
            ]
        }
    });
    content.push_str(&vegalite_div("owner-lines", "Lines Owned by Author", &spec));

    // Lines owned by org
    let data = query_json_array(
        conn,
        "SELECT COALESCE(i.org, 'Unknown') AS org, SUM(fo.lines_owned) AS lines
         FROM file_ownership fo
         JOIN identities i ON fo.identity_id = i.id
         WHERE fo.snapshot_id = ?1 AND i.is_bot = 0
         GROUP BY org ORDER BY lines DESC",
        &[&snapshot_id],
    )?;
    if data
        .iter()
        .any(|v| v.get("org").and_then(|o| o.as_str()) != Some("Unknown"))
    {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "org", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "lines", "type": "quantitative", "title": "Lines Owned"},
                "color": {"field": "org", "type": "nominal", "legend": null},
                "tooltip": [
                    {"field": "org", "type": "nominal"},
                    {"field": "lines", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "owner-org",
            "Lines Owned by Organization",
            &spec,
        ));
    }

    // Lines owned by subsystem
    let data = query_json_array(
        conn,
        "SELECT s.name AS subsystem, SUM(fo.lines_owned) AS lines
         FROM file_ownership fo
         JOIN file_subsystems fs ON fs.path = fo.path
         JOIN subsystems s ON s.id = fs.subsystem_id
         WHERE fo.snapshot_id = ?1
         GROUP BY s.name ORDER BY lines DESC",
        &[&snapshot_id],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "subsystem", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "lines", "type": "quantitative", "title": "Lines"},
                "color": {"field": "lines", "type": "quantitative",
                          "scale": {"scheme": "greens"}, "legend": null},
                "tooltip": [
                    {"field": "subsystem", "type": "nominal"},
                    {"field": "lines", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("owner-subsystem", "Lines by Subsystem", &spec));
    }

    // Code age distribution — filter by date range if provided
    let age_df = dr.sql("c.author_date");
    let data = query_json_array(
        conn,
        &format!(
            "SELECT strftime('%Y', c.author_date) AS year, count(*) AS lines
             FROM blame_lines bl
             JOIN commits c ON c.hash = bl.orig_commit
             WHERE bl.snapshot_id = ?1{age_df}
             GROUP BY year ORDER BY year"
        ),
        &[&snapshot_id],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": {"type": "bar"},
            "encoding": {
                "x": {"field": "year", "type": "ordinal", "title": "Year"},
                "y": {"field": "lines", "type": "quantitative", "title": "Surviving Lines"},
                "color": {"field": "lines", "type": "quantitative",
                          "scale": {"scheme": "viridis"}, "legend": null},
                "tooltip": [
                    {"field": "year", "type": "ordinal"},
                    {"field": "lines", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "code-age",
            "Code Age Distribution (Surviving Lines by Year)",
            &spec,
        ));
    }

    Ok(("Ownership".to_string(), content))
}
