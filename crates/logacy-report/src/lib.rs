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
    "files",
    "identities",
    "releases",
    "hotspots",
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

/// Options that influence report rendering (e.g. issue-tracker linking).
#[derive(Debug, Default)]
pub struct ReportOptions {
    /// URL template for linking tickets. Use `{ticket}` as placeholder.
    pub ticket_url: Option<String>,
}

// ── Public API ───────────────────────────────────────────────────────────────

pub fn run_report(
    conn: &Connection,
    template: &str,
    output_dir: &Path,
    range: &DateRange,
    opts: &ReportOptions,
) -> Result<std::path::PathBuf> {
    let (title, content) = match template {
        "overview" => report_overview(conn, range)?,
        "contributors" => report_contributors(conn, range)?,
        "subsystems" => report_subsystems(conn, range)?,
        "reviews" => report_reviews(conn, range, opts)?,
        "ownership" => report_ownership(conn, range)?,
        "files" => report_files(conn, range)?,
        "identities" => report_identities(conn, range)?,
        "releases" => report_releases(conn, range)?,
        "hotspots" => report_hotspots(conn, range)?,
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

/// Like `html_table_section` but optionally wraps one column in hyperlinks.
/// `link_col` is `Some((column_index, url_template))` where `{ticket}` in the
/// template is replaced with the cell value.
fn html_table_section_linked(
    title: &str,
    headers: &[&str],
    rows: &[Vec<String>],
    link_col: Option<(usize, &str)>,
) -> String {
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
            let content = if link_col.is_some_and(|(col, _)| col == i) && !cell.is_empty() {
                let url = link_col.unwrap().1.replace("{ticket}", cell);
                format!(
                    r#"<a href="{}" target="_blank" rel="noopener">{}</a>"#,
                    html_escape(&url),
                    html_escape(cell)
                )
            } else {
                html_escape(cell)
            };
            s.push_str(&format!("<td{}>{}</td>", cls, content));
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

    // Language Distribution (horizontal bar, top 15)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT cf.language, count(*) AS file_changes
             FROM commit_files cf
             JOIN commits c ON c.hash = cf.commit_hash
             WHERE cf.language != 'Other'{cf_date}
             GROUP BY cf.language ORDER BY file_changes DESC LIMIT 15"
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
                "y": {"field": "language", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "file_changes", "type": "quantitative", "title": "File Changes"},
                "color": {"field": "file_changes", "type": "quantitative",
                          "scale": {"scheme": "tealblues"}, "legend": null},
                "tooltip": [
                    {"field": "language", "type": "nominal"},
                    {"field": "file_changes", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("language-dist", "Language Distribution", &spec));
    }

    // Work Type Breakdown (bar: test/docs/build/source × lines changed)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT cf.category,
                    COALESCE(SUM(cf.insertions), 0) + COALESCE(SUM(cf.deletions), 0) AS lines_changed,
                    count(*) AS file_changes
             FROM commit_files cf
             JOIN commits c ON c.hash = cf.commit_hash
             WHERE 1=1{cf_date}
             GROUP BY cf.category ORDER BY lines_changed DESC"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 200,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "category", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "lines_changed", "type": "quantitative", "title": "Lines Changed"},
                "color": {
                    "field": "category", "type": "nominal",
                    "scale": {
                        "domain": ["source", "test", "docs", "build"],
                        "range": ["#4c72b0", "#55a868", "#c44e52", "#8172b2"]
                    },
                    "legend": null
                },
                "tooltip": [
                    {"field": "category", "type": "nominal"},
                    {"field": "lines_changed", "type": "quantitative"},
                    {"field": "file_changes", "type": "quantitative", "title": "File Changes"}
                ]
            }
        });
        content.push_str(&vegalite_div("work-type", "Work Type Breakdown", &spec));
    }

    // Commit Activity Heatmap (weekday × hour-of-day, UTC)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT CAST(strftime('%w', author_date) AS INTEGER) AS dow,
                    CAST(strftime('%H', author_date) AS INTEGER) AS hour,
                    count(*) AS commits
             FROM v_commits WHERE author_is_bot = 0{df}
             GROUP BY dow, hour"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 200,
            "data": {"values": data},
            "mark": "rect",
            "encoding": {
                "x": {"field": "hour", "type": "ordinal", "title": "Hour (UTC)"},
                "y": {
                    "field": "dow", "type": "ordinal", "title": "Day",
                    "sort": [0, 1, 2, 3, 4, 5, 6],
                    "axis": {"labelExpr": "['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][datum.value]"}
                },
                "color": {
                    "field": "commits", "type": "quantitative",
                    "scale": {"scheme": "blues"}, "title": "Commits"
                },
                "tooltip": [
                    {"field": "dow", "type": "ordinal", "title": "Day"},
                    {"field": "hour", "type": "ordinal", "title": "Hour"},
                    {"field": "commits", "type": "quantitative"}
                ]
            },
            "config": {"axis": {"grid": false}}
        });
        content.push_str(&vegalite_div(
            "commit-heatmap",
            "Commit Activity Heatmap (UTC)",
            &spec,
        ));
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
                COALESCE(vio.org, '') AS org,
                count(DISTINCT c.hash) AS commits,
                COALESCE(reviews.review_count, 0) AS reviews,
                COALESCE(subs.subsystem_count, 0) AS subsystems,
                min(c.author_date) AS first_commit,
                max(c.author_date) AS last_commit,
                CAST(julianday(max(c.author_date)) - julianday(min(c.author_date)) AS INTEGER) AS tenure_days
             FROM identities i
             LEFT JOIN v_identity_org vio ON vio.identity_id = i.id
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

    // Contributor Language Profile (top 20 contributors × their primary languages)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT i.canonical_name AS author, cf.language, count(*) AS file_changes
             FROM identities i
             JOIN commits c ON c.author_id = i.id
             JOIN commit_files cf ON cf.commit_hash = c.hash
             WHERE i.is_bot = 0 AND cf.language != 'Other'{df_cf}
               AND i.id IN (
                   SELECT c2.author_id FROM commits c2
                   WHERE c2.author_id IS NOT NULL{df2}
                   GROUP BY c2.author_id ORDER BY count(*) DESC LIMIT 20
               )
             GROUP BY i.canonical_name, cf.language
             ORDER BY file_changes DESC",
            df_cf = dr.sql("c.author_date"),
            df2 = dr.sql("c2.author_date"),
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 500,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "author", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "file_changes", "type": "quantitative", "stack": "normalize",
                       "title": "Language Share"},
                "color": {"field": "language", "type": "nominal", "title": "Language"},
                "tooltip": [
                    {"field": "author", "type": "nominal"},
                    {"field": "language", "type": "nominal"},
                    {"field": "file_changes", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "contributor-languages",
            "Contributor Language Profile (Top 20)",
            &spec,
        ));
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

    // Dormant Subsystems — no commits in >180 days
    let (headers, rows) = query_table(
        conn,
        "SELECT s.name AS subsystem,
                max(c.author_date) AS last_activity,
                CAST(julianday('now') - julianday(max(c.author_date)) AS INTEGER) AS days_dormant
         FROM subsystems s
         JOIN file_subsystems fs ON fs.subsystem_id = s.id
         JOIN commit_files cf ON cf.path = fs.path
         JOIN commits c ON c.hash = cf.commit_hash
         GROUP BY s.id
         HAVING days_dormant > 180
         ORDER BY days_dormant DESC",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Dormant Subsystems (>180 days inactive)",
            &header_refs,
            &rows,
        ));
    }

    // Unmapped Files — files not in any subsystem
    let (headers, rows) = query_table(
        conn,
        "SELECT
            (SELECT count(DISTINCT cf.path) FROM commit_files cf
             LEFT JOIN file_subsystems fs ON fs.path = cf.path
             WHERE fs.path IS NULL) AS unmapped_files,
            (SELECT count(DISTINCT cf.path) FROM commit_files cf) AS total_files,
            CAST(
                100.0 * (SELECT count(DISTINCT cf.path) FROM commit_files cf
                          LEFT JOIN file_subsystems fs ON fs.path = cf.path
                          WHERE fs.path IS NULL)
                / MAX(1, (SELECT count(DISTINCT cf.path) FROM commit_files cf))
            AS INTEGER) AS unmapped_pct",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Unmapped Files Overview",
            &header_refs,
            &rows,
        ));
    }

    // Unmapped files breakdown by language
    let data = query_json_array(
        conn,
        "SELECT cf.language, count(DISTINCT cf.path) AS files
         FROM commit_files cf
         LEFT JOIN file_subsystems fs ON fs.path = cf.path
         WHERE fs.path IS NULL AND cf.language != 'Other'
         GROUP BY cf.language ORDER BY files DESC LIMIT 15",
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "language", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "files", "type": "quantitative", "title": "Unmapped Files"},
                "color": {"value": "#c44e52"},
                "tooltip": [
                    {"field": "language", "type": "nominal"},
                    {"field": "files", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "unmapped-by-language",
            "Unmapped Files by Language",
            &spec,
        ));
    }

    // Subsystem Maintainer Summary — top recent committer + top code owner per subsystem
    let (headers, rows) = query_table(
        conn,
        "SELECT s.name AS subsystem,
                recent.author AS top_recent_committer,
                recent.commits AS recent_commits_90d,
                owner.author AS top_code_owner,
                owner.lines_owned
         FROM subsystems s
         LEFT JOIN (
             SELECT fs.subsystem_id, i.canonical_name AS author, count(DISTINCT c.hash) AS commits,
                    ROW_NUMBER() OVER (PARTITION BY fs.subsystem_id ORDER BY count(DISTINCT c.hash) DESC) AS rk
             FROM file_subsystems fs
             JOIN commit_files cf ON cf.path = fs.path
             JOIN commits c ON c.hash = cf.commit_hash
             JOIN identities i ON c.author_id = i.id
             WHERE i.is_bot = 0
               AND c.author_date >= datetime('now', '-90 days')
             GROUP BY fs.subsystem_id, i.id
         ) recent ON recent.subsystem_id = s.id AND recent.rk = 1
         LEFT JOIN (
             SELECT fs.subsystem_id, i.canonical_name AS author, SUM(fo.lines_owned) AS lines_owned,
                    ROW_NUMBER() OVER (PARTITION BY fs.subsystem_id ORDER BY SUM(fo.lines_owned) DESC) AS rk
             FROM file_subsystems fs
             JOIN file_ownership fo ON fo.path = fs.path
             JOIN (SELECT id FROM blame_snapshots ORDER BY id DESC LIMIT 1) bs ON bs.id = fo.snapshot_id
             JOIN identities i ON fo.identity_id = i.id
             WHERE i.is_bot = 0
             GROUP BY fs.subsystem_id, i.id
         ) owner ON owner.subsystem_id = s.id AND owner.rk = 1
         ORDER BY s.name",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Subsystem Maintainer Summary",
            &header_refs,
            &rows,
        ));
    }

    Ok(("Subsystems".to_string(), content))
}

// ── Report: Reviews ──────────────────────────────────────────────────────────

fn report_reviews(conn: &Connection, dr: &DateRange, opts: &ReportOptions) -> Result<(String, String)> {
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
        // Link ticket column (index 0) to issue tracker if configured
        let link_col = opts.ticket_url.as_ref().map(|tmpl| (0usize, tmpl.as_str()));
        content.push_str(&html_table_section_linked(
            "Longest Review Latency (author\u{2192}commit date gap)",
            &header_refs,
            &rows,
            link_col,
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
        "SELECT COALESCE(vio.org, 'Unknown') AS org, SUM(fo.lines_owned) AS lines
         FROM file_ownership fo
         JOIN identities i ON fo.identity_id = i.id
         LEFT JOIN v_identity_org vio ON vio.identity_id = i.id
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
            "SELECT strftime('%Y', c.author_date) AS year, sum(bh.line_count) AS lines
             FROM blame_hunks bh
             JOIN commits c ON c.hash = bh.orig_commit
             WHERE bh.snapshot_id = ?1{age_df}
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

// ── Report: Files ────────────────────────────────────────────────────────────

fn dir_expr(col: &str) -> String {
    format!(
        "CASE WHEN instr({col}, '/') > 0 THEN substr({col}, 1, instr({col}, '/') - 1) ELSE '(root)' END"
    )
}

fn report_files(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
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
                "Files".to_string(),
                "<div class=\"chart-section\"><h2>No blame snapshots</h2>\
                 <p>Run <code>logacy blame</code> first.</p></div>"
                    .to_string(),
            ));
        }
    };

    // ── LINE-LEVEL ──

    // Section 1: Code Age Distribution
    let age_df = dr.sql("c.author_date");
    let data = query_json_array(
        conn,
        &format!(
            "SELECT strftime('%Y', c.author_date) AS year, sum(bh.line_count) AS lines
             FROM blame_hunks bh
             JOIN commits c ON c.hash = bh.orig_commit
             WHERE bh.snapshot_id = ?1{age_df}
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

    // Section 2: Ownership Concentration (cumulative % of lines by top N contributors)
    let rows = query_json_array(
        conn,
        "SELECT i.canonical_name AS author, SUM(fo.lines_owned) AS lines
         FROM file_ownership fo
         JOIN identities i ON fo.identity_id = i.id
         WHERE fo.snapshot_id = ?1 AND i.is_bot = 0
         GROUP BY i.id ORDER BY lines DESC",
        &[&snapshot_id],
    )?;
    if !rows.is_empty() {
        let total: f64 = rows
            .iter()
            .filter_map(|r| r.get("lines").and_then(|v| v.as_f64()))
            .sum();
        let mut cumulative = 0.0;
        let mut cum_data: Vec<Value> = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            let lines = row.get("lines").and_then(|v| v.as_f64()).unwrap_or(0.0);
            cumulative += lines;
            let pct = if total > 0.0 {
                (cumulative / total * 100.0).round()
            } else {
                0.0
            };
            cum_data.push(json!({
                "rank": i + 1,
                "author": row.get("author").and_then(|v| v.as_str()).unwrap_or(""),
                "cumulative_pct": pct
            }));
            if pct >= 100.0 {
                break;
            }
        }
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "layer": [
                {
                    "data": {"values": cum_data},
                    "layer": [
                        {
                            "mark": {"type": "line", "interpolate": "monotone"},
                            "encoding": {
                                "x": {"field": "rank", "type": "quantitative", "title": "Top N Contributors"},
                                "y": {"field": "cumulative_pct", "type": "quantitative", "title": "Cumulative % of Lines", "scale": {"domain": [0, 100]}},
                                "tooltip": [
                                    {"field": "rank", "type": "quantitative"},
                                    {"field": "author", "type": "nominal"},
                                    {"field": "cumulative_pct", "type": "quantitative", "title": "Cumulative %"}
                                ]
                            }
                        },
                        {
                            "mark": {"type": "point", "filled": true, "size": 30},
                            "encoding": {
                                "x": {"field": "rank", "type": "quantitative"},
                                "y": {"field": "cumulative_pct", "type": "quantitative"},
                                "tooltip": [
                                    {"field": "rank", "type": "quantitative"},
                                    {"field": "author", "type": "nominal"},
                                    {"field": "cumulative_pct", "type": "quantitative", "title": "Cumulative %"}
                                ]
                            }
                        }
                    ]
                },
                {
                    "data": {"values": [{}]},
                    "mark": {"type": "rule", "strokeDash": [4, 4], "color": "#e45756"},
                    "encoding": {
                        "y": {"datum": 80}
                    }
                }
            ]
        });
        content.push_str(&vegalite_div(
            "ownership-concentration",
            "Ownership Concentration (Cumulative Line Ownership)",
            &spec,
        ));
    }

    // ── FILE-LEVEL ──

    // Section 3: File Hotspots (top 30 most-changed files)
    let df = dr.sql("c.author_date");
    let data = query_json_array(
        conn,
        &format!(
            "SELECT cf.path, COALESCE(cf.language, '') AS language, count(*) AS commits
             FROM commit_files cf
             JOIN commits c ON c.hash = cf.commit_hash AND c.first_parent = 1
             WHERE 1=1{df}
             GROUP BY cf.path ORDER BY commits DESC LIMIT 30"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 500,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "path", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "commits", "type": "quantitative", "title": "Commits"},
                "color": {"field": "language", "type": "nominal", "title": "Language"},
                "tooltip": [
                    {"field": "path", "type": "nominal"},
                    {"field": "commits", "type": "quantitative"},
                    {"field": "language", "type": "nominal"}
                ]
            }
        });
        content.push_str(&vegalite_div("file-hotspots", "File Hotspots (Most-Changed Files)", &spec));
    }

    // Section 4: Largest Files (top 30 by surviving lines)
    let data = query_json_array(
        conn,
        "SELECT fo.path, SUM(fo.lines_owned) AS lines,
                COALESCE((SELECT cf.language FROM commit_files cf WHERE cf.path = fo.path ORDER BY cf.rowid DESC LIMIT 1), '') AS language
         FROM file_ownership fo
         WHERE fo.snapshot_id = ?1
         GROUP BY fo.path ORDER BY lines DESC LIMIT 30",
        &[&snapshot_id],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 500,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "path", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "lines", "type": "quantitative", "title": "Lines of Code"},
                "color": {"field": "language", "type": "nominal", "title": "Language"},
                "tooltip": [
                    {"field": "path", "type": "nominal"},
                    {"field": "lines", "type": "quantitative"},
                    {"field": "language", "type": "nominal"}
                ]
            }
        });
        content.push_str(&vegalite_div("largest-files", "Largest Files (by Surviving Lines)", &spec));
    }

    // Section 5: Knowledge Silos (files where one person owns >80%)
    let (headers, rows) = query_table(
        conn,
        "SELECT fo.path AS File,
                i.canonical_name AS \"Sole Owner\",
                fo.lines_owned AS Lines,
                ROUND(fo.fraction * 100, 1) AS \"Ownership %\",
                COALESCE((SELECT cf.language FROM commit_files cf WHERE cf.path = fo.path ORDER BY cf.rowid DESC LIMIT 1), '') AS Language
         FROM file_ownership fo
         JOIN identities i ON fo.identity_id = i.id
         WHERE fo.snapshot_id = ?1 AND fo.fraction > 0.8 AND fo.lines_owned >= 50
         ORDER BY fo.lines_owned DESC LIMIT 50",
        &[&snapshot_id],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Knowledge Silos (Single Owner >80%)",
            &header_refs,
            &rows,
        ));
    }

    // Section 6: File Ownership Detail Table
    let (headers, rows) = query_table(
        conn,
        "SELECT fo.path AS File,
                (SELECT i2.canonical_name FROM file_ownership fo2
                 JOIN identities i2 ON fo2.identity_id = i2.id
                 WHERE fo2.snapshot_id = fo.snapshot_id AND fo2.path = fo.path
                 ORDER BY fo2.lines_owned DESC LIMIT 1) AS \"Primary Owner\",
                ROUND((SELECT MAX(fo2.fraction) FROM file_ownership fo2
                 WHERE fo2.snapshot_id = fo.snapshot_id AND fo2.path = fo.path) * 100, 1) AS \"Top %\",
                (SELECT count(*) FROM file_ownership fo2
                 WHERE fo2.snapshot_id = fo.snapshot_id AND fo2.path = fo.path
                   AND fo2.fraction >= 0.05) AS \"Bus Factor\",
                SUM(fo.lines_owned) AS Lines,
                COALESCE((SELECT cf.language FROM commit_files cf WHERE cf.path = fo.path ORDER BY cf.rowid DESC LIMIT 1), '') AS Language
         FROM file_ownership fo
         WHERE fo.snapshot_id = ?1
         GROUP BY fo.path
         ORDER BY Lines DESC LIMIT 50",
        &[&snapshot_id],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "File Ownership Detail (Top 50 Files)",
            &header_refs,
            &rows,
        ));
    }

    // ── DIRECTORY-LEVEL ──

    let dir = dir_expr("fo.path");

    // Section 7: Directory Ownership (stacked bar)
    let dir_owners = query_json_array(
        conn,
        &format!(
            "SELECT {dir} AS directory, i.canonical_name AS author, SUM(fo.lines_owned) AS lines
             FROM file_ownership fo
             JOIN identities i ON fo.identity_id = i.id
             WHERE fo.snapshot_id = ?1 AND i.is_bot = 0
             GROUP BY directory, i.id ORDER BY directory, lines DESC"
        ),
        &[&snapshot_id],
    )?;
    if !dir_owners.is_empty() {
        // Keep top 5 owners per directory, group rest as "Others"
        let mut by_dir: std::collections::HashMap<String, Vec<(String, f64)>> =
            std::collections::HashMap::new();
        for row in &dir_owners {
            let d = row.get("directory").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let a = row.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let l = row.get("lines").and_then(|v| v.as_f64()).unwrap_or(0.0);
            by_dir.entry(d).or_default().push((a, l));
        }
        let mut stacked_data: Vec<Value> = Vec::new();
        for (d, owners) in &by_dir {
            for (i, (author, lines)) in owners.iter().enumerate() {
                if i < 5 {
                    stacked_data.push(json!({"directory": d, "author": author, "lines": lines}));
                } else if i == 5 {
                    let others: f64 = owners[5..].iter().map(|(_, l)| l).sum();
                    stacked_data.push(json!({"directory": d, "author": "Others", "lines": others}));
                    break;
                }
            }
        }
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": stacked_data},
            "mark": "bar",
            "encoding": {
                "x": {"field": "directory", "type": "nominal", "title": "Directory", "sort": "-y"},
                "y": {"field": "lines", "type": "quantitative", "title": "Lines", "stack": "zero"},
                "color": {"field": "author", "type": "nominal", "title": "Author"},
                "tooltip": [
                    {"field": "directory", "type": "nominal"},
                    {"field": "author", "type": "nominal"},
                    {"field": "lines", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("dir-ownership", "Directory Ownership", &spec));
    }

    // Section 8: Directory Churn
    let dir_cf = dir_expr("cf.path");
    let data = query_json_array(
        conn,
        &format!(
            "SELECT {dir_cf} AS directory, count(*) AS commits
             FROM commit_files cf
             JOIN commits c ON c.hash = cf.commit_hash AND c.first_parent = 1
             WHERE 1=1{df}
             GROUP BY directory ORDER BY commits DESC"
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
                "y": {"field": "directory", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "commits", "type": "quantitative", "title": "Commits"},
                "color": {"value": "#4c72b0"},
                "tooltip": [
                    {"field": "directory", "type": "nominal"},
                    {"field": "commits", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("dir-churn", "Directory Churn", &spec));
    }

    // Section 9: Directory Bus Factor
    let (headers, rows) = query_table(
        conn,
        &format!(
            "WITH dir_stats AS (
                SELECT {dir} AS directory,
                       SUM(fo.lines_owned) AS total_lines,
                       COUNT(DISTINCT fo.identity_id) AS contributors,
                       fo.identity_id,
                       i.canonical_name,
                       SUM(fo.lines_owned) AS author_lines
                FROM file_ownership fo
                JOIN identities i ON fo.identity_id = i.id
                WHERE fo.snapshot_id = ?1 AND i.is_bot = 0
                GROUP BY directory, fo.identity_id
            )
            SELECT ds.directory AS Directory,
                   SUM(ds.author_lines) AS \"Total Lines\",
                   COUNT(DISTINCT ds.identity_id) AS Contributors,
                   (SELECT ds2.canonical_name FROM dir_stats ds2
                    WHERE ds2.directory = ds.directory
                    ORDER BY ds2.author_lines DESC LIMIT 1) AS \"Primary Owner\",
                   ROUND(MAX(ds.author_lines) * 100.0 / SUM(ds.author_lines), 1) AS \"Top %\"
            FROM dir_stats ds
            GROUP BY ds.directory
            ORDER BY \"Total Lines\" DESC"
        ),
        &[&snapshot_id],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Directory Bus Factor",
            &header_refs,
            &rows,
        ));
    }

    Ok(("Files".to_string(), content))
}

// ── Report: Identities ──────────────────────────────────────────────────────

fn report_identities(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("c.author_date");

    // Resolution summary — raw pairs, identities, bots, org coverage, trailer resolution
    let (headers, rows) = query_table(
        conn,
        "SELECT
            (SELECT count(*) FROM identity_aliases) AS raw_aliases,
            (SELECT count(*) FROM identities) AS identities,
            (SELECT count(*) FROM identities WHERE is_bot = 1) AS bots,
            (SELECT count(DISTINCT identity_id) FROM identity_affiliations) AS with_org,
            (SELECT count(*) FROM trailers t
             WHERE t.key IN ('Signed-off-by','Reviewed-by','Tested-by','Acked-by')
               AND t.identity_id IS NOT NULL) AS resolved_trailers,
            (SELECT count(*) FROM trailers t
             WHERE t.key IN ('Signed-off-by','Reviewed-by','Tested-by','Acked-by')) AS total_trailers",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Identity Resolution Summary",
            &header_refs,
            &rows,
        ));
    }

    // Identity table — canonical name/email alongside all raw aliases, with activity stats
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT
                i.canonical_name AS name,
                i.canonical_email AS email,
                COALESCE(vio.org, '') AS org,
                CASE WHEN i.is_bot = 1 THEN 'yes' ELSE '' END AS bot,
                aliases.alias_count AS aliases,
                aliases.emails AS alias_emails,
                COALESCE(commits.cnt, 0) AS commits,
                COALESCE(reviews.cnt, 0) AS reviews
             FROM identities i
             LEFT JOIN v_identity_org vio ON vio.identity_id = i.id
             LEFT JOIN (
                 SELECT ia.identity_id,
                        count(*) AS alias_count,
                        GROUP_CONCAT(ia.email, ', ') AS emails
                 FROM identity_aliases ia
                 GROUP BY ia.identity_id
             ) aliases ON aliases.identity_id = i.id
             LEFT JOIN (
                 SELECT c.author_id, count(*) AS cnt
                 FROM commits c WHERE 1=1{df}
                 GROUP BY c.author_id
             ) commits ON commits.author_id = i.id
             LEFT JOIN (
                 SELECT t.identity_id, count(*) AS cnt
                 FROM trailers t
                 JOIN commits c ON c.hash = t.commit_hash
                 WHERE t.key = 'Reviewed-by'{df}
                 GROUP BY t.identity_id
             ) reviews ON reviews.identity_id = i.id
             ORDER BY commits DESC, reviews DESC
             LIMIT 100"
        ),
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Identities (Top 100 by Commits)",
            &header_refs,
            &rows,
        ));
    }

    // Aliases per identity distribution — how many raw emails map to each identity
    let data = query_json_array(
        conn,
        "SELECT alias_count, count(*) AS identities
         FROM (
             SELECT ia.identity_id, count(*) AS alias_count
             FROM identity_aliases ia
             GROUP BY ia.identity_id
         )
         GROUP BY alias_count ORDER BY alias_count",
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 200,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "x": {"field": "alias_count", "type": "ordinal", "title": "Aliases per Identity"},
                "y": {"field": "identities", "type": "quantitative", "title": "Identities"},
                "color": {"value": "#4c72b0"},
                "tooltip": [
                    {"field": "alias_count", "type": "ordinal", "title": "Aliases"},
                    {"field": "identities", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "alias-distribution",
            "Alias Distribution (Emails per Identity)",
            &spec,
        ));
    }

    // Org distribution — identities per org
    let data = query_json_array(
        conn,
        "SELECT COALESCE(vio.org, 'Unaffiliated') AS org, count(*) AS identities
         FROM identities i
         LEFT JOIN v_identity_org vio ON vio.identity_id = i.id
         WHERE i.is_bot = 0
         GROUP BY org ORDER BY identities DESC",
        &[],
    )?;
    if data.len() > 1 {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "org", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "identities", "type": "quantitative", "title": "Contributors"},
                "color": {"field": "org", "type": "nominal", "legend": null},
                "tooltip": [
                    {"field": "org", "type": "nominal"},
                    {"field": "identities", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "org-distribution",
            "Contributors by Organization",
            &spec,
        ));
    }

    // Bot accounts
    let (headers, rows) = query_table(
        conn,
        "SELECT i.canonical_name AS name, i.canonical_email AS email,
                COALESCE(commits.cnt, 0) AS commits,
                COALESCE(trailers.cnt, 0) AS trailer_mentions
         FROM identities i
         LEFT JOIN (
             SELECT c.author_id, count(*) AS cnt FROM commits c GROUP BY c.author_id
         ) commits ON commits.author_id = i.id
         LEFT JOIN (
             SELECT t.identity_id, count(*) AS cnt FROM trailers t GROUP BY t.identity_id
         ) trailers ON trailers.identity_id = i.id
         WHERE i.is_bot = 1
         ORDER BY commits DESC",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section("Bot Accounts", &header_refs, &rows));
    }

    // Unresolved trailer values — identity trailers that couldn't be matched
    let (headers, rows) = query_table(
        conn,
        "SELECT t.key, t.value, count(*) AS occurrences
         FROM trailers t
         WHERE t.key IN ('Signed-off-by', 'Reviewed-by', 'Tested-by', 'Acked-by')
           AND t.identity_id IS NULL
         GROUP BY t.key, t.value
         ORDER BY occurrences DESC
         LIMIT 50",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Unresolved Trailer Values (Top 50)",
            &header_refs,
            &rows,
        ));
    }

    // Identities with most aliases — potential merge candidates or complex histories
    let (headers, rows) = query_table(
        conn,
        "SELECT i.canonical_name AS name, i.canonical_email AS email,
                COALESCE(vio.org, '') AS org,
                count(ia.email) AS alias_count,
                GROUP_CONCAT(ia.email, ', ') AS all_emails
         FROM identities i
         LEFT JOIN v_identity_org vio ON vio.identity_id = i.id
         JOIN identity_aliases ia ON ia.identity_id = i.id
         WHERE i.is_bot = 0
         GROUP BY i.id
         HAVING alias_count > 1
         ORDER BY alias_count DESC
         LIMIT 30",
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Multi-Alias Identities (Merged Emails)",
            &header_refs,
            &rows,
        ));
    }

    Ok(("Identities".to_string(), content))
}

fn report_releases(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("cr.release_date");

    // Check if we have any release data
    let release_count: i64 = conn.query_row("SELECT count(*) FROM tags", [], |r| r.get(0))?;
    if release_count == 0 {
        content.push_str(r#"<div class="chart-section"><h2>No Release Data</h2><p>No tags found. Run <code>logacy index</code> on a repository with tags.</p></div>"#);
        return Ok(("Releases".to_string(), content));
    }

    // Release summary table
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT t.name AS release,
                    substr(t.created_at, 1, 10) AS date,
                    count(DISTINCT cr.commit_hash) AS commits,
                    count(DISTINCT c.author_email) AS contributors,
                    COALESCE(sum(c.insertions), 0) AS insertions,
                    COALESCE(sum(c.deletions), 0) AS deletions,
                    CASE WHEN t.is_annotated THEN 'yes' ELSE 'no' END AS annotated
             FROM tags t
             LEFT JOIN commit_releases cr ON cr.release_tag = t.name
             LEFT JOIN commits c ON c.hash = cr.commit_hash
             WHERE 1=1 {}
             GROUP BY t.name
             ORDER BY t.created_at DESC
             LIMIT 50",
            df
        ),
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section("Release Summary", &header_refs, &rows));
    }

    // Commits per release (bar chart)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT t.name AS release, count(cr.commit_hash) AS commits
             FROM tags t
             LEFT JOIN commit_releases cr ON cr.release_tag = t.name
             WHERE 1=1 {}
             GROUP BY t.name
             HAVING commits > 0
             ORDER BY t.created_at ASC",
            df
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": 300,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "x": {"field": "release", "type": "nominal", "sort": null,
                       "axis": {"labelAngle": -45}},
                "y": {"field": "commits", "type": "quantitative", "title": "Commits"},
                "tooltip": [
                    {"field": "release", "type": "nominal"},
                    {"field": "commits", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("commits-per-release", "Commits per Release", &spec));
    }

    // Contributors per release (bar chart)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT t.name AS release, count(DISTINCT c.author_email) AS contributors
             FROM tags t
             JOIN commit_releases cr ON cr.release_tag = t.name
             JOIN commits c ON c.hash = cr.commit_hash
             WHERE 1=1 {}
             GROUP BY t.name
             ORDER BY t.created_at ASC",
            df
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": 300,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "x": {"field": "release", "type": "nominal", "sort": null,
                       "axis": {"labelAngle": -45}},
                "y": {"field": "contributors", "type": "quantitative", "title": "Contributors"},
                "color": {"value": "#e17055"},
                "tooltip": [
                    {"field": "release", "type": "nominal"},
                    {"field": "contributors", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "contributors-per-release",
            "Contributors per Release",
            &spec,
        ));
    }

    // Release cadence — days between consecutive releases (bar chart)
    let data = query_json_array(
        conn,
        "SELECT name AS release,
                CAST(julianday(created_at) - julianday(lag(created_at) OVER (ORDER BY created_at)) AS INTEGER) AS days
         FROM tags
         ORDER BY created_at ASC",
        &[],
    )?;
    // Filter out the first entry (NULL days) and any negative values
    let cadence_data: Vec<_> = data
        .into_iter()
        .filter(|v| {
            v.get("days")
                .and_then(|d| d.as_i64())
                .map(|d| d > 0)
                .unwrap_or(false)
        })
        .collect();
    if !cadence_data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": 300,
            "data": {"values": cadence_data},
            "mark": "bar",
            "encoding": {
                "x": {"field": "release", "type": "nominal", "sort": null,
                       "axis": {"labelAngle": -45}},
                "y": {"field": "days", "type": "quantitative", "title": "Days Since Previous Release"},
                "color": {"value": "#00b894"},
                "tooltip": [
                    {"field": "release", "type": "nominal"},
                    {"field": "days", "type": "quantitative", "title": "Days"}
                ]
            }
        });
        content.push_str(&vegalite_div("release-cadence", "Release Cadence", &spec));
    }

    Ok(("Releases".to_string(), content))
}

// ── Report: Hotspots ────────────────────────────────────────────────────────

fn report_hotspots(conn: &Connection, dr: &DateRange) -> Result<(String, String)> {
    let mut content = String::new();
    let df = dr.sql("c.author_date");

    // Section 1: Files by total hunk count (most-fragmented changes)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT ch.path, count(*) AS hunks, count(DISTINCT ch.commit_hash) AS commits,
                    round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks
             FROM commit_hunks ch
             JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
             WHERE 1=1{df}
             GROUP BY ch.path ORDER BY hunks DESC LIMIT 30"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 500,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "y": {"field": "path", "type": "nominal", "sort": "-x", "title": null},
                "x": {"field": "hunks", "type": "quantitative", "title": "Total Hunks"},
                "color": {"field": "avg_hunks", "type": "quantitative",
                          "scale": {"scheme": "orangered"}, "title": "Avg Hunks/Commit"},
                "tooltip": [
                    {"field": "path", "type": "nominal"},
                    {"field": "hunks", "type": "quantitative", "title": "Total Hunks"},
                    {"field": "commits", "type": "quantitative"},
                    {"field": "avg_hunks", "type": "quantitative", "title": "Avg Hunks/Commit"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "hunk-hotspots",
            "Most-Fragmented Files (Total Hunks)",
            &spec,
        ));
    }

    // Section 2: Hunk scatter — complexity vs churn
    let scatter = query_json_array(
        conn,
        &format!(
            "SELECT ch.path,
                    count(DISTINCT ch.commit_hash) AS commits,
                    round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks,
                    sum(ch.new_lines + ch.old_lines) AS total_churn
             FROM commit_hunks ch
             JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
             WHERE 1=1{df}
             GROUP BY ch.path
             HAVING commits >= 5
             ORDER BY total_churn DESC LIMIT 200"
        ),
        &[],
    )?;
    if !scatter.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 400,
            "data": {"values": scatter},
            "mark": {"type": "circle", "opacity": 0.7},
            "encoding": {
                "x": {"field": "commits", "type": "quantitative", "title": "Commits", "scale": {"type": "log"}},
                "y": {"field": "avg_hunks", "type": "quantitative", "title": "Avg Hunks per Commit"},
                "size": {"field": "total_churn", "type": "quantitative", "title": "Total Churn (lines)"},
                "color": {"field": "avg_hunks", "type": "quantitative",
                          "scale": {"scheme": "orangered"}, "legend": null},
                "tooltip": [
                    {"field": "path", "type": "nominal"},
                    {"field": "commits", "type": "quantitative"},
                    {"field": "avg_hunks", "type": "quantitative"},
                    {"field": "total_churn", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "hunk-scatter",
            "Complexity vs Churn (files with 5+ commits)",
            &spec,
        ));
    }

    // Section 3: Most-touched line regions across all files
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT ch.path AS File,
                    ch.new_start AS Line,
                    ch.new_lines AS Span,
                    count(DISTINCT ch.commit_hash) AS Touches,
                    min(c.author_date) AS \"First Touch\",
                    max(c.author_date) AS \"Last Touch\"
             FROM commit_hunks ch
             JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
             WHERE ch.new_lines > 0{df}
             GROUP BY ch.path, ch.new_start
             ORDER BY Touches DESC LIMIT 50"
        ),
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Most-Touched Line Regions (Top 50)",
            &header_refs,
            &rows,
        ));
    }

    // Section 4: Hunk size distribution
    let data = query_json_array(
        conn,
        &format!(
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
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": "bar",
            "encoding": {
                "x": {"field": "size_bucket", "type": "ordinal", "title": "Hunk Size (lines changed)",
                       "sort": ["1", "2-5", "6-20", "21-50", "51-100", "100+"]},
                "y": {"field": "hunks", "type": "quantitative", "title": "Count"},
                "color": {"value": "#6c5ce7"},
                "tooltip": [
                    {"field": "size_bucket", "type": "ordinal", "title": "Size"},
                    {"field": "hunks", "type": "quantitative"}
                ]
            }
        });
        content.push_str(&vegalite_div("hunk-size-dist", "Hunk Size Distribution", &spec));
    }

    // Section 5: Hunks per commit over time (monthly trend)
    let data = query_json_array(
        conn,
        &format!(
            "SELECT strftime('%Y-%m', c.author_date) AS month,
                    round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS avg_hunks,
                    count(*) AS total_hunks
             FROM commit_hunks ch
             JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
             WHERE 1=1{df}
             GROUP BY month ORDER BY month"
        ),
        &[],
    )?;
    if !data.is_empty() {
        let spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container", "height": 300,
            "data": {"values": data},
            "mark": {"type": "line", "interpolate": "monotone"},
            "encoding": {
                "x": {"field": "month", "type": "temporal", "title": "Month"},
                "y": {"field": "avg_hunks", "type": "quantitative", "title": "Avg Hunks per Commit"},
                "tooltip": [
                    {"field": "month", "type": "temporal"},
                    {"field": "avg_hunks", "type": "quantitative"},
                    {"field": "total_hunks", "type": "quantitative", "title": "Total Hunks"}
                ]
            }
        });
        content.push_str(&vegalite_div(
            "hunk-trend",
            "Change Fragmentation Over Time (Avg Hunks per Commit)",
            &spec,
        ));
    }

    // Section 6: Files with highest avg hunks per commit (most scattered changes)
    let (headers, rows) = query_table(
        conn,
        &format!(
            "SELECT ch.path AS File,
                    count(DISTINCT ch.commit_hash) AS Commits,
                    count(*) AS \"Total Hunks\",
                    round(count(*) * 1.0 / count(DISTINCT ch.commit_hash), 1) AS \"Avg Hunks/Commit\",
                    round(avg(ch.new_lines + ch.old_lines), 1) AS \"Avg Hunk Size\"
             FROM commit_hunks ch
             JOIN commits c ON c.hash = ch.commit_hash AND c.first_parent = 1
             WHERE 1=1{df}
             GROUP BY ch.path
             HAVING count(DISTINCT ch.commit_hash) >= 10
             ORDER BY \"Avg Hunks/Commit\" DESC LIMIT 30"
        ),
        &[],
    )?;
    if !rows.is_empty() {
        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
        content.push_str(&html_table_section(
            "Most Scattered Changes (10+ commits, by Avg Hunks/Commit)",
            &header_refs,
            &rows,
        ));
    }

    Ok(("Hotspots".to_string(), content))
}
