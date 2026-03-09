use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use logacy_core::LogacyContext;

#[derive(Parser)]
#[command(name = "logacy", about = "Git repository analytics engine")]
struct Cli {
    /// Path to git repository
    #[arg(long, global = true)]
    repo: Option<PathBuf>,

    /// Path to database
    #[arg(long, global = true)]
    db: Option<PathBuf>,

    /// Path to config file
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Increase verbosity (-v, -vv)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Suppress non-error output
    #[arg(short, long, global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize logacy for a repository
    Init {
        /// Overwrite existing database
        #[arg(long)]
        force: bool,
    },

    /// Materialize git history into the database
    Index {
        /// Full reindex (discard existing data)
        #[arg(long)]
        full: bool,

        /// Follow only first-parent commits
        #[arg(long)]
        first_parent: bool,

        /// Follow all commits (not just first-parent)
        #[arg(long, conflicts_with = "first_parent")]
        all: bool,
    },

    /// Run SQL against the logacy database
    Query {
        /// SQL query to execute
        sql: Option<String>,

        /// Read SQL from file
        #[arg(long)]
        file: Option<PathBuf>,

        /// Output format
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Resolve author/committer identities via .mailmap
    Identity,

    /// Import MAINTAINERS file and map files to subsystems
    Maintain,

    /// Take a git-blame snapshot at HEAD
    Blame,

    /// Generate HTML reports with embedded Vega-Lite charts
    Report {
        /// Report template (overview, contributors, subsystems, reviews, ownership)
        #[arg(long)]
        template: Option<String>,

        /// Output directory
        #[arg(long)]
        output: Option<PathBuf>,

        /// Generate all reports
        #[arg(long)]
        all: bool,

        /// Only include commits on or after this date (YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,

        /// Only include commits before this date (YYYY-MM-DD)
        #[arg(long)]
        until: Option<String>,
    },

    /// Show indexing state
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up tracing
    let level = if cli.quiet {
        "error"
    } else {
        match cli.verbose {
            0 => "warn",
            1 => "info",
            _ => "debug",
        }
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .with_target(false)
        .init();

    let ctx = LogacyContext::discover(
        cli.repo.as_deref(),
        cli.db.as_deref(),
        cli.config.as_deref(),
    )?;

    match cli.command {
        Commands::Init { force } => cmd_init(&ctx, force),
        Commands::Index { full, first_parent, all } => {
            cmd_index(&ctx, full, first_parent, all)
        }
        Commands::Query { sql, file, format } => cmd_query(&ctx, sql, file, &format),
        Commands::Identity => cmd_identity(&ctx),
        Commands::Maintain => cmd_maintain(&ctx),
        Commands::Blame => cmd_blame(&ctx),
        Commands::Report { template, output, all, since, until } => {
            cmd_report(&ctx, template, output, all, since, until)
        }
        Commands::Status => cmd_status(&ctx),
    }
}

fn cmd_init(ctx: &LogacyContext, force: bool) -> Result<()> {
    let logacy_dir = ctx.logacy_dir();

    if ctx.db_path.exists() && !force {
        anyhow::bail!(
            "database already exists at {}. Use --force to overwrite.",
            ctx.db_path.display()
        );
    }

    std::fs::create_dir_all(&logacy_dir)
        .with_context(|| format!("failed to create {}", logacy_dir.display()))?;

    if force && ctx.db_path.exists() {
        std::fs::remove_file(&ctx.db_path)?;
    }

    let conn = logacy_db::open(&ctx.db_path)?;
    logacy_db::create_schema(&conn)?;

    println!("Initialized logacy database at {}", ctx.db_path.display());
    Ok(())
}

fn cmd_index(ctx: &LogacyContext, full: bool, first_parent: bool, all: bool) -> Result<()> {
    let config = ctx.load_config()?;
    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;

    let use_first_parent = if all {
        false
    } else if first_parent {
        true
    } else {
        config.index.first_parent
    };

    let opts = logacy_index::IndexOptions {
        full,
        first_parent: use_first_parent,
        include_diff_stats: config.index.include_diff_stats,
        include_file_list: config.index.include_file_list,
    };

    logacy_index::run_index(&ctx.repo_path, &conn, &config, &opts)
}

fn cmd_query(ctx: &LogacyContext, sql: Option<String>, file: Option<PathBuf>, format: &str) -> Result<()> {
    let query = match (sql, file) {
        (Some(s), _) => s,
        (None, Some(f)) => std::fs::read_to_string(&f)
            .with_context(|| format!("failed to read SQL file {}", f.display()))?,
        (None, None) => anyhow::bail!("provide SQL as an argument or via --file"),
    };

    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;

    let mut stmt = conn.prepare(&query)?;
    let column_count = stmt.column_count();
    let column_names: Vec<String> = (0..column_count)
        .map(|i| stmt.column_name(i).unwrap().to_string())
        .collect();

    let rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..column_count {
                let val: String = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => "NULL".to_string(),
                    Ok(rusqlite::types::ValueRef::Integer(n)) => n.to_string(),
                    Ok(rusqlite::types::ValueRef::Real(f)) => format!("{:.4}", f),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        String::from_utf8_lossy(s).to_string()
                    }
                    Ok(rusqlite::types::ValueRef::Blob(b)) => format!("<blob {}>", b.len()),
                    Err(_) => "?".to_string(),
                };
                vals.push(val);
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;

    match format {
        "csv" => {
            println!("{}", column_names.join(","));
            for row in &rows {
                println!("{}", row.join(","));
            }
        }
        "json" => {
            let json_rows: Vec<serde_json::Value> = rows
                .iter()
                .map(|row| {
                    let mut map = serde_json::Map::new();
                    for (i, val) in row.iter().enumerate() {
                        map.insert(
                            column_names[i].clone(),
                            serde_json::Value::String(val.clone()),
                        );
                    }
                    serde_json::Value::Object(map)
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&json_rows).unwrap());
        }
        "jsonl" => {
            for row in &rows {
                let mut map = serde_json::Map::new();
                for (i, val) in row.iter().enumerate() {
                    map.insert(
                        column_names[i].clone(),
                        serde_json::Value::String(val.clone()),
                    );
                }
                println!("{}", serde_json::Value::Object(map));
            }
        }
        _ => {
            // table format
            let mut table = comfy_table::Table::new();
            table.set_header(&column_names);
            for row in &rows {
                table.add_row(row);
            }
            println!("{table}");
        }
    }

    println!("({} rows)", rows.len());
    Ok(())
}

fn cmd_identity(ctx: &LogacyContext) -> Result<()> {
    let config = ctx.load_config()?;
    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;
    logacy_index::identity::run_identity(&ctx.repo_path, &conn, &config)
}

fn cmd_maintain(ctx: &LogacyContext) -> Result<()> {
    let config = ctx.load_config()?;
    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;
    logacy_maintain::run_maintain(&ctx.repo_path, &conn, &config)
}

fn cmd_blame(ctx: &LogacyContext) -> Result<()> {
    let config = ctx.load_config()?;
    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;
    logacy_blame::run_blame(&ctx.repo_path, &conn, &config)
}

fn cmd_report(
    ctx: &LogacyContext,
    template: Option<String>,
    output: Option<PathBuf>,
    all: bool,
    since: Option<String>,
    until: Option<String>,
) -> Result<()> {
    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;
    let output_dir = output.unwrap_or_else(|| ctx.logacy_dir().join("reports"));
    let range = logacy_report::DateRange { since, until };

    let templates: Vec<&str> = if all {
        logacy_report::TEMPLATES.to_vec()
    } else if let Some(ref t) = template {
        vec![t.as_str()]
    } else {
        vec!["overview"]
    };

    for tmpl in &templates {
        let path = logacy_report::run_report(&conn, tmpl, &output_dir, &range)?;
        println!("Generated {}", path.display());
    }

    Ok(())
}

fn cmd_status(ctx: &LogacyContext) -> Result<()> {
    if !ctx.db_path.exists() {
        println!("logacy not initialized. Run `logacy init` first.");
        return Ok(());
    }

    let conn = logacy_db::open_and_migrate(&ctx.db_path)?;

    let schema_version = logacy_db::get_meta(&conn, "schema_version")?
        .unwrap_or_else(|| "unknown".to_string());
    let last_commit = logacy_db::get_meta(&conn, "last_indexed_commit")?
        .unwrap_or_else(|| "none".to_string());

    let commit_count: i64 = conn.query_row("SELECT count(*) FROM commits", [], |r| r.get(0))?;

    let date_range: (String, String) = if commit_count > 0 {
        let min: String = conn.query_row(
            "SELECT min(author_date) FROM commits",
            [],
            |r| r.get(0),
        )?;
        let max: String = conn.query_row(
            "SELECT max(author_date) FROM commits",
            [],
            |r| r.get(0),
        )?;
        (min, max)
    } else {
        ("N/A".to_string(), "N/A".to_string())
    };

    let trailer_count: i64 = conn.query_row("SELECT count(*) FROM trailers", [], |r| r.get(0))?;
    let file_count: i64 = conn.query_row("SELECT count(*) FROM commit_files", [], |r| r.get(0))?;
    let identity_count: i64 = conn.query_row("SELECT count(*) FROM identities", [], |r| r.get(0))?;
    let subsystem_count: i64 = conn.query_row("SELECT count(*) FROM subsystems", [], |r| r.get(0))?;
    let blame_snapshot_count: i64 = conn.query_row("SELECT count(*) FROM blame_snapshots", [], |r| r.get(0))?;
    let blame_line_count: i64 = conn.query_row("SELECT count(*) FROM blame_lines", [], |r| r.get(0))?;

    println!("logacy status");
    println!("  Database:       {}", ctx.db_path.display());
    println!("  Schema version: {}", schema_version);
    println!("  Last indexed:   {}", if last_commit.is_empty() { "none" } else { &last_commit });
    println!("  Commits:        {}", commit_count);
    println!("  Trailers:       {}", trailer_count);
    println!("  File records:   {}", file_count);
    println!("  Identities:     {}", identity_count);
    println!("  Subsystems:     {}", subsystem_count);
    println!("  Blame snapshots:{}", blame_snapshot_count);
    println!("  Blame lines:    {}", blame_line_count);
    println!("  Date range:     {} .. {}", date_range.0, date_range.1);

    Ok(())
}
