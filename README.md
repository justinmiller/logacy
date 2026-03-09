# logacy

A single Rust binary that materializes git history into a queryable SQLite
database — commits, identities, file-level diffs, trailers, subsystem mappings,
and line-level blame — then generates self-contained HTML reports with
interactive charts.

logacy reads a git repository and writes structured, queryable data to a
local SQLite database. It parses commits, resolves contributor identities
across aliases and organizations via `.mailmap`, extracts ticket references and
component tags from commit subjects, computes per-file diff statistics, indexes
commit trailers (Reviewed-by, Signed-off-by, Tested-by, etc.), parses
MAINTAINERS files to map files to subsystems, and runs parallel git blame to
compute code ownership snapshots.

The database is the primary output. Query it with `logacy query`, `sqlite3`,
DuckDB, Jupyter, Grafana, or any SQLite-compatible tool. Or generate
self-contained HTML reports with embedded Vega-Lite charts.

## Quick Start

```sh
cargo install --path crates/logacy-cli

cd /path/to/your/repo
logacy init
logacy index --full
logacy identity
logacy maintain          # if repo has a MAINTAINERS file
logacy blame
logacy report --all
logacy query "SELECT count(*) FROM commits"
```

## Design Principles

- **Data-first** — The SQLite database is the primary output. Reports and
  dashboards are consumption layers over a stable schema.
- **Incremental by default** — After the initial index, only new commits are
  processed.
- **Single binary, subcommands** — One `cargo install` gives you everything.
  Unix philosophy at the data layer (composable tables, SQL as glue).
- **No runtime dependencies** — No Python, Node, or external services. SQLite
  is statically linked. Report JS is embedded in the binary.
- **Temporal accuracy** — Organizations acquire each other, contributors change
  employers, identities merge. The data model captures this with first-class
  time ranges.

## Architecture

### Cargo Workspace

```
logacy/
  Cargo.toml                    # workspace root
  crates/
    logacy-cli/              # binary entry point, clap CLI
    logacy-core/             # shared types, config loading
    logacy-db/               # schema, migrations, query helpers
    logacy-index/            # commit/trailer/identity materialization
    logacy-blame/            # git blame engine (parallel, via git2)
    logacy-maintain/         # MAINTAINERS parser, subsystem mapping
    logacy-report/           # HTML report generation (Vega-Lite)
```

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `gix` (gitoxide) | Commit walking, tree diffs, mailmap parsing |
| `git2` (libgit2) | Blame (mature rename/copy tracking) |
| `rusqlite` | Embedded SQLite (bundled, no system dependency) |
| `clap` | CLI argument parsing with derive macros |
| `regex` | Ticket/component extraction from commit subjects |
| `rayon` | Parallel blame execution |
| `glob` | MAINTAINERS file pattern matching |
| `minijinja` | HTML report templating |
| `comfy-table` | Terminal table output |
| `indicatif` | Progress bars during indexing |
| `chrono` | Datetime handling |
| `serde` + `toml` | Configuration file parsing |
| `serde_json` | JSON output, Vega-Lite spec generation |

## Subcommands

### `logacy init`

Creates the `.logacy/` directory and initializes the SQLite database with
the full schema.

```sh
logacy init            # fails if database already exists
logacy init --force    # overwrites existing database
```

### `logacy index`

Materializes git history into the database. Walks commits from HEAD, extracts
metadata, parses trailers, and computes per-file diff statistics.

```sh
logacy index           # incremental (only new commits since last run)
logacy index --full    # full reindex (clears and rebuilds all tables)
logacy index --all     # follow all commits, not just first-parent
```

**What it indexes:**

- Commit metadata: hash, author, committer, dates, subject, body
- Ticket and component extraction from commit subjects (configurable regex)
- Commit trailers: Signed-off-by, Reviewed-by, Tested-by, Change-Id, etc.
- Per-file diff statistics: path, status (A/M/D/R), insertions, deletions
- File classification: language (from extension/filename) and category
  (source, test, docs, build — from path heuristics)
- Aggregate insertions/deletions per commit

**Incremental indexing** is the default. logacy records `last_indexed_commit`
and on subsequent runs only processes commits between HEAD and that marker.

### `logacy identity`

Resolves contributor identities by importing `.mailmap`, merging aliases, and
backfilling foreign keys across the database.

```sh
logacy identity
```

**Resolution pipeline:**

1. Collects all distinct (name, email) pairs from commits and identity trailers
2. Resolves each pair through `.mailmap` to canonical (name, email)
3. Merges identities sharing the same canonical name across different emails
   (handles employer changes: e.g., `user@sun.com` and `user@oracle.com`)
4. Picks the most recently used email as the canonical email
5. Marks bots from configured email/name patterns
6. Backfills `author_id` and `committer_id` on all commits
7. Resolves `identity_id` on identity trailers (Signed-off-by, Reviewed-by, etc.)
8. Loads temporal org-domain mappings from config

### `logacy maintain`

Parses a MAINTAINERS file (Linux kernel format) and maps repository files to
subsystems. Resolves reviewer names/emails to identity IDs.

```sh
logacy maintain
```

**What it does:**

1. Reads the MAINTAINERS file (path from config, default `MAINTAINERS`)
2. Parses subsystem blocks: name, status (S:), reviewers (R:), file patterns
   (F:), exclude patterns (X:)
3. Resolves reviewer emails/names to `identity_id` values
4. Glob-matches F:/X: patterns against `commit_files` paths to build
   `file_subsystems` mapping
5. Creates `v_subsystem_contributors` view ranking contributors per subsystem

### `logacy blame`

Computes git blame snapshots for all files at HEAD, producing line-level
attribution and aggregated code ownership fractions.

```sh
logacy blame
```

**How it works:**

- Uses `git2` (libgit2) for blame — mature rename/copy tracking
- Parallel execution via rayon (each thread opens its own `git2::Repository`)
- Filters out files matching `exclude_paths` globs, `binary_extensions`, or
  exceeding `max_file_size`
- Idempotent: skips if a snapshot already exists for the HEAD commit
- Produces `blame_lines` (per-line attribution) and `file_ownership`
  (aggregated lines/fraction per identity per file)

### `logacy report`

Generates self-contained HTML reports with embedded Vega-Lite charts. All
JavaScript is bundled into the binary — no CDN or network requests required.

```sh
logacy report --all                          # generate all 5 reports
logacy report --template overview            # single report
logacy report --template contributors --since 2024-01-01
logacy report --output /tmp/reports --all    # custom output directory
```

**Available templates:**

| Template | Description |
|----------|-------------|
| `overview` | Project dashboard: commits over time, top contributors, org share, subsystem breakdown, language distribution, work type breakdown, commit activity heatmap |
| `contributors` | Per-author detail: commit counts, review counts, subsystem involvement, tenure, language profile |
| `subsystems` | Per-subsystem health: activity, contributor count, reviewer coverage, bus factor, dormant subsystem detection, unmapped files, maintainer summary |
| `reviews` | Review network: who reviews whom, review counts, cross-org patterns |
| `ownership` | Blame-based ownership: lines by author/org/subsystem, code age distribution |

Reports are written to `.logacy/reports/` by default (one HTML file per template).

### `logacy query`

Runs arbitrary SQL against the database with multiple output formats.

```sh
logacy query "SELECT count(*) FROM commits"
logacy query --format csv "SELECT * FROM commits LIMIT 10"
logacy query --format json "SELECT * FROM identities WHERE is_bot = 1"
logacy query --format jsonl "SELECT hash, subject FROM commits"
logacy query --file report.sql
```

**Output formats:**

| Format | Description |
|--------|-------------|
| `table` | ASCII box-drawing table (default) |
| `csv` | Comma-separated values with header row |
| `json` | Pretty-printed JSON array of objects |
| `jsonl` | One JSON object per line (streaming) |

### `logacy status`

Shows the current state of the database.

```
logacy status
  Database:       .logacy/logacy.db
  Schema version: 1
  Last indexed:   ac1c006f9916...
  Commits:        26808
  Trailers:       137422
  File records:   117503
  Identities:     499
  Subsystems:     52
  Blame snapshot: ac1c006f9916...
  Date range:     1999-01-23T... .. 2026-02-28T...
```

## Global Options

```
--repo <PATH>       Path to git repository (default: current directory)
--db <PATH>         Path to database (default: .logacy/logacy.db)
--config <PATH>     Path to config file (default: logacy.toml)
-v, --verbose       Increase verbosity (-v info, -vv debug)
-q, --quiet         Suppress non-error output
```

## Configuration

logacy is configured via a `logacy.toml` file at the repository root.
All sections are optional; sensible defaults are applied.

```toml
[repository]
# Regex with capture group to extract ticket ID from commit subject
ticket_pattern = 'LU-(\d+)'
# Regex with capture group to extract component from commit subject
component_pattern = 'LU-\d+\s+([^:]+):'

[trailers]
# Trailers representing people (get identity_id resolved)
identity_keys = ["Signed-off-by", "Reviewed-by", "Tested-by", "Acked-by"]
# Trailers representing metadata (stored as-is)
metadata_keys = ["Change-Id", "Reviewed-on", "Test-Parameters", "Fixes"]

[identity]
mailmap = true  # import .mailmap (default: true)
bot_emails = ["ci@example.com"]
bot_names = ["jenkins"]

# Temporal org-domain mapping (handles acquisitions)
[[identity.orgs]]
domain = "oldcompany.com"
org = "Old Company"
until = "2010-01-01"

[[identity.orgs]]
domain = "oldcompany.com"
org = "Acquiring Company"
from = "2010-01-01"

# Multiple domains for same org
[[identity.orgs]]
domains = ["whamcloud.com", "ddn.com"]
org = "Whamcloud/DDN"

[index]
first_parent = true        # follow only first-parent commits (default)
include_diff_stats = true  # compute insertions/deletions per file (default)
include_file_list = true   # record changed file paths per commit (default)

[maintainers]
file = "MAINTAINERS"       # path to MAINTAINERS file
format = "linux"           # parser format

[blame]
exclude_paths = ["**/*.am", "**/Makefile"]
max_file_size = 1_048_576  # skip files larger than 1MB
binary_extensions = [".o", ".so", ".png", ".jpg"]
```

## Database Schema

The database lives at `.logacy/logacy.db` (SQLite with WAL mode). All tables
use `INSERT OR IGNORE` semantics for idempotent reindexing.

### Tables

**`commits`** — One row per indexed commit.

| Column | Type | Description |
|--------|------|-------------|
| `hash` | TEXT PK | Full commit SHA |
| `author_name` | TEXT | Raw author name from git |
| `author_email` | TEXT | Raw author email from git |
| `committer_name` | TEXT | Raw committer name from git |
| `committer_email` | TEXT | Raw committer email from git |
| `author_id` | INTEGER FK | Resolved identity (populated by `identity`) |
| `committer_id` | INTEGER FK | Resolved identity (populated by `identity`) |
| `author_date` | TEXT | ISO 8601 datetime |
| `commit_date` | TEXT | ISO 8601 datetime |
| `subject` | TEXT | First line of commit message |
| `body` | TEXT | Remainder of commit message |
| `ticket` | TEXT | Extracted ticket ID (e.g., "6142") |
| `component` | TEXT | Extracted component name |
| `is_merge` | INTEGER | 1 if merge commit |
| `first_parent` | INTEGER | 1 if reached via first-parent walk |
| `insertions` | INTEGER | Total lines added |
| `deletions` | INTEGER | Total lines removed |

**`trailers`** — Parsed commit trailers (Signed-off-by, Reviewed-by, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `commit_hash` | TEXT FK | References commits(hash) |
| `key` | TEXT | Trailer key (e.g., "Reviewed-by") |
| `value` | TEXT | Trailer value (e.g., "Jane Doe \<jane@example.com\>") |
| `identity_id` | INTEGER FK | Resolved identity for identity trailers |
| `seq` | INTEGER | Order within commit |

**`commit_files`** — Per-file diff statistics for each commit.

| Column | Type | Description |
|--------|------|-------------|
| `commit_hash` | TEXT FK | References commits(hash) |
| `path` | TEXT | File path |
| `status` | TEXT | A (add), M (modify), D (delete), R (rename) |
| `insertions` | INTEGER | Lines added in this file |
| `deletions` | INTEGER | Lines removed in this file |
| `language` | TEXT | Detected language (e.g., C, Python, Rust, Shell, Other) |
| `category` | TEXT | File category: source, test, docs, or build |

**`identities`** — Canonical contributor identities.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Identity ID |
| `canonical_name` | TEXT | Resolved display name |
| `canonical_email` | TEXT | Resolved email (most recently used) |
| `is_bot` | INTEGER | 1 if bot account |
| `org` | TEXT | Organization |

**`identity_aliases`** — Maps raw emails to canonical identities.

| Column | Type | Description |
|--------|------|-------------|
| `identity_id` | INTEGER FK | References identities(id) |
| `name` | TEXT | Raw name variant |
| `email` | TEXT PK | Raw email (lookup key) |

**`org_domains`** — Temporal organization mapping for email domains.

| Column | Type | Description |
|--------|------|-------------|
| `domain` | TEXT | Email domain |
| `org` | TEXT | Organization name |
| `valid_from` | TEXT | Start date (NULL = beginning of time) |
| `valid_until` | TEXT | End date (NULL = still current) |

**`subsystems`** — Subsystem definitions from MAINTAINERS.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Subsystem ID |
| `name` | TEXT UNIQUE | Subsystem name |
| `status` | TEXT | Maintenance status (e.g., Maintained, Supported) |
| `updated_at` | TEXT | Last update timestamp |

**`subsystem_reviewers`** — Maps subsystems to their designated reviewers.

| Column | Type | Description |
|--------|------|-------------|
| `subsystem_id` | INTEGER FK | References subsystems(id) |
| `identity_id` | INTEGER FK | References identities(id) |

**`subsystem_paths`** — File patterns that define each subsystem's scope.

| Column | Type | Description |
|--------|------|-------------|
| `subsystem_id` | INTEGER FK | References subsystems(id) |
| `pattern` | TEXT | Glob pattern (F: entries) |
| `is_exclude` | INTEGER | 1 for exclude patterns (X: entries) |

**`file_subsystems`** — Materialized file-to-subsystem mapping.

| Column | Type | Description |
|--------|------|-------------|
| `path` | TEXT | File path |
| `subsystem_id` | INTEGER FK | References subsystems(id) |

**`blame_snapshots`** — Blame snapshot metadata.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Snapshot ID |
| `commit_hash` | TEXT UNIQUE | Commit SHA this snapshot was taken at |
| `created_at` | TEXT | Creation timestamp |

**`blame_lines`** — Line-level blame attribution.

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_id` | INTEGER FK | References blame_snapshots(id) |
| `path` | TEXT | File path |
| `line_number` | INTEGER | Line number in file |
| `orig_commit` | TEXT | Commit that last modified this line |
| `identity_id` | INTEGER FK | References identities(id) |

**`file_ownership`** — Aggregated code ownership per file.

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_id` | INTEGER FK | References blame_snapshots(id) |
| `path` | TEXT | File path |
| `identity_id` | INTEGER FK | References identities(id) |
| `lines_owned` | INTEGER | Number of lines attributed to this identity |
| `fraction` | REAL | Fraction of file owned (0.0–1.0) |

### Views

**`v_commits`** — Commits with resolved identity names and temporal org
lookup. Falls back to raw author_name/author_email when identity resolution
hasn't been run. Org is resolved via a correlated subquery against `org_domains`
with date bounds, falling back to `identities.org`.

```sql
SELECT resolved_author_name, resolved_author_email, author_org, author_is_bot,
       -- plus all columns from commits
FROM v_commits
```

**`v_reviews`** — Review relationships (author + reviewer pairs per commit).
Filters out bots. Includes org for both author and reviewer.

**`v_subsystem_activity`** — Commit activity joined through file paths to
subsystems via `file_subsystems` (requires `maintain` import).

**`v_subsystem_contributors`** — Ranks contributors per subsystem by commit
count. Includes an `is_reviewer` flag indicating whether the contributor is
a designated reviewer for that subsystem.

### Indexes

Indexes on `commits(author_date)`, `commits(ticket)`, `commits(component)`,
`commits(author_id)`, `trailers(key)`, `trailers(identity_id)`,
`commit_files(path)`, `commit_files(language)`, `blame_lines(identity_id)`,
`blame_lines(orig_commit)`.

## Example Queries

### Top contributors by commit count

```sql
SELECT i.canonical_name, count(*) as commits
FROM commits c
JOIN identities i ON c.author_id = i.id
WHERE i.is_bot = 0
GROUP BY i.id
ORDER BY commits DESC
LIMIT 10;
```

### Language breakdown

```sql
SELECT language, count(*) as file_changes,
       sum(insertions) as lines_added
FROM commit_files
WHERE language != 'Other'
GROUP BY language
ORDER BY file_changes DESC;
```

### Work type breakdown (test/docs/build/source)

```sql
SELECT category,
       count(*) as file_changes,
       sum(insertions) + sum(deletions) as lines_changed
FROM commit_files
GROUP BY category
ORDER BY lines_changed DESC;
```

### Most changed files

```sql
SELECT path, count(*) as changes,
       sum(insertions) as total_ins,
       sum(deletions) as total_del
FROM commit_files
GROUP BY path
ORDER BY changes DESC
LIMIT 10;
```

### Top reviewers

```sql
SELECT i.canonical_name, count(*) as reviews
FROM trailers t
JOIN identities i ON t.identity_id = i.id
WHERE t.key = 'Reviewed-by' AND i.is_bot = 0
GROUP BY i.id
ORDER BY reviews DESC
LIMIT 10;
```

### Contributors to a specific subsystem/directory

```sql
SELECT i.canonical_name as author,
       count(DISTINCT c.hash) as commits,
       sum(cf.insertions) as lines_added,
       count(DISTINCT cf.path) as files_touched
FROM commits c
JOIN identities i ON c.author_id = i.id
JOIN commit_files cf ON cf.commit_hash = c.hash
WHERE cf.path LIKE 'src/module/%'
  AND i.is_bot = 0
GROUP BY i.id
ORDER BY commits DESC
LIMIT 10;
```

### Dominant contributor per year

```sql
WITH yearly AS (
    SELECT strftime('%Y', c.author_date) as year,
           i.canonical_name as author,
           count(*) as commits
    FROM commits c
    JOIN identities i ON c.author_id = i.id
    WHERE i.is_bot = 0
    GROUP BY year, i.id
),
ranked AS (
    SELECT year, author, commits,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY commits DESC) as rn
    FROM yearly
)
SELECT year, author, commits
FROM ranked WHERE rn = 1
ORDER BY year;
```

### Commits per ticket

```sql
SELECT ticket, count(*) as commits,
       min(author_date) as first_commit,
       max(author_date) as last_commit
FROM commits
WHERE ticket IS NOT NULL
GROUP BY ticket
ORDER BY commits DESC
LIMIT 10;
```

### Review network (who reviews whom)

```sql
SELECT author_i.canonical_name as author,
       reviewer_i.canonical_name as reviewer,
       count(*) as reviews
FROM commits c
JOIN identities author_i ON c.author_id = author_i.id
JOIN trailers t ON t.commit_hash = c.hash AND t.key = 'Reviewed-by'
JOIN identities reviewer_i ON t.identity_id = reviewer_i.id
WHERE author_i.is_bot = 0 AND reviewer_i.is_bot = 0
GROUP BY author_i.id, reviewer_i.id
ORDER BY reviews DESC
LIMIT 20;
```

### Subsystem bus factor (contributors for 80% of commits)

```sql
WITH sub_commits AS (
    SELECT s.name as subsystem, i.canonical_name as author,
           count(DISTINCT c.hash) as commits
    FROM file_subsystems fs
    JOIN subsystems s ON fs.subsystem_id = s.id
    JOIN commit_files cf ON cf.path = fs.path
    JOIN commits c ON cf.commit_hash = c.hash
    JOIN identities i ON c.author_id = i.id
    WHERE i.is_bot = 0
    GROUP BY s.id, i.id
),
sub_totals AS (
    SELECT subsystem, sum(commits) as total FROM sub_commits GROUP BY subsystem
),
ranked AS (
    SELECT sc.subsystem, sc.author, sc.commits,
           1.0 * sum(sc.commits) OVER (
               PARTITION BY sc.subsystem ORDER BY sc.commits DESC
           ) / st.total as cumulative_frac
    FROM sub_commits sc
    JOIN sub_totals st ON sc.subsystem = st.subsystem
)
SELECT subsystem, count(*) as contributors_for_80pct
FROM ranked
WHERE cumulative_frac <= 0.80 OR commits = (
    SELECT max(commits) FROM sub_commits sc2 WHERE sc2.subsystem = ranked.subsystem
)
GROUP BY subsystem
ORDER BY contributors_for_80pct;
```

### Top code owners by lines

```sql
SELECT i.canonical_name, sum(fo.lines_owned) as total_lines,
       count(DISTINCT fo.path) as files_owned
FROM file_ownership fo
JOIN identities i ON fo.identity_id = i.id
JOIN blame_snapshots bs ON fo.snapshot_id = bs.id
WHERE i.is_bot = 0
GROUP BY i.id
ORDER BY total_lines DESC
LIMIT 10;
```

### Cross-org review patterns

```sql
SELECT author_org, reviewer_org, count(*) as reviews
FROM v_reviews
GROUP BY author_org, reviewer_org
ORDER BY reviews DESC
LIMIT 20;
```

## Implementation Status

| Phase | Status | Description |
|-------|--------|-------------|
| 1. Foundation | Done | init, index, query, status |
| 2. Identity | Done | mailmap import, identity merge, bot detection, backfill |
| 3. Subsystems | Done | MAINTAINERS parser, file-to-subsystem mapping |
| 4. Blame | Done | git2 blame, ownership snapshots, parallel execution |
| 5. Reports | Done | HTML generation via minijinja + embedded Vega-Lite |

## License

MIT
