use serde::Deserialize;
use std::path::Path;

use anyhow::{Context, Result};

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub repository: RepositoryConfig,
    #[serde(default)]
    pub trailers: TrailerConfig,
    #[serde(default)]
    pub identity: IdentityConfig,
    #[serde(default)]
    pub maintainers: MaintainersConfig,
    #[serde(default)]
    pub blame: BlameConfig,
    #[serde(default)]
    pub index: IndexConfig,
    #[serde(default)]
    pub releases: ReleasesConfig,
}

#[derive(Debug, Deserialize, Default)]
pub struct RepositoryConfig {
    pub ticket_pattern: Option<String>,
    pub component_pattern: Option<String>,
    /// URL template for linking tickets to an issue tracker.
    /// Use `{ticket}` as placeholder, e.g. `https://jira.example.com/browse/{ticket}`
    /// or `https://github.com/owner/repo/issues/{ticket}`.
    pub ticket_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TrailerConfig {
    #[serde(default = "TrailerConfig::default_identity_keys")]
    pub identity_keys: Vec<String>,
    #[serde(default = "TrailerConfig::default_metadata_keys")]
    pub metadata_keys: Vec<String>,
}

impl Default for TrailerConfig {
    fn default() -> Self {
        Self {
            identity_keys: Self::default_identity_keys(),
            metadata_keys: Self::default_metadata_keys(),
        }
    }
}

impl TrailerConfig {
    fn default_identity_keys() -> Vec<String> {
        ["Signed-off-by", "Reviewed-by", "Tested-by", "Acked-by"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn default_metadata_keys() -> Vec<String> {
        ["Change-Id", "Reviewed-on", "Test-Parameters", "Fixes"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct IdentityConfig {
    #[serde(default = "default_true")]
    pub mailmap: bool,
    #[serde(default)]
    pub bot_emails: Vec<String>,
    #[serde(default)]
    pub bot_names: Vec<String>,
    #[serde(default)]
    pub orgs: Vec<OrgDomain>,
    #[serde(default)]
    pub org_overrides: Vec<OrgOverride>,
    #[serde(default)]
    pub aliases: Vec<IdentityAlias>,
}

/// Explicit identity alias: merge multiple name/email pairs into one identity.
/// Overrides or supplements `.mailmap` resolution.
#[derive(Debug, Deserialize)]
pub struct IdentityAlias {
    /// Canonical display name for this person.
    pub name: String,
    /// All known email addresses. The first is used as canonical_email.
    #[serde(default, deserialize_with = "string_or_vec")]
    pub emails: Vec<String>,
    /// Optional org affiliation (creates an identity_affiliations row).
    pub org: Option<String>,
}

/// Direct org assignment for identities that can't be resolved by email domain
/// (e.g., contributors using personal email addresses).
#[derive(Debug, Deserialize)]
pub struct OrgOverride {
    pub name: Option<String>,
    pub email: Option<String>,
    pub org: String,
}

#[derive(Debug, Deserialize)]
pub struct OrgDomain {
    /// Single domain (backward compat). Use `domains` for multiple.
    #[serde(default)]
    pub domain: Option<String>,
    /// Multiple domains for the same org.
    #[serde(default, deserialize_with = "string_or_vec")]
    pub domains: Vec<String>,
    pub org: String,
}

impl OrgDomain {
    /// Returns all domains (merging `domain` and `domains` fields).
    pub fn all_domains(&self) -> Vec<&str> {
        let mut out: Vec<&str> = self.domains.iter().map(|s| s.as_str()).collect();
        if let Some(ref d) = self.domain {
            if !out.iter().any(|x| x == &d.as_str()) {
                out.push(d.as_str());
            }
        }
        out
    }
}

fn string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a string or list of strings")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(vec![v.to_string()])
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                out.push(s);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

#[derive(Debug, Deserialize, Default)]
pub struct MaintainersConfig {
    #[serde(default = "default_maintainers_file")]
    pub file: String,
    #[serde(default = "default_maintainers_format")]
    pub format: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct BlameConfig {
    #[serde(default)]
    pub exclude_paths: Vec<String>,
    #[serde(default = "default_max_file_size")]
    pub max_file_size: u64,
    #[serde(default)]
    pub binary_extensions: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct IndexConfig {
    #[serde(default = "default_true")]
    pub first_parent: bool,
    #[serde(default = "default_true")]
    pub include_diff_stats: bool,
    #[serde(default = "default_true")]
    pub include_file_list: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            first_parent: true,
            include_diff_stats: true,
            include_file_list: true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ReleasesConfig {
    /// Glob pattern to filter tags, e.g. "v*"
    pub tag_pattern: Option<String>,
    /// Branch patterns for release branches (future use), e.g. ["release/*"]
    #[serde(default)]
    pub branch_patterns: Vec<String>,
    /// Whether to map commits to their containing release
    #[serde(default = "default_true")]
    pub map_commits: bool,
}

impl Default for ReleasesConfig {
    fn default() -> Self {
        Self {
            tag_pattern: None,
            branch_patterns: Vec::new(),
            map_commits: true,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_maintainers_file() -> String {
    "MAINTAINERS".to_string()
}

fn default_maintainers_format() -> String {
    "linux".to_string()
}

fn default_max_file_size() -> u64 {
    1_048_576
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            tracing::info!("no config file at {}, using defaults", path.display());
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config from {}", path.display()))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| format!("failed to parse config from {}", path.display()))?;
        Ok(config)
    }
}
