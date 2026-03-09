pub mod config;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Resolved paths for a logacy project.
pub struct LogacyContext {
    pub repo_path: PathBuf,
    pub db_path: PathBuf,
    pub config_path: PathBuf,
}

impl LogacyContext {
    pub fn discover(repo: Option<&Path>, db: Option<&Path>, config: Option<&Path>) -> Result<Self> {
        let repo_path = match repo {
            Some(p) => p.to_path_buf(),
            None => std::env::current_dir().context("failed to get current directory")?,
        };

        let logacy_dir = repo_path.join(".logacy");
        let db_path = db
            .map(PathBuf::from)
            .unwrap_or_else(|| logacy_dir.join("logacy.db"));
        let config_path = config
            .map(PathBuf::from)
            .unwrap_or_else(|| repo_path.join("logacy.toml"));

        Ok(Self {
            repo_path,
            db_path,
            config_path,
        })
    }

    pub fn logacy_dir(&self) -> PathBuf {
        self.repo_path.join(".logacy")
    }

    pub fn load_config(&self) -> Result<config::Config> {
        config::Config::load(&self.config_path)
    }
}
