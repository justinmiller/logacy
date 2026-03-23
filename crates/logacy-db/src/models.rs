use diesel::prelude::*;
use serde::Serialize;

use crate::schema::{
    blame_snapshots, commit_files, commits, identities, identity_aliases, subsystems, trailers,
};

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = identities)]
pub struct Identity {
    pub id: i32,
    pub canonical_name: String,
    pub canonical_email: String,
    pub is_bot: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = identity_aliases)]
pub struct IdentityAlias {
    pub identity_id: i32,
    pub name: Option<String>,
    pub email: String,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = commits)]
pub struct Commit {
    pub hash: String,
    pub author_name: String,
    pub author_email: String,
    pub committer_name: String,
    pub committer_email: String,
    pub author_id: Option<i32>,
    pub committer_id: Option<i32>,
    pub author_date: String,
    pub commit_date: String,
    pub subject: String,
    pub body: Option<String>,
    pub ticket: Option<String>,
    pub component: Option<String>,
    pub is_merge: i32,
    pub first_parent: i32,
    pub insertions: Option<i32>,
    pub deletions: Option<i32>,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = trailers)]
pub struct Trailer {
    pub commit_hash: String,
    pub key: String,
    pub value: String,
    pub identity_id: Option<i32>,
    pub seq: i32,
    pub parsed_name: Option<String>,
    pub parsed_email: Option<String>,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = commit_files)]
pub struct CommitFile {
    pub commit_hash: String,
    pub path: String,
    pub status: String,
    pub insertions: Option<i32>,
    pub deletions: Option<i32>,
    pub language: String,
    pub category: String,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = subsystems)]
pub struct Subsystem {
    pub id: i32,
    pub name: String,
    pub status: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = blame_snapshots)]
pub struct BlameSnapshot {
    pub id: i32,
    pub commit_hash: String,
    pub created_at: String,
}
