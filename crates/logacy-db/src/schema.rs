diesel::table! {
    logacy_meta (key) {
        key -> Text,
        value -> Text,
    }
}

diesel::table! {
    identities (id) {
        id -> Integer,
        canonical_name -> Text,
        canonical_email -> Text,
        is_bot -> Integer,
    }
}

diesel::table! {
    identity_aliases (email) {
        identity_id -> Integer,
        name -> Nullable<Text>,
        email -> Text,
    }
}

diesel::table! {
    identity_emails (identity_id, email) {
        identity_id -> Integer,
        email -> Text,
        first_seen_at -> Nullable<Text>,
        last_seen_at -> Nullable<Text>,
        commit_count -> Integer,
        trailer_count -> Integer,
        source -> Text,
        is_preferred -> Integer,
    }
}

diesel::table! {
    organizations (id) {
        id -> Integer,
        name -> Text,
    }
}

diesel::table! {
    org_domain_rules (id) {
        id -> Integer,
        org_id -> Integer,
        domain -> Text,
        valid_from -> Nullable<Text>,
        valid_until -> Nullable<Text>,
    }
}

diesel::table! {
    identity_affiliations (id) {
        id -> Integer,
        identity_id -> Integer,
        org_id -> Integer,
        valid_from -> Nullable<Text>,
        valid_until -> Nullable<Text>,
        source -> Text,
    }
}

diesel::table! {
    commits (hash) {
        hash -> Text,
        author_name -> Text,
        author_email -> Text,
        committer_name -> Text,
        committer_email -> Text,
        author_id -> Nullable<Integer>,
        committer_id -> Nullable<Integer>,
        author_date -> Text,
        commit_date -> Text,
        subject -> Text,
        body -> Nullable<Text>,
        ticket -> Nullable<Text>,
        component -> Nullable<Text>,
        is_merge -> Integer,
        first_parent -> Integer,
        insertions -> Nullable<Integer>,
        deletions -> Nullable<Integer>,
    }
}

diesel::table! {
    trailers (commit_hash, key, seq) {
        commit_hash -> Text,
        key -> Text,
        value -> Text,
        identity_id -> Nullable<Integer>,
        seq -> Integer,
        parsed_name -> Nullable<Text>,
        parsed_email -> Nullable<Text>,
    }
}

diesel::table! {
    commit_files (commit_hash, path) {
        commit_hash -> Text,
        path -> Text,
        status -> Text,
        insertions -> Nullable<Integer>,
        deletions -> Nullable<Integer>,
        language -> Text,
        category -> Text,
    }
}

diesel::table! {
    commit_hunks (commit_hash, path, seq) {
        commit_hash -> Text,
        path -> Text,
        old_start -> Integer,
        old_lines -> Integer,
        new_start -> Integer,
        new_lines -> Integer,
        seq -> Integer,
    }
}

diesel::table! {
    subsystems (id) {
        id -> Integer,
        name -> Text,
        status -> Nullable<Text>,
        updated_at -> Text,
    }
}

diesel::table! {
    subsystem_reviewers (subsystem_id, identity_id) {
        subsystem_id -> Integer,
        identity_id -> Integer,
    }
}

diesel::table! {
    subsystem_paths (subsystem_id, pattern) {
        subsystem_id -> Integer,
        pattern -> Text,
        is_exclude -> Integer,
    }
}

diesel::table! {
    file_subsystems (path, subsystem_id) {
        path -> Text,
        subsystem_id -> Integer,
    }
}

diesel::table! {
    blame_snapshots (id) {
        id -> Integer,
        commit_hash -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    blame_hunks (snapshot_id, path, start_line) {
        snapshot_id -> Integer,
        path -> Text,
        start_line -> Integer,
        line_count -> Integer,
        orig_commit -> Text,
        identity_id -> Integer,
    }
}

diesel::table! {
    file_ownership (snapshot_id, path, identity_id) {
        snapshot_id -> Integer,
        path -> Text,
        identity_id -> Integer,
        lines_owned -> Integer,
        fraction -> Double,
    }
}

diesel::table! {
    commit_org_attribution (commit_hash) {
        commit_hash -> Text,
        org_id -> Nullable<Integer>,
        org_name -> Nullable<Text>,
        source -> Text,
        matched_email -> Nullable<Text>,
        matched_domain -> Nullable<Text>,
        matched_rule_id -> Nullable<Integer>,
    }
}

diesel::table! {
    trailer_org_attribution (commit_hash, key, seq) {
        commit_hash -> Text,
        key -> Text,
        seq -> Integer,
        org_id -> Nullable<Integer>,
        org_name -> Nullable<Text>,
        source -> Text,
        matched_email -> Nullable<Text>,
        matched_domain -> Nullable<Text>,
        matched_rule_id -> Nullable<Integer>,
    }
}

diesel::table! {
    v_identity_org (identity_id) {
        identity_id -> Integer,
        org -> Nullable<Text>,
    }
}

diesel::table! {
    v_commits (hash) {
        hash -> Text,
        author_name -> Text,
        author_email -> Text,
        committer_name -> Text,
        committer_email -> Text,
        author_id -> Nullable<Integer>,
        committer_id -> Nullable<Integer>,
        author_date -> Text,
        commit_date -> Text,
        subject -> Text,
        body -> Nullable<Text>,
        ticket -> Nullable<Text>,
        component -> Nullable<Text>,
        is_merge -> Integer,
        first_parent -> Integer,
        insertions -> Nullable<Integer>,
        deletions -> Nullable<Integer>,
        resolved_author_name -> Text,
        resolved_author_email -> Text,
        author_org -> Nullable<Text>,
        author_is_bot -> Nullable<Integer>,
    }
}

diesel::table! {
    v_reviews (hash, reviewer) {
        hash -> Text,
        ticket -> Nullable<Text>,
        component -> Nullable<Text>,
        author -> Text,
        reviewer -> Text,
        author_date -> Text,
        author_org -> Nullable<Text>,
        reviewer_org -> Nullable<Text>,
    }
}

diesel::table! {
    v_subsystem_activity (subsystem, author, author_date) {
        subsystem -> Text,
        ticket -> Nullable<Text>,
        component -> Nullable<Text>,
        author_date -> Text,
        author -> Text,
        org -> Nullable<Text>,
    }
}

diesel::table! {
    v_subsystem_contributors (subsystem_id, identity_id) {
        subsystem_id -> Integer,
        subsystem -> Text,
        identity_id -> Integer,
        canonical_name -> Text,
        commits -> BigInt,
        lines_added -> BigInt,
        lines_removed -> BigInt,
        last_commit -> Nullable<Text>,
        first_commit -> Nullable<Text>,
        is_reviewer -> Integer,
        lines_owned -> BigInt,
    }
}

diesel::table! {
    tags (name) {
        name -> Text,
        target_commit -> Text,
        tag_object_hash -> Nullable<Text>,
        is_annotated -> Integer,
        tagger_name -> Nullable<Text>,
        tagger_email -> Nullable<Text>,
        tagger_date -> Nullable<Text>,
        annotation -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::table! {
    commit_releases (commit_hash) {
        commit_hash -> Text,
        release_tag -> Text,
        release_date -> Text,
    }
}

diesel::joinable!(commit_releases -> commits (commit_hash));
diesel::joinable!(commit_releases -> tags (release_tag));

diesel::joinable!(identity_aliases -> identities (identity_id));
diesel::joinable!(identity_emails -> identities (identity_id));
diesel::joinable!(commits -> identities (author_id));
diesel::joinable!(trailers -> commits (commit_hash));
diesel::joinable!(trailers -> identities (identity_id));
diesel::joinable!(commit_files -> commits (commit_hash));
diesel::joinable!(commit_hunks -> commits (commit_hash));
diesel::joinable!(subsystem_reviewers -> subsystems (subsystem_id));
diesel::joinable!(subsystem_reviewers -> identities (identity_id));
diesel::joinable!(subsystem_paths -> subsystems (subsystem_id));
diesel::joinable!(file_subsystems -> subsystems (subsystem_id));
diesel::joinable!(blame_hunks -> blame_snapshots (snapshot_id));
diesel::joinable!(blame_hunks -> identities (identity_id));
diesel::joinable!(file_ownership -> blame_snapshots (snapshot_id));
diesel::joinable!(file_ownership -> identities (identity_id));
diesel::joinable!(org_domain_rules -> organizations (org_id));
diesel::joinable!(commit_org_attribution -> commits (commit_hash));
diesel::joinable!(commit_org_attribution -> organizations (org_id));
diesel::joinable!(trailer_org_attribution -> organizations (org_id));

diesel::allow_tables_to_appear_in_same_query!(
    logacy_meta,
    identities,
    identity_aliases,
    identity_emails,
    organizations,
    org_domain_rules,
    identity_affiliations,
    commits,
    trailers,
    commit_files,
    commit_hunks,
    subsystems,
    subsystem_reviewers,
    subsystem_paths,
    file_subsystems,
    blame_snapshots,
    blame_hunks,
    file_ownership,
    commit_org_attribution,
    trailer_org_attribution,
    v_identity_org,
    v_commits,
    v_reviews,
    v_subsystem_activity,
    v_subsystem_contributors,
    tags,
    commit_releases,
);
