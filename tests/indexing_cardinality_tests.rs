//! Exploratory and adversarial tests for indexing and cardinality controls.
//! Principles: break it, assume the code is wrong, write tests that should fail.

use std::time::Duration;

use tempfile::TempDir;

use ugnos::{DbConfig, DbCore, DbError, TagSet};

fn make_segments_cfg(dir: &std::path::Path, max_cardinality: Option<u64>) -> DbConfig {
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.to_path_buf();
    cfg.enable_segments = true;
    cfg.enable_wal = false;
    cfg.enable_snapshots = false;
    cfg.flush_interval = Duration::from_secs(3600);
    cfg.max_series_cardinality = max_cardinality;
    cfg
}

fn tags(pairs: &[(&str, &str)]) -> TagSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// --- Cardinality: tests that should fail if enforcement is wrong ---

/// If the implementation is wrong, this might succeed when it should fail:
/// inserting one more distinct series key than the limit must return an error.
#[test]
fn breakit_cardinality_over_limit_returns_error() {
    let dir = TempDir::new().unwrap();
    let cfg = make_segments_cfg(dir.path(), Some(2));
    let db = DbCore::with_config(cfg).unwrap();

    let t0 = 1000u64;
    db.insert("s", t0, 1.0, tags(&[("a", "1")])).unwrap();
    db.insert("s", t0 + 1, 2.0, tags(&[("a", "2")])).unwrap();
    let r = db.insert("s", t0 + 2, 3.0, tags(&[("a", "3")]));
    assert!(
        r.is_err(),
        "insert beyond cardinality limit should fail"
    );
    let e = r.unwrap_err();
    match &e {
        DbError::SeriesCardinalityLimitExceeded { current, limit, scope } => {
            assert_eq!(*current, 2);
            assert_eq!(*limit, 2);
            assert!(!scope.is_empty());
        }
        _ => panic!("expected SeriesCardinalityLimitExceeded, got {:?}", e),
    }
}

/// Exactly at limit must succeed; if the implementation is wrong it might reject.
#[test]
fn breakit_cardinality_exactly_at_limit_succeeds() {
    let dir = TempDir::new().unwrap();
    let cfg = make_segments_cfg(dir.path(), Some(3));
    let db = DbCore::with_config(cfg).unwrap();

    let t0 = 1000u64;
    db.insert("s", t0, 1.0, tags(&[("a", "1")])).unwrap();
    db.insert("s", t0 + 1, 2.0, tags(&[("a", "2")])).unwrap();
    let r = db.insert("s", t0 + 2, 3.0, tags(&[("a", "3")]));
    assert!(
        r.is_ok(),
        "insert at exactly cardinality limit must succeed"
    );
}

/// Same series+tags twice must not count as two; if implementation is wrong we might hit limit too early.
#[test]
fn breakit_cardinality_duplicate_series_key_does_not_increase_count() {
    let dir = TempDir::new().unwrap();
    let cfg = make_segments_cfg(dir.path(), Some(1));
    let db = DbCore::with_config(cfg).unwrap();

    let t0 = 1000u64;
    let tag = tags(&[("x", "y")]);
    db.insert("s", t0, 1.0, tag.clone()).unwrap();
    db.insert("s", t0 + 1, 2.0, tag).unwrap();
    db.flush().unwrap();
    let out = db.query("s", t0..t0 + 10, None).unwrap();
    assert_eq!(out.len(), 2, "both points must be stored");
}

/// Different series names with same tags = 2 distinct keys. If implementation is wrong it might count as 1.
#[test]
fn breakit_cardinality_different_series_count_separately() {
    let dir = TempDir::new().unwrap();
    let cfg = make_segments_cfg(dir.path(), Some(2));
    let db = DbCore::with_config(cfg).unwrap();

    let t0 = 1000u64;
    let tag = tags(&[("k", "v")]);
    db.insert("s1", t0, 1.0, tag.clone()).unwrap();
    db.insert("s2", t0, 2.0, tag).unwrap();
    let r = db.insert("s3", t0, 3.0, tags(&[("k", "v")]));
    assert!(r.is_err(), "third distinct series key must be rejected");
}

/// Cardinality limits must remain enforceable across restarts.
///
/// If the implementation is wrong (e.g. cardinality tracker is not rebuilt from durable state),
/// restarting the DB will "forget" existing series and incorrectly allow inserts beyond the limit.
#[test]
fn breakit_cardinality_limit_cannot_be_bypassed_by_restart() {
    let dir = TempDir::new().unwrap();
    let cfg = make_segments_cfg(dir.path(), Some(2));

    {
        let db = DbCore::with_config(cfg.clone()).unwrap();
        let t0 = 1000u64;
        db.insert("s", t0, 1.0, tags(&[("a", "1")])).unwrap();
        db.insert("s", t0 + 1, 2.0, tags(&[("a", "2")])).unwrap();
        db.flush().unwrap();
    } // drop: simulates process restart

    let mut db2 = DbCore::with_config(cfg).unwrap();
    db2.recover().unwrap();

    let r = db2.insert("s", 2000u64, 3.0, tags(&[("a", "3")]));
    assert!(
        matches!(r, Err(DbError::SeriesCardinalityLimitExceeded { .. })),
        "restart must not bypass cardinality limit; got: {r:?}"
    );
}

/// Cardinality enforcement must be per-tenant/namespace when configured.
///
/// This uses a tag-based scope resolver (`DbConfig::cardinality_scope_tag_key`) so callers can
/// enforce limits per tenant without changing the ingest API surface.
#[test]
fn breakit_cardinality_is_enforced_per_scope_tag_key() {
    let dir = TempDir::new().unwrap();
    let mut cfg = make_segments_cfg(dir.path(), Some(2));
    cfg.cardinality_scope_tag_key = Some("tenant".to_string());
    let db = DbCore::with_config(cfg).unwrap();

    let t0 = 1000u64;
    // Tenant A hits its limit.
    db.insert(
        "s",
        t0,
        1.0,
        tags(&[("tenant", "acme/prod"), ("a", "1")]),
    )
    .unwrap();
    db.insert(
        "s",
        t0 + 1,
        2.0,
        tags(&[("tenant", "acme/prod"), ("a", "2")]),
    )
    .unwrap();
    let r = db.insert(
        "s",
        t0 + 2,
        3.0,
        tags(&[("tenant", "acme/prod"), ("a", "3")]),
    );
    assert!(
        matches!(r, Err(DbError::SeriesCardinalityLimitExceeded { .. })),
        "tenant scope must be enforced independently; got: {r:?}"
    );

    // Tenant B is independent and should still accept up to its own limit.
    db.insert(
        "s",
        t0 + 10,
        10.0,
        tags(&[("tenant", "beta"), ("a", "1")]),
    )
    .unwrap();
    db.insert(
        "s",
        t0 + 11,
        11.0,
        tags(&[("tenant", "beta"), ("a", "2")]),
    )
    .unwrap();
    let r2 = db.insert(
        "s",
        t0 + 12,
        12.0,
        tags(&[("tenant", "beta"), ("a", "3")]),
    );
    assert!(r2.is_err(), "tenant beta must also enforce its own limit");
}

// --- Tag index: tests that should fail if index is wrong or unused ---

/// Query with tag filter must return the same points whether tag index is enabled or not.
/// If the index path is buggy, results might differ.
#[test]
fn breakit_tag_filter_results_match_with_and_without_tag_index() {
    let t0 = 10_000u64;
    let mut filter = TagSet::new();
    filter.insert("host".to_string(), "b".to_string());

    // With tag index (default)
    let dir1 = TempDir::new().unwrap();
    let mut cfg1 = make_segments_cfg(dir1.path(), None);
    cfg1.segment_store.enable_tag_index = true;
    let db1 = DbCore::with_config(cfg1).unwrap();
    db1.insert("m", t0, 1.0, tags(&[("host", "a")])).unwrap();
    db1.insert("m", t0 + 1, 2.0, tags(&[("host", "b")])).unwrap();
    db1.insert("m", t0 + 2, 3.0, tags(&[("host", "b")])).unwrap();
    db1.insert("m", t0 + 3, 4.0, tags(&[("host", "c")])).unwrap();
    db1.flush().unwrap();
    let with_index = db1.query("m", t0..t0 + 100, Some(&filter)).unwrap();

    // Without tag index
    let dir2 = TempDir::new().unwrap();
    let mut cfg2 = make_segments_cfg(dir2.path(), None);
    cfg2.segment_store.enable_tag_index = false;
    let db2 = DbCore::with_config(cfg2).unwrap();
    db2.insert("m", t0, 1.0, tags(&[("host", "a")])).unwrap();
    db2.insert("m", t0 + 1, 2.0, tags(&[("host", "b")])).unwrap();
    db2.insert("m", t0 + 2, 3.0, tags(&[("host", "b")])).unwrap();
    db2.insert("m", t0 + 3, 4.0, tags(&[("host", "c")])).unwrap();
    db2.flush().unwrap();
    let without_index = db2.query("m", t0..t0 + 100, Some(&filter)).unwrap();

    assert_eq!(
        with_index, without_index,
        "tag filter must return same results with and without tag index"
    );
    assert_eq!(with_index.len(), 2);
    assert_eq!(with_index, vec![(t0 + 1, 2.0), (t0 + 2, 3.0)]);
}

/// Filter that matches no rows must return empty, not panic or wrong data.
#[test]
fn breakit_tag_filter_no_match_returns_empty() {
    let dir = TempDir::new().unwrap();
    let db = DbCore::with_config(make_segments_cfg(dir.path(), None)).unwrap();
    let t0 = 1000u64;
    db.insert("s", t0, 1.0, tags(&[("a", "1")])).unwrap();
    db.flush().unwrap();

    let filter = tags(&[("a", "nonexistent")]);
    let out = db.query("s", t0..t0 + 10, Some(&filter)).unwrap();
    assert!(out.is_empty());
}

/// Empty tag filter must return all rows in range.
#[test]
fn breakit_tag_filter_empty_returns_all_in_range() {
    let dir = TempDir::new().unwrap();
    let db = DbCore::with_config(make_segments_cfg(dir.path(), None)).unwrap();
    let t0 = 1000u64;
    db.insert("s", t0, 1.0, tags(&[("a", "1")])).unwrap();
    db.insert("s", t0 + 1, 2.0, tags(&[("a", "2")])).unwrap();
    db.flush().unwrap();

    let empty: TagSet = TagSet::new();
    let out = db.query("s", t0..t0 + 10, Some(&empty)).unwrap();
    assert_eq!(out.len(), 2);
}

/// Multiple tag filter (AND semantics): row must match all.
#[test]
fn breakit_tag_filter_multiple_tags_and_semantics() {
    let dir = TempDir::new().unwrap();
    let db = DbCore::with_config(make_segments_cfg(dir.path(), None)).unwrap();
    let t0 = 1000u64;
    db.insert("s", t0, 1.0, tags(&[("a", "1"), ("b", "x")])).unwrap();
    db.insert("s", t0 + 1, 2.0, tags(&[("a", "1"), ("b", "y")])).unwrap();
    db.insert("s", t0 + 2, 3.0, tags(&[("a", "1"), ("b", "x")])).unwrap();
    db.flush().unwrap();

    let filter = tags(&[("a", "1"), ("b", "x")]);
    let out = db.query("s", t0..t0 + 10, Some(&filter)).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out, vec![(t0, 1.0), (t0 + 2, 3.0)]);
}
