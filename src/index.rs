//! Indexing and cardinality controls: tag block index (inverted index / bitmap) and
//! series cardinality estimation and hard limits per tenant/namespace.

use crate::error::DbError;
use crate::types::TagSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// Default scope when no tenant/namespace is specified.
pub const DEFAULT_CARDINALITY_SCOPE: &str = "default";

/// Canonical series key: (series name, sorted tag set) for stable hashing and equality.
/// Tag set is sorted by key then value so that two equivalent TagSets produce the same key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    series: String,
    /// Sorted (k, v) pairs for deterministic ordering.
    tags_sorted: Vec<(String, String)>,
}

impl SeriesKey {
    /// Builds a canonical series key from series name and tag set.
    pub fn new(series: &str, tags: &TagSet) -> Self {
        let mut tags_sorted: Vec<_> = tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        tags_sorted.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        Self {
            series: series.to_string(),
            tags_sorted,
        }
    }
}

/// Tracks distinct series keys per scope and enforces a configurable hard limit.
///
/// Cardinality is the number of distinct (series name, tag set) combinations.
/// Used to prevent high-cardinality abuse and ensure predictable resource usage.
#[derive(Debug)]
pub struct CardinalityTracker {
    /// scope -> set of series keys
    by_scope: RwLock<HashMap<String, HashSet<SeriesKey>>>,
    /// Optional hard limit per scope (None = no limit).
    limit_per_scope: Option<u64>,
}

impl CardinalityTracker {
    /// Creates a tracker with an optional global limit (applied to the default scope)
    /// or per-scope limits. For now we use a single limit applied to the default scope.
    pub fn new(limit: Option<u64>) -> Self {
        Self {
            by_scope: RwLock::new(HashMap::new()),
            limit_per_scope: limit,
        }
    }

    /// Returns current cardinality for the given scope.
    pub fn current_count(&self, scope: &str) -> u64 {
        let guard = self.by_scope.read().expect("cardinality tracker lock");
        guard.get(scope).map(|s| s.len() as u64).unwrap_or(0)
    }

    /// Registers a series key in the given scope. Returns `Ok(())` if the key was new and
    /// (after insertion) cardinality is still within limit, or if the key was already present.
    /// Returns `Err(SeriesCardinalityLimitExceeded)` if adding this key would exceed the limit.
    pub fn register(&self, scope: &str, series: &str, tags: &TagSet) -> Result<(), DbError> {
        let _ = self.register_and_was_new(scope, series, tags)?;
        Ok(())
    }

    /// Like [`Self::register`], but returns whether the series key was newly added.
    ///
    /// - `Ok(true)`: key was new and added.
    /// - `Ok(false)`: key already existed (no change).
    /// - `Err(..)`: adding would exceed the limit.
    pub fn register_and_was_new(
        &self,
        scope: &str,
        series: &str,
        tags: &TagSet,
    ) -> Result<bool, DbError> {
        let key = SeriesKey::new(series, tags);
        let mut guard = self.by_scope.write().expect("cardinality tracker lock");
        let set = guard.entry(scope.to_string()).or_default();
        if set.contains(&key) {
            return Ok(false);
        }
        let limit = match self.limit_per_scope {
            Some(l) => l,
            None => {
                set.insert(key);
                return Ok(true);
            }
        };
        let new_count = set.len() as u64 + 1;
        if new_count > limit {
            return Err(DbError::SeriesCardinalityLimitExceeded {
                current: set.len() as u64,
                limit,
                scope: scope.to_string(),
            });
        }
        set.insert(key);
        Ok(true)
    }

    /// Seeds the tracker with known series keys (e.g. after recovery from segments).
    /// Does not enforce limit; use when rebuilding state from durable storage.
    pub fn seed(&self, scope: &str, keys: impl Iterator<Item = (String, TagSet)>) {
        let mut guard = self.by_scope.write().expect("cardinality tracker lock");
        let set = guard.entry(scope.to_string()).or_default();
        for (series, tags) in keys {
            set.insert(SeriesKey::new(&series, &tags));
        }
    }

    /// Seeds the tracker with pre-canonicalized series keys.
    /// Does not enforce limit; use when rebuilding state from durable storage.
    pub fn seed_series_keys(&self, scope: &str, keys: impl Iterator<Item = SeriesKey>) {
        let mut guard = self.by_scope.write().expect("cardinality tracker lock");
        let set = guard.entry(scope.to_string()).or_default();
        for k in keys {
            set.insert(k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn series_key_canonical_ordering() {
        let t1 = tags(&[("b", "2"), ("a", "1")]);
        let t2 = tags(&[("a", "1"), ("b", "2")]);
        assert_eq!(SeriesKey::new("s", &t1), SeriesKey::new("s", &t2));
    }

    #[test]
    fn cardinality_tracker_no_limit_accepts_all() {
        let tracker = CardinalityTracker::new(None);
        for i in 0..1000 {
            tracker
                .register(
                    DEFAULT_CARDINALITY_SCOPE,
                    "s",
                    &tags(&[("i", &i.to_string())]),
                )
                .unwrap();
        }
        assert_eq!(tracker.current_count(DEFAULT_CARDINALITY_SCOPE), 1000);
    }

    #[test]
    fn cardinality_tracker_enforces_limit() {
        let tracker = CardinalityTracker::new(Some(2));
        tracker
            .register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "1")]))
            .unwrap();
        tracker
            .register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "2")]))
            .unwrap();
        let r = tracker.register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "3")]));
        assert!(matches!(
            r,
            Err(DbError::SeriesCardinalityLimitExceeded {
                current: 2,
                limit: 2,
                ..
            })
        ));
    }

    #[test]
    fn cardinality_tracker_duplicate_does_not_increase_count() {
        let tracker = CardinalityTracker::new(Some(1));
        tracker
            .register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "1")]))
            .unwrap();
        tracker
            .register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "1")]))
            .unwrap();
        assert_eq!(tracker.current_count(DEFAULT_CARDINALITY_SCOPE), 1);
    }

    #[test]
    fn cardinality_tracker_exactly_at_limit_succeeds() {
        let tracker = CardinalityTracker::new(Some(2));
        tracker
            .register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "1")]))
            .unwrap();
        tracker
            .register(DEFAULT_CARDINALITY_SCOPE, "s", &tags(&[("a", "2")]))
            .unwrap();
        assert_eq!(tracker.current_count(DEFAULT_CARDINALITY_SCOPE), 2);
    }
}
