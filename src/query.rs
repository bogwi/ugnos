use crate::error::DbError;
use crate::types::{TagSet, TimeSeriesChunk, Timestamp, Value};
use rayon::prelude::*;
use std::ops::Range;
use std::sync::RwLockReadGuard;

/// Executes a query against a single time series chunk.
///
/// # Arguments
/// * `chunk_guard` - A read guard for the `TimeSeriesChunk` to query.
/// * `time_range` - The time range (start inclusive, end exclusive) for the query.
/// * `tag_filter` - An optional filter for tags. If present, only points matching all filter tags are returned.
///
/// # Returns
/// * `Result<Vec<(Timestamp, Value)>, DbError>` - A vector of (timestamp, value) tuples matching the query criteria.
pub fn execute_query(
    // Takes a read guard to ensure data doesn't change during query execution
    chunk_guard: RwLockReadGuard<TimeSeriesChunk>,
    time_range: Range<Timestamp>,
    tag_filter: Option<&TagSet>,
) -> Result<Vec<(Timestamp, Value)>, DbError> {
    // Ensure the time range is valid
    if time_range.start >= time_range.end {
        return Err(DbError::InvalidTimeRange {
            start: time_range.start,
            end: time_range.end,
        });
    }

    let chunk = &*chunk_guard; // Dereference the guard to access the chunk

    // --- Optimization: Find potential index range using binary search --- //
    // Find the first index >= time_range.start
    let start_idx = chunk
        .timestamps
        .partition_point(|&ts| ts < time_range.start);

    // Find the first index >= time_range.end
    let end_idx = chunk.timestamps.partition_point(|&ts| ts < time_range.end);

    // If the range is empty or invalid, return early
    if start_idx >= end_idx {
        return Ok(Vec::new());
    }

    // --- Parallel Filtering and Collection --- //
    let results: Vec<(Timestamp, Value)> = (start_idx..end_idx)
        .into_par_iter() // Parallel iterator over the relevant index range
        .filter_map(|i| {
            // Apply tag filter if present
            let tags_match = tag_filter.is_none_or(|filter| check_tags(&chunk.tags[i], filter));

            if tags_match {
                // If tags match (or no filter), return the data point
                Some((chunk.timestamps[i], chunk.values[i]))
            } else {
                // Otherwise, filter it out
                None
            }
        })
        .collect(); // Collect the results into a Vec

    Ok(results)
}

/// Checks if a data point's tags contain all the tags specified in the filter.
#[inline]
fn check_tags(point_tags: &TagSet, filter_tags: &TagSet) -> bool {
    // The point must have at least as many tags as the filter
    if point_tags.len() < filter_tags.len() {
        return false;
    }
    // Every key-value pair in the filter must exist in the point's tags
    filter_tags
        .iter()
        .all(|(key, value)| point_tags.get(key) == Some(value))
}

// ---------------------------------------------------------------------------
// Vectorized aggregates and reductions (single-pass over slices)
// ---------------------------------------------------------------------------
//
// Official PromQL rules (Prometheus documentation, unchanged in 3.x):
// https://prometheus.io/docs/prometheus/latest/querying/operators/
//
// • sum(v): "sums up sample values in v in the same way as the + binary operator
//   does between two values." → Follows IEEE 754 exactly → any NaN anywhere →
//   whole result is NaN.
//
// • avg(v): "divides the sum of v by the number of aggregated samples in the
//   same way as the / binary operator." → Same as sum() / count() → NaN propagates.
//
// • count(v): "returns the number of values at that timestamp" → Counts every
//   sample that exists, including NaN and ±Inf. (NaN is a valid float sample.)
//
// • min(v) / max(v) (the only special case): "following IEEE 754 floating point
//   arithmetic, which in particular implies that NaN is only ever considered a
//   minimum or maximum if all aggregated values are NaN."

/// Returns the last `(timestamp, value)` in a non-empty slice assumed sorted by timestamp.
///
/// Used for instant vector evaluation (one sample per series at evaluation time).
/// Returns `None` if the slice is empty.
#[inline]
pub fn reduce_last(points: &[(Timestamp, Value)]) -> Option<(Timestamp, Value)> {
    points.last().copied()
}

/// Vectorized sum over a value slice. PromQL `sum()` semantics: IEEE-754 floating-point
/// addition—no filtering. NaN and infinities propagate (+∞ + x = +∞, −∞ + x = −∞,
/// +∞ + −∞ = NaN, NaN + anything = NaN). Empty slice yields 0.0.
#[inline]
pub fn aggregate_sum(values: &[Value]) -> Value {
    values.iter().copied().sum()
}

/// Vectorized average. PromQL `avg()` semantics: sum/count over all values using
/// IEEE-754; any NaN propagates. Empty slice yields NaN.
#[inline]
pub fn aggregate_avg(values: &[Value]) -> Value {
    let n = values.len();
    if n == 0 {
        f64::NAN
    } else {
        let sum: Value = values.iter().copied().sum();
        sum / (n as Value)
    }
}

/// Vectorized minimum. PromQL `min()` semantics: NaN is the result only when all
/// aggregated values are NaN (minNum-style); otherwise NaN is ignored. Empty yields NaN.
#[inline]
pub fn aggregate_min(values: &[Value]) -> Value {
    if values.is_empty() {
        return f64::NAN;
    }
    values.iter().copied().reduce(f64::min).unwrap()
}

/// Vectorized maximum. PromQL `max()` semantics: NaN is the result only when all
/// aggregated values are NaN (maxNum-style); otherwise NaN is ignored. Empty yields NaN.
#[inline]
pub fn aggregate_max(values: &[Value]) -> Value {
    if values.is_empty() {
        return f64::NAN;
    }
    values.iter().copied().reduce(f64::max).unwrap()
}

/// Count of values (PromQL `count()`). No filtering; counts all elements.
#[inline]
pub fn aggregate_count(values: &[Value]) -> Value {
    values.len() as Value
}

// ---------------------------------------------------------------------------
// Range functions (PromQL range vector semantics)
// ---------------------------------------------------------------------------

/// Counter-aware increase: walks forward through samples and adds back resets.
///
/// When a sample value is lower than the previous, this is treated as a counter
/// reset: the new value is added (as if the counter went to 0 then back up).
fn counter_increase(points: &[(Timestamp, Value)]) -> Value {
    let mut increase = 0.0;
    let mut prev = points[0].1;
    for &(_, val) in &points[1..] {
        if val < prev {
            increase += val;
        } else {
            increase += val - prev;
        }
        prev = val;
    }
    increase
}

/// Per-second rate of increase (counter-aware, Prometheus `rate()` semantics).
///
/// Requires at least 2 finite samples. Returns `None` if fewer or if the time
/// span between first and last sample is zero.
pub fn compute_rate(points: &[(Timestamp, Value)]) -> Option<Value> {
    if points.len() < 2 {
        return None;
    }
    let first_ts = points.first()?.0;
    let last_ts = points.last()?.0;
    let span_ns = last_ts.saturating_sub(first_ts);
    if span_ns == 0 {
        return None;
    }
    let inc = counter_increase(points);
    Some(inc / (span_ns as f64 / 1e9))
}

/// Total increase over the range (counter-aware, Prometheus `increase()` semantics).
///
/// Returns `None` if fewer than 2 samples.
pub fn compute_increase(points: &[(Timestamp, Value)]) -> Option<Value> {
    if points.len() < 2 {
        return None;
    }
    Some(counter_increase(points))
}

/// Average of sample values over the range (Prometheus `avg_over_time()`).
///
/// PromQL: sum of values / count of values (same as `/` binary operator). No filtering;
/// any NaN propagates. Returns `None` only if the range is empty.
pub fn compute_avg_over_time(points: &[(Timestamp, Value)]) -> Option<Value> {
    if points.is_empty() {
        return None;
    }
    let sum: Value = points.iter().map(|&(_, v)| v).sum();
    Some(sum / (points.len() as Value))
}

/// Maximum sample value over the range (Prometheus `max_over_time()`).
///
/// PromQL: IEEE 754; NaN is the result only when all values are NaN (maxNum-style).
/// Returns `None` if the range is empty.
pub fn compute_max_over_time(points: &[(Timestamp, Value)]) -> Option<Value> {
    points.iter().map(|&(_, v)| v).reduce(f64::max)
}

/// Minimum sample value over the range (Prometheus `min_over_time()`).
///
/// PromQL: IEEE 754; NaN is the result only when all values are NaN (minNum-style).
/// Returns `None` if the range is empty.
pub fn compute_min_over_time(points: &[(Timestamp, Value)]) -> Option<Value> {
    points.iter().map(|&(_, v)| v).reduce(f64::min)
}

/// Sum of sample values over the range (Prometheus `sum_over_time()`).
///
/// PromQL: sums values the same way as the `+` binary operator (IEEE 754). No filtering;
/// NaN and infinities propagate. Returns `None` only if the range is empty.
pub fn compute_sum_over_time(points: &[(Timestamp, Value)]) -> Option<Value> {
    if points.is_empty() {
        return None;
    }
    Some(points.iter().map(|&(_, v)| v).sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataPoint, TimeSeriesChunk};
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Helper function to get current timestamp in nanoseconds
    fn get_current_timestamp() -> Timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    // Helper function to create a TagSet
    fn create_tags(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // Helper function to create and populate a TimeSeriesChunk
    fn create_test_chunk(points: Vec<DataPoint>) -> TimeSeriesChunk {
        let mut chunk = TimeSeriesChunk::default();
        for point in points {
            chunk.timestamps.push(point.timestamp);
            chunk.values.push(point.value);
            chunk.tags.push(point.tags);
        }
        chunk
    }

    #[test]
    fn test_execute_query_time_range() {
        // Create test data with real timestamps
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts3 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts4 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts5 = get_current_timestamp();

        // Ensure our timestamps are ordered
        assert!(ts1 < ts2 && ts2 < ts3 && ts3 < ts4 && ts4 < ts5);

        let points = vec![
            DataPoint {
                timestamp: ts1,
                value: 1.0,
                tags: TagSet::new(),
            },
            DataPoint {
                timestamp: ts2,
                value: 2.0,
                tags: TagSet::new(),
            },
            DataPoint {
                timestamp: ts3,
                value: 3.0,
                tags: TagSet::new(),
            },
            DataPoint {
                timestamp: ts4,
                value: 4.0,
                tags: TagSet::new(),
            },
            DataPoint {
                timestamp: ts5,
                value: 5.0,
                tags: TagSet::new(),
            },
        ];

        let chunk = create_test_chunk(points);
        let chunk_arc = Arc::new(RwLock::new(chunk));

        // Test full range query
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let all_results = execute_query(chunk_guard, ts1..(ts5 + 1), None).unwrap();
            assert_eq!(all_results.len(), 5);
            assert_eq!(
                all_results,
                vec![(ts1, 1.0), (ts2, 2.0), (ts3, 3.0), (ts4, 4.0), (ts5, 5.0),]
            );
        }

        // Test partial range query
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let partial_results = execute_query(chunk_guard, ts2..(ts4 + 1), None).unwrap();
            assert_eq!(partial_results.len(), 3);
            assert_eq!(partial_results, vec![(ts2, 2.0), (ts3, 3.0), (ts4, 4.0),]);
        }

        // Test range with no matching data
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let no_results = execute_query(chunk_guard, (ts5 + 1)..(ts5 + 100), None).unwrap();
            assert_eq!(no_results.len(), 0);
        }

        // Test invalid range
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let invalid_range_result = execute_query(chunk_guard, ts3..ts2, None);
            assert!(invalid_range_result.is_err());

            if let Err(DbError::InvalidTimeRange { start, end }) = invalid_range_result {
                assert_eq!(start, ts3);
                assert_eq!(end, ts2);
            } else {
                panic!("Expected InvalidTimeRange error");
            }
        }

        // Test equal start/end (invalid)
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let equal_range_result = execute_query(chunk_guard, ts3..ts3, None);
            assert!(equal_range_result.is_err());
        }
    }

    #[test]
    fn test_execute_query_with_tags() {
        // Create tags
        let tags_host1 = create_tags(&[("host", "server1"), ("region", "us-east")]);
        let tags_host2 = create_tags(&[("host", "server2"), ("region", "us-east")]);
        let tags_host3 = create_tags(&[("host", "server3"), ("region", "us-west")]);

        // Create test data with real timestamps
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts3 = get_current_timestamp();

        let points = vec![
            DataPoint {
                timestamp: ts1,
                value: 1.0,
                tags: tags_host1.clone(),
            },
            DataPoint {
                timestamp: ts2,
                value: 2.0,
                tags: tags_host2.clone(),
            },
            DataPoint {
                timestamp: ts3,
                value: 3.0,
                tags: tags_host3.clone(),
            },
        ];

        let chunk = create_test_chunk(points);
        let chunk_arc = Arc::new(RwLock::new(chunk));

        // Test with host filter
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let host1_filter = create_tags(&[("host", "server1")]);
            let host1_results =
                execute_query(chunk_guard, ts1..(ts3 + 1), Some(&host1_filter)).unwrap();
            assert_eq!(host1_results.len(), 1);
            assert_eq!(host1_results[0], (ts1, 1.0));
        }

        // Test with region filter
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let region_east_filter = create_tags(&[("region", "us-east")]);
            let region_results =
                execute_query(chunk_guard, ts1..(ts3 + 1), Some(&region_east_filter)).unwrap();
            assert_eq!(region_results.len(), 2);
            assert_eq!(region_results, vec![(ts1, 1.0), (ts2, 2.0)]);
        }

        // Test with multiple tag filters
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let multi_filter = create_tags(&[("host", "server2"), ("region", "us-east")]);
            let multi_results =
                execute_query(chunk_guard, ts1..(ts3 + 1), Some(&multi_filter)).unwrap();
            assert_eq!(multi_results.len(), 1);
            assert_eq!(multi_results[0], (ts2, 2.0));
        }

        // Test with non-matching filter
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let no_match_filter = create_tags(&[("host", "nonexistent")]);
            let no_match_results =
                execute_query(chunk_guard, ts1..(ts3 + 1), Some(&no_match_filter)).unwrap();
            assert_eq!(no_match_results.len(), 0);
        }
    }

    #[test]
    fn test_check_tags() {
        // Test exact match
        let point_tags = create_tags(&[("host", "server1"), ("region", "us-east")]);
        let filter_exact = create_tags(&[("host", "server1"), ("region", "us-east")]);
        assert!(check_tags(&point_tags, &filter_exact));

        // Test subset match
        let filter_subset = create_tags(&[("host", "server1")]);
        assert!(check_tags(&point_tags, &filter_subset));

        // Test non-match (different value)
        let filter_diff_value = create_tags(&[("host", "server2")]);
        assert!(!check_tags(&point_tags, &filter_diff_value));

        // Test non-match (non-existent key)
        let filter_bad_key = create_tags(&[("nonexistent", "value")]);
        assert!(!check_tags(&point_tags, &filter_bad_key));

        // Test non-match (too many tags in filter)
        let filter_too_many =
            create_tags(&[("host", "server1"), ("region", "us-east"), ("extra", "tag")]);
        assert!(!check_tags(&point_tags, &filter_too_many));

        // Test with empty point tags
        let empty_tags = TagSet::new();
        let any_filter = create_tags(&[("host", "any")]);
        assert!(!check_tags(&empty_tags, &any_filter));

        // Test with empty filter
        let empty_filter = TagSet::new();
        assert!(check_tags(&point_tags, &empty_filter));
    }

    #[test]
    fn test_execute_query_edge_cases() {
        // Create a chunk with no data
        let empty_chunk = TimeSeriesChunk::default();
        let empty_arc = Arc::new(RwLock::new(empty_chunk));

        // Query on empty chunk
        {
            let empty_guard = empty_arc.read().unwrap();
            let empty_results = execute_query(empty_guard, 0..100, None).unwrap();
            assert_eq!(empty_results.len(), 0);
        }

        // Create a chunk with one point
        let ts = get_current_timestamp();
        let point = DataPoint {
            timestamp: ts,
            value: 42.0,
            tags: create_tags(&[("single", "point")]),
        };

        let single_chunk = create_test_chunk(vec![point]);
        let single_arc = Arc::new(RwLock::new(single_chunk));

        // Test exact match time range
        {
            let single_guard = single_arc.read().unwrap();
            let exact_results = execute_query(single_guard, ts..(ts + 1), None).unwrap();
            assert_eq!(exact_results.len(), 1);
            assert_eq!(exact_results[0], (ts, 42.0));
        }

        // Test range that starts exactly at the point
        {
            let single_guard = single_arc.read().unwrap();
            let start_at_results = execute_query(single_guard, ts..(ts + 100), None).unwrap();
            assert_eq!(start_at_results.len(), 1);
        }

        // Test range that ends exactly at the point (exclusive, so should return nothing)
        {
            let single_guard = single_arc.read().unwrap();
            let end_at_results = execute_query(single_guard, (ts - 100)..ts, None).unwrap();
            assert_eq!(end_at_results.len(), 0);
        }
    }

    // --- Vectorized aggregates and reduce_last ---

    #[test]
    fn test_reduce_last() {
        assert_eq!(reduce_last(&[]), None);
        assert_eq!(reduce_last(&[(100, 1.0)]), Some((100, 1.0)));
        assert_eq!(
            reduce_last(&[(100, 1.0), (200, 2.0), (300, 3.0)]),
            Some((300, 3.0))
        );
    }

    #[test]
    fn test_aggregate_sum() {
        // PromQL sum(): IEEE-754, no filtering; empty => 0.0
        assert_eq!(aggregate_sum(&[]), 0.0);
        assert_eq!(aggregate_sum(&[1.0]), 1.0);
        assert_eq!(aggregate_sum(&[1.0, 2.0, 3.0]), 6.0);
        assert!(aggregate_sum(&[f64::NAN]).is_nan());
        assert!(aggregate_sum(&[1.0, f64::NAN, 2.0]).is_nan());
        assert_eq!(aggregate_sum(&[f64::INFINITY, 1.0]), f64::INFINITY);
        assert_eq!(aggregate_sum(&[f64::NEG_INFINITY, 2.0]), f64::NEG_INFINITY);
        assert!(aggregate_sum(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
    }

    #[test]
    fn test_aggregate_avg() {
        // PromQL avg(): sum/count over all; any NaN => NaN
        assert!(aggregate_avg(&[]).is_nan());
        assert_eq!(aggregate_avg(&[4.0]), 4.0);
        assert_eq!(aggregate_avg(&[2.0, 4.0, 6.0]), 4.0);
        assert!(aggregate_avg(&[f64::NAN]).is_nan());
        assert!(aggregate_avg(&[1.0, f64::NAN, 3.0]).is_nan());
    }

    #[test]
    fn test_aggregate_min_max() {
        // PromQL min/max(): NaN only wins when all values are NaN
        assert!(aggregate_min(&[]).is_nan());
        assert!(aggregate_max(&[]).is_nan());
        assert_eq!(aggregate_min(&[3.0, 1.0, 2.0]), 1.0);
        assert_eq!(aggregate_max(&[3.0, 1.0, 2.0]), 3.0);
        assert_eq!(aggregate_min(&[1.0, f64::NAN, 2.0]), 1.0);
        assert_eq!(aggregate_max(&[1.0, f64::NAN, 2.0]), 2.0);
        assert_eq!(aggregate_min(&[f64::NAN, -5.0, f64::NAN]), -5.0);
        assert!(aggregate_min(&[f64::NAN]).is_nan());
        assert!(aggregate_max(&[f64::NAN]).is_nan());
    }

    #[test]
    fn test_aggregate_count() {
        // PromQL count(): count all elements, no filtering
        assert_eq!(aggregate_count(&[]), 0.0);
        assert_eq!(aggregate_count(&[1.0, 2.0, 3.0]), 3.0);
        assert_eq!(aggregate_count(&[1.0, f64::NAN, 2.0]), 3.0);
        assert_eq!(aggregate_count(&[f64::NAN]), 1.0);
    }

    // --- Range function tests ---

    #[test]
    fn test_compute_rate_monotonic_counter() {
        let points = vec![
            (1_000_000_000u64, 10.0),
            (2_000_000_000, 20.0),
            (3_000_000_000, 30.0),
            (4_000_000_000, 40.0),
        ];
        let rate = compute_rate(&points).unwrap();
        assert!(
            (rate - 10.0).abs() < 1e-9,
            "rate should be 10/s, got {}",
            rate
        );
    }

    #[test]
    fn test_compute_rate_with_counter_reset() {
        let points = vec![
            (1_000_000_000u64, 100.0),
            (2_000_000_000, 150.0),
            (3_000_000_000, 20.0), // reset
            (4_000_000_000, 70.0),
        ];
        // increase: (150-100) + 20 + (70-20) = 50 + 20 + 50 = 120
        // span: 3 seconds
        let rate = compute_rate(&points).unwrap();
        assert!(
            (rate - 40.0).abs() < 1e-9,
            "rate should be 40/s, got {}",
            rate
        );
    }

    #[test]
    fn test_compute_rate_needs_two_points() {
        assert!(compute_rate(&[]).is_none());
        assert!(compute_rate(&[(1_000_000_000, 5.0)]).is_none());
    }

    #[test]
    fn test_compute_rate_zero_timespan() {
        let points = vec![(1_000_000_000u64, 5.0), (1_000_000_000, 10.0)];
        assert!(compute_rate(&points).is_none());
    }

    #[test]
    fn test_compute_increase_monotonic() {
        let points = vec![
            (1_000_000_000u64, 0.0),
            (2_000_000_000, 10.0),
            (3_000_000_000, 25.0),
        ];
        assert_eq!(compute_increase(&points).unwrap(), 25.0);
    }

    #[test]
    fn test_compute_increase_with_reset() {
        let points = vec![
            (1_000_000_000u64, 100.0),
            (2_000_000_000, 50.0), // reset
            (3_000_000_000, 80.0),
        ];
        // (50) + (80-50) = 50 + 30 = 80
        assert_eq!(compute_increase(&points).unwrap(), 80.0);
    }

    #[test]
    fn test_compute_avg_over_time() {
        let points = vec![
            (1_000_000_000u64, 10.0),
            (2_000_000_000, 20.0),
            (3_000_000_000, 30.0),
        ];
        assert_eq!(compute_avg_over_time(&points).unwrap(), 20.0);
    }

    #[test]
    fn test_compute_avg_over_time_nan_propagates() {
        let points = vec![
            (1_000_000_000u64, 10.0),
            (2_000_000_000, f64::NAN),
            (3_000_000_000, 30.0),
        ];
        assert!(compute_avg_over_time(&points).unwrap().is_nan());
    }

    #[test]
    fn test_compute_avg_over_time_empty() {
        assert!(compute_avg_over_time(&[]).is_none());
    }

    #[test]
    fn test_compute_max_min_over_time() {
        let points = vec![
            (1_000_000_000u64, 10.0),
            (2_000_000_000, 30.0),
            (3_000_000_000, 20.0),
        ];
        assert_eq!(compute_max_over_time(&points).unwrap(), 30.0);
        assert_eq!(compute_min_over_time(&points).unwrap(), 10.0);
    }

    #[test]
    fn test_compute_max_min_over_time_nan_only_when_all_nan() {
        let points = vec![
            (1_000_000_000u64, f64::NAN),
            (2_000_000_000, 5.0),
            (3_000_000_000, 15.0),
        ];
        assert_eq!(compute_max_over_time(&points).unwrap(), 15.0);
        assert_eq!(compute_min_over_time(&points).unwrap(), 5.0);
    }

    #[test]
    fn test_compute_sum_over_time() {
        let points = vec![
            (1_000_000_000u64, 1.0),
            (2_000_000_000, 2.0),
            (3_000_000_000, 3.0),
        ];
        assert_eq!(compute_sum_over_time(&points).unwrap(), 6.0);
    }

    #[test]
    fn test_compute_sum_over_time_nan_propagates() {
        let points = vec![
            (1_000_000_000u64, 1.0),
            (2_000_000_000, f64::NAN),
            (3_000_000_000, 3.0),
        ];
        assert!(compute_sum_over_time(&points).unwrap().is_nan());
    }

    // --- Adversarial range function tests ---

    #[test]
    fn test_compute_rate_double_reset() {
        let points = vec![
            (1_000_000_000u64, 50.0),
            (2_000_000_000, 10.0), // reset 1
            (3_000_000_000, 5.0),  // reset 2
            (4_000_000_000, 30.0),
        ];
        // increase: 10 + 5 + (30-5) = 10 + 5 + 25 = 40
        // span: 3s → rate = 40/3
        let rate = compute_rate(&points).unwrap();
        assert!((rate - 40.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_compute_rate_constant_counter() {
        let points = vec![
            (1_000_000_000u64, 42.0),
            (2_000_000_000, 42.0),
            (3_000_000_000, 42.0),
        ];
        assert_eq!(compute_rate(&points).unwrap(), 0.0);
    }

    #[test]
    fn test_range_functions_all_nan() {
        let points = vec![(1_000_000_000u64, f64::NAN), (2_000_000_000, f64::NAN)];
        assert!(compute_avg_over_time(&points).unwrap().is_nan());
        assert!(compute_max_over_time(&points).unwrap().is_nan());
        assert!(compute_min_over_time(&points).unwrap().is_nan());
    }
}
