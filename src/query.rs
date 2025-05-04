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
    let end_idx = chunk
        .timestamps
        .partition_point(|&ts| ts < time_range.end);

    // If the range is empty or invalid, return early
    if start_idx >= end_idx {
        return Ok(Vec::new());
    }

    // --- Parallel Filtering and Collection --- //
    let results: Vec<(Timestamp, Value)> = (start_idx..end_idx)
        .into_par_iter() // Parallel iterator over the relevant index range
        .filter_map(|i| {
            // Apply tag filter if present
            let tags_match = tag_filter.map_or(true, |filter| {
                check_tags(&chunk.tags[i], filter)
            });

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

// --- Potential future additions --- //
// fn execute_aggregate_query(...) { ... }
// fn execute_downsample_query(...) { ... }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataPoint, TimeSeriesChunk};
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::thread;

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
            DataPoint { timestamp: ts1, value: 1.0, tags: TagSet::new() },
            DataPoint { timestamp: ts2, value: 2.0, tags: TagSet::new() },
            DataPoint { timestamp: ts3, value: 3.0, tags: TagSet::new() },
            DataPoint { timestamp: ts4, value: 4.0, tags: TagSet::new() },
            DataPoint { timestamp: ts5, value: 5.0, tags: TagSet::new() },
        ];
        
        let chunk = create_test_chunk(points);
        let chunk_arc = Arc::new(RwLock::new(chunk));
        
        // Test full range query
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let all_results = execute_query(chunk_guard, ts1..(ts5 + 1), None).unwrap();
            assert_eq!(all_results.len(), 5);
            assert_eq!(all_results, vec![
                (ts1, 1.0),
                (ts2, 2.0),
                (ts3, 3.0),
                (ts4, 4.0),
                (ts5, 5.0),
            ]);
        }
        
        // Test partial range query
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let partial_results = execute_query(chunk_guard, ts2..(ts4 + 1), None).unwrap();
            assert_eq!(partial_results.len(), 3);
            assert_eq!(partial_results, vec![
                (ts2, 2.0),
                (ts3, 3.0),
                (ts4, 4.0),
            ]);
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
            DataPoint { timestamp: ts1, value: 1.0, tags: tags_host1.clone() },
            DataPoint { timestamp: ts2, value: 2.0, tags: tags_host2.clone() },
            DataPoint { timestamp: ts3, value: 3.0, tags: tags_host3.clone() },
        ];
        
        let chunk = create_test_chunk(points);
        let chunk_arc = Arc::new(RwLock::new(chunk));
        
        // Test with host filter
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let host1_filter = create_tags(&[("host", "server1")]);
            let host1_results = execute_query(chunk_guard, ts1..(ts3 + 1), Some(&host1_filter)).unwrap();
            assert_eq!(host1_results.len(), 1);
            assert_eq!(host1_results[0], (ts1, 1.0));
        }
        
        // Test with region filter
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let region_east_filter = create_tags(&[("region", "us-east")]);
            let region_results = execute_query(chunk_guard, ts1..(ts3 + 1), Some(&region_east_filter)).unwrap();
            assert_eq!(region_results.len(), 2);
            assert_eq!(region_results, vec![(ts1, 1.0), (ts2, 2.0)]);
        }
        
        // Test with multiple tag filters
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let multi_filter = create_tags(&[("host", "server2"), ("region", "us-east")]);
            let multi_results = execute_query(chunk_guard, ts1..(ts3 + 1), Some(&multi_filter)).unwrap();
            assert_eq!(multi_results.len(), 1);
            assert_eq!(multi_results[0], (ts2, 2.0));
        }
        
        // Test with non-matching filter
        {
            let chunk_guard = chunk_arc.read().unwrap();
            let no_match_filter = create_tags(&[("host", "nonexistent")]);
            let no_match_results = execute_query(chunk_guard, ts1..(ts3 + 1), Some(&no_match_filter)).unwrap();
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
        let filter_too_many = create_tags(&[
            ("host", "server1"), 
            ("region", "us-east"),
            ("extra", "tag")
        ]);
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
            tags: create_tags(&[("single", "point")])
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
}

