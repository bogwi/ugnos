#![allow(unused_imports)] // Allow unused imports for now during development

use ugnos::*;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc; 
use std::thread;
use std::time::Duration;
use std::path::PathBuf;
use std::fs;
use std::time::SystemTime;

// Helper function to create a TagSet from a slice of tuples
fn tags_from(pairs: &[(&str, &str)]) -> TagSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// Helper function to sort query results for comparison
fn sort_results(results: &mut Vec<(Timestamp, Value)>) {
    results.sort_by_key(|&(ts, _)| ts);
}

// Helper function to create and clean test directories
fn setup_test_dir(dir_name: &str) -> PathBuf {
    let path = PathBuf::from(format!("./test_{}", dir_name));
    // Clean up any previous test data
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_insert_and_query_single_series() {
    // Use a short flush interval for testing
    let db = DbCore::new(Duration::from_millis(50));
    let series_name = "test_series_1";

    let tags1 = tags_from(&[("host", "serverA"), ("region", "us-east")]);
    let tags2 = tags_from(&[("host", "serverB"), ("region", "us-west")]);

    db.insert(series_name, 100, 10.5, tags1.clone()).unwrap();
    db.insert(series_name, 200, 11.0, tags1.clone()).unwrap();
    db.insert(series_name, 300, 12.5, tags2.clone()).unwrap();
    db.insert(series_name, 150, 9.0, tags1.clone()).unwrap(); // Out of order insert

    // Wait longer than flush interval to ensure data is persisted
    // Or trigger flush manually for deterministic tests
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(100)); // Give flush thread time

    // Query all data
    let mut results_all = db.query(series_name, 0..400, None).unwrap();
    sort_results(&mut results_all);
    assert_eq!(results_all.len(), 4);
    assert_eq!(results_all, vec![(100, 10.5), (150, 9.0), (200, 11.0), (300, 12.5)]);

    // Query specific time range
    let mut results_range = db.query(series_name, 120..250, None).unwrap();
    sort_results(&mut results_range);
    assert_eq!(results_range.len(), 2);
    assert_eq!(results_range, vec![(150, 9.0), (200, 11.0)]);

    // Query with tag filter (match)
    let filter1 = tags_from(&[("host", "serverA")]);
    let mut results_tag1 = db.query(series_name, 0..400, Some(&filter1)).unwrap();
    sort_results(&mut results_tag1);
    assert_eq!(results_tag1.len(), 3);
    assert_eq!(results_tag1, vec![(100, 10.5), (150, 9.0), (200, 11.0)]);

    // Query with tag filter (match multiple)
    let filter1_full = tags_from(&[("host", "serverA"), ("region", "us-east")]);
    let mut results_tag1_full = db.query(series_name, 0..400, Some(&filter1_full)).unwrap();
    sort_results(&mut results_tag1_full);
    assert_eq!(results_tag1_full.len(), 3);
    assert_eq!(results_tag1_full, vec![(100, 10.5), (150, 9.0), (200, 11.0)]);

    // Query with tag filter (match other tags)
    let filter2 = tags_from(&[("region", "us-west")]);
    let mut results_tag2 = db.query(series_name, 0..400, Some(&filter2)).unwrap();
    sort_results(&mut results_tag2);
    assert_eq!(results_tag2.len(), 1);
    assert_eq!(results_tag2, vec![(300, 12.5)]);

    // Query with tag filter (no match)
    let filter_no_match = tags_from(&[("host", "serverC")]);
    let results_no_match = db.query(series_name, 0..400, Some(&filter_no_match)).unwrap();
    assert!(results_no_match.is_empty());

    // Query with tag filter (partial match but not all)
    let filter_partial = tags_from(&[("host", "serverA"), ("region", "us-west")]);
    let results_partial = db.query(series_name, 0..400, Some(&filter_partial)).unwrap();
    assert!(results_partial.is_empty());
}

#[test]
fn test_query_non_existent_series() {
    let db = DbCore::default(); // Use default flush interval
    let result = db.query("non_existent_series", 0..100, None);
    match result {
        Err(DbError::SeriesNotFound(name)) => assert_eq!(name, "non_existent_series"),
        _ => panic!("Expected SeriesNotFound error"),
    }
}

#[test]
fn test_insert_multiple_series() {
    let db = DbCore::new(Duration::from_millis(50));
    let series1 = "cpu_usage";
    let series2 = "memory_usage";

    let tags_s1 = tags_from(&[("host", "server1")]);
    let tags_s2 = tags_from(&[("host", "server2")]);

    db.insert(series1, 100, 0.8, tags_s1.clone()).unwrap();
    db.insert(series2, 110, 55.2, tags_s2.clone()).unwrap();
    db.insert(series1, 200, 0.7, tags_s1.clone()).unwrap();
    db.insert(series2, 210, 56.8, tags_s2.clone()).unwrap();

    db.flush().unwrap();
    thread::sleep(Duration::from_millis(100));

    // Query series 1
    let mut results1 = db.query(series1, 0..300, None).unwrap();
    sort_results(&mut results1);
    assert_eq!(results1, vec![(100, 0.8), (200, 0.7)]);

    // Query series 2
    let mut results2 = db.query(series2, 0..300, None).unwrap();
    sort_results(&mut results2);
    assert_eq!(results2, vec![(110, 55.2), (210, 56.8)]);
}

#[test]
fn test_concurrent_inserts() {
    let db = Arc::new(DbCore::new(Duration::from_millis(20))); // Use Arc for sharing across threads
    let num_threads = 4;
    let points_per_thread = 100;
    let series_name = "concurrent_series";

    let mut handles = vec![];

    for i in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let start_ts = (i * points_per_thread) as u64;
            for j in 0..points_per_thread {
                let ts = start_ts + j as u64;
                let val = ts as f64 * 1.1;
                let tags = tags_from(&[("thread_id", &i.to_string()), ("point_id", &j.to_string())]);
                db_clone.insert(series_name, ts, val, tags).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Flush and wait
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(100));

    // Query all data
    let total_points = num_threads * points_per_thread;
    let results = db.query(series_name, 0..(total_points as u64), None).unwrap();

    assert_eq!(results.len(), total_points);

    // Optional: Verify some specific points or properties if needed
    // (Sorting and checking exact values might be complex due to potential minor timing variations)
    let mut timestamps: Vec<u64> = results.iter().map(|(ts, _)| *ts).collect();
    timestamps.sort();
    for i in 0..(total_points as u64) {
        assert_eq!(timestamps[i as usize], i);
    }
}

#[test]
fn test_invalid_time_range() {
    let db = DbCore::default();
    db.insert("test", 100, 1.0, TagSet::new()).unwrap();
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(100));

    let result = db.query("test", 100..50, None);
    match result {
        Err(DbError::InvalidTimeRange { start, end }) => {
            assert_eq!(start, 100);
            assert_eq!(end, 50);
        }
        _ => panic!("Expected InvalidTimeRange error"),
    }

    let result_equal = db.query("test", 100..100, None);
     match result_equal {
        Err(DbError::InvalidTimeRange { start, end }) => {
            assert_eq!(start, 100);
            assert_eq!(end, 100);
        }
        _ => panic!("Expected InvalidTimeRange error for equal start/end"),
    }
}

#[test]
fn test_snapshot_and_recover() {
    // Create a test directory
    let data_dir = setup_test_dir("snapshot_recover");
    
    // Configure database with persistence enabled
    let config = DbConfig {
        flush_interval: Duration::from_millis(50),
        data_dir: data_dir.clone(),
        wal_buffer_size: 10,
        enable_wal: true,
        enable_snapshots: true,
        snapshot_interval: Duration::from_secs(10), // Long enough to not trigger automatically
    };
    
    // Lists to store timestamps for verification
    let mut pre_snapshot_timestamps = Vec::new();
    let mut post_snapshot_timestamps = Vec::new();
    
    // Create database and insert test data
    {
        let db = DbCore::with_config(config.clone()).unwrap();
        let series_name = "test_snapshot_series";
        
        let tags1 = tags_from(&[("host", "serverA"), ("region", "us-east")]);
        let tags2 = tags_from(&[("host", "serverB"), ("region", "us-west")]);
        
        println!("Inserting pre-snapshot data...");
        // Insert initial batch with real timestamps (100 points)
        for i in 0..100 {
            // Use current system time + incremental offset for unique timestamps
            let timestamp = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64 + i;
            
            let val = i as f64 * 1.5;
            let tags = if i % 2 == 0 { tags1.clone() } else { tags2.clone() };
            db.insert(series_name, timestamp, val, tags).unwrap();
            pre_snapshot_timestamps.push((timestamp, val));
            
            // Small sleep to ensure unique timestamps
            std::thread::sleep(Duration::from_nanos(1));
        }
        
        // Flush data to storage
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        
        println!("Creating snapshot...");
        // Create snapshot
        db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(100)); // Give time for snapshot to complete
        
        println!("Inserting post-snapshot data...");
        // Insert additional data after snapshot (50 points)
        for i in 0..50 {
            // Use current system time + incremental offset for unique timestamps
            let timestamp = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64 + i;
            
            let val = i as f64 * 2.0;
            let tags = if i % 2 == 0 { tags1.clone() } else { tags2.clone() };
            db.insert(series_name, timestamp, val, tags).unwrap();
            post_snapshot_timestamps.push((timestamp, val));
            
            // Small sleep to ensure unique timestamps
            std::thread::sleep(Duration::from_nanos(1));
        }
        
        // Flush again to ensure WAL has the newer entries
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        
        // DB will be dropped here and WAL should be flushed
        println!("First DB instance being dropped...");
    }
    
    println!("Creating new DB instance and recovering data...");
    // Now create a new DB instance and recover data
    {
        let mut db = DbCore::with_config(config).unwrap();
        
        // Recover data
        db.recover().unwrap();
        
        // Verify all data was recovered
        let series_name = "test_snapshot_series";
        
        // Create timestamp range that covers all our data
        let min_ts = pre_snapshot_timestamps.iter()
            .chain(post_snapshot_timestamps.iter())
            .map(|(ts, _)| *ts)
            .min()
            .unwrap_or(0);
        
        let max_ts = pre_snapshot_timestamps.iter()
            .chain(post_snapshot_timestamps.iter())
            .map(|(ts, _)| *ts)
            .max()
            .unwrap_or(u64::MAX);
        
        let mut results = db.query(series_name, min_ts..(max_ts + 1), None).unwrap();
        sort_results(&mut results);
        
        println!("Expected {} total points ({} pre-snapshot, {} post-snapshot)",
            pre_snapshot_timestamps.len() + post_snapshot_timestamps.len(),
            pre_snapshot_timestamps.len(),
            post_snapshot_timestamps.len());
        println!("Recovered {} points", results.len());
        
        // Both pre-snapshot and post-snapshot data should be recovered
        assert_eq!(results.len(), pre_snapshot_timestamps.len() + post_snapshot_timestamps.len(),
            "Expected {} total points, but got {}", 
            pre_snapshot_timestamps.len() + post_snapshot_timestamps.len(), 
            results.len());
        
        // Check that pre-snapshot points were recovered
        for (timestamp, value) in &pre_snapshot_timestamps {
            assert!(results.contains(&(*timestamp, *value)), 
                "Pre-snapshot point ({}, {}) not found in recovered data", timestamp, value);
        }
        
        // Check that post-snapshot points were recovered (critical test)
        for (timestamp, value) in &post_snapshot_timestamps {
            assert!(results.contains(&(*timestamp, *value)), 
                "Post-snapshot point ({}, {}) not found in recovered data", timestamp, value);
        }
        
        println!("All data points were successfully recovered!");
    }
    
    // Clean up
    let _ = fs::remove_dir_all(data_dir);
}

#[test]
fn test_recover_with_no_persistence() {
    // Create config with persistence disabled
    let config = DbConfig {
        flush_interval: Duration::from_millis(50),
        data_dir: PathBuf::from("./temp_no_persistence"),
        wal_buffer_size: 10,
        enable_wal: false,
        enable_snapshots: false,
        snapshot_interval: Duration::from_secs(10),
    };
    
    // Create database
    let mut db = DbCore::with_config(config).unwrap();
    
    // Recover should succeed even with no persistence enabled
    let result = db.recover();
    assert!(result.is_ok());
}

#[test]
fn test_wal_recovery_only() {
    // Create a test directory
    let data_dir = setup_test_dir("wal_only");
    
    // Configure database with WAL only (no snapshots)
    let config = DbConfig {
        flush_interval: Duration::from_millis(50),
        data_dir: data_dir.clone(),
        wal_buffer_size: 10,
        enable_wal: true,
        enable_snapshots: false, // No snapshots
        snapshot_interval: Duration::from_secs(10),
    };
    
    // Create database and insert test data
    {
        let db = DbCore::with_config(config.clone()).unwrap();
        let series_name = "test_wal_series";
        
        let tags = tags_from(&[("host", "serverA"), ("region", "us-east")]);
        
        for i in 0..50 {
            let ts = 1000 + i * 10;
            let val = i as f64;
            db.insert(series_name, ts, val, tags.clone()).unwrap();
        }
        
        // Flush data to storage and WAL
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        
        // DB will be dropped here and WAL should be flushed
    }
    
    // Now create a new DB instance and recover data
    {
        let mut db = DbCore::with_config(config).unwrap();
        
        // Recover data
        db.recover().unwrap();
        
        // Verify all data was recovered from WAL
        let series_name = "test_wal_series";
        let mut results = db.query(series_name, 0..2000, None).unwrap();
        sort_results(&mut results);
        
        // Should have 50 points
        assert_eq!(results.len(), 50);
        
        // Check some specific points
        assert!(results.contains(&(1000, 0.0))); // First point
        assert!(results.contains(&(1490, 49.0))); // Last point
        
        // Verify timestamps are as expected
        for i in 0..50 {
            let ts = 1000 + i * 10;
            let val = i as f64;
            assert!(results.contains(&(ts, val)));
        }
    }
    
    // Clean up
    let _ = fs::remove_dir_all(data_dir);
}

#[test]
fn test_snapshot_method_error() {
    // Create a test directory and clean it up at the end
    let data_dir = setup_test_dir("snapshot_method_error");
    
    // Configure database with snapshots disabled
    let config = DbConfig {
        flush_interval: Duration::from_millis(50),
        data_dir: data_dir.clone(),
        wal_buffer_size: 10,
        enable_wal: true,
        enable_snapshots: false, // Snapshots disabled
        snapshot_interval: Duration::from_secs(10),
    };
    
    // Create database
    let db = DbCore::with_config(config).unwrap();
    
    // Snapshot should fail because it's disabled
    let result = db.snapshot();
    assert!(result.is_err());
    
    if let Err(DbError::ConfigError(msg)) = result {
        assert!(msg.contains("Snapshots are not enabled"));
    } else {
        panic!("Expected ConfigError but got something else");
    }
    
    // Clean up
    let _ = fs::remove_dir_all(data_dir);
}

#[test]
fn test_wal_recovery_after_snapshot() {
    // Create a test directory
    let data_dir = setup_test_dir("snapshot_wal_recovery");
    
    // Configure database with persistence enabled
    let config = DbConfig {
        flush_interval: Duration::from_millis(50),
        data_dir: data_dir.clone(),
        wal_buffer_size: 5, // Small buffer to ensure frequent WAL writes
        enable_wal: true,
        enable_snapshots: true,
        snapshot_interval: Duration::from_secs(10),
    };
    
    let series_name = "test_recovery_series";
    
    // Track data for verification
    let mut pre_snapshot_timestamps = Vec::new();
    let mut post_snapshot_timestamps = Vec::new();
    let mut unflushed_timestamps = Vec::new();
    
    // Create database, create snapshot, then insert more data
    {
        let db = DbCore::with_config(config.clone()).unwrap();
        
        // Insert initial batch (10 points)
        let tags = tags_from(&[("host", "server1"), ("region", "us-east")]);
        for i in 0..10 {
            // Use real system timestamps
            let timestamp = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64 + i;
            
            let val = i as f64;
            db.insert(series_name, timestamp, val, tags.clone()).unwrap();
            pre_snapshot_timestamps.push((timestamp, val));
            
            // Small sleep to ensure unique timestamps
            std::thread::sleep(Duration::from_nanos(1));
        }
        
        // Force a flush
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        
        // Create a snapshot
        db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(100));
        
        // Insert a second batch after the snapshot (10 more points)
        for i in 10..20 {
            // Use real system timestamps
            let timestamp = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64 + i;
            
            let val = i as f64;
            db.insert(series_name, timestamp, val, tags.clone()).unwrap();
            post_snapshot_timestamps.push((timestamp, val));
            
            // Small sleep to ensure unique timestamps
            std::thread::sleep(Duration::from_nanos(1));
        }
        
        // Force flush to ensure WAL entries are written
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        
        // Insert a third batch without flushing (might not be recovered depending on implementation)
        for i in 20..30 {
            // Use real system timestamps
            let timestamp = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64 + i;
            
            let val = i as f64;
            db.insert(series_name, timestamp, val, tags.clone()).unwrap();
            unflushed_timestamps.push((timestamp, val));
            
            // Small sleep to ensure unique timestamps
            std::thread::sleep(Duration::from_nanos(1));
        }
        
        // Don't flush explicitly - test if WAL auto-flush on drop works
    }
    
    // Create a new instance and recover
    {
        let mut db = DbCore::with_config(config).unwrap();
        db.recover().unwrap();
        
        // Calculate query range
        let min_ts = pre_snapshot_timestamps.iter()
            .chain(post_snapshot_timestamps.iter())
            .chain(unflushed_timestamps.iter())
            .map(|(ts, _)| *ts)
            .min()
            .unwrap_or(0);
        
        let max_ts = pre_snapshot_timestamps.iter()
            .chain(post_snapshot_timestamps.iter())
            .chain(unflushed_timestamps.iter())
            .map(|(ts, _)| *ts)
            .max()
            .unwrap_or(u64::MAX);
        
        // Query all data
        let mut results = db.query(series_name, min_ts..(max_ts + 1), None).unwrap();
        sort_results(&mut results);
        
        // Should have at least 20 points (pre-snapshot + post-snapshot), possibly 30 if WAL auto-flush works
        let expected_min = pre_snapshot_timestamps.len() + post_snapshot_timestamps.len();
        let expected_max = expected_min + unflushed_timestamps.len();
        
        println!("Recovered {} points", results.len());
        println!("Expected between {} and {} points", expected_min, expected_max);
        
        assert!(
            results.len() >= expected_min,
            "Expected at least {} points, got {}",
            expected_min,
            results.len()
        );
        
        // Verify all pre-snapshot points were recovered
        for (timestamp, value) in &pre_snapshot_timestamps {
            assert!(
                results.contains(&(*timestamp, *value)),
                "Pre-snapshot point ({}, {}) not found in recovered data",
                timestamp,
                value
            );
        }
        
        // Verify all post-snapshot points were recovered
        for (timestamp, value) in &post_snapshot_timestamps {
            assert!(
                results.contains(&(*timestamp, *value)),
                "Post-snapshot point ({}, {}) not found in recovered data",
                timestamp,
                value
            );
        }
        
        // If we recovered more than 20 points, check which unflushed points were recovered
        if results.len() > expected_min {
            let unflushed_recovered = unflushed_timestamps.iter()
                .filter(|(ts, val)| results.contains(&(*ts, *val)))
                .count();
            
            println!(
                "Recovered {}/{} points from unflushed batch (auto-flush on drop)",
                unflushed_recovered,
                unflushed_timestamps.len()
            );
        }
    }
    
    // Clean up
    let _ = fs::remove_dir_all(data_dir);
}

#[test]
fn test_empty_inserts_and_tags() {
    let db = DbCore::new(Duration::from_millis(50));
    
    // Test with empty tag set
    let empty_tags = TagSet::new();
    let series_name = "empty_tags_series";
    
    // Insert with empty tags
    let timestamp = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    db.insert(series_name, timestamp, 42.0, empty_tags.clone()).unwrap();
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(100));
    
    // Query with empty tag filter
    let results = db.query(series_name, 0..(timestamp+1), Some(&empty_tags)).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (timestamp, 42.0));
    
    // Test series with no data
    let empty_series = "no_data_series";
    let results = db.query(empty_series, 0..u64::MAX, None);
    assert!(matches!(results, Err(DbError::SeriesNotFound(_))));
    
    // Query with non-matching tag filter
    let non_matching_tags = tags_from(&[("region", "nowhere")]);
    let results = db.query(series_name, 0..(timestamp+1), Some(&non_matching_tags)).unwrap();
    assert_eq!(results.len(), 0, "Query with non-matching tags should return empty results");
    
    // Test with flush on empty buffer
    db.flush().unwrap();
}

#[test]
fn test_large_number_of_points() {
    // Create DB with short flush interval for testing
    let db = DbCore::new(Duration::from_millis(50));
    let series_name = "large_series";
    
    // Number of points to insert
    let num_points = 10_000; // 10k points
    
    println!("Inserting {} points...", num_points);
    let start_time = std::time::Instant::now();
    
    // Insert a large number of points with real timestamps
    for i in 0..num_points {
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64 + i as u64;
        
        let value = i as f64 * 0.1;
        
        // Alternate between different tag sets
        let tags = if i % 3 == 0 {
            tags_from(&[("region", "us-east"), ("host", "server1")])
        } else if i % 3 == 1 {
            tags_from(&[("region", "us-west"), ("host", "server2")])
        } else {
            tags_from(&[("region", "eu-central"), ("host", "server3")])
        };
        
        db.insert(series_name, timestamp, value, tags).unwrap();
        
        // Add small sleep occasionally to ensure unique timestamps and prevent resource exhaustion
        if i % 1000 == 0 {
            thread::sleep(Duration::from_nanos(1));
        }
    }
    
    let insert_duration = start_time.elapsed();
    println!("Inserted {} points in {:?}", num_points, insert_duration);
    
    // Flush and ensure data is persisted
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(200)); // Give more time for flush
    
    // Get min and max timestamps for querying
    let min_timestamp = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let max_timestamp = min_timestamp + num_points as u64;
    
    // Query with different filters to verify data integrity
    let query_start = std::time::Instant::now();
    
    // Query us-east region (should be ~1/3 of points)
    let us_east_filter = tags_from(&[("region", "us-east")]);
    let us_east_results = db.query(series_name, 0..max_timestamp, Some(&us_east_filter)).unwrap();
    assert!(us_east_results.len() > num_points / 4, "Expected at least 1/4 of points in us-east");
    assert!(us_east_results.len() < num_points / 2, "Expected less than 1/2 of points in us-east");
    
    // Query eu-central + server3 (should be ~1/3 of points)
    let eu_server3_filter = tags_from(&[("region", "eu-central"), ("host", "server3")]);
    let eu_server3_results = db.query(series_name, 0..max_timestamp, Some(&eu_server3_filter)).unwrap();
    assert!(eu_server3_results.len() > num_points / 4, "Expected at least 1/4 of points for eu-central+server3");
    assert!(eu_server3_results.len() < num_points / 2, "Expected less than 1/2 of points for eu-central+server3");
    
    // Query all points
    let all_results = db.query(series_name, 0..max_timestamp, None).unwrap();
    assert_eq!(all_results.len(), num_points, "All points should be returned in full query");
    
    let query_duration = query_start.elapsed();
    println!("Queried {} points in {:?}", num_points, query_duration);
}

#[test]
fn test_specific_tag_combinations() {
    let db = DbCore::new(Duration::from_millis(50));
    let series_name = "tag_combinations";
    
    // Create points with various tag combinations
    let test_data = vec![
        // Hierarchical tags: region > datacenter > rack > host
        (tags_from(&[("region", "us-east"), ("datacenter", "dc1"), ("rack", "r1"), ("host", "h1"), ("service", "api")]), 100.0),
        (tags_from(&[("region", "us-east"), ("datacenter", "dc1"), ("rack", "r1"), ("host", "h2"), ("service", "db")]), 200.0),
        (tags_from(&[("region", "us-east"), ("datacenter", "dc1"), ("rack", "r2"), ("host", "h3"), ("service", "api")]), 300.0),
        (tags_from(&[("region", "us-east"), ("datacenter", "dc2"), ("rack", "r3"), ("host", "h4"), ("service", "cache")]), 400.0),
        (tags_from(&[("region", "us-west"), ("datacenter", "dc3"), ("rack", "r4"), ("host", "h5"), ("service", "api")]), 500.0),
        (tags_from(&[("region", "us-west"), ("datacenter", "dc3"), ("rack", "r4"), ("host", "h6"), ("service", "db")]), 600.0),
        
        // Multiple values for the same keys (different points)
        (tags_from(&[("os", "linux"), ("version", "ubuntu"), ("kernel", "5.4")]), 700.0),
        (tags_from(&[("os", "linux"), ("version", "centos"), ("kernel", "4.18")]), 800.0),
        (tags_from(&[("os", "windows"), ("version", "server2019"), ("kernel", "10.0")]), 900.0),
        
        // Special characters in tags
        (tags_from(&[("user-agent", "Mozilla/5.0"), ("path", "/api/v1/users"), ("status", "200")]), 1000.0),
        
        // Empty tag values and unicode
        (tags_from(&[("tag_with_empty_value", ""), ("unicode", "你好世界")]), 1100.0),
        
        // Many tags on a single point
        (tags_from(&[
            ("t1", "v1"), ("t2", "v2"), ("t3", "v3"), ("t4", "v4"), ("t5", "v5"),
            ("t6", "v6"), ("t7", "v7"), ("t8", "v8"), ("t9", "v9"), ("t10", "v10")
        ]), 1200.0)
    ];
    
    // Insert all test data with real timestamps
    for (i, (tags, value)) in test_data.iter().enumerate() {
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64 + i as u64;
            
        db.insert(series_name, timestamp, *value, tags.clone()).unwrap();
        
        // Small sleep to ensure unique timestamps
        thread::sleep(Duration::from_nanos(1));
    }
    
    // Flush to ensure all data is persisted
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(100));
    
    // Query with various tag filters
    
    // 1. Hierarchical query (region + datacenter)
    let region_dc_filter = tags_from(&[("region", "us-east"), ("datacenter", "dc1")]);
    let region_dc_results = db.query(series_name, 0..u64::MAX, Some(&region_dc_filter)).unwrap();
    assert_eq!(region_dc_results.len(), 3, "Expected 3 points matching us-east + dc1");
    
    // 2. Query for a specific service across all regions
    let service_filter = tags_from(&[("service", "api")]);
    let service_results = db.query(series_name, 0..u64::MAX, Some(&service_filter)).unwrap();
    assert_eq!(service_results.len(), 3, "Expected 3 points with service=api");
    
    // 3. Query with OS + kernel version filter
    let os_kernel_filter = tags_from(&[("os", "linux"), ("kernel", "5.4")]);
    let os_kernel_results = db.query(series_name, 0..u64::MAX, Some(&os_kernel_filter)).unwrap();
    assert_eq!(os_kernel_results.len(), 1, "Expected 1 point with os=linux + kernel=5.4");
    
    // 4. Query with special characters
    let special_filter = tags_from(&[("status", "200")]);
    let special_results = db.query(series_name, 0..u64::MAX, Some(&special_filter)).unwrap();
    assert_eq!(special_results.len(), 1, "Expected 1 point with status=200");
    
    // 5. Query with empty tag value
    let empty_value_filter = tags_from(&[("tag_with_empty_value", "")]);
    let empty_value_results = db.query(series_name, 0..u64::MAX, Some(&empty_value_filter)).unwrap();
    assert_eq!(empty_value_results.len(), 1, "Expected 1 point with empty tag value");
    
    // 6. Query with multiple specific tags that should match only one point
    let complex_filter = tags_from(&[("t1", "v1"), ("t5", "v5"), ("t10", "v10")]);
    let complex_results = db.query(series_name, 0..u64::MAX, Some(&complex_filter)).unwrap();
    assert_eq!(complex_results.len(), 1, "Expected 1 point with multiple specific tags");
    
    // 7. Query with non-existent tag should return empty results
    let nonexistent_filter = tags_from(&[("nonexistent", "value")]);
    let nonexistent_results = db.query(series_name, 0..u64::MAX, Some(&nonexistent_filter)).unwrap();
    assert_eq!(nonexistent_results.len(), 0, "Expected 0 points with non-existent tag");
}

#[test]
fn zz_cleanup_remove_data() {
    println!("Cleaning up integration test data directory...");
    let _ = fs::remove_dir_all(PathBuf::from("./data"));
    println!("Integration test cleanup complete.");
}

