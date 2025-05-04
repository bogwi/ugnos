use crate::error::DbError;
use crate::types::DataPoint;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Type alias for the buffer of a single series, protected by a Mutex.
// Arc allows sharing the Mutex across threads if needed, though here it's owned by the WriteBuffer HashMap.
type SeriesWriteBuffer = Arc<Mutex<Vec<DataPoint>>>;

/// A sharded write buffer for staging incoming data points before flushing to storage.
/// Uses a HashMap where keys are series names and values are mutex-protected Vecs.
#[derive(Debug, Default)]
pub struct WriteBuffer {
    buffers: HashMap<String, SeriesWriteBuffer>,
}

impl WriteBuffer {
    /// Stages a single data point into the buffer for the corresponding series.
    /// If the series buffer doesn't exist, it's created.
    /// Acquires a lock only on the specific series buffer being written to.
    pub fn stage(&mut self, series: &str, point: DataPoint) -> Result<(), DbError> {
        let buffer_arc = self
            .buffers
            .entry(series.to_string()) // Clone series name for ownership in HashMap
            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));

        // Lock the specific series buffer and append the point
        let mut buffer_guard = buffer_arc.lock()?; // Propagate PoisonError
        buffer_guard.push(point);

        Ok(())
    }

    /// Drains all data from all series buffers, returning it for flushing.
    /// This requires acquiring locks on all buffers sequentially.
    /// Consider potential performance implications if there are many series.
    /// Returns a HashMap mapping series names to their drained data points.
    pub fn drain_all_buffers(&mut self) -> HashMap<String, Vec<DataPoint>> {
        let mut drained_data = HashMap::new();
        for (series_name, buffer_arc) in self.buffers.iter() {
            // Attempt to lock the buffer. If poisoned, perhaps log and skip?
            if let Ok(mut buffer_guard) = buffer_arc.lock() {
                // Drain the buffer if it's not empty
                if !buffer_guard.is_empty() {
                    // Use std::mem::take to efficiently drain the Vec
                    let points = std::mem::take(&mut *buffer_guard);
                    drained_data.insert(series_name.clone(), points);
                }
            } else {
                // Log or handle the poison error appropriately
                // For now, we just skip this buffer if poisoned
                eprintln!("Warning: Buffer for series 	{}	 is poisoned, skipping drain.", series_name);
            }
        }
        // Optionally, remove entries from self.buffers if they are now empty and haven't been written to recently?
        // Or keep them around to avoid reallocation.
        drained_data
    }

    // /// Alternative: Drain only buffers exceeding a certain size threshold.
    // pub fn drain_ready_buffers(&mut self, size_threshold: usize) -> HashMap<String, Vec<DataPoint>> {
    //     // ... implementation ...
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TagSet, Timestamp, Value};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::thread;
    use std::sync::Arc;

    fn create_point(ts: Timestamp, val: Value, tags: TagSet) -> DataPoint {
        DataPoint {
            timestamp: ts,
            value: val,
            tags,
        }
    }

    fn get_current_timestamp() -> Timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn create_tags(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_stage_single_point() {
        let mut buffer = WriteBuffer::default();
        let series = "test_series";
        
        // Create a point with real timestamp
        let ts = get_current_timestamp();
        let tags = create_tags(&[("host", "server1")]);
        let point = create_point(ts, 42.0, tags);
        
        // Stage the point
        buffer.stage(series, point.clone()).unwrap();
        
        // Drain and verify
        let drained = buffer.drain_all_buffers();
        
        assert_eq!(drained.len(), 1, "Should have one series");
        assert!(drained.contains_key(series), "Should contain our series");
        
        let points = &drained[series];
        assert_eq!(points.len(), 1, "Should have one point");
        assert_eq!(points[0].timestamp, ts);
        assert_eq!(points[0].value, 42.0);
    }
    
    #[test]
    fn test_stage_multiple_points_same_series() {
        let mut buffer = WriteBuffer::default();
        let series = "multi_point_series";
        
        // Create points with real timestamps
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts3 = get_current_timestamp();
        
        let tags = create_tags(&[("host", "server1")]);
        
        // Stage multiple points to the same series
        buffer.stage(series, create_point(ts1, 1.0, tags.clone())).unwrap();
        buffer.stage(series, create_point(ts2, 2.0, tags.clone())).unwrap();
        buffer.stage(series, create_point(ts3, 3.0, tags.clone())).unwrap();
        
        // Drain and verify
        let drained = buffer.drain_all_buffers();
        
        assert_eq!(drained.len(), 1, "Should have one series");
        
        let points = &drained[series];
        assert_eq!(points.len(), 3, "Should have three points");
        
        // Points should be in the order they were inserted (no sorting in buffer)
        assert_eq!(points[0].timestamp, ts1);
        assert_eq!(points[0].value, 1.0);
        
        assert_eq!(points[1].timestamp, ts2);
        assert_eq!(points[1].value, 2.0);
        
        assert_eq!(points[2].timestamp, ts3);
        assert_eq!(points[2].value, 3.0);
    }
    
    #[test]
    fn test_stage_multiple_series() {
        let mut buffer = WriteBuffer::default();
        
        // Create series names
        let series1 = "series1";
        let series2 = "series2";
        
        // Create points with real timestamps
        let ts1 = get_current_timestamp();
        let ts2 = get_current_timestamp() + 100;
        
        let tags1 = create_tags(&[("region", "us-east")]);
        let tags2 = create_tags(&[("region", "us-west")]);
        
        // Stage points to different series
        buffer.stage(series1, create_point(ts1, 1.0, tags1.clone())).unwrap();
        buffer.stage(series2, create_point(ts2, 2.0, tags2.clone())).unwrap();
        
        // Drain and verify
        let drained = buffer.drain_all_buffers();
        
        assert_eq!(drained.len(), 2, "Should have two series");
        assert!(drained.contains_key(series1), "Should contain series1");
        assert!(drained.contains_key(series2), "Should contain series2");
        
        let points1 = &drained[series1];
        assert_eq!(points1.len(), 1, "Series1 should have one point");
        assert_eq!(points1[0].timestamp, ts1);
        assert_eq!(points1[0].value, 1.0);
        
        let points2 = &drained[series2];
        assert_eq!(points2.len(), 1, "Series2 should have one point");
        assert_eq!(points2[0].timestamp, ts2);
        assert_eq!(points2[0].value, 2.0);
    }
    
    #[test]
    fn test_drain_empty_buffer() {
        let mut buffer = WriteBuffer::default();
        
        // Drain without staging any points
        let drained = buffer.drain_all_buffers();
        
        assert_eq!(drained.len(), 0, "Drained data should be empty");
    }
    
    #[test]
    fn test_drain_leaves_buffers_empty() {
        let mut buffer = WriteBuffer::default();
        let series = "test_series";
        
        // Stage a point
        let ts = get_current_timestamp();
        let tags = create_tags(&[("host", "server1")]);
        buffer.stage(series, create_point(ts, 1.0, tags)).unwrap();
        
        // Drain
        let first_drain = buffer.drain_all_buffers();
        assert_eq!(first_drain.len(), 1, "First drain should contain our series");
        
        // Drain again - should be empty
        let second_drain = buffer.drain_all_buffers();
        assert_eq!(second_drain.len(), 0, "Second drain should be empty");
    }
    
    #[test]
    fn test_multithreaded_stage() {
        use std::sync::{Arc, Mutex};
        use std::thread;
        
        // Create a shared buffer
        let buffer = Arc::new(Mutex::new(WriteBuffer::default()));
        let series = "multithreaded_series";
        
        // Number of threads and points per thread
        let num_threads = 4;
        let points_per_thread = 25;
        
        // Create threads to stage points concurrently
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let buffer_clone = Arc::clone(&buffer);
            let series_name = series.to_string();
            
            let handle = thread::spawn(move || {
                for i in 0..points_per_thread {
                    // Create a unique timestamp
                    let ts = get_current_timestamp() + (thread_id * 1000 + i) as u64;
                    let value = (thread_id * 100 + i) as f64;
                    
                    // Create tags with thread ID
                    let tags = create_tags(&[
                        ("thread_id", &thread_id.to_string()),
                        ("point_id", &i.to_string())
                    ]);
                    
                    let point = create_point(ts, value, tags);
                    
                    // Acquire lock and stage point
                    let mut buffer_guard = buffer_clone.lock().unwrap();
                    buffer_guard.stage(&series_name, point).unwrap();
                    
                    // Small sleep to allow thread interleaving
                    thread::sleep(std::time::Duration::from_nanos(1));
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Drain the buffer and verify
        let mut buffer_guard = buffer.lock().unwrap();
        let drained = buffer_guard.drain_all_buffers();
        
        // Should have one series with num_threads * points_per_thread points
        assert_eq!(drained.len(), 1, "Should have one series");
        
        let points = &drained[series];
        assert_eq!(
            points.len(), 
            num_threads * points_per_thread, 
            "Should have {} points", 
            num_threads * points_per_thread
        );
        
        // Verify each thread's points are present
        for thread_id in 0..num_threads {
            for i in 0..points_per_thread {
                // Find point with matching thread_id and point_id tags
                let found = points.iter().any(|p| {
                    p.tags.get("thread_id") == Some(&thread_id.to_string()) &&
                    p.tags.get("point_id") == Some(&i.to_string())
                });
                
                assert!(
                    found, 
                    "Point with thread_id={}, point_id={} not found", 
                    thread_id, 
                    i
                );
            }
        }
    }
    
    #[test]
    fn test_stage_with_different_tag_combinations() {
        let mut buffer = WriteBuffer::default();
        let series = "tag_test_series";
        
        // Create points with different tag combinations
        let ts_base = get_current_timestamp();
        
        // Point with no tags
        let no_tags = TagSet::new();
        buffer.stage(series, create_point(ts_base, 1.0, no_tags)).unwrap();
        
        // Point with one tag
        let one_tag = create_tags(&[("region", "us-east")]);
        buffer.stage(series, create_point(ts_base + 1, 2.0, one_tag)).unwrap();
        
        // Point with multiple tags
        let multi_tags = create_tags(&[
            ("region", "eu-west"), 
            ("host", "server2"),
            ("service", "api"),
            ("version", "1.0")
        ]);
        buffer.stage(series, create_point(ts_base + 2, 3.0, multi_tags.clone())).unwrap();
        
        // Drain and verify
        let drained = buffer.drain_all_buffers();
        let points = &drained[series];
        
        assert_eq!(points.len(), 3, "Should have three points");
        
        // First point should have no tags
        assert_eq!(points[0].tags.len(), 0, "First point should have no tags");
        
        // Second point should have one tag
        assert_eq!(points[1].tags.len(), 1, "Second point should have one tag");
        assert_eq!(points[1].tags.get("region"), Some(&"us-east".to_string()));
        
        // Third point should have multiple tags
        assert_eq!(points[2].tags.len(), 4, "Third point should have four tags");
        assert_eq!(points[2].tags, multi_tags);
    }
}

