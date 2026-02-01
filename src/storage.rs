use crate::error::DbError;
use crate::types::{DataPoint, TimeSeriesChunk};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Simple in-memory storage for time series data.
/// Data is stored per series in TimeSeriesChunk.
/// Uses RwLock for concurrent read access during queries and exclusive write access during flushes.
#[derive(Debug, Default)]
pub struct InMemoryStorage {
    series_data: HashMap<String, Arc<RwLock<TimeSeriesChunk>>>,
}

impl InMemoryStorage {
    /// Appends a batch of data points (flushed from the buffer) to the corresponding series chunk.
    /// Creates the series chunk if it doesn't exist.
    /// **Crucially, sorts the chunk by timestamp after appending.**
    pub fn append_batch(&mut self, data: HashMap<String, Vec<DataPoint>>) -> Result<(), DbError> {
        for (series_name, points) in data {
            if points.is_empty() {
                continue;
            }

            let chunk_arc = self
                .series_data
                .entry(series_name.clone())
                .or_insert_with(|| Arc::new(RwLock::new(TimeSeriesChunk::default())));

            let mut chunk_guard = chunk_arc.write()?;
            chunk_guard.append_batch(points);

            // --- Sort the chunk by timestamp after appending --- //
            // Combine columns into tuples for sorting
            let mut combined: Vec<_> = chunk_guard
                .timestamps
                .iter()
                .zip(chunk_guard.values.iter())
                .zip(chunk_guard.tags.iter())
                .map(|((&ts, &val), tag)| (ts, val, tag.clone())) // Clone tag for ownership
                .collect();

            // Sort based on timestamp
            combined.sort_unstable_by_key(|&(ts, _, _)| ts);

            // Clear existing vectors and push sorted data back
            chunk_guard.timestamps.clear();
            chunk_guard.values.clear();
            chunk_guard.tags.clear();

            chunk_guard.timestamps.reserve(combined.len());
            chunk_guard.values.reserve(combined.len());
            chunk_guard.tags.reserve(combined.len());

            for (ts, val, tag) in combined {
                chunk_guard.timestamps.push(ts);
                chunk_guard.values.push(val);
                chunk_guard.tags.push(tag);
            }
            // --- End sorting --- //
        }
        Ok(())
    }

    /// Appends points directly to a series. Used for recovery.
    pub fn append_points(&mut self, series: &str, points: Vec<DataPoint>) -> Result<(), DbError> {
        if points.is_empty() {
            return Ok(());
        }

        let chunk_arc = self
            .series_data
            .entry(series.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(TimeSeriesChunk::default())));

        let mut chunk_guard = chunk_arc.write()?;
        chunk_guard.append_batch(points);

        // Sort the chunk by timestamp
        let mut combined: Vec<_> = chunk_guard
            .timestamps
            .iter()
            .zip(chunk_guard.values.iter())
            .zip(chunk_guard.tags.iter())
            .map(|((&ts, &val), tag)| (ts, val, tag.clone()))
            .collect();

        combined.sort_unstable_by_key(|&(ts, _, _)| ts);

        chunk_guard.timestamps.clear();
        chunk_guard.values.clear();
        chunk_guard.tags.clear();

        chunk_guard.timestamps.reserve(combined.len());
        chunk_guard.values.reserve(combined.len());
        chunk_guard.tags.reserve(combined.len());

        for (ts, val, tag) in combined {
            chunk_guard.timestamps.push(ts);
            chunk_guard.values.push(val);
            chunk_guard.tags.push(tag);
        }

        Ok(())
    }

    /// Retrieves a read-only reference (via Arc clone) to the chunk for a given series.
    pub fn get_chunk_for_query(
        &self,
        series: &str,
    ) -> Option<Arc<RwLock<TimeSeriesChunk>>> {
        self.series_data.get(series).cloned()
    }
    
    /// Returns a reference to all series for snapshot creation
    pub fn get_all_series(&self) -> &HashMap<String, Arc<RwLock<TimeSeriesChunk>>> {
        &self.series_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TagSet, Timestamp, Value};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::thread;

    fn create_point(ts: Timestamp, val: Value) -> DataPoint {
        DataPoint {
            timestamp: ts,
            value: val,
            tags: TagSet::new(),
        }
    }

    fn create_point_with_tags(ts: Timestamp, val: Value, tags: TagSet) -> DataPoint {
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
    fn test_append_and_sort() {
        let mut storage = InMemoryStorage::default();
        let series = "test_sort";
        let points1 = vec![create_point(100, 1.0), create_point(300, 3.0)];
        let points2 = vec![create_point(50, 0.5), create_point(200, 2.0)];

        let mut batch1 = HashMap::new();
        batch1.insert(series.to_string(), points1);
        storage.append_batch(batch1).unwrap();

        let mut batch2 = HashMap::new();
        batch2.insert(series.to_string(), points2);
        storage.append_batch(batch2).unwrap();

        let chunk_arc = storage.get_chunk_for_query(series).unwrap();
        let chunk_guard = chunk_arc.read().unwrap();

        assert_eq!(chunk_guard.len(), 4);
        assert_eq!(chunk_guard.timestamps, vec![50, 100, 200, 300]);
        assert_eq!(chunk_guard.values, vec![0.5, 1.0, 2.0, 3.0]);
    }
    
    #[test]
    fn test_append_batch_multiple_series() {
        let mut storage = InMemoryStorage::default();
        
        // Create three series with real timestamps
        let ts1 = get_current_timestamp();
        let ts2 = ts1 + 100;
        let ts3 = ts1 + 200;
        
        let mut batch = HashMap::new();
        batch.insert("series1".to_string(), vec![create_point(ts1, 1.0), create_point(ts2, 1.1)]);
        batch.insert("series2".to_string(), vec![create_point(ts3, 2.0)]);
        batch.insert("series3".to_string(), vec![create_point(ts2, 3.0), create_point(ts1, 3.1)]);
        
        storage.append_batch(batch).unwrap();
        
        // Verify series1
        let chunk_arc = storage.get_chunk_for_query("series1").unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        assert_eq!(chunk_guard.len(), 2);
        assert_eq!(chunk_guard.timestamps, vec![ts1, ts2]);
        assert_eq!(chunk_guard.values, vec![1.0, 1.1]);
        
        // Verify series2
        let chunk_arc = storage.get_chunk_for_query("series2").unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        assert_eq!(chunk_guard.len(), 1);
        assert_eq!(chunk_guard.timestamps, vec![ts3]);
        assert_eq!(chunk_guard.values, vec![2.0]);
        
        // Verify series3 (should be sorted by timestamp)
        let chunk_arc = storage.get_chunk_for_query("series3").unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        assert_eq!(chunk_guard.len(), 2);
        assert_eq!(chunk_guard.timestamps, vec![ts1, ts2]); // Sorted
        assert_eq!(chunk_guard.values, vec![3.1, 3.0]); // Values match sorted order
    }
    
    #[test]
    fn test_append_points() {
        let mut storage = InMemoryStorage::default();
        let series = "test_append_points";
        
        // Generate real timestamps with small delays to ensure uniqueness
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts3 = get_current_timestamp();
        
        // Create points with out-of-order timestamps to test sorting
        let points = vec![
            create_point(ts2, 2.0),
            create_point(ts1, 1.0),
            create_point(ts3, 3.0),
        ];
        
        storage.append_points(series, points).unwrap();
        
        // Verify points were added and sorted
        let chunk_arc = storage.get_chunk_for_query(series).unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        
        assert_eq!(chunk_guard.len(), 3);
        assert_eq!(chunk_guard.timestamps, vec![ts1, ts2, ts3]); // Should be sorted
        assert_eq!(chunk_guard.values, vec![1.0, 2.0, 3.0]); // Values should match the sorted order
    }
    
    #[test]
    fn test_get_chunk_for_query() {
        let mut storage = InMemoryStorage::default();
        let series = "test_get_chunk";
        
        // Create and add a point
        let ts = get_current_timestamp();
        let points = vec![create_point(ts, 42.0)];
        
        let mut batch = HashMap::new();
        batch.insert(series.to_string(), points);
        storage.append_batch(batch).unwrap();
        
        // Test getting an existing chunk
        let chunk_opt = storage.get_chunk_for_query(series);
        assert!(chunk_opt.is_some());
        
        let chunk_arc = chunk_opt.unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        assert_eq!(chunk_guard.len(), 1);
        assert_eq!(chunk_guard.timestamps[0], ts);
        assert_eq!(chunk_guard.values[0], 42.0);
        
        // Test getting a non-existent chunk
        let non_existent = storage.get_chunk_for_query("non_existent");
        assert!(non_existent.is_none());
    }
    
    #[test]
    fn test_get_all_series() {
        let mut storage = InMemoryStorage::default();
        
        // Create three series with real timestamps
        let ts1 = get_current_timestamp();
        let ts2 = ts1 + 100;
        
        let mut batch = HashMap::new();
        batch.insert("series1".to_string(), vec![create_point(ts1, 1.0)]);
        batch.insert("series2".to_string(), vec![create_point(ts2, 2.0)]);
        
        storage.append_batch(batch).unwrap();
        
        // Get all series
        let all_series = storage.get_all_series();
        
        // Verify the number of series
        assert_eq!(all_series.len(), 2);
        
        // Verify series names
        assert!(all_series.contains_key("series1"));
        assert!(all_series.contains_key("series2"));
        
        // Verify series contents
        let series1_arc = all_series.get("series1").unwrap();
        let series1_guard = series1_arc.read().unwrap();
        assert_eq!(series1_guard.timestamps[0], ts1);
        
        let series2_arc = all_series.get("series2").unwrap();
        let series2_guard = series2_arc.read().unwrap();
        assert_eq!(series2_guard.timestamps[0], ts2);
    }
    
    #[test]
    fn test_append_with_tags() {
        let mut storage = InMemoryStorage::default();
        let series = "test_tags";
        
        // Create real timestamps
        let ts1 = get_current_timestamp();
        thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = get_current_timestamp();
        
        // Create tags
        let tags1 = create_tags(&[("region", "us-east"), ("host", "server1")]);
        let tags2 = create_tags(&[("region", "us-west"), ("host", "server2")]);
        
        // Create points with tags
        let points = vec![
            create_point_with_tags(ts1, 1.0, tags1.clone()),
            create_point_with_tags(ts2, 2.0, tags2.clone()),
        ];
        
        let mut batch = HashMap::new();
        batch.insert(series.to_string(), points);
        storage.append_batch(batch).unwrap();
        
        // Verify points and tags
        let chunk_arc = storage.get_chunk_for_query(series).unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        
        assert_eq!(chunk_guard.len(), 2);
        
        // Verify tags for first point
        assert_eq!(chunk_guard.tags[0], tags1);
        assert_eq!(chunk_guard.tags[0].get("region"), Some(&"us-east".to_string()));
        
        // Verify tags for second point
        assert_eq!(chunk_guard.tags[1], tags2);
        assert_eq!(chunk_guard.tags[1].get("host"), Some(&"server2".to_string()));
    }
    
    #[test]
    fn test_empty_batch() {
        let mut storage = InMemoryStorage::default();
        
        // Try to append an empty batch
        let mut empty_batch = HashMap::new();
        empty_batch.insert("empty_series".to_string(), Vec::new());
        
        storage.append_batch(empty_batch).unwrap();
        
        // The series should not be created
        assert!(storage.get_chunk_for_query("empty_series").is_none());
    }
    
    #[test]
    fn test_append_out_of_order_points() {
        let mut storage = InMemoryStorage::default();
        let series = "out_of_order";
        
        // Create timestamps with guaranteed ordering
        let ts1 = get_current_timestamp();
        let ts2 = ts1 + 1000;
        let ts3 = ts1 + 2000;
        let ts4 = ts1 + 3000;
        let ts5 = ts1 + 4000;
        
        // First batch - in order
        let mut batch1 = HashMap::new();
        batch1.insert(series.to_string(), vec![
            create_point(ts1, 1.0),
            create_point(ts2, 2.0),
            create_point(ts3, 3.0),
        ]);
        storage.append_batch(batch1).unwrap();
        
        // Second batch - mixed order, some before, some after, some between existing points
        let mut batch2 = HashMap::new();
        batch2.insert(series.to_string(), vec![
            create_point(ts5, 5.0), // after existing points
            create_point(ts2 - 500, 1.5), // between existing points
            create_point(ts1 - 500, 0.5), // before all existing points
            create_point(ts4, 4.0), // after existing points
        ]);
        storage.append_batch(batch2).unwrap();
        
        // Verify all points are stored in correct order
        let chunk_arc = storage.get_chunk_for_query(series).unwrap();
        let chunk_guard = chunk_arc.read().unwrap();
        
        assert_eq!(chunk_guard.len(), 7);
        
        // Expected timestamps in sorted order
        let expected_ts = vec![ts1 - 500, ts1, ts2 - 500, ts2, ts3, ts4, ts5];
        let expected_values = vec![0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0];
        
        assert_eq!(chunk_guard.timestamps, expected_ts);
        assert_eq!(chunk_guard.values, expected_values);
    }
}

