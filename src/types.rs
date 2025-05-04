use std::collections::HashMap;

/// Timestamp type (nanoseconds since epoch).
pub type Timestamp = u64;

/// Value type.
pub type Value = f64;

/// TagSet type (using a HashMap for flexibility).
pub type TagSet = HashMap<String, String>;

/// Represents a single data point received via API or stored temporarily in buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub timestamp: Timestamp,
    pub value: Value,
    pub tags: TagSet,
}

/// Represents a chunk of time-series data in columnar format within storage.
/// Using Arc for tags might be slightly more efficient if tags are often duplicated,
/// but let's stick to owned TagSet for simplicity first as per design.
#[derive(Debug, Default, Clone)] // Clone needed for potential storage operations
pub struct TimeSeriesChunk {
    pub timestamps: Vec<Timestamp>,
    pub values: Vec<Value>,
    // Storing full TagSet per point. Optimization (e.g., interning) can be added later.
    pub tags: Vec<TagSet>,
}

impl TimeSeriesChunk {
    /// Appends a DataPoint to the chunk. Assumes timestamp is monotonically increasing
    /// or that sorting will happen elsewhere if needed.
    pub fn append(&mut self, point: DataPoint) {
        self.timestamps.push(point.timestamp);
        self.values.push(point.value);
        self.tags.push(point.tags);
    }

    /// Appends multiple data points. Assumes timestamps are monotonically increasing.
    pub fn append_batch(&mut self, points: Vec<DataPoint>) {
        let additional_capacity = points.len();
        self.timestamps.reserve(additional_capacity);
        self.values.reserve(additional_capacity);
        self.tags.reserve(additional_capacity);

        for point in points {
            self.timestamps.push(point.timestamp);
            self.values.push(point.value);
            self.tags.push(point.tags);
        }
    }

    /// Returns the number of data points in the chunk.
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Returns true if the chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
}

