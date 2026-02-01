use ugnos::{DbCore, DbConfig, DbError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Get current time in nanoseconds since epoch
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn main() -> Result<(), DbError> {
    // Create a configuration with a temporary directory for persistence
    let config = DbConfig {
        flush_interval: Duration::from_millis(100),
        data_dir: PathBuf::from("./demo_data"),
        wal_buffer_size: 10, // Small value for demonstration
        enable_wal: true,
        enable_snapshots: true,
        snapshot_interval: Duration::from_secs(5), // Snapshot every 5 seconds
        ..DbConfig::default()
    };
    
    println!("Creating database with persistence enabled");
    println!("WAL and snapshots will be stored in: {:?}", config.data_dir);
    
    // Create a new database instance
    let db = DbCore::with_config(config.clone())?;
    
    // Show the active configuration
    let active_config = db.get_config();
    println!("Active configuration:");
    println!("  Flush interval: {:?}", active_config.flush_interval);
    println!("  Snapshot interval: {:?}", active_config.snapshot_interval);
    println!("  WAL buffer size: {}", active_config.wal_buffer_size);
    
    // Insert some test data
    println!("Inserting test data...");
    for i in 0..100 {
        let timestamp = now() + i * 1_000_000; // Spread out over time
        let value = i as f64 / 10.0;
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), format!("server{}", i % 5));
        tags.insert("region".to_string(), if i % 2 == 0 {"west"} else {"east"}.to_string());
        
        db.insert("cpu_usage", timestamp, value, tags)?;
        
        if i % 10 == 0 {
            println!("Inserted {} data points", i + 1);
        }
    }
    
    // Force a flush to ensure data is persisted
    println!("Forcing a flush...");
    db.flush()?;
    
    // Trigger a snapshot
    println!("Creating a snapshot...");
    db.snapshot()?;
    
    // Sleep to allow background operations to complete
    println!("Waiting for background operations to complete...");
    std::thread::sleep(Duration::from_secs(2));
    
    // Query the data to verify it's there
    println!("Querying data...");
    let time_range = 0..u64::MAX; // All data
    let results = db.query("cpu_usage", time_range, None)?;
    println!("Found {} data points", results.len());
    
    // Simulate a restart
    println!("\nSimulating database restart...");
    // Let the first db instance drop, which will flush WAL
    drop(db);
    
    // Create a new instance pointing to the same data directory
    println!("Creating new database instance and recovering data...");
    let mut db2 = DbCore::with_config(config)?;
    
    // Recover data
    db2.recover()?;
    
    // Query again to verify recovery
    println!("Querying recovered data...");
    let time_range = 0..u64::MAX;
    let results = db2.query("cpu_usage", time_range, None)?;
    println!("Found {} data points after recovery", results.len());
    
    // Success!
    println!("\nPersistence demonstration completed successfully!");
    
    Ok(())
} 