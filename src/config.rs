use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the XLim database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Port to listen on for client connections
    pub port: u16,
    
    /// Directory to store data files
    pub data_dir: PathBuf,
    
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    
    /// Cache size in megabytes
    pub cache_size_mb: usize,
}

impl Config {
    /// Create a new configuration with default values
    pub fn default() -> Self {
        Self {
            port: 7878,
            data_dir: PathBuf::from("./data"),
            max_connections: 100,
            cache_size_mb: 128,
        }
    }
    
    /// Load configuration from a file
    pub fn from_file(path: &str) -> crate::error::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config = serde_json::from_str(&contents)?;
        Ok(config)
    }
    
    /// Save configuration to a file
    pub fn save_to_file(&self, path: &str) -> crate::error::Result<()> {
        let contents = serde_json::to_string_pretty(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }
    
    /// Get the path to the database files
    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("db")
    }
    
    /// Get the path to the metadata files
    pub fn metadata_path(&self) -> PathBuf {
        self.data_dir.join("metadata")
    }
    
    /// Get the path to the log files
    pub fn log_path(&self) -> PathBuf {
        self.data_dir.join("logs")
    }
} 