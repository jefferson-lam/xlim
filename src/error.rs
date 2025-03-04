use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, XLimError>;

#[derive(Error, Debug)]
pub enum XLimError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Document not found: {0}")]
    DocumentNotFound(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
} 