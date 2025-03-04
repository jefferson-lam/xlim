use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::{Result, XLimError};

/// A document in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Unique identifier for the document
    #[serde(default = "Uuid::new_v4")]
    pub id: Uuid,
    
    /// Creation timestamp
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    
    /// Last update timestamp
    #[serde(default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
    
    /// Document data
    pub data: Map<String, Value>,
}

impl Document {
    /// Create a new empty document
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            data: Map::new(),
        }
    }
    
    /// Create a document from a JSON string
    pub fn from_json(json: &str) -> Result<Self> {
        let mut doc: Document = serde_json::from_str(json)?;
        
        // Ensure we have an ID and timestamps
        if doc.id == Uuid::nil() {
            doc.id = Uuid::new_v4();
        }
        
        // Update timestamps if needed
        if doc.created_at.timestamp() == 0 {
            doc.created_at = Utc::now();
        }
        
        if doc.updated_at.timestamp() == 0 {
            doc.updated_at = Utc::now();
        }
        
        Ok(doc)
    }
    
    /// Convert the document to a JSON string
    pub fn to_json(&self) -> Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
    
    /// Convert the document to a pretty-printed JSON string
    pub fn to_json_pretty(&self) -> Result<String> {
        let json = serde_json::to_string_pretty(self)?;
        Ok(json)
    }
    
    /// Set a field in the document
    pub fn set<T: Into<Value>>(mut self, key: &str, value: T) -> Self {
        self.data.insert(key.to_string(), value.into());
        self.updated_at = Utc::now();
        self
    }
    
    /// Get a field from the document
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }
    
    /// Remove a field from the document
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.updated_at = Utc::now();
        self.data.remove(key)
    }
    
    /// Check if the document contains a field
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }
    
    /// Get all fields in the document
    pub fn fields(&self) -> Vec<&String> {
        self.data.keys().collect()
    }
    
    /// Merge another document into this one
    pub fn merge(&mut self, other: &Document) {
        for (key, value) in &other.data {
            self.data.insert(key.clone(), value.clone());
        }
        self.updated_at = Utc::now();
    }
}

/// A collection of documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    /// Name of the collection
    pub name: String,
    
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    
    /// Metadata for the collection
    pub metadata: HashMap<String, Value>,
}

impl Collection {
    /// Create a new collection
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }
    
    /// Set metadata for the collection
    pub fn set_metadata<T: Into<Value>>(&mut self, key: &str, value: T) {
        self.metadata.insert(key.to_string(), value.into());
        self.updated_at = Utc::now();
    }
    
    /// Get metadata from the collection
    pub fn get_metadata(&self, key: &str) -> Option<&Value> {
        self.metadata.get(key)
    }
} 