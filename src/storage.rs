use dashmap::DashMap;
use log::{debug, error, info};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::path::Path;
use std::sync::Arc;

use crate::document::{Collection, Document};
use crate::error::{Result, XLimError};

/// Storage engine for the database
pub struct StorageEngine {
    /// RocksDB instance
    db: Arc<DB>,
    
    /// Cache of collections
    collections: DashMap<String, Collection>,
}

impl StorageEngine {
    /// Create a new storage engine
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Create RocksDB options
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_keep_log_file_num(10);
        options.set_max_open_files(1000);
        options.set_use_fsync(false);
        options.set_bytes_per_sync(8388608); // 8MB
        options.optimize_for_point_lookup(64 * 1024 * 1024); // 64MB
        options.set_table_cache_num_shard_bits(6);
        options.set_max_write_buffer_number(6);
        options.set_write_buffer_size(128 * 1024 * 1024); // 128MB
        options.set_target_file_size_base(64 * 1024 * 1024); // 64MB
        options.set_min_write_buffer_number_to_merge(2);
        options.set_level_zero_stop_writes_trigger(36);
        options.set_level_zero_slowdown_writes_trigger(28);
        options.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        options.set_max_background_jobs(6);
        options.set_max_background_compactions(4);
        options.set_max_background_flushes(2);
        
        // Define column families
        let cf_names = vec!["default", "collections", "documents", "indexes", "metadata"];
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, options.clone()))
            .collect();
        
        // Open the database
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)
            .map_err(|e| XLimError::Storage(format!("Failed to open database: {}", e)))?;
        
        let db = Arc::new(db);
        
        // Load collections
        let collections = DashMap::new();
        let cf_collections = db.cf_handle("collections")
            .ok_or_else(|| XLimError::Storage("Collections column family not found".to_string()))?;
        
        let iter = db.iterator_cf(&cf_collections, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item.map_err(|e| XLimError::Storage(format!("Failed to read collection: {}", e)))?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            let collection: Collection = bincode::deserialize(&value)
                .map_err(|e| XLimError::Storage(format!("Failed to deserialize collection: {}", e)))?;
            
            collections.insert(key_str, collection);
        }
        
        info!("Loaded {} collections from storage", collections.len());
        
        Ok(Self {
            db,
            collections,
        })
    }
    
    /// Get a collection by name
    pub fn get_collection(&self, name: &str) -> Result<Collection> {
        if let Some(collection) = self.collections.get(name) {
            return Ok(collection.clone());
        }
        
        Err(XLimError::CollectionNotFound(name.to_string()))
    }
    
    /// Create a new collection
    pub fn create_collection(&self, name: &str) -> Result<Collection> {
        if self.collections.contains_key(name) {
            return Err(XLimError::InvalidOperation(format!("Collection '{}' already exists", name)));
        }
        
        let collection = Collection::new(name);
        
        // Serialize and store the collection
        let cf_collections = self.db.cf_handle("collections")
            .ok_or_else(|| XLimError::Storage("Collections column family not found".to_string()))?;
        
        let serialized = bincode::serialize(&collection)
            .map_err(|e| XLimError::Storage(format!("Failed to serialize collection: {}", e)))?;
        
        self.db.put_cf(&cf_collections, name.as_bytes(), serialized)
            .map_err(|e| XLimError::Storage(format!("Failed to store collection: {}", e)))?;
        
        // Add to cache
        self.collections.insert(name.to_string(), collection.clone());
        
        info!("Created collection: {}", name);
        
        Ok(collection)
    }
    
    /// Delete a collection
    pub fn delete_collection(&self, name: &str) -> Result<()> {
        if !self.collections.contains_key(name) {
            return Err(XLimError::CollectionNotFound(name.to_string()));
        }
        
        // Remove from storage
        let cf_collections = self.db.cf_handle("collections")
            .ok_or_else(|| XLimError::Storage("Collections column family not found".to_string()))?;
        
        self.db.delete_cf(&cf_collections, name.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to delete collection: {}", e)))?;
        
        // Remove from cache
        self.collections.remove(name);
        
        // Delete all documents in the collection
        let cf_documents = self.db.cf_handle("documents")
            .ok_or_else(|| XLimError::Storage("Documents column family not found".to_string()))?;
        
        let prefix = format!("{}:", name);
        let iter = self.db.iterator_cf(&cf_documents, rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));
        
        for item in iter {
            let (key, _) = item.map_err(|e| XLimError::Storage(format!("Failed to read document: {}", e)))?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            
            if !key_str.starts_with(&prefix) {
                break;
            }
            
            self.db.delete_cf(&cf_documents, key)
                .map_err(|e| XLimError::Storage(format!("Failed to delete document: {}", e)))?;
        }
        
        info!("Deleted collection: {}", name);
        
        Ok(())
    }
    
    /// Insert a document into a collection
    pub fn insert_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        if !self.collections.contains_key(collection_name) {
            return Err(XLimError::CollectionNotFound(collection_name.to_string()));
        }
        
        let cf_documents = self.db.cf_handle("documents")
            .ok_or_else(|| XLimError::Storage("Documents column family not found".to_string()))?;
        
        let key = format!("{}:{}", collection_name, document.id);
        let serialized = bincode::serialize(document)
            .map_err(|e| XLimError::Storage(format!("Failed to serialize document: {}", e)))?;
        
        self.db.put_cf(&cf_documents, key.as_bytes(), serialized)
            .map_err(|e| XLimError::Storage(format!("Failed to store document: {}", e)))?;
        
        debug!("Inserted document {} into collection {}", document.id, collection_name);
        
        Ok(())
    }
    
    /// Get a document from a collection
    pub fn get_document(&self, collection_name: &str, document_id: &str) -> Result<Document> {
        if !self.collections.contains_key(collection_name) {
            return Err(XLimError::CollectionNotFound(collection_name.to_string()));
        }
        
        let cf_documents = self.db.cf_handle("documents")
            .ok_or_else(|| XLimError::Storage("Documents column family not found".to_string()))?;
        
        let key = format!("{}:{}", collection_name, document_id);
        let value = self.db.get_cf(&cf_documents, key.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to read document: {}", e)))?
            .ok_or_else(|| XLimError::DocumentNotFound(document_id.to_string()))?;
        
        let document: Document = bincode::deserialize(&value)
            .map_err(|e| XLimError::Storage(format!("Failed to deserialize document: {}", e)))?;
        
        Ok(document)
    }
    
    /// Update a document in a collection
    pub fn update_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        if !self.collections.contains_key(collection_name) {
            return Err(XLimError::CollectionNotFound(collection_name.to_string()));
        }
        
        let cf_documents = self.db.cf_handle("documents")
            .ok_or_else(|| XLimError::Storage("Documents column family not found".to_string()))?;
        
        let key = format!("{}:{}", collection_name, document.id);
        
        // Check if document exists
        let exists = self.db.get_cf(&cf_documents, key.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to read document: {}", e)))?
            .is_some();
        
        if !exists {
            return Err(XLimError::DocumentNotFound(document.id.to_string()));
        }
        
        let serialized = bincode::serialize(document)
            .map_err(|e| XLimError::Storage(format!("Failed to serialize document: {}", e)))?;
        
        self.db.put_cf(&cf_documents, key.as_bytes(), serialized)
            .map_err(|e| XLimError::Storage(format!("Failed to update document: {}", e)))?;
        
        debug!("Updated document {} in collection {}", document.id, collection_name);
        
        Ok(())
    }
    
    /// Delete a document from a collection
    pub fn delete_document(&self, collection_name: &str, document_id: &str) -> Result<()> {
        if !self.collections.contains_key(collection_name) {
            return Err(XLimError::CollectionNotFound(collection_name.to_string()));
        }
        
        let cf_documents = self.db.cf_handle("documents")
            .ok_or_else(|| XLimError::Storage("Documents column family not found".to_string()))?;
        
        let key = format!("{}:{}", collection_name, document_id);
        
        // Check if document exists
        let exists = self.db.get_cf(&cf_documents, key.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to read document: {}", e)))?
            .is_some();
        
        if !exists {
            return Err(XLimError::DocumentNotFound(document_id.to_string()));
        }
        
        self.db.delete_cf(&cf_documents, key.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to delete document: {}", e)))?;
        
        debug!("Deleted document {} from collection {}", document_id, collection_name);
        
        Ok(())
    }
    
    /// List all documents in a collection
    pub fn list_documents(&self, collection_name: &str) -> Result<Vec<Document>> {
        if !self.collections.contains_key(collection_name) {
            return Err(XLimError::CollectionNotFound(collection_name.to_string()));
        }
        
        let cf_documents = self.db.cf_handle("documents")
            .ok_or_else(|| XLimError::Storage("Documents column family not found".to_string()))?;
        
        let prefix = format!("{}:", collection_name);
        let iter = self.db.iterator_cf(&cf_documents, rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));
        
        let mut documents = Vec::new();
        
        for item in iter {
            let (key, value) = item.map_err(|e| XLimError::Storage(format!("Failed to read document: {}", e)))?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            
            if !key_str.starts_with(&prefix) {
                break;
            }
            
            let document: Document = bincode::deserialize(&value)
                .map_err(|e| XLimError::Storage(format!("Failed to deserialize document: {}", e)))?;
            
            documents.push(document);
        }
        
        Ok(documents)
    }
    
    /// Store a value in the metadata column family
    pub fn store_metadata<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let cf_metadata = self.db.cf_handle("metadata")
            .ok_or_else(|| XLimError::Storage("Metadata column family not found".to_string()))?;
        
        let serialized = bincode::serialize(value)
            .map_err(|e| XLimError::Storage(format!("Failed to serialize metadata: {}", e)))?;
        
        self.db.put_cf(&cf_metadata, key.as_bytes(), serialized)
            .map_err(|e| XLimError::Storage(format!("Failed to store metadata: {}", e)))?;
        
        Ok(())
    }
    
    /// Get a value from the metadata column family
    pub fn get_metadata<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let cf_metadata = self.db.cf_handle("metadata")
            .ok_or_else(|| XLimError::Storage("Metadata column family not found".to_string()))?;
        
        let value = self.db.get_cf(&cf_metadata, key.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to read metadata: {}", e)))?;
        
        if let Some(value) = value {
            let deserialized: T = bincode::deserialize(&value)
                .map_err(|e| XLimError::Storage(format!("Failed to deserialize metadata: {}", e)))?;
            
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }
    
    /// Delete a value from the metadata column family
    pub fn delete_metadata(&self, key: &str) -> Result<()> {
        let cf_metadata = self.db.cf_handle("metadata")
            .ok_or_else(|| XLimError::Storage("Metadata column family not found".to_string()))?;
        
        self.db.delete_cf(&cf_metadata, key.as_bytes())
            .map_err(|e| XLimError::Storage(format!("Failed to delete metadata: {}", e)))?;
        
        Ok(())
    }
} 