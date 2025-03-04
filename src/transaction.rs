use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::document::Document;
use crate::error::{Result, XLimError};
use crate::storage::StorageEngine;

/// Transaction operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    /// Insert a document
    Insert,
    /// Update a document
    Update,
    /// Delete a document
    Delete,
}

/// A transaction operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// Type of operation
    pub op_type: OperationType,
    
    /// Collection name
    pub collection: String,
    
    /// Document ID
    pub document_id: Uuid,
    
    /// Document data (for insert and update)
    pub document: Option<Document>,
}

/// A database transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction ID
    pub id: Uuid,
    
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    
    /// Operations in the transaction
    pub operations: Vec<Operation>,
    
    /// Whether the transaction has been committed
    pub committed: bool,
}

impl Transaction {
    /// Create a new transaction
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            operations: Vec::new(),
            committed: false,
        }
    }
    
    /// Add an insert operation to the transaction
    pub fn insert(&mut self, collection: &str, document: Document) -> &mut Self {
        let operation = Operation {
            op_type: OperationType::Insert,
            collection: collection.to_string(),
            document_id: document.id,
            document: Some(document),
        };
        
        self.operations.push(operation);
        
        self
    }
    
    /// Add an update operation to the transaction
    pub fn update(&mut self, collection: &str, document: Document) -> &mut Self {
        let operation = Operation {
            op_type: OperationType::Update,
            collection: collection.to_string(),
            document_id: document.id,
            document: Some(document),
        };
        
        self.operations.push(operation);
        
        self
    }
    
    /// Add a delete operation to the transaction
    pub fn delete(&mut self, collection: &str, document_id: Uuid) -> &mut Self {
        let operation = Operation {
            op_type: OperationType::Delete,
            collection: collection.to_string(),
            document_id,
            document: None,
        };
        
        self.operations.push(operation);
        
        self
    }
}

/// Transaction manager for handling database transactions
pub struct TransactionManager {
    /// Storage engine
    storage: Arc<StorageEngine>,
    
    /// Active transactions
    active_transactions: Mutex<Vec<Transaction>>,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            active_transactions: Mutex::new(Vec::new()),
        }
    }
    
    /// Begin a new transaction
    pub fn begin(&self) -> Transaction {
        let transaction = Transaction::new();
        
        let mut active_transactions = self.active_transactions.lock().unwrap();
        active_transactions.push(transaction.clone());
        
        transaction
    }
    
    /// Commit a transaction
    pub fn commit(&self, transaction_id: Uuid) -> Result<()> {
        let mut active_transactions = self.active_transactions.lock().unwrap();
        
        let transaction_index = active_transactions
            .iter()
            .position(|t| t.id == transaction_id)
            .ok_or_else(|| XLimError::Transaction(format!("Transaction not found: {}", transaction_id)))?;
        
        let mut transaction = active_transactions.remove(transaction_index);
        
        if transaction.committed {
            return Err(XLimError::Transaction(format!("Transaction already committed: {}", transaction_id)));
        }
        
        // Execute operations
        for operation in &transaction.operations {
            match operation.op_type {
                OperationType::Insert => {
                    if let Some(document) = &operation.document {
                        self.storage.insert_document(&operation.collection, document)?;
                    } else {
                        return Err(XLimError::Transaction("Insert operation missing document".to_string()));
                    }
                }
                OperationType::Update => {
                    if let Some(document) = &operation.document {
                        self.storage.update_document(&operation.collection, document)?;
                    } else {
                        return Err(XLimError::Transaction("Update operation missing document".to_string()));
                    }
                }
                OperationType::Delete => {
                    self.storage.delete_document(&operation.collection, &operation.document_id.to_string())?;
                }
            }
        }
        
        // Mark as committed
        transaction.committed = true;
        
        Ok(())
    }
    
    /// Rollback a transaction
    pub fn rollback(&self, transaction_id: Uuid) -> Result<()> {
        let mut active_transactions = self.active_transactions.lock().unwrap();
        
        let transaction_index = active_transactions
            .iter()
            .position(|t| t.id == transaction_id)
            .ok_or_else(|| XLimError::Transaction(format!("Transaction not found: {}", transaction_id)))?;
        
        let transaction = active_transactions.remove(transaction_index);
        
        if transaction.committed {
            return Err(XLimError::Transaction(format!("Cannot rollback committed transaction: {}", transaction_id)));
        }
        
        Ok(())
    }
    
    /// Get a transaction by ID
    pub fn get_transaction(&self, transaction_id: Uuid) -> Result<Transaction> {
        let active_transactions = self.active_transactions.lock().unwrap();
        
        let transaction = active_transactions
            .iter()
            .find(|t| t.id == transaction_id)
            .ok_or_else(|| XLimError::Transaction(format!("Transaction not found: {}", transaction_id)))?;
        
        Ok(transaction.clone())
    }
    
    /// Get all active transactions
    pub fn get_active_transactions(&self) -> Vec<Transaction> {
        let active_transactions = self.active_transactions.lock().unwrap();
        active_transactions.clone()
    }
} 