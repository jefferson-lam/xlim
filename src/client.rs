use async_trait::async_trait;
use log::{debug, error};
use serde_json::json;
use std::io::{Error as IoError, ErrorKind};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::document::Document;
use crate::error::{Result, XLimError};
use crate::query::{Query, QueryBuilder};

/// A client for the XLim database
pub struct Client {
    /// Connection to the server
    connection: Arc<Mutex<TcpStream>>,
    
    /// Server address
    address: String,
}

impl Client {
    /// Connect to a database server
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let addr_str = format!("{:?}", addr);
        let stream = TcpStream::connect(addr).await?;
        
        let client = Self {
            connection: Arc::new(Mutex::new(stream)),
            address: addr_str,
        };
        
        // Test the connection
        client.ping().await?;
        
        Ok(client)
    }
    
    /// Ping the server
    pub async fn ping(&self) -> Result<()> {
        let response = self.send_command("PING").await?;
        
        if response.trim() != "PONG" {
            return Err(XLimError::Connection(format!("Unexpected response from server: {}", response)));
        }
        
        Ok(())
    }
    
    /// Create a collection
    pub async fn create_collection(&self, name: &str) -> Result<Collection> {
        let response = self.send_command(&format!("CREATE {}", name)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        Ok(Collection {
            client: self.clone(),
            name: name.to_string(),
        })
    }
    
    /// Drop a collection
    pub async fn drop_collection(&self, name: &str) -> Result<()> {
        let response = self.send_command(&format!("DROP {}", name)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        Ok(())
    }
    
    /// Get a collection
    pub async fn collection(&self, name: &str) -> Collection {
        Collection {
            client: self.clone(),
            name: name.to_string(),
        }
    }
    
    /// Begin a transaction
    pub async fn begin_transaction(&self) -> Result<Transaction> {
        let response = self.send_command("BEGIN").await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        // Parse the transaction ID
        let parts: Vec<&str> = response.trim().split(": ").collect();
        
        if parts.len() != 2 {
            return Err(XLimError::Database(format!("Invalid response from server: {}", response)));
        }
        
        let transaction_id = Uuid::parse_str(parts[1])
            .map_err(|_| XLimError::Database(format!("Invalid transaction ID: {}", parts[1])))?;
        
        Ok(Transaction {
            client: self.clone(),
            id: transaction_id,
        })
    }
    
    /// Send a command to the server
    async fn send_command(&self, command: &str) -> Result<String> {
        let mut connection = self.connection.lock().await;
        
        // Send the command
        connection.write_all(command.as_bytes()).await?;
        
        // Read the response
        let mut buffer = [0; 4096];
        let n = connection.read(&mut buffer).await?;
        
        if n == 0 {
            return Err(XLimError::Connection("Connection closed by server".to_string()));
        }
        
        let response = String::from_utf8_lossy(&buffer[..n]).to_string();
        
        Ok(response.trim().to_string())
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            address: self.address.clone(),
        }
    }
}

/// A collection in the database
pub struct Collection {
    /// Client connection
    client: Client,
    
    /// Collection name
    name: String,
}

impl Collection {
    /// Get the collection name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Insert a document into the collection
    pub async fn insert(&self, document: Document) -> Result<Uuid> {
        let json = document.to_json()?;
        let response = self.client.send_command(&format!("INSERT {} {}", self.name, json)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        // Parse the document ID
        let parts: Vec<&str> = response.trim().split(": ").collect();
        
        if parts.len() != 2 {
            return Err(XLimError::Database(format!("Invalid response from server: {}", response)));
        }
        
        let document_id = Uuid::parse_str(parts[1])
            .map_err(|_| XLimError::Database(format!("Invalid document ID: {}", parts[1])))?;
        
        Ok(document_id)
    }
    
    /// Get a document from the collection
    pub async fn get(&self, id: &str) -> Result<Document> {
        let response = self.client.send_command(&format!("GET {} {}", self.name, id)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        let document = Document::from_json(&response)?;
        
        Ok(document)
    }
    
    /// Update a document in the collection
    pub async fn update(&self, document: Document) -> Result<()> {
        let json = document.to_json()?;
        let response = self.client.send_command(&format!("UPDATE {} {}", self.name, json)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        Ok(())
    }
    
    /// Delete a document from the collection
    pub async fn delete(&self, id: &str) -> Result<()> {
        let response = self.client.send_command(&format!("DELETE {} {}", self.name, id)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        Ok(())
    }
    
    /// List all documents in the collection
    pub async fn list(&self) -> Result<Vec<Document>> {
        let response = self.client.send_command(&format!("LIST {}", self.name)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        // Parse the document IDs
        let lines: Vec<&str> = response.trim().split('\n').collect();
        
        if lines.len() < 1 {
            return Err(XLimError::Database("Invalid response from server".to_string()));
        }
        
        let mut documents = Vec::new();
        
        for i in 1..lines.len() {
            let line = lines[i];
            
            if line.starts_with("- ") {
                let parts: Vec<&str> = line[2..].split(": ").collect();
                
                if parts.len() == 2 {
                    let id = parts[0];
                    
                    match self.get(id).await {
                        Ok(document) => documents.push(document),
                        Err(e) => error!("Failed to get document {}: {}", id, e),
                    }
                }
            }
        }
        
        Ok(documents)
    }
    
    /// Create a query builder for this collection
    pub fn find(&self) -> CollectionQueryBuilder {
        CollectionQueryBuilder {
            collection: self.clone(),
            query_builder: QueryBuilder::new(),
        }
    }
}

impl Clone for Collection {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            name: self.name.clone(),
        }
    }
}

/// A query builder for a collection
pub struct CollectionQueryBuilder {
    /// Collection to query
    collection: Collection,
    
    /// Query builder
    query_builder: QueryBuilder,
}

impl CollectionQueryBuilder {
    /// Add a filter condition to the query
    pub fn filter<T: Into<serde_json::Value>>(&mut self, field: &str, operator: &str, value: T) -> Result<&mut Self> {
        self.query_builder.filter(field, operator, value)?;
        Ok(self)
    }
    
    /// Add a logical operator to the query
    pub fn logical_operator(&mut self, operator: &str) -> Result<&mut Self> {
        self.query_builder.logical_operator(operator)?;
        Ok(self)
    }
    
    /// Add a sort field to the query
    pub fn sort(&mut self, field: &str, ascending: bool) -> &mut Self {
        self.query_builder.sort(field, ascending);
        self
    }
    
    /// Set the maximum number of results to return
    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.query_builder.limit(limit);
        self
    }
    
    /// Set the number of results to skip
    pub fn skip(&mut self, skip: usize) -> &mut Self {
        self.query_builder.skip(skip);
        self
    }
    
    /// Set the fields to include in the results
    pub fn project(&mut self, fields: Vec<&str>) -> &mut Self {
        self.query_builder.project(fields);
        self
    }
    
    /// Execute the query
    pub async fn execute(&self) -> Result<Vec<Document>> {
        // For now, we'll just list all documents and filter them client-side
        // In a real implementation, we would send the query to the server
        let documents = self.collection.list().await?;
        let query = self.query_builder.build();
        
        query.apply(documents)
    }
}

/// A transaction in the database
pub struct Transaction {
    /// Client connection
    client: Client,
    
    /// Transaction ID
    id: Uuid,
}

impl Transaction {
    /// Get the transaction ID
    pub fn id(&self) -> Uuid {
        self.id
    }
    
    /// Commit the transaction
    pub async fn commit(&self) -> Result<()> {
        let response = self.client.send_command(&format!("COMMIT {}", self.id)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        Ok(())
    }
    
    /// Rollback the transaction
    pub async fn rollback(&self) -> Result<()> {
        let response = self.client.send_command(&format!("ROLLBACK {}", self.id)).await?;
        
        if response.starts_with("ERROR:") {
            return Err(XLimError::Database(response[7..].trim().to_string()));
        }
        
        Ok(())
    }
} 