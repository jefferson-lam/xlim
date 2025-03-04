# XLim - A Lightweight NoSQL Database

XLim is a lightweight, high-performance NoSQL database written in Rust. It provides a flexible document storage system with support for JSON documents, key-value pairs, and basic querying capabilities.

## Features

- Document-oriented storage
- JSON document support
- Persistent storage using RocksDB
- Concurrent access with multi-threading support
- Command-line interface
- Programmatic API for Rust applications
- ACID transactions
- Basic query capabilities

## Getting Started

### Prerequisites

- Rust 1.82.0 or later
- Cargo package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/xlim.git
cd xlim

# Build the project
cargo build --release

# Run the database server
cargo run --release -- server

# Or use the CLI
cargo run --release -- --help
```

### Basic Usage

```rust
use xlim::client::Client;
use xlim::document::Document;

// Connect to the database
let client = Client::connect("localhost:7878").await?;

// Create a collection
let collection = client.create_collection("users").await?;

// Insert a document
let doc = Document::new()
    .set("name", "John Doe")
    .set("email", "john@example.com")
    .set("age", 30);
    
let id = collection.insert(doc).await?;

// Query documents
let results = collection.find()
    .filter("age", ">", 25)
    .limit(10)
    .execute()
    .await?;
```

## Architecture

XLim is built with a modular architecture:

- **Storage Engine**: Handles data persistence using RocksDB
- **Query Engine**: Processes and optimizes queries
- **Transaction Manager**: Ensures ACID properties
- **Server**: Manages client connections and request handling
- **Client API**: Provides a clean interface for applications

## License

This project is licensed under the MIT License - see the LICENSE file for details. 