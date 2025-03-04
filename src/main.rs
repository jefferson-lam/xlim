use clap::{Parser, Subcommand};
use env_logger::Env;
use log::{error, info};
use std::path::PathBuf;

mod client;
mod config;
mod document;
mod error;
mod query;
mod server;
mod storage;
mod transaction;

use crate::config::Config;
use crate::error::Result;
use crate::server::Server;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the XLim database server
    Server {
        /// Port to listen on
        #[arg(short, long, default_value_t = 7878)]
        port: u16,

        /// Data directory
        #[arg(short, long, default_value = "./data")]
        data_dir: PathBuf,
    },
    /// Run a query against the database
    Query {
        /// Query string in XLim query language
        query: String,

        /// Server address
        #[arg(short, long, default_value = "localhost:7878")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { port, data_dir } => {
            info!("Starting XLim server on port {}", port);
            
            if !data_dir.exists() {
                std::fs::create_dir_all(&data_dir)?;
                info!("Created data directory: {:?}", data_dir);
            }
            
            let config = Config {
                port,
                data_dir,
                max_connections: 100,
                cache_size_mb: 128,
            };
            
            let server = Server::new(config)?;
            server.start().await?;
            
            Ok(())
        }
        Commands::Query { query, server } => {
            info!("Executing query: {} on server {}", query, server);
            
            // TODO: Implement query execution against server
            println!("Query execution not yet implemented");
            
            Ok(())
        }
    }
}
