pub mod schema;
pub mod writer;

use anyhow::{Context as _, Result};
use neo4rs::{ConfigBuilder, Graph};
use std::env;

#[derive(Clone)]
pub struct Neo4jClient {
    graph: Graph,
}

impl Neo4jClient {
    pub async fn connect_from_env() -> Result<Self> {
        let uri = env::var("NEO4J_URI").unwrap_or_else(|_| "127.0.0.1:7687".to_string());
        let user = env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
        let password = env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "neo4j".to_string());
        let fetch_size: usize = env::var("NEO4J_FETCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);

        let config = ConfigBuilder::default()
            .uri(uri)
            .user(user)
            .password(password)
            .fetch_size(fetch_size)
            .build()
            .context("failed to build neo4j config")?;

        let graph = Graph::connect(config)
            .await
            .context("failed to connect to neo4j")?;

        Ok(Self { graph })
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub async fn run_migrations(&self) -> Result<()> {
        schema::run_migrations(&self.graph).await
    }
}
