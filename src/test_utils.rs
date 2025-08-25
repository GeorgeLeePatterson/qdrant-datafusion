use std::collections::VecDeque;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use testcontainers::core::wait::LogWaitStrategy;
use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

pub const ENDPOINT_ENV: &str = "QDRANT_ENDPOINT";
pub const VERSION_ENV: &str = "QDRANT_VERSION";
pub const REST_PORT_ENV: &str = "QDRANT_NATIVE_PORT";
pub const GRPC_PORT_ENV: &str = "QDRANT_HTTP_PORT";
pub const API_KEY_ENV: &str = "QDRANT_API_KEY";
pub const QDRANT_API_KEY_ENV: &str = "QDRANT__SERVICE__API_KEY";

pub const QDRANT_VERSION: &str = "latest";
pub const QDRANT_REST_PORT: u16 = 6333;
pub const QDRANT_GRPC_PORT: u16 = 6334;
pub const QDRANT_ENDPOINT: &str = "localhost";
pub const QDRANT_CONFIG_SRC: &str = "tests/bin/";
pub const QDRANT_CONFIG_DEST: &str = "/qdrant/config/config.yaml";
pub const QDRANT_API_KEY: &str = "qdrant-datafusion-api-key";

// Initialize tracing in a test setup
pub fn init_tracing(directives: Option<&[(&str, &str)]>) {
    let rust_log = env::var("RUST_LOG").unwrap_or_default();

    let stdio_logger = tracing_subscriber::fmt::Layer::default()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_filter(get_filter(&rust_log, directives));

    // Initialize only if not already set (avoids multiple subscribers in tests)
    if tracing::subscriber::set_global_default(tracing_subscriber::registry().with(stdio_logger))
        .is_ok()
    {
        debug!("Tracing initialized with RUST_LOG={rust_log}");
    }
}

/// Common tracing filters
///
/// # Panics
#[allow(unused)]
pub fn get_filter(rust_log: &str, directives: Option<&[(&str, &str)]>) -> EnvFilter {
    let mut env_dirs = vec![];
    let level = if rust_log.is_empty() {
        LevelFilter::WARN.to_string()
    } else if let Ok(level) = LevelFilter::from_str(rust_log) {
        level.to_string()
    } else {
        let mut parts = rust_log.split(',');
        let level = parts.next().and_then(|p| LevelFilter::from_str(p).ok());
        env_dirs = parts
            .map(|s| s.split('=').collect::<VecDeque<_>>())
            .filter(|s| s.len() == 2)
            .map(|mut s| (s.pop_front().unwrap(), s.pop_front().unwrap()))
            .collect::<Vec<_>>();
        level.unwrap_or(LevelFilter::WARN).to_string()
    };

    let mut filter = EnvFilter::new(level)
        .add_directive("ureq=info".parse().unwrap())
        .add_directive("tokio=info".parse().unwrap())
        .add_directive("runtime=error".parse().unwrap())
        .add_directive("opentelemetry_sdk=off".parse().unwrap());

    if let Some(directives) = directives {
        for (key, value) in directives {
            filter = filter.add_directive(format!("{key}={value}").parse().unwrap());
        }
    }

    for (key, value) in env_dirs {
        filter = filter.add_directive(format!("{key}={value}").parse().unwrap());
    }

    filter
}

/// # Panics
/// You bet it panics. Better be careful.
pub async fn create_container(conf: Option<&str>) -> Arc<QdrantContainer> {
    let c = QdrantContainer::try_new(conf).await.expect("Failed to initialize Qdrant container");
    Arc::new(c)
}

pub struct QdrantContainer {
    pub endpoint:  String,
    pub rest_port: u16,
    pub grpc_port: u16,
    pub api_key:   String,
    container:     RwLock<Option<ContainerAsync<GenericImage>>>,
}

impl QdrantContainer {
    /// # Errors
    pub async fn try_new(conf: Option<&str>) -> Result<Self, TestcontainersError> {
        // Env vars
        let version = env::var(VERSION_ENV).unwrap_or(QDRANT_VERSION.to_string());
        let rest_port = env::var(REST_PORT_ENV)
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(QDRANT_REST_PORT);
        let grpc_port = env::var(GRPC_PORT_ENV)
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(QDRANT_GRPC_PORT);
        let api_key = env::var(API_KEY_ENV).ok().unwrap_or(QDRANT_API_KEY.into());

        // Get image
        let image = GenericImage::new("qdrant/qdrant", &version)
            .with_exposed_port(rest_port.tcp())
            .with_exposed_port(grpc_port.tcp())
            .with_wait_for(testcontainers::core::WaitFor::Log(LogWaitStrategy::stdout_or_stderr(
                "Qdrant gRPC listening",
            )))
            .with_env_var(QDRANT_API_KEY_ENV, &api_key)
            .with_mount(Mount::bind_mount(
                format!(
                    "{}/{QDRANT_CONFIG_SRC}/{}",
                    env!("CARGO_MANIFEST_DIR"),
                    conf.unwrap_or("config.yaml")
                ),
                QDRANT_CONFIG_DEST,
            ));

        // Start container
        let container = image.start().await?;

        // Ports
        let rest_port = container.get_host_port_ipv4(rest_port).await?;
        let grpc_port = container.get_host_port_ipv4(grpc_port).await?;

        // Endpoint & URL
        let endpoint = env::var(ENDPOINT_ENV).unwrap_or(QDRANT_ENDPOINT.to_string());

        // Pause
        sleep(Duration::from_secs(2)).await;

        let container = RwLock::new(Some(container));
        Ok(QdrantContainer {
            endpoint,
            rest_port,
            grpc_port,
            api_key: api_key.to_string(),
            container,
        })
    }

    pub fn get_url(&self) -> String { format!("http://{}:{}", self.endpoint, self.grpc_port) }

    pub fn get_api_key(&self) -> &str { &self.api_key }

    /// # Errors
    pub async fn shutdown(&self) -> Result<(), TestcontainersError> {
        let mut container = self.container.write().await;
        if let Some(container) = container.take() {
            let _ = container
                .stop_with_timeout(Some(0))
                .await
                .inspect_err(|error| {
                    error!(?error, "Failed to stop container, will attempt to remove");
                })
                .ok();
            let _ = container
                .rm()
                .await
                .inspect_err(|error| {
                    error!(?error, "Failed to rm container, cleanup manually");
                })
                .ok();
        }
        Ok(())
    }
}

