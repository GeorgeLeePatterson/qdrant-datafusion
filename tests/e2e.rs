#![allow(unused_crate_dependencies)]

mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    ("hyper", "error"),
    // --
];

#[cfg(feature = "test-utils")]
e2e_test!(connects, tests::test_connects, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;

    use qdrant_client::Qdrant;
    use qdrant_datafusion::error::Result;
    use qdrant_datafusion::test_utils::QdrantContainer;

    fn create_qdrant_client(c: &Arc<QdrantContainer>) -> Result<Qdrant> {
        let api_key = c.get_api_key();
        let url = c.get_url();
        eprintln!(">> Connecting to Qdrant @ {url}");
        Qdrant::from_url(&url).api_key(api_key).build().map_err(Into::into)
    }

    pub(super) async fn test_connects(c: Arc<QdrantContainer>) -> Result<()> {
        eprintln!("> Testing Qdrant connection");
        let client = create_qdrant_client(&c)?;
        let shouldnt_exist = client.collection_exists("non-existent-collection").await?;
        assert!(!shouldnt_exist, "No collections created");
        eprintln!(">> Connection test succeeded");
        Ok(())
    }
}
