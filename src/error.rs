pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("DataFusion error: {0:?}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Qdrant error: {0:?}")]
    Qdrant(Box<qdrant_client::QdrantError>),
    #[error("Collection info not found for collection '{0}'")]
    MissingCollectionInfo(String),
    #[error("Collection info params not found for collection '{0}'")]
    MissingCollectionInfoParams(String),
}

impl From<qdrant_client::QdrantError> for Error {
    fn from(err: qdrant_client::QdrantError) -> Self { Error::Qdrant(Box::new(err)) }
}
