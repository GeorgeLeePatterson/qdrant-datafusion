pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Qdrant error: {0:?}")]
    Qdrant(Box<qdrant_client::QdrantError>),
}

impl From<qdrant_client::QdrantError> for Error {
    fn from(err: qdrant_client::QdrantError) -> Self { Error::Qdrant(Box::new(err)) }
}
