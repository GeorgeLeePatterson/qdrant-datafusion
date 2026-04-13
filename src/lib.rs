#![doc = include_str!("../README.md")]

mod analyzer;
pub mod arrow;
pub mod context;
pub mod error;
pub mod expr_fn;
pub mod prelude;
mod qdrant;
pub mod stream;
pub mod table;
#[cfg(feature = "test-utils")]
pub mod test_utils;

#[cfg(feature = "test-utils")]
mod dev_deps {
    use testcontainers as _;
    use tokio as _;
    use tracing as _;
    use tracing_subscriber as _;
}
