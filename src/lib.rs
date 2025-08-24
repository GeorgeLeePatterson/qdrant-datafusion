#![doc = include_str!("../README.md")]

pub mod arrow;
pub mod error;
pub mod prelude;
pub mod stream;
pub mod table;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod udfs;
pub mod utils;

#[cfg(feature = "test-utils")]
mod dev_deps {
    use {testcontainers as _, tokio as _, tracing as _, tracing_subscriber as _};
}
