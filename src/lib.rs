#![doc = include_str!("../README.md")]

pub mod error;
pub mod prelude;
#[cfg(feature = "test-utils")]
pub mod test_utils;

#[cfg(feature = "test-utils")]
mod dev_deps {
    use {testcontainers as _, tokio as _, tracing_subscriber as _};
}
