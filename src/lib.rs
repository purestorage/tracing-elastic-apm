//! Elastic APM ingest API support layer.
//!
//! Use the `new_layer` function to create the layer with given `Config`.

use anyhow::Result as AnyResult;

use crate::{config::Config, layer::ApmLayer};

mod apm_client;
pub mod config;
pub mod layer;
pub mod model;
mod visitor;

pub use apm_client::{ApmClient, Batch, Sender};

/// Constructs a new telemetry layer for a given APM configuration.
pub fn new_layer<T>(service_name: String, config: Config) -> AnyResult<ApmLayer<T>>
where
    T: apm_client::Sender,
{
    ApmLayer::new(config, service_name)
}
