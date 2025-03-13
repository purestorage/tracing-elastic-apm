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
pub use visitor::{ApmVisitor, ToVisited};

/// Constructs a new telemetry layer for a given APM configuration.
pub fn new_layer(
    service_name: String,
    config: Config,
) -> AnyResult<ApmLayer<ApmClient, ApmVisitor>> {
    ApmLayer::new(config, service_name)
}

/// Constructs a new telemetry layer for a given APM configuration.
/// Generic over the sender type, but it is provided by caller.
pub fn new_layer_with_sender<T, U>(
    service_name: String,
    config: Config,
    sender: T,
) -> AnyResult<ApmLayer<T, U>>
where
    T: apm_client::Sender,
    U: visitor::ToVisited,
{
    ApmLayer::new_with(config, service_name, sender)
}
