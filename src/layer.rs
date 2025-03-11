use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result as AnyResult;
use rand::random;
use serde_json::{json, Value};
use tracing::{
    span::{Attributes, Record},
    Event, Id, Level, Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

use crate::{
    apm_client::Batch,
    config::Config,
    model::{Agent, Error, Log, Metadata, Service, Span, Transaction},
    visitor::{ApmVisitor, TraceIdVisitor},
};

#[derive(Copy, Clone)]
struct TraceContext {
    pub trace_id: u128,
}

struct SpanContext {
    pub idle: Duration,
    pub busy: Duration,
    pub instant: Instant,
    pub first_entered_timestamp: Option<u64>,
}

/// Telemetry capability that publishes events and spans to Elastic APM.
pub struct ApmLayer<T> {
    client: T,
    metadata: Value,
    timing_metadata_labels: bool,
}

impl<S, T> Layer<S> for ApmLayer<T>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    T: crate::apm_client::Sender + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let timestamp = Instant::now();

        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();

        let mut visitor = ApmVisitor::default();
        attrs.record(&mut visitor);

        extensions.insert(visitor);
        extensions.insert(SpanContext {
            idle: Duration::new(0, 0),
            busy: Duration::new(0, 0),
            instant: timestamp,
            first_entered_timestamp: None,
        });

        let name = span.name().to_string();

        if let Some(parent_id) = span.parent().map(|span_ref| span_ref.id()) {
            let parent_span = ctx.span(&parent_id).expect("Span parent not found!");
            let parent_extensions = parent_span.extensions();
            let trace_ctx = parent_extensions
                .get::<TraceContext>()
                .expect("Trace context not found!");

            let new_span = Span {
                id: id.into_u64().to_string(),
                trace_id: trace_ctx.trace_id.to_string(),
                parent_id: parent_id.into_u64().to_string(),
                timestamp: now,
                name,
                span_type: "custom".to_string(),
                ..Default::default()
            };

            extensions.insert(new_span);
            extensions.insert(*trace_ctx);
        } else {
            let mut visitor = TraceIdVisitor::default();
            attrs.record(&mut visitor);

            let trace_ctx = TraceContext {
                trace_id: visitor.0.unwrap_or_else(random),
            };

            let new_transaction = Transaction {
                id: id.into_u64().to_string(),
                transaction_type: "custom".to_string(),
                trace_id: trace_ctx.trace_id.to_string(),
                timestamp: now,
                name: Some(name),
                ..Default::default()
            };

            extensions.insert(new_transaction);
            extensions.insert(trace_ctx);
        }
    }

    fn on_record(&self, span: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(span).expect("Span not found!");
        let mut extensions = span.extensions_mut();

        let visitor = extensions
            .get_mut::<ApmVisitor>()
            .expect("Visitor not found!");
        values.record(visitor);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if metadata.level() != &Level::ERROR {
            return;
        }

        let parent_id = if let Some(parent_id) = event.parent() {
            // explicit parent
            Some(parent_id.clone())
        } else if event.is_root() {
            // don't bother checking thread local if span is explicitly root according to this fn
            None
        } else {
            ctx.current_span().id().cloned()
        };

        if let Some(parent_id) = &parent_id {
            let span = ctx.span(parent_id).expect("Span not found!");
            let extensions = span.extensions();
            let trace_ctx = extensions
                .get::<TraceContext>()
                .expect("Trace context not found!");

            let mut visitor = ApmVisitor::default();
            event.record(&mut visitor);

            let error = Error {
                id: random::<u128>().to_string(),
                trace_id: Some(trace_ctx.trace_id.to_string()),
                parent_id: Some(parent_id.into_u64().to_string()),
                culprit: Some(metadata.target().to_string()),
                log: Some(Log {
                    level: Some(metadata.level().to_string()),
                    message: visitor
                        .0
                        .get("message")
                        .map(|message| message.to_string())
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                ..Default::default()
            };

            let span_ctx = extensions
                .get::<SpanContext>()
                .expect("Span context not found!");

            let metadata = self.create_metadata(&visitor, span_ctx, None, None, metadata);
            let batch = Batch::new(metadata, None, None, Some(json!(error)));
            self.client.send_batch(batch);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found!");
        let mut extensions = span.extensions_mut();

        let span_ctx = extensions
            .get_mut::<SpanContext>()
            .expect("Span context not found!");

        let instant = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        span_ctx.idle += span_ctx.instant.elapsed();
        span_ctx.instant = instant;
        span_ctx.first_entered_timestamp.get_or_insert(timestamp);
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found!");
        let mut extensions = span.extensions_mut();

        let span_ctx = extensions
            .get_mut::<SpanContext>()
            .expect("Span context not found!");

        let instant = Instant::now();
        span_ctx.busy += span_ctx.instant.elapsed();
        span_ctx.instant = instant;
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("Span not found!");
        let mut extensions = span.extensions_mut();
        let visitor = extensions
            .remove::<ApmVisitor>()
            .expect("Visitor not found!");
        let span_ctx = extensions
            .remove::<SpanContext>()
            .expect("Span context not found!");

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let span_metadata = span.metadata();

        let batch = if let Some(mut span) = extensions.remove::<Span>() {
            let metadata = self.create_metadata(
                &visitor,
                &span_ctx,
                Some(span.timestamp),
                span_ctx.first_entered_timestamp,
                span_metadata,
            );

            let real_timestamp = span_ctx.first_entered_timestamp.unwrap_or(span.timestamp);
            span.duration = (now - real_timestamp) as f32 / 1000.;
            span.timestamp = real_timestamp;
            Batch::new(metadata, None, Some(json!(span)), None)
        } else if let Some(mut transaction) = extensions.remove::<Transaction>() {
            let metadata = self.create_metadata(
                &visitor,
                &span_ctx,
                Some(transaction.timestamp),
                span_ctx.first_entered_timestamp,
                span_metadata,
            );

            let real_timestamp = span_ctx
                .first_entered_timestamp
                .unwrap_or(transaction.timestamp);
            transaction.duration = (now - real_timestamp) as f32 / 1000.;
            transaction.timestamp = real_timestamp;
            Batch::new(metadata, Some(json!(transaction)), None, None)
        } else {
            return;
        };

        self.client.send_batch(batch);
    }
}

impl<T> ApmLayer<T>
where
    T: crate::apm_client::Sender,
{
    pub(crate) fn new(mut config: Config, service_name: String) -> AnyResult<Self> {
        let metadata = Metadata {
            service: Service {
                name: service_name,
                version: config
                    .service
                    .as_mut()
                    .and_then(|service| service.version.take()),
                environment: config
                    .service
                    .as_mut()
                    .and_then(|service| service.environment.take()),
                language: config
                    .service
                    .as_mut()
                    .and_then(|service| service.language.take()),
                runtime: config
                    .service
                    .as_mut()
                    .and_then(|service| service.runtime.take()),
                framework: config
                    .service
                    .as_mut()
                    .and_then(|service| service.framework.take()),
                agent: Agent {
                    name: "tracing-elastic-apm".to_string(),
                    version: version::version!().to_string(),
                    ephemeral_id: None,
                },
                node: config
                    .service
                    .as_mut()
                    .and_then(|service| service.node.take()),
            },
            process: config.process,
            system: config.system,
            user: config.user,
            cloud: config.cloud,
            labels: None,
        };

        Ok(ApmLayer {
            client: T::new(
                config.apm_address,
                config.authorization,
                config.allow_invalid_certs,
                config.root_cert_path,
            )?,
            metadata: json!(metadata),
            timing_metadata_labels: false,
        })
    }

    pub fn with_timing_metadata_labels(&mut self) {
        self.timing_metadata_labels = true;
    }

    fn create_metadata(
        &self,
        visitor: &ApmVisitor,
        span_ctx: &SpanContext,
        timestamp: Option<u64>,
        first_entered_timestamp: Option<u64>,
        meta: &'static tracing::Metadata<'static>,
    ) -> Value {
        let mut metadata = self.metadata.clone();

        if !visitor.0.is_empty() {
            metadata["labels"] = json!(visitor.0);
            metadata["labels"]["level"] = json!(meta.level().to_string());
            metadata["labels"]["target"] = json!(meta.target().to_string());
            if self.timing_metadata_labels {
                metadata["labels"]["timestamp"] = json!(timestamp);
                metadata["labels"]["first_entered_timestamp"] = json!(first_entered_timestamp);
                metadata["labels"]["busy_ns"] = json!(span_ctx.busy.as_nanos());
                metadata["labels"]["idle_ns"] = json!(span_ctx.idle.as_nanos());
            }
        }

        metadata
    }
}
