use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result as AnyResult;
use rand::prelude::*;
use serde_json::{json, Value};
use tracing::{
    field::Visit,
    span::{Attributes, Record},
    Event, Id, Level, Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, registry::Registry, Layer};

use crate::{
    apm_client::Batch,
    config::{Config, PARENT_ID_FIELD_NAME, TRACE_ID_FIELD_NAME},
    model::{Agent, Error, Log, Metadata, Service, Span, Transaction},
    visitor::RootLinkVisitor,
    ApmClient, ToVisited,
};

#[derive(Copy, Clone)]
struct TraceContext {
    pub trace_id: u128,
}

/// Returns the `u128` trace id of the currently-active span, as assigned by the
/// [`ApmLayer`], or `None` if there is no active span (or no registry in the
/// dispatcher).
///
/// Use this to splice a *detached* future onto the in-flight trace: read the id
/// before spawning, then record it as the [`TRACE_ID_FIELD_NAME`] field on a
/// `parent: None` span (alongside [`PARENT_ID_FIELD_NAME`] = the current span
/// id) so the spawned work becomes its own transaction yet stays linked.
///
/// Assumes the global subscriber is a `tracing_subscriber::Registry` stack
/// (the usual `registry().with(ApmLayer)` shape); returns `None` otherwise.
pub fn current_trace_id() -> Option<u128> {
    let id = tracing::Span::current().id()?;
    tracing::dispatcher::get_default(|dispatch| {
        let registry = dispatch.downcast_ref::<Registry>()?;
        let span = registry.span(&id)?;
        let trace_id = span.extensions().get::<TraceContext>()?.trace_id;
        Some(trace_id)
    })
}

/// Builds per-event `context.tags` from the visited fields.
///
/// Elastic indexes `transaction.context.tags` / `span.context.tags` /
/// `error.context.tags` as the per-document `labels.*`, so user-recorded fields
/// belong here rather than in the stream-global `metadata.labels`.
///
/// - A leading `labels.` prefix is stripped so the APM key matches the ECS log
///   nesting (e.g. `labels.pipeline_id` -> tag `pipeline_id` -> `labels.pipeline_id`
///   in the UI, instead of the de-dotted `labels.labels_pipeline_id`).
/// - `message` is consumed by the error log, so it is not duplicated as a tag.
/// - `custom.*` fields carry nested, unindexable data (ECS `custom`) that is not
///   valid as a flat label value, so they are dropped.
///
/// Returns `None` when nothing remains, leaving `context` untouched.
fn visited_to_tags<U: ToVisited>(visitor: &U) -> Option<crate::model::Tags> {
    let tags: crate::model::Tags = visitor
        .to_visited()
        .iter()
        .filter(|(key, _)| {
            let key = key.as_str();
            key != "message"
                && key != TRACE_ID_FIELD_NAME
                && key != PARENT_ID_FIELD_NAME
                && !key.starts_with("custom.")
        })
        .map(|(key, value)| {
            let key = key
                .strip_prefix("labels.")
                .unwrap_or(key.as_str())
                .to_string();
            (key, value.clone())
        })
        .collect();

    (!tags.is_empty()).then_some(tags)
}

struct SpanContext {
    pub idle: Duration,
    pub busy: Duration,
    pub instant: Instant,
    pub first_entered_timestamp: Option<u64>,
}

/// Telemetry capability that publishes events and spans to Elastic APM.
pub struct ApmLayer<T, U> {
    client: T,
    metadata: Value,
    timing_metadata_labels: bool,
    _phantom: std::marker::PhantomData<U>,
}

impl<S, T, U> Layer<S> for ApmLayer<T, U>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    T: crate::apm_client::Sender + 'static,
    U: ToVisited + Default + Visit + Send + Sync + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let timestamp = Instant::now();

        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();

        let mut visitor = U::default();
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
            let mut visitor = RootLinkVisitor::default();
            attrs.record(&mut visitor);

            let trace_ctx = TraceContext {
                trace_id: visitor.trace_id.unwrap_or_else(random),
            };

            // A manually supplied `parent_id` makes this a *child transaction*
            // (own id/latency, but nested under the given span in the same
            // trace) instead of a detached root — see `current_trace_id`.
            let new_transaction = Transaction {
                id: id.into_u64().to_string(),
                transaction_type: "custom".to_string(),
                trace_id: trace_ctx.trace_id.to_string(),
                parent_id: visitor.parent_id.map(|id| id.to_string()),
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

        let visitor = extensions.get_mut::<U>().expect("Visitor not found!");
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
            if let Some(span) = ctx.span(parent_id) {
                let extensions = span.extensions();
                let trace_ctx = extensions
                    .get::<TraceContext>()
                    .expect("Trace context not found!");

                let mut visitor = U::default();
                event.record(&mut visitor);

                let error = Error {
                    id: random::<u128>().to_string(),
                    trace_id: Some(trace_ctx.trace_id.to_string()),
                    parent_id: Some(parent_id.into_u64().to_string()),
                    culprit: Some(metadata.target().to_string()),
                    context: visited_to_tags(&visitor).map(|tags| {
                        crate::model::TransactionContext {
                            tags: Some(tags),
                            ..Default::default()
                        }
                    }),
                    log: Some(Log {
                        level: Some(metadata.level().to_string()),
                        message: visitor
                            .to_visited()
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
        let visitor = extensions.remove::<U>().expect("Visitor not found!");
        let span_ctx = extensions
            .remove::<SpanContext>()
            .expect("Span context not found!");

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let span_metadata = span.metadata();

        let tags = visited_to_tags(&visitor);

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

            if let Some(tags) = tags {
                match span.context.as_mut() {
                    Some(context) => context.tags = Some(tags),
                    None => {
                        span.context = Some(crate::model::SpanContext {
                            tags: Some(tags),
                            ..Default::default()
                        })
                    }
                }
            }

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

            if let Some(tags) = tags {
                match transaction.context.as_mut() {
                    Some(context) => context.tags = Some(tags),
                    None => {
                        transaction.context = Some(crate::model::TransactionContext {
                            tags: Some(tags),
                            ..Default::default()
                        })
                    }
                }
            }

            Batch::new(metadata, Some(json!(transaction)), None, None)
        } else {
            return;
        };

        self.client.send_batch(batch);
    }
}

impl<U> ApmLayer<ApmClient, U>
where
    U: ToVisited,
{
    pub(crate) fn new(mut config: Config, service_name: String) -> AnyResult<Self> {
        let apm_address = config.apm_address.clone();
        let authorization = config.authorization.take();
        let allow_invalid_certs = config.allow_invalid_certs;
        let root_cert_path = config.root_cert_path.take();

        let metadata = Self::setup_metadata(config, service_name);

        Ok(ApmLayer {
            client: ApmClient::new(
                apm_address,
                authorization,
                allow_invalid_certs,
                root_cert_path,
            )?,
            metadata: json!(metadata),
            timing_metadata_labels: false,
            _phantom: Default::default(),
        })
    }
}

impl<T, U> ApmLayer<T, U>
where
    T: crate::apm_client::Sender,
    U: ToVisited,
{
    pub fn new_with(config: Config, service_name: String, sender: T) -> AnyResult<Self> {
        let metadata = Self::setup_metadata(config, service_name);

        Ok(ApmLayer {
            client: sender,
            metadata: json!(metadata),
            timing_metadata_labels: false,
            _phantom: Default::default(),
        })
    }

    pub fn with_timing_metadata_labels(&mut self) {
        self.timing_metadata_labels = true;
    }

    fn setup_metadata(mut config: Config, service_name: String) -> Metadata {
        Metadata {
            service: Service {
                name: service_name,
                id: config
                    .service
                    .as_mut()
                    .and_then(|service| service.id.take()),
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
                agent: config
                    .service
                    .as_mut()
                    .and_then(|service| service.agent.take())
                    .unwrap_or_else(|| Agent {
                        name: "tracing-elastic-apm".to_string(),
                        version: version::version!().to_string(),
                        ephemeral_id: None,
                        activation_method: None,
                    }),
                node: config
                    .service
                    .as_mut()
                    .and_then(|service| service.node.take()),
            },
            process: config.process,
            system: config.system,
            user: config.user,
            cloud: config.cloud,
            network: config.network,
            labels: config.labels,
        }
    }

    fn create_metadata(
        &self,
        visitor: &U,
        span_ctx: &SpanContext,
        timestamp: Option<u64>,
        first_entered_timestamp: Option<u64>,
        meta: &'static tracing::Metadata<'static>,
    ) -> Value {
        let mut metadata = self.metadata.clone();

        // Per-event user fields are routed into the event's `context.tags` (see
        // `visited_to_tags`); `metadata.labels` keeps the static, stream-global
        // labels (e.g. `team` / `project`) plus the diagnostic level/target/timing.
        if !visitor.to_visited().is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use fxhash::FxHashMap;

    struct TestVisited(FxHashMap<String, Value>);

    impl ToVisited for TestVisited {
        fn to_visited(&self) -> &FxHashMap<String, Value> {
            &self.0
        }
    }

    fn visited(pairs: &[(&str, Value)]) -> TestVisited {
        TestVisited(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn strips_labels_prefix_and_keeps_unprefixed_keys() {
        let tags = visited_to_tags(&visited(&[
            ("labels.pipeline_id", json!("abc")),
            ("team", json!("core")),
        ]))
        .expect("tags should be present");

        // `labels.pipeline_id` -> `pipeline_id` so the APM UI shows `labels.pipeline_id`.
        assert_eq!(tags.get("pipeline_id"), Some(&json!("abc")));
        assert!(!tags.contains_key("labels.pipeline_id"));
        // Unprefixed keys pass through verbatim.
        assert_eq!(tags.get("team"), Some(&json!("core")));
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn drops_message_and_custom_fields() {
        let tags = visited_to_tags(&visited(&[
            ("labels.job_id", json!("j1")),
            ("message", json!("hello")),
            ("custom.payload", json!({ "nested": true })),
        ]))
        .expect("tags should be present");

        assert_eq!(tags.get("job_id"), Some(&json!("j1")));
        assert!(!tags.contains_key("message"));
        assert!(!tags.contains_key("custom.payload"));
        assert!(!tags.contains_key("payload"));
        assert_eq!(tags.len(), 1);
    }

    #[test]
    fn empty_visitor_yields_no_tags() {
        assert!(visited_to_tags(&visited(&[])).is_none());
    }

    #[test]
    fn only_filtered_fields_yields_no_tags() {
        let only_filtered = visited(&[("message", json!("hi")), ("custom.x", json!("y"))]);
        assert!(visited_to_tags(&only_filtered).is_none());
    }
}
