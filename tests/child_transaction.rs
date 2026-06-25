//! Prototype: a detached span linked to its trigger as a *child transaction*
//! (own id/latency, same trace, nested under the trigger) rather than a child
//! span or a detached root.
//!
//! Mirrors the scheduler shape: an "rpc" span is the in-flight trigger; the
//! "job_task" is spawned detached (`parent: None`) but stamped with the
//! trigger's trace id + span id so APM still links it.

use std::sync::{Arc, Mutex};

use serde_json::Value;
use tracing_elastic_apm::{
    config::Config, current_trace_id, new_layer_with_sender, ApmVisitor, Batch, Sender,
};
use tracing_subscriber::{layer::SubscriberExt, registry};

#[derive(Clone, Default)]
struct CollectingSender(Arc<Mutex<Vec<Batch>>>);

impl Sender for CollectingSender {
    fn send_batch(&self, batch: Batch) {
        self.0.lock().unwrap().push(batch);
    }
}

#[test]
fn detached_span_becomes_linked_child_transaction() {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let sender = CollectingSender(collected.clone());

    let layer = new_layer_with_sender::<_, ApmVisitor>(
        "scheduler".to_string(),
        Config::new("http://localhost:8200".to_string()),
        sender,
    )
    .expect("layer");

    let subscriber = registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    // The in-flight trigger (e.g. the RPC handler span). Root => Transaction.
    let rpc = tracing::info_span!("rpc");
    let (trace_id, rpc_id) = {
        let _entered = rpc.enter();
        // Caller-side reads, done *before* spawning the detached future.
        let trace_id = current_trace_id().expect("trace id while rpc is current");
        let rpc_id = rpc.id().expect("rpc has an id").into_u64();
        (trace_id, rpc_id)
    };

    // The detached, long-lived job: its own root (`parent: None`) so it does
    // NOT keep `rpc` open, but stamped to link back to it.
    {
        let _job = tracing::info_span!(
            parent: None,
            "job_task",
            trace_id = trace_id,
            parent_id = rpc_id,
            labels.job_id = "job-123",
        );
    } // dropped here -> on_close emits the job transaction

    drop(rpc); // emits the rpc transaction

    let batches = collected.lock().unwrap();
    let transactions: Vec<&Value> = batches
        .iter()
        .filter_map(|b| b.transaction.as_ref())
        .collect();

    let job = transactions
        .iter()
        .find(|t| t["name"] == Value::from("job_task"))
        .expect("job_task transaction emitted");
    let rpc_txn = transactions
        .iter()
        .find(|t| t["name"] == Value::from("rpc"))
        .expect("rpc transaction emitted");

    // It is its OWN transaction, not a span...
    assert_ne!(job["id"], rpc_txn["id"]);
    // ...sharing the trigger's trace...
    assert_eq!(job["trace_id"], rpc_txn["trace_id"]);
    assert_eq!(job["trace_id"], Value::from(trace_id.to_string()));
    // ...nested under the trigger via parent_id.
    assert_eq!(job["parent_id"], Value::from(rpc_id.to_string()));
    assert_eq!(job["parent_id"], rpc_txn["id"]);

    // Structural linkage fields must NOT leak into labels/tags.
    let tags = &job["context"]["tags"];
    assert!(tags.get("trace_id").is_none());
    assert!(tags.get("parent_id").is_none());
    assert_eq!(tags["job_id"], Value::from("job-123"));

    println!(
        "job_task transaction:\n{}",
        serde_json::to_string_pretty(job).unwrap()
    );
}
