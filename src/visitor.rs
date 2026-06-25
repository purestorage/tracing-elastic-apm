use std::fmt::Debug;

use fxhash::FxHashMap;
use serde::Serialize;
use serde_json::{json, Value};
use tracing::field::{Field, Visit};

use crate::config::{PARENT_ID_FIELD_NAME, TRACE_ID_FIELD_NAME};

pub trait ToVisited {
    fn to_visited(&self) -> &FxHashMap<String, Value>;
}

#[derive(Default)]
pub struct ApmVisitor(pub(crate) FxHashMap<String, Value>);

impl Visit for ApmVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert_value(field, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert_value(field, value);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert_value(field, value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert_value(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.insert_value(field, format!("{:?}", value));
    }
}

impl ToVisited for ApmVisitor {
    fn to_visited(&self) -> &FxHashMap<String, Value> {
        &self.0
    }
}

impl ApmVisitor {
    #[inline]
    pub fn insert_value<T>(&mut self, field: &Field, value: T)
    where
        T: Serialize,
    {
        self.0.insert(field.name().to_string(), json!(value));
    }
}

/// Captures the optional linkage fields used to splice a `parent: None` span
/// into an existing trace: [`TRACE_ID_FIELD_NAME`] (a `u128`) and
/// [`PARENT_ID_FIELD_NAME`] (a `u64` registry span id). Only consulted for
/// spans with no registry parent — i.e. ones that become their own
/// transaction.
#[derive(Default)]
pub(crate) struct RootLinkVisitor {
    pub(crate) trace_id: Option<u128>,
    pub(crate) parent_id: Option<u64>,
}

impl Visit for RootLinkVisitor {
    fn record_i64(&mut self, _field: &Field, _value: i64) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == PARENT_ID_FIELD_NAME {
            self.parent_id = Some(value);
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if field.name() == TRACE_ID_FIELD_NAME {
            self.trace_id = Some(value);
        }
    }

    fn record_bool(&mut self, _field: &Field, _value: bool) {}

    fn record_str(&mut self, _field: &Field, _value: &str) {}

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}
}
