use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use axiom_rs::Client;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::field::Field;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use typed_builder::TypedBuilder;

/// maxium retry times for sending
const MAX_RETRIES: usize = 10;

#[derive(TypedBuilder)]
pub struct ConfigBuilder {
    pub token: String,
    pub org_id: String,
    pub dataset: String,
    pub application: String,
    pub environment: String,
}

impl ConfigBuilder {
    pub fn into_layer(self) -> AxiomLoggingLayer {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let client = Arc::new(
            Client::builder()
                .with_token(self.token)
                .with_org_id(self.org_id)
                .build()
                .unwrap(),
        );
        tokio::spawn(axiom_backend_worker(
            rx,
            client.clone(),
            self.dataset.clone(),
        ));
        AxiomLoggingLayer {
            application: self.application,
            environment: self.environment,
            tx,
        }
    }
}

pub(crate) async fn axiom_backend_worker(
    mut rx: UnboundedReceiver<LogEvent>,
    client: Arc<Client>,
    dataset: String,
) {
    let mut buf = Vec::with_capacity(10);

    while rx.recv_many(&mut buf, 10).await > 0 {
        let mut retries = 0;
        while retries < MAX_RETRIES {
            let res = client.ingest(dataset.clone(), &buf).await;
            if let Err(e) = res {
                retries += 1;
                println!("fail to send logs to axiom: {}", e);
            } else {
                break;
            }
        }

        buf.clear();
    }
}
#[derive(Debug)]
pub struct AxiomLoggingLayer {
    application: String,
    environment: String,
    tx: UnboundedSender<LogEvent>,
}

impl<S> Layer<S> for AxiomLoggingLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);

        let log_event = LogEvent {
            _time: Utc::now().timestamp_millis(),
            application: self.application.to_owned(),
            environment: self.environment.to_owned(),
            level: event.metadata().level().to_string(),
            target: visitor
                .log_target
                .map(|it| it.to_owned())
                .unwrap_or_else(|| event.metadata().target().to_string()),
            message: visitor.message.unwrap_or_default(),
            fields: serde_json::to_value(visitor.fields)
                .expect("cannot serde a hashmap, it's a bug"),
        };

        if let Err(e) = self.tx.send(log_event) {
            tracing::error!(err=%e, "fail to send log event to given channel");
        }
    }
}

#[derive(Default)]
pub struct JsonVisitor<'a> {
    log_target: Option<String>,
    message: Option<String>,
    fields: HashMap<&'a str, serde_json::Value>,
}

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record_value(field.name(), Value::from(value));
    }

    /// Visit a signed 64-bit integer value.
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field.name(), Value::from(value));
    }

    /// Visit an unsigned 64-bit integer value.
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field.name(), Value::from(value));
    }

    /// Visit a boolean value.
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field.name(), Value::from(value));
    }

    /// Visit a string value.
    fn record_str(&mut self, field: &Field, value: &str) {
        let field_name = field.name();
        match field_name {
            "log.target" => {
                self.log_target = Some(value.to_owned());
            }
            "message" => {
                self.message = Some(value.to_owned());
            }
            n if n.starts_with("log.") => {}
            n => {
                self.record_value(n, Value::from(value));
            }
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(
            field.name(),
            serde_json::Value::from(format!("{:?}", value)),
        );
    }
}

impl<'a> JsonVisitor<'a> {
    fn record_value(&mut self, name: &'a str, value: Value) {
        match name {
            "message" => {
                self.message = value.as_str().map(|it| it.to_owned());
            }
            n if n.starts_with("r#") => {
                self.fields.insert(&n[2..], value);
            }
            n => {
                self.fields.insert(n, value);
            }
        }
    }
}

#[derive(serde::Serialize, Debug)]
pub struct LogEvent {
    _time: i64,
    application: String,
    environment: String,
    level: String,
    target: String,
    message: String,
    fields: Value,
}
