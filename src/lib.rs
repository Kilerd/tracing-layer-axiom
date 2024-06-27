use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use axiom_rs::Client;
use serde_json::{Value};
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
            client: client,
            dataset: self.dataset,
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
    while let Some(message) = rx.recv().await {
        // let client = self.client.clone();
        // let dataset = self.dataset.clone();
        //
        let mut retries = 0;
        while retries < MAX_RETRIES {
            let res = client
                .ingest(
                    dataset.clone(),
                    vec![serde_json::to_value(&message)
                        .expect("the log event should be serded, it must be a bug")],
                )
                .await;
            if let Err(e) = res {
                retries += 1;
                println!("fail to send logs to axiom: {}", e);
            } else {
                break;
            }
        }
    }
}

#[derive(Debug)]
pub struct AxiomLoggingLayer {
    client: Arc<Client>,
    dataset: String,
    application: String,
    environment: String,
    tx: UnboundedSender<LogEvent>,
}

impl<S: Subscriber> Layer<S> for AxiomLoggingLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut log_fields = LogFields {
            message: None,
            fields: HashMap::default(),
        };

        let mut visitor = JsonVisitor(log_fields);
        event.record(&mut visitor);

        let log_fields = visitor.0;
        let log_event = LogEvent {
            application: self.application.to_owned(),
            environment: self.environment.to_owned(),
            level: event.metadata().level().to_string(),
            target: event.metadata().target().to_string(),
            name: event.metadata().name().to_string(),
            message: log_fields.message.unwrap_or_default(),
            fields: serde_json::to_value(log_fields.fields)
                .expect("cannot serde a hashmap, it's a bug"),
        };

        dbg!(&log_event);

        if let Err(e) = self.tx.send(log_event) {
            tracing::error!(err=%e, "fail to send log event to given channel");
        }
    }
}

pub struct JsonVisitor<'a>(LogFields<'a>);

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.fields.insert(field.name(), Value::from(value));
    }

    /// Visit a signed 64-bit integer value.
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.fields.insert(field.name(), Value::from(value));
    }

    /// Visit an unsigned 64-bit integer value.
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.fields.insert(field.name(), Value::from(value));
    }

    /// Visit a boolean value.
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.fields.insert(field.name(), Value::from(value));
    }

    /// Visit a string value.
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.fields.insert(field.name(), Value::from(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let field_name = field.name();

        match field_name {
            n if n.starts_with("log.") => {}
            n if n.starts_with("r#") => {
                self.0
                    .fields
                    .insert(&n[2..], serde_json::Value::from(format!("{:?}", value)));
            }

            n => {
                self.0
                    .fields
                    .insert(n, serde_json::Value::from(format!("{:?}", value)));
            }
        }
    }
}

impl<'a> JsonVisitor<'a> {
    fn trim_string_char(string: &mut String) {
        if string.starts_with('"') {
            *string = (&string[1..string.len()]).to_owned();
        }
        if string.ends_with('"') {
            *string = (&string[0..string.len() - 1]).to_owned();
        }
    }
}

pub struct LogFields<'a> {
    message: Option<String>,
    fields: HashMap<&'a str, serde_json::Value>,
}

#[derive(serde::Serialize, Debug)]
pub struct LogEvent {
    application: String,
    environment: String,
    level: String,
    target: String,
    name: String,
    message: String,
    fields: Value,
}
