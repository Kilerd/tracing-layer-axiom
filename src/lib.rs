use std::fmt;
use std::sync::Arc;

use axiom_rs::Client;
use serde_json::{json, Value};
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
}

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
            fields: json!({}),
        };

        let mut visitor = JsonVisitor(&mut log_fields);
        event.record(&mut visitor);

        let log_event = LogEvent {
            application: self.application.to_owned(),
            environment: self.environment.to_owned(),
            level: event.metadata().level().to_string(),
            target: event.metadata().target().to_string(),
            name: event.metadata().name().to_string(),
            message: log_fields.message.unwrap_or_default(),
            fields: log_fields.fields,
        };

        if let Err(e) = self.tx.send(log_event) {
            tracing::error!(err=%e, "fail to send log event to given channel");
        }
    }
}

pub struct JsonVisitor<'a>(&'a mut LogFields);

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let field_name = field.name();
        if field_name.eq("message") {
            let mut msg = format!("{:?}", value);
            Self::trim_string_char(&mut msg);
            self.0.message = Some(msg);
        }
        if let Some(container) = self.0.fields.as_object_mut() {
            let mut string = format!("{:?}", value);
            Self::trim_string_char(&mut string);
            container.insert(field_name.to_string(), Value::String(string));
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

pub struct LogFields {
    message: Option<String>,
    fields: Value,
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
