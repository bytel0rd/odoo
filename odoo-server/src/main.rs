#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::fmt::Debug;
use std::sync::Arc;

use futures::{SinkExt, TryStreamExt};
use tokio::net::TcpListener;

use odoo_core::key_listeners::ListenerHub;
use odoo_core::key_store::KeyStore;

use crate::server::OdooServer;

mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "INFO");
    pretty_env_logger::init();

    let keystore = Arc::new(KeyStore::new());
    let event_hub = Arc::new(ListenerHub::new());

    let (tx, rx) = crossbeam::channel::unbounded();
    keystore.register_listener("Listener_Hub".to_string(), tx.clone());

    let event_hub_async = event_hub.clone();
    tokio::spawn(async move {
        while let Ok(v) = rx.recv() {
            event_hub_async.notify(v).expect("notify_hub_listeners");
        }
    });

    info!("Odoo server listening on port: {}", "9058");
    let listener = TcpListener::bind("127.0.0.1:9058").await?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        let keystore_async = keystore.clone();
        let event_hub_async = event_hub.clone();
        tokio::spawn(async move {
            OdooServer::serve_socket(keystore_async, event_hub_async,  stream).await.expect("serving socket:");
        });
    }
}
