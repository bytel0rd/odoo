#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::fmt::Debug;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::ReadHalf;
use tokio::sync;
use odoo_core::encoder::{Message, MessageEncoder, MessageType};

use odoo_core::executor::execute;
use odoo_core::helpers::read_from_stream;
use odoo_core::key_listeners::ListenerHub;
use odoo_core::key_store::KeyStore;
use odoo_core::parser::Command;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "TRACE");
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

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        let keystore_async = keystore.clone();
        let event_hub_async = event_hub.clone();

        tokio::spawn(async move {
            handle_socket_connect(&mut stream, keystore_async, event_hub_async).await.expect("Failed to handle_socket");
        });
    }
}

async fn handle_socket_connect(stream: &mut TcpStream, keystore: Arc<KeyStore>, listener_hub: Arc<ListenerHub>) -> Result<(), Box<dyn std::error::Error>> {
    let encoder = MessageEncoder::new()?;
    let (mut r, mut w) = stream.split();
    let (tx, mut rx) = sync::broadcast::channel(10);
    let id = listener_hub.register_listener(tx.clone()).expect("Error");
    let mut buf = read_from_stream(&mut r).await.unwrap();
    if let Some(first) = buf.iter().position(|&b| b == 0) {
        buf.truncate(first);
    }
    trace!("raw_read_stream {:?}", &buf);
    let req_message = encoder.decode(buf.as_slice())?;
    debug!("raw_commands {:?}", req_message);
    let command = Command::parse_command(&req_message).expect("Unable to parse command");
    listener_hub.register_event(&id, command.get_event_key()).expect("Listening for event key");
    let tx_async = tx.clone();
    tokio::spawn(async move {
        debug!("executing command: {:?}", &command);
        execute(keystore.as_ref(), command, tx_async).expect("Error Executing Commands");
    });

    while let result = rx.recv().await {
        match result {
            Ok(v) => {
                debug!("Sending response to client");
                let store_value = v.as_ref().clone();
                let data = vec![store_value.map(|v| v.value).unwrap_or_default()];
                let message = Message {
                    r#type: MessageType::RESPONSE,
                    data,
                    timestamp: Some(chrono::Utc::now().timestamp()),
                };
                let buffer = encoder.encode(&message)?;
                w.write_all(buffer.as_slice()).await.unwrap();
            }
            Err(err) => {
                error!("channel error {:?}", &err);
            }
        }
    }
    // w.write_all("SERVER: READ_STREAM".as_bytes()).await.expect("write to client");
    Ok(())
}
