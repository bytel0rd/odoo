use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync;

use odoo_core::encoder::{Message, MessageType};
use odoo_core::executor::execute;
use odoo_core::helpers::BoxedError;
use odoo_core::key_listeners::ListenerHub;
use odoo_core::key_store::KeyStore;
use odoo_core::parser::Command;
use odoo_net::net_stream::OdooNetStream;

type ChannelMessage = (Message, Option<sync::mpsc::Sender<Message>>);

pub struct OdooServer {
    keystore: Arc<KeyStore>,
    listener_hub: Arc<ListenerHub>,
    sender: sync::broadcast::Sender<ChannelMessage>,
    receiver: sync::broadcast::Receiver<ChannelMessage>,
}

#[derive(Debug)]
pub enum OdooClientError {
    UnableToConnectToClient(String),
    UnableToSendRequest(String),
    ServerError(String),
    GeneralError(BoxedError),
}

impl OdooServer {
    pub async fn serve_socket(key_store: Arc<KeyStore>, listener_hub: Arc<ListenerHub>, mut socket: TcpStream) -> Result<(), OdooClientError> {
        let net_stream = OdooNetStream::wrap_server_stream(socket);
        let stream = Arc::new(net_stream);
        loop {
            let reply = stream.clone();
            let key_store = key_store.clone();
            let listener_hub = listener_hub.clone();

            let is_done = stream.read(move |bytes| {
                debug!("SERVER RECEIVED: {} bytes", bytes.len());
                match serde_cbor::from_slice::<Message>(bytes) {
                    Ok(message) => {
                        let handle = Handle::current();
                        let _ = handle.enter();
                        let key_store = key_store.clone();
                        let listener_hub = listener_hub.clone();
                        let reply = reply.clone();
                        handle.spawn(async move {
                            OdooServer::execute_command(key_store, listener_hub, &message, reply.clone()).await;
                            if let Err(_) = reply.close_connection().await {
                                error!("Failed to close connection stream");
                            }
                        });
                    }
                    Err(err) => {
                        error!("unable to deserialize message from read request: {}", err.to_string());
                        OdooServer::write_reply(reply.clone(), &OdooServer::create_error_message("Unable to deserialize message", None));
                    }
                }
                return false;
            }).await.expect("to read from stream");
            tokio::time::sleep(Duration::from_millis(2)).await;
            if is_done {
                break;
            }
        }
        Ok(())
    }


    async fn execute_command(key_store: Arc<KeyStore>, listener_hub: Arc<ListenerHub>, message: &Message, reply: Arc<OdooNetStream>) {
        let reply_id = message.id;
        info!("ACK: {:?}", message.get_message_id());
        let parsed_command = Command::parse_command(message);
        if parsed_command.is_err() {
            let err = parsed_command.unwrap_err();
            OdooServer::write_reply(reply, &OdooServer::create_error_message(err.to_string().as_str(), reply_id));
            return;
        }
        let command = parsed_command.unwrap();
        info!("Executing: {}", &command.get_event_key());

        let (tx, mut rx) = tokio::sync::mpsc::channel(500);
        let tx_1 = tx.clone();
        let reply_1 = reply.clone();
        tokio::spawn(async move {
            if let Err(err) = execute(key_store.as_ref(), listener_hub, reply_id, command, tx_1).await {
                OdooServer::write_reply(reply_1, &OdooServer::create_error_message(err.to_string().as_str(), reply_id));
                return;
            }
        });

        while let Some(message) = rx.recv().await {
            OdooServer::write_reply(reply.clone(), &message);
        }
    }

    fn write_reply(stream: Arc<OdooNetStream>, message: &Message) -> () {
        match serde_cbor::to_vec(message) {
            Ok(data) => {
                let handle = Handle::current();
                let _ = handle.enter();
                handle.spawn(async move {
                    if let Err(_) = stream.write(data.as_slice()).await {
                        error!("Failed to write send reply");
                    }
                });
            }
            Err(err) => {
                error!("Failed to deserialize message: {}", err.to_string());
            }
        }

    }

    fn close_stream(stream: Arc<OdooNetStream>) -> () {
        let handle = Handle::current();
        let _ = handle.enter();
        handle.spawn(async move {
            if let Err(_) = stream.close_connection().await {
                error!("Error closing connection stream");
            }
        });
    }


    fn create_error_message(message: &str, id: Option<uuid::Uuid>) -> Message {
        Message {
            r#type: MessageType::ERROR,
            data: vec![message.as_bytes().to_vec()],
            timestamp: Some(chrono::Utc::now().timestamp()),
            id,
        }
    }
}


