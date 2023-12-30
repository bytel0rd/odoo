use std::fmt::Debug;
use std::sync::Arc;

use futures::{SinkExt, TryStreamExt};
use log::{debug, error};
use tokio::net::TcpStream;
use tokio::sync;
use tokio::sync::broadcast;
use tokio_serde::formats::SymmetricalCbor;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use odoo_core::encoder::{Message, MessageType};
use odoo_core::executor::execute;
use odoo_core::helpers::BoxedError;
use odoo_core::key_listeners::ListenerHub;
use odoo_core::key_store::KeyStore;
use odoo_core::parser::Command;

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
        let (mut r, mut w) = tokio::io::split(socket);
        let (tcp_socket_entry, tcp_socket_exit) = sync::broadcast::channel(500);
        let (reply_socket_entry, mut reply_socket_exit) = sync::mpsc::unbounded_channel();

        let reader_task = tokio::spawn(OdooServer::handle_reader(r, tcp_socket_entry.clone()));
        let execute_task = tokio::spawn(OdooServer::execute_command(tcp_socket_exit, key_store, listener_hub, reply_socket_entry));
        let writer_task = tokio::spawn(OdooServer::handle_writer(w, reply_socket_exit));

        tokio::try_join!(reader_task, execute_task, writer_task).unwrap();
        Ok(())
    }

    async fn execute_command(mut tcp_socket_exit: broadcast::Receiver<Message>, key_store: Arc<KeyStore>,
                             listener_hub: Arc<ListenerHub>,
                             reply_entry: sync::mpsc::UnboundedSender<Message>) {
        loop {
            match tcp_socket_exit.recv().await {
                Ok(req_message) => {
                    debug!("req-message {:?}", &req_message);
                    let parsed_command = Command::parse_command(&req_message);
                    if parsed_command.is_err() {
                        let err = parsed_command.unwrap_err();
                        if let Err(err) = reply_entry.send(OdooServer::create_error_message(err.to_string().as_str(), req_message.id.clone())) {
                            error!("Unable to send parsing error to response channel {:?}", &err);
                            return;
                        }
                        continue;
                    }
                    let command = parsed_command.unwrap();
                    debug!("raw_command {:?}", &command);

                    let reply_id = req_message.id;
                    if let Err(err) = execute(key_store.as_ref(), listener_hub.clone(), reply_id, command, reply_entry.clone()).await {
                        if let Err(err) = reply_entry.send(OdooServer::create_error_message(err.to_string().as_str(), req_message.id.clone())) {
                            error!("Unable to execute command error to response channel {:?}", &err);
                            return;
                        }
                    }
                }
                Err(err) => {
                    error!("Error receiving from tcp_socket_exit");
                    return;
                }
            }
        }
    }

    async fn handle_writer(mut writer: tokio::io::WriteHalf<TcpStream>, mut reply_exit: sync::mpsc::UnboundedReceiver<Message>) {
        let length_delimited = FramedWrite::new(writer, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalCbor::<Message>::default(),
        );
        while let Some(message) = reply_exit.recv().await {
            debug!("replying message-ID: {:?}", &message.id);
            if let Err(err) = serialized.send(message).await {
                error!("Error writing message to tcp network stream: {:?}", &err);
            }
        }
    }

    async fn handle_reader(mut reader: tokio::io::ReadHalf<TcpStream>,
                           mut tcp_socket_entry: broadcast::Sender<Message>) {
        let length_delimited = FramedRead::new(reader, LengthDelimitedCodec::new());
        let mut deserialized = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalCbor::<Message>::default(),
        );

        loop {
            let tx2 = tcp_socket_entry.clone();
            match deserialized.try_next().await {
                Ok(value) => {
                    // trace!("polling tcp stream: {:?}",  &value);
                    if let Some(request) = value {
                        if let Err(err) = tx2.send(request) {
                            error!("Error sending message to receiver stream: {}", &err.to_string());
                            return;
                        }
                    }
                }
                Err(err) => {
                    error!("Error reading from tcp connection: {:?}", &err);
                    return;
                }
            };
        }
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


