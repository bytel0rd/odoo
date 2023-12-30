use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt, TryStreamExt};
use log::{debug, error, trace};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync;
use tokio::sync::{broadcast, RwLock};
use tokio_serde::formats::SymmetricalCbor;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

use odoo_core::encoder::{Message, MessageType};
use odoo_core::helpers::BoxedError;

type ChannelMessage = (Message, Option<sync::mpsc::Sender<Message>>);

pub struct OdooClient {
    host_url: String,
    sender: sync::broadcast::Sender<ChannelMessage>,
    reciver: sync::broadcast::Receiver<ChannelMessage>,
}

#[derive(Debug)]
pub enum OdooClientError {
    UnableToConnectToClient(String),
    UnableToSendRequest(String),
    ServerError(String),
    GeneralError(BoxedError),
}

impl OdooClient {
    pub fn new(host_url: String) -> Self {
        let (tx, rx) = sync::broadcast::channel(5);
        let tx2 = tx.clone();
        let host_url_async = host_url.clone();
        tokio::spawn(async move {
            OdooClient::write_channel(host_url_async.as_str(), tx2).await.expect("Failed connection to odoo server");
        });

        OdooClient {
            host_url,
            sender: tx,
            reciver: rx,
        }
    }

    pub async fn set_key(&self, key: &str, value: &[u8], timeout_in_mills_secs: Option<i64>) -> Result<(), OdooClientError> {
        let mut cmds = vec![];
        cmds.push("SET".as_bytes().to_vec());
        cmds.push(key.as_bytes().to_vec());
        cmds.push(value.to_vec());
        let timeout = timeout_in_mills_secs.unwrap_or(-1i64).to_string();
        cmds.push(timeout.as_bytes().to_vec());
        let message = Message {
            r#type: MessageType::REQUEST,
            data: cmds,
            timestamp: Some(chrono::Utc::now().timestamp()),
            id: Some(Uuid::new_v4()),
        };

        self.sender.send((message, None))
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        Ok(())
    }


    pub async fn get_key(&self, key: &str) -> Result<Option<Vec<u8>>, OdooClientError> {
        let (tx, mut rx) = sync::mpsc::channel(1);

        let mut cmds = vec![];
        cmds.push("GET".as_bytes().to_vec());
        cmds.push(key.as_bytes().to_vec());
        let message = Message {
            r#type: MessageType::REQUEST,
            data: cmds,
            timestamp: Some(chrono::Utc::now().timestamp()),
            id: Some(Uuid::new_v4()),
        };

        self.sender.send((message, Some(tx)))
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        while let Some(message) = rx.recv().await {
            let raw_data = message.data.get(0).map(|v| v.to_owned());
            match message.r#type {
                MessageType::RESPONSE => {
                    return Ok(raw_data);
                }
                _ => {
                    if let Some(bytes) = raw_data {
                        let error_message = String::from_utf8(bytes)
                            .map_err(|e| OdooClientError::GeneralError(BoxedError::new(e)))?;
                        return Err(OdooClientError::ServerError(error_message));
                    }
                }
            }
        }

        return Ok(None);
    }

    pub async fn append_to_stream(&self, key: &str, value: &[u8], timeout_in_mills_secs: Option<i64>) -> Result<(), OdooClientError> {
        let mut cmds = vec![];
        cmds.push("APPEND".as_bytes().to_vec());
        cmds.push(key.as_bytes().to_vec());
        cmds.push(value.to_vec());
        let timeout = timeout_in_mills_secs.unwrap_or(-1i64).to_string();
        cmds.push(timeout.as_bytes().to_vec());
        let message = Message {
            r#type: MessageType::REQUEST,
            data: cmds,
            timestamp: Some(chrono::Utc::now().timestamp()),
            id: Some(Uuid::new_v4()),
        };

        self.sender.send((message, None))
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        Ok(())
    }

    pub async fn listen_to_stream<C>(&self, key: &str, checkpoint_time: Option<i64>, limit: Option<i64>, callback: C) -> Result<(), OdooClientError>
        where C: Fn(Option<Vec<u8>>) -> () {

        // RESUME {stream} {last_time?} {limit?} {replay?}
        let mut cmds = vec![];
        cmds.push("RESUME".as_bytes().to_vec());
        cmds.push(key.as_bytes().to_vec());
        let checkpoint_time = checkpoint_time.map(|v| v.to_string()).unwrap_or("NULL".to_string());
        cmds.push(checkpoint_time.as_bytes().to_vec());
        let limit = limit.unwrap_or(-1i64).to_string();
        cmds.push(limit.as_bytes().to_vec());

        let message = Message {
            r#type: MessageType::REQUEST,
            data: cmds,
            timestamp: Some(chrono::Utc::now().timestamp()),
            id: Some(Uuid::new_v4()),
        };

        let mut socket = OdooClient::open_tcp_stream(self.host_url.as_str()).await?;
        let (mut r, mut w) = tokio::io::split(socket);
        let length_delimited = FramedWrite::new(w, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalCbor::<Message>::default(),
        );
        if let Err(err) = serialized.send(message).await {
            error!("Error sending message to network stream: {:?}", &err);
        }

        let length_delimited = FramedRead::new(r, LengthDelimitedCodec::new());
        let mut deserialized = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalCbor::<Message>::default(),
        );

        loop {
            match deserialized.next().await {
                None => {
                    trace!("received nothing");
                    break;
                }
                Some(message) => {
                    match message {
                        Ok(response_message) => {
                            debug!("Received value: {:?}",  &response_message);
                            let data = response_message.data.get(0).map(|v| v.clone());
                            callback(data);
                        }
                        Err(err) => {
                            error!("Error serializing network stream: {:?}", &err);
                            break;
                        }
                    };
                }
            }
        }

        Ok(())
    }


    async fn open_tcp_stream(host_url: &str) -> Result<TcpStream, OdooClientError> {
        TcpStream::connect(host_url).await
            .map_err(|e| {
                let host_url = host_url.to_string();
                error!("Unable to open tcp connection to: {} Ex: {:?}", &host_url, e);
                OdooClientError::UnableToConnectToClient(host_url)
            })
    }

    async fn write_channel(host_url: &str, mut tx: sync::broadcast::Sender<ChannelMessage>) -> Result<(), OdooClientError> {
        let mut socket = OdooClient::open_tcp_stream(host_url).await?;
        let (mut r, mut w) = tokio::io::split(socket);

        let writer_task = tokio::spawn(OdooClient::handle_writer(w, tx.subscribe()));
        let reader_task = tokio::spawn(OdooClient::handle_reader(r, tx.subscribe()));

        tokio::try_join!(reader_task,  writer_task).unwrap();


        Ok(())
    }

    async fn handle_writer(mut writer: tokio::io::WriteHalf<TcpStream>, mut rx: broadcast::Receiver<ChannelMessage>) {
        let length_delimited = FramedWrite::new(writer, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalCbor::<Message>::default(),
        );

        println!("3: listening to writing for server");
        loop {
            println!("3-1: going to pick things to write");
            match rx.recv().await {
                Ok((message, _)) => {
                    debug!("sending message-ID: {:?}", &message.id);
                    if let Err(err) = serialized.send(message).await {
                        error!("Error sending message to network stream: {:?}", &err);
                    }
                }
                Err(err) => {
                    error!("Error receiving req messages: {:?}", &err);
                }
            }
        }
    }

    async fn handle_reader(mut reader: tokio::io::ReadHalf<TcpStream>, mut rx: broadcast::Receiver<ChannelMessage>) {
        let length_delimited = FramedRead::new(reader, LengthDelimitedCodec::new());
        let mut deserialized = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalCbor::<Message>::default(),
        );

        let reciever_map = Arc::new(RwLock::new(BTreeMap::new()));
        let reciever_async = reciever_map.clone();
        tokio::spawn(async move {
            while let Ok((message, some_receiver)) = rx.recv().await {
                if let Some(reply_channel) = some_receiver {
                    let mut channels = reciever_async.write().await;
                    channels.insert(message.id, reply_channel);
                }
            }
        });

        println!("5: waiting to receive");
        tokio::time::sleep(Duration::from_secs(5)).await;

        while let Some(message) = deserialized.next().await {
            match message {
                Ok(response_message) => {
                    debug!("Received value: {:?}",  &response_message);
                    let mut map = reciever_map.write().await;
                    let id = response_message.id.clone();
                    if let Some(receiver) = map.get(&id) {
                        if let Err(err) = receiver.send(response_message).await {
                            error!("Error sending message to receiver stream: {:?}", &err);
                            map.remove(&id);
                        }
                    }
                }
                Err(err) => {
                    error!("Error serializing network stream: {:?}", &err);
                }
            };
        }
    }
}


