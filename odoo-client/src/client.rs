use std::fmt::Debug;
use std::time::Duration;

use futures::{SinkExt, StreamExt, TryStreamExt};
use log::{error, trace};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tokio::sync;
use uuid::Uuid;

use odoo_core::encoder::{Message, MessageType};
use odoo_core::helpers::BoxedError;
use odoo_net::net_stream::{OdooNetErr, OdooNetStream, StreamBreaker};

type ChannelMessage = (Message, Option<sync::mpsc::Sender<Message>>);

pub struct OdooClient {
    host_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OdooClientError {
    #[error("UnableToConnectToClient: {0}")]
    UnableToConnectToClient(String),

    #[error("UnableToSendRequest: {0}")]
    UnableToSendRequest(String),

    #[error("ServerError: {0}")]
    ServerError(String),

    #[error("OdooClientError: {0:?}")]
    GeneralError(BoxedError),
}

impl OdooClient {
    pub fn new(host_url: String) -> Self {
        OdooClient {
            host_url,
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

        let client = OdooNetStream::connect(self.host_url.as_str(), StreamBreaker::zero_delimiter())
            .await
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        match serde_cbor::to_vec(&message) {
            Ok(data) => {
                client.write(data.as_slice()).await
                    .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;
            }
            Err(err) => {
                return Err(OdooClientError::UnableToSendRequest(format!("Failed to deserialize: {}", err.to_string())));
            }
        }

        Ok(())
    }


    pub async fn get_key(&self, key: &str) -> Result<Option<Vec<u8>>, OdooClientError> {
        let (tx, mut rx) = sync::mpsc::channel(5);

        let mut cmds = vec![];
        cmds.push("GET".as_bytes().to_vec());
        cmds.push(key.as_bytes().to_vec());
        let message = Message {
            r#type: MessageType::REQUEST,
            data: cmds,
            timestamp: Some(chrono::Utc::now().timestamp()),
            id: Some(Uuid::new_v4()),
        };

        let client = OdooNetStream::connect(self.host_url.as_str(), StreamBreaker::zero_delimiter())
            .await
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        match serde_cbor::to_vec(&message) {
            Ok(data) => {
                client.write(data.as_slice()).await
                    .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;
            }
            Err(err) => {
                return Err(OdooClientError::UnableToSendRequest(format!("Failed to deserialize: {}", err.to_string())));
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        let message_id = message.id.clone();
        let tx_1 = tx.clone();
        tokio::spawn(async move {
            let tx_2 = tx_1.clone();
            let result = client.read(move |bytes| {
                match serde_cbor::from_slice(bytes) {
                    Ok(data) => {
                        let tx_1 = tx_1.clone();
                        Handle::current().spawn(async move {
                            if let Err(_) = tx_1.send(data).await {
                                trace!("Failed to send net stream data to receiver");
                            }
                        });
                    }
                    Err(err) => {
                        let error_message = OdooClient::create_error_message(
                            format!("Failed to deserialize: {}", err.to_string()).as_str(),
                            message_id);
                        let tx_1 = tx_1.clone();
                        Handle::current().spawn(async move {
                            if let Err(_) = tx_1.send(error_message).await {
                                trace!("Failed to error to receiver channel");
                            }
                        });
                    }
                }
                return false;
            }).await;
            if let Err(err) = result {
                let error_message = OdooClient::create_error_message(
                    format!("Failed to read from stream: {}", err.to_string()).as_str(),
                    message_id);

                Handle::current().spawn(async move {
                    if let Err(_) = tx_2.send(error_message).await {
                        trace!("Failed to send error to receiver channel");
                    }
                });
            }
        });


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

    pub async fn append_to_stream(&self, key: &str, mut rx: sync::mpsc::UnboundedReceiver<Vec<u8>>, timeout_in_mills_secs: Option<i64>) -> Result<(), OdooClientError> {
        let client = OdooNetStream::connect(self.host_url.as_str(), StreamBreaker::zero_delimiter())
            .await
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        while let Some(data) = rx.recv().await {
            let mut cmds = vec![];
            cmds.push("APPEND".as_bytes().to_vec());
            cmds.push(key.as_bytes().to_vec());
            cmds.push(data);
            let timeout = timeout_in_mills_secs.unwrap_or(-1i64).to_string();
            cmds.push(timeout.as_bytes().to_vec());
            let message = Message {
                r#type: MessageType::REQUEST,
                data: cmds,
                timestamp: Some(chrono::Utc::now().timestamp()),
                id: Some(Uuid::new_v4()),
            };
            match serde_cbor::to_vec(&message) {
                Ok(data) => {
                    client.write(data.as_slice()).await
                        .map_err(|err| OdooClientError::UnableToSendRequest("Failed to send append stream message".to_string()))?;
                }
                Err(err) => {
                    return Err(OdooClientError::UnableToSendRequest(format!("Failed to deserialize: {}", err.to_string())));
                }
            }
        }


        client.close_connection().await
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        Ok(())
    }

    pub async fn listen_to_stream(&self, key: &str, checkpoint_time: Option<i64>, limit: Option<i64>, tx: sync::mpsc::Sender<Result<Vec<u8>, String>>) -> Result<(), OdooClientError>
    {

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

        let client = OdooNetStream::connect(self.host_url.as_str(), StreamBreaker::zero_delimiter())
            .await
            .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;

        match serde_cbor::to_vec(&message) {
            Ok(data) => {
                client.write(data.as_slice()).await
                    .map_err(|err| OdooClientError::GeneralError(BoxedError::new(err)))?;
            }
            Err(err) => {
                return Err(OdooClientError::UnableToSendRequest(format!("Failed to deserialize: {}", err.to_string())));
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        let tx_2 = tx.clone();

        loop {
            let tx_1 = tx.clone();

            let result = client.read(move |bytes| {
                let tx_1 = tx_1.clone();
                match serde_cbor::from_slice::<Message>(bytes) {
                    Ok(message) => {
                        let tx_1 = tx_1.clone();
                        Handle::current().spawn(async move {
                            let raw_data = message.data.get(0).map(|v| v.to_owned());
                            match message.r#type {
                                MessageType::STREAM => {
                                    if let Some(data) = raw_data {
                                        if let Err(_) = tx_1.send(Ok(data)).await {
                                            trace!("Failed to send net stream data to receiver");
                                        }
                                    }
                                }
                                _ => {
                                    if let Some(bytes) = raw_data {
                                        match String::from_utf8(bytes) {
                                            Ok(error_message) => {
                                                if let Err(_) = tx_1.send(Err(error_message)).await {
                                                    trace!("Failed to send net stream error to receiver");
                                                }
                                            }
                                            Err(error) => {
                                                if let Err(_) = tx_1.send(Err("Unable to parse error bytes".to_string())).await {
                                                    trace!("Failed to send net stream error to receiver");
                                                }
                                            }
                                        }
                                    }
                                }
                            };
                        });
                    }
                    Err(err) => {
                        let error_message = format!("Failed to deserialize: {}", err.to_string());
                        let tx_1 = tx_1.clone();
                        Handle::current().spawn(async move {
                            if let Err(_) = tx_1.send(Err(error_message)).await {
                                trace!("Failed to error to receiver channel");
                            }
                        });
                    }
                }
                return false;
            }).await;
            // match result {
            //     Ok(status) => {
            //         trace!("Read progressing: {}", status);
            //         if status {
            //             //break
            //         }
            //     }
            //     Err(err) => {
            //         let error_message = format!("Failed to read from stream: {}", err.to_string());
            //         let tx_1 = tx_1.clone();
            //         Handle::current().spawn(async move {
            //             if let Err(_) = tx_1.send(Err(error_message)).await {
            //                 trace!("Failed to send error to receiver channel");
            //             }
            //         });
            //         // break;
            //     }
            // }
        }


        return Ok(());
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


