use std::fmt::Debug;
use log::error;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use odoo_core::encoder::{Message, MessageEncoder, MessageType};
use odoo_core::helpers::{BoxedError, read_from_stream};

use crate::client::OdooClientError::GeneralError;

pub struct OdooClient {
    host_url: String,
    encoder: MessageEncoder,
}

#[derive(Debug)]
pub enum OdooClientError {
    UnableToConnectToClient(String),
    UnableToEncodeRequest(String),
    GeneralError(BoxedError),
}

impl OdooClient {

    pub fn new (host_url: String) -> Self {
        OdooClient {
            host_url,
            encoder: MessageEncoder::new().unwrap(),
        }
    }

    pub async fn set_key(&self, key: &str, value: &[u8], timeout_in_mills_secs: Option<i64>) -> Result<(), OdooClientError> {
        let mut stream = self.open_tcp_stream().await?;
        let (_, mut w) = stream.split();

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
        };

        let bytes = self.encoder.encode(&message)
            .map_err(|e| GeneralError(BoxedError::new(e)))?;

        w.write_all(bytes.as_slice()).await
            .map_err(|e| GeneralError(BoxedError::new(e)))
    }

    pub async fn get_key(&self, key: &str) -> Result<Option<Vec<u8>>, OdooClientError> {
        let mut stream = self.open_tcp_stream().await?;
        let (mut r, mut w) = stream.split();

        let mut cmds = vec![];
        cmds.push("GET".as_bytes().to_vec());
        cmds.push(key.as_bytes().to_vec());
        let message = Message {
            r#type: MessageType::REQUEST,
            data: cmds,
            timestamp: Some(chrono::Utc::now().timestamp()),
        };

        // write instructions
        let bytes = self.encoder.encode(&message)
            .map_err(|e| GeneralError(BoxedError::new(e)))?;

        w.write_all(bytes.as_slice()).await
            .map_err(|e| GeneralError(BoxedError::new(e)))?;

        // read response
        let mut read_buffer = read_from_stream(&mut r).await
            .map_err(|e| GeneralError(e))?;

        if let Some(index) =read_buffer.iter().position(|&v| v == 0) {
            read_buffer.truncate(index);
        }

        dbg!(&read_buffer);

        let response_message = self.encoder.decode(read_buffer.as_slice())
            .map_err(|e| GeneralError(BoxedError::new(e)))?;

        Ok(response_message.data.get(0).map(|v| v.to_owned()))
    }

    pub async fn append_to_stream(&self, key: &str, value: &[u8], timeout_in_mills_secs: Option<i64>) -> Result<(), OdooClientError> {
        let mut stream = self.open_tcp_stream().await?;
        let (_, mut w) = stream.split();

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
        };

        let bytes = self.encoder.encode(&message)
            .map_err(|e| GeneralError(BoxedError::new(e)))?;

        w.write_all(bytes.as_slice()).await
            .map_err(|e| GeneralError(BoxedError::new(e)))
    }


    pub async fn listen_to_stream<C>(&self, key: &str, checkpoint_time: Option<i64>, limit: Option<i64>, callback: C) -> Result<(), OdooClientError>
        where C: Fn(Option<Vec<u8>>) -> () {
        let mut stream = self.open_tcp_stream().await?;
        let (mut r, mut w) = stream.split();
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
        };

        let bytes = self.encoder.encode(&message)
            .map_err(|e| GeneralError(BoxedError::new(e)))?;
        if let Err(err) = w.write_all(bytes.as_slice()).await {
            error!("error sending request to tcp stream")
        }


        loop {
            // read response
            let read_buffer = match read_from_stream(&mut r).await {
                Ok(v) => {
                    v
                }
                Err(err) => {
                    return Err(GeneralError(err));
                }
            };

            if read_buffer.is_empty() {
                break;
            }

            let response_message = match self.encoder.decode(read_buffer.as_slice()) {
                Ok(v) => {
                    v
                }
                Err(err) => {
                    return Err(GeneralError(BoxedError::new(err)));
                }

            };

            let data = response_message.data.get(0).map(|v| v.to_owned());
            callback(data);
        }


        Ok(())
    }


    async fn open_tcp_stream(&self) -> Result<TcpStream, OdooClientError> {
        TcpStream::connect("127.0.0.1:8080").await
            .map_err(|e| {
                error!("Unable to open tcp connection to: {} Ex: {:?}", &self.host_url, e);
                OdooClientError::UnableToConnectToClient(self.host_url.clone())
            })
    }
}


