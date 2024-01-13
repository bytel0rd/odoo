use std::hash::Hasher;
use std::io::{BufRead, ErrorKind, Read};

use bytes::Buf;
use log::{debug, error, trace};
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

struct OdooNetInternalStream {
    writer: RwLock<tokio::io::WriteHalf<TcpStream>>,
    reader: RwLock<tokio::io::ReadHalf<TcpStream>>,
}

impl OdooNetInternalStream {
    fn new(stream: tokio::net::TcpStream) -> Self {
        let (mut read_stream, mut write_stream) = tokio::io::split(stream);
        OdooNetInternalStream {
            writer: RwLock::new(write_stream),
            reader: RwLock::new(read_stream),
        }
    }

    pub async fn connect(url: &str) -> Result<Self, OdooNetErr> {
        let stream = tokio::net::TcpStream::connect(url).await
            .map_err(|err| OdooNetErr::GeneralError(Box::new(err)))?;
        Ok(OdooNetInternalStream::new(stream))
    }
}


pub struct OdooNetStream {
    pub(crate) internal_tcp_stream: RwLock<OdooNetInternalStream>,
    pub(crate) url: String,
    pub(crate) reconnect: bool,
    pub(crate) stream_break: StreamBreaker,
}


pub struct StreamBreaker {
    // when sequence is found it yields the read stream
    pub delimiter: u8,

    // when sequence is found it stops the stream
    pub end_of_stream: Vec<u8>,
}

//
impl StreamBreaker {
    pub fn zero_delimiter() -> Self {
        StreamBreaker { delimiter: 0, end_of_stream: vec![0] }
    }
    pub fn has_byte_ended(&self, buffer: &[u8]) -> bool {
        self.end_of_stream.as_slice() == buffer
    }
}

// - open tcp stream
// - split tcp stream
// - map stream to channels
// - split stream to delimiters
// - implement reconnection

#[derive(Debug, thiserror::Error)]
pub enum OdooNetErr {
    #[error("NetError: {0:?}")]
    GeneralError(Box<dyn std::error::Error>),

    #[error("WriteError: {0:?}")]
    WriteError(String),

    #[error("ReadError: {0:?}")]
    ReadError(String),
}

impl OdooNetStream {
    pub fn wrap_server_stream(stream: tokio::net::TcpStream) -> Self {
        OdooNetStream {
            internal_tcp_stream: RwLock::new(OdooNetInternalStream::new(stream)),
            url: "".to_string(),
            reconnect: false,
            stream_break: StreamBreaker::zero_delimiter(),
        }
    }

    pub async fn connect(url: &str, stream_breaker: StreamBreaker) -> Result<Self, OdooNetErr> {
        let stream = tokio::net::TcpStream::connect(url).await
            .map_err(|err| OdooNetErr::GeneralError(Box::new(err)))?;
        let net_stream = OdooNetStream {
            internal_tcp_stream: RwLock::new(OdooNetInternalStream::new(stream)),
            url: url.to_string(),
            reconnect: true,
            stream_break: stream_breaker,
        };

        Ok(net_stream)
    }

    pub async fn write(&self, bytes: &[u8]) -> Result<(), OdooNetErr> {
        let mut buffer = bytes.to_vec();
        buffer.push(self.stream_break.delimiter);

        let stream = self.internal_tcp_stream.read().await;

        trace!("acquiring tcp write lock");
        let mut writer = stream.writer.write().await;
        trace!("acquired tcp write lock");

        if let Err(err) = writer.write(buffer.as_slice()).await {
            error!("Error writing to tcp stream: {}", err.to_string());
            match err.kind() {
                // ErrorKind::BrokenPipe => {
                //     if self.reconnect {
                //         trace!("trying to reopen stream");
                //         let connected_stream = OdooNetInternalStream::connect(self.url.as_str()).await?;
                //         stream.reader = connected_stream.reader;
                //         stream.writer = connected_stream.writer;
                //     } else {
                //         return Err(OdooNetErr::GeneralError(Box::new(err)));
                //     }
                // }
                _ => {
                    return Err(OdooNetErr::GeneralError(Box::new(err)));
                }
            }
        }
        Ok(())
    }


    pub async fn close_connection(&self) -> Result<(), OdooNetErr> {
        self.write(&[]).await
    }

    pub async fn read<C>(&self, callback: C) -> Result<bool, OdooNetErr> where C: Fn(&[u8]) -> bool {
        match self.internal_tcp_stream.try_read() {
            Ok(stream) => {
                trace!("acquiring tcp read lock");
                let mut reader = stream.reader.write().await;
                trace!("acquired tcp read lock");
                loop {
                    let mut read_buffer = bytes::BytesMut::with_capacity(1028);
                    match reader.read_buf(&mut read_buffer).await {
                        Ok(n) => {
                            trace!("read {} bytes from socket", n);
                            if n == 0 {
                                return Ok(true);
                            }
                            let mut reader = read_buffer.reader();

                            loop {
                                let mut byte_part = vec![];
                                match reader.read_until(self.stream_break.delimiter, &mut byte_part) {
                                    Ok(read) => {
                                        if byte_part.len() == 0 {
                                            // continue;
                                            break;
                                        }
                                        if self.stream_break.has_byte_ended(byte_part.as_slice()) {
                                            return Ok(true);
                                        }
                                        let mut byte_part = byte_part.iter()
                                            .filter_map(|&b| {
                                                if b != self.stream_break.delimiter {
                                                    return Some(b);
                                                }
                                                return None;
                                            })
                                            .collect::<Vec<u8>>();
                                        if !callback(byte_part.as_slice()) {
                                            trace!("Read callback terminated");
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        trace!("error reading upto delimiter: {}", err.to_string());
                                        break;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("Error writing to tcp stream: {}", err.to_string());
                            match err.kind() {
                                // ErrorKind::BrokenPipe => {
                                //     if self.reconnect {
                                //         trace!("trying to reopen stream");
                                //         let connected_stream = OdooNetInternalStream::connect(self.url.as_str()).await?;
                                //         stream.reader = connected_stream.reader;
                                //         stream.writer = connected_stream.writer;
                                //     }
                                // }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(err) => {
                debug!("Unable to acquire write lock on tcp stream to read: {}", err.to_string());
                return Err(OdooNetErr::ReadError(err.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod odoo_net_stream_test {
    use std::sync::Arc;
    use std::time::Duration;

    use log::info;
    use tokio::net::TcpListener;
    use tokio::runtime::Handle;

    use super::*;

    fn init_logger() {
        std::env::set_var("RUST_LOG", "TRACE");
        pretty_env_logger::init();
    }


    async fn create_server() {
        let listener = TcpListener::bind("0.0.0.0:4000").await
            .expect("should listen on port: 4000");
        info!("server listening on port: 4000");
        loop {
            let (mut stream, _) = listener.accept().await.expect("should accept connections");
            let net_stream = OdooNetStream {
                internal_tcp_stream: RwLock::new(OdooNetInternalStream::new(stream)),
                url: "".to_string(),
                reconnect: false,
                stream_break: StreamBreaker::zero_delimiter(),
            };
            tokio::spawn(async move {
                let stream = Arc::new(net_stream);
                loop {
                    let writer = stream.clone();
                    stream.read(move |bytes| {
                        info!("SERVER RECEIVED: bytes: {:?}", bytes);
                        let writer = writer.clone();
                        let handle = Handle::current();
                        handle.spawn(async move {
                            info!("now running on a worker thread");
                            writer.write("server: received1".as_bytes()).await.expect("should write message");
                            writer.write("server: received2".as_bytes()).await.expect("should write message");
                            writer.close_connection().await.expect("should write message");
                        });
                        return false;
                    }).await.expect("to read from stream");
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            });
        }
    }

    #[tokio::test]
    async fn it_should_send_and_receive_stream() {
        init_logger();
        tokio::spawn(create_server());
        tokio::time::sleep(Duration::from_secs(1)).await;
        let client_net_stream = crate::net_stream::OdooNetStream::connect("0.0.0.0:4000", StreamBreaker::zero_delimiter())
            .await.expect("create client connection");

        let client_net_stream = Arc::new(client_net_stream);
        loop {
            let writer = client_net_stream.clone();
            writer.write("client_sent".as_bytes()).await.expect("write to server");
            client_net_stream.read(move |bytes| {
                info!("CLIENT RECEIVED: bytes: {:?}", bytes);
                let writer = writer.clone();
                let handle = Handle::current();
                handle.spawn(async move {
                    writer.write("client_sent2".as_bytes()).await.expect("should write message");
                });
                return false;
            }).await.expect("to read from stream");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}
