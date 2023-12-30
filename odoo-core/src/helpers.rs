use std::error::Error;
use std::fmt::{Debug, Formatter, write};

use futures::prelude::*;
use log::trace;
use serde_cbor::Value;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio_serde::formats::SymmetricalCbor;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

use crate::encoder::Message;

pub fn hash_key_to_unsigned_int(key: &[u8]) -> u64 {
    const_xxh3(key)
}


pub async fn read_from_stream<'a>(r: &mut ReadHalf<'a>) -> Result<Vec<u8>, BoxedError> {
    let mut buf = [0; 1024];
    loop {
        let mut pre_read_len = buf.len();
        let buf_len = match r.read(&mut buf).await {
            Ok(0) => {
                break;
            }
            Ok(n) => {
                n
            }
            Err(err) => {
                return Err(BoxedError::new(err));
            }
        };
        if pre_read_len == buf.len() {
            trace!("Finished reading buffer stream");
            break;
        }
    }


    let mut buf = buf.to_vec();
    if let Some(index) = buf.iter().position(|&v| v == 0) {
        buf.truncate(index);
    }

    Ok(buf)
}


pub struct BoxedError(Box<dyn Error>);

impl BoxedError {
    pub fn new<T: Error + Debug + 'static>(error: T) -> Self {
        BoxedError(Box::new(error))
    }
}

impl Debug for BoxedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write(f, format_args!("{}", self.0.as_ref().to_string()))
    }
}


pub async fn send_tcp_message<'a>(mut w: WriteHalf<'a>, message: Message) -> Result<(), BoxedError> {
    let length_delimited = FramedWrite::new(w, LengthDelimitedCodec::new());
    let mut serialized = tokio_serde::SymmetricallyFramed::new(
        length_delimited,
        SymmetricalCbor::<Message>::default(),
    );

    serialized.send(message).await
        .map_err(|err| BoxedError::new(err))
}
