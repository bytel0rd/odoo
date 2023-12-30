use std::error::Error;


use serde::{Deserialize, Serialize};
use crate::helpers::BoxedError;

pub struct MessageEncoder;

#[derive( Debug, thiserror::Error)]
pub enum EncoderError {
    #[error("Unable to deserialize message: {0:?}")]
    UnableToDeserializeError(Option<BoxedError>)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum MessageType {
    REQUEST,
    RESPONSE,
    STREAM,
    ERROR,
}

impl Default for MessageType {
    fn default() -> Self { MessageType::ERROR }
}


#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Message {
    pub r#type: MessageType,
    pub data: Vec<Vec<u8>>,
    pub timestamp: Option<i64>,
    pub id: Option<uuid::Uuid>
}

impl MessageEncoder {
    pub fn new() -> Result<Self, EncoderError> {
        Ok(MessageEncoder)
    }

    pub fn encode(&self, data: &Message) -> Result<Vec<u8>, EncoderError> {
        serde_cbor::to_vec(data)
            .map_err(|e| EncoderError::UnableToDeserializeError(Some(BoxedError::new(e))))
    }

    pub fn decode(&self, data: &[u8]) -> Result<Message, EncoderError> {
        serde_cbor::from_slice(data)
            .map_err(|e| EncoderError::UnableToDeserializeError(Some(BoxedError::new(e))))
    }
}