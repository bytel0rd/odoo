use std::sync::Arc;

use log::{debug, error, trace};
use tokio::sync;
use uuid::Uuid;

use crate::encoder::{Message, MessageType};
use crate::helpers::hash_key_to_unsigned_int;
use crate::key_listeners::{ListenerHub, ListenerHubError};
use crate::key_store::{KeyStore, StoreItem};
use crate::parser::Command;

pub async fn execute(store: &KeyStore,
                     listener_hub: Arc<ListenerHub>,
                     request_id: Option<Uuid>,
                     cmd: Command, channel: sync::mpsc::UnboundedSender<Message>) -> Result<(), ListenerHubError> {
    match &cmd {
        Command::SET(key, value) => {
            store.add_key(key.as_str(), value.value.to_vec(), value.duration)
        }
        Command::GET(key) => {
            let store_item = store.get_key(key.as_str());
            let result = Message {
                r#type: MessageType::RESPONSE,
                data: vec![store_item.map(|v| v.value).unwrap_or(vec![])],
                timestamp: Some(chrono::Utc::now().timestamp()),
                id: request_id,
            };
            channel.send(result)
                .map_err(|err| {
                    error!("Unable to send reply for get key: {} Error: {:?}", key.as_str(), err);
                    ListenerHubError::ReplyFailedError
                })?;
        }
        Command::DELETE(key) => {
            store.delete_key(key.as_str());
        }
        Command::APPEND(stream_key, stream_update) => {
            store.append_stream(stream_key.as_str(), stream_update.value.to_vec(), stream_update.duration);
        }
        Command::RESUME(stream_key, stream_opts) => {
            let (tx, mut rx) = sync::broadcast::channel(100);
            let id = listener_hub.register_listener(tx.clone())
                .map_err(|err| {
                    error!("Unable to send reply for stream key: {} Error: {:?}", stream_key.as_str(), err);
                    ListenerHubError::ReplyFailedError
                })?;

            let d = tx.clone();
            let id_2 = request_id;
            let c_2 = channel.clone();
            tokio::spawn(async move {
                while let Ok(item) = d.subscribe().recv().await {
                    match item.as_ref().clone() {
                        None => {
                            trace!("received nothing from the hub");
                        }
                        Some(_) => {
                            trace!("sending update stream: {:?}", id_2);
                        }
                    }
                }
            });
            let updates = store.resume_stream_for_key(stream_key.as_str(),
                                                      stream_opts.checkpoint.map(|v| v as i64));

            listener_hub.register_event(&id, hash_key_to_unsigned_int(stream_key.as_bytes()))
                .map_err(|err| {
                    error!("Failed to register to listen to events: {} Error: {:?}", stream_key.as_str(), err);
                    ListenerHubError::RegisterError("Failed to register to listen to events".to_string())
                })?;

            let chunks = updates.chunks(stream_opts.limit.unwrap_or(50) as usize);
            for chunk in chunks {
                for event in chunk {
                    let message = Message {
                        r#type: MessageType::STREAM,
                        data: vec![event.value.to_owned()],
                        timestamp: Some(chrono::Utc::now().timestamp()),
                        id: request_id,
                    };
                    channel.send(message)
                        .map_err(|err| {
                            error!("Unable to send reply for stream key: {} Error: {:?}", stream_key.as_str(), err);
                            ListenerHubError::ReplyFailedError
                        })?;
                }
            }

            trace!("starting to listen for hub notifications");
            while let Ok(item) = tx.subscribe().recv().await {
                match item.as_ref().clone() {
                    None => {
                        trace!("received nothing from the hub");
                    }
                    Some(store_item) => {
                        let data = vec![store_item.value];
                        let message = Message {
                            r#type: MessageType::STREAM,
                            data,
                            timestamp: Some(chrono::Utc::now().timestamp()),
                            id: request_id,
                        };
                        debug!("sending update stream: {}", stream_key.as_str());
                        channel.send(message)
                            .map_err(|err| {
                                error!("Unable to send reply for stream key: {} Error: {:?}", stream_key.as_str(), err);
                                ListenerHubError::ReplyFailedError
                            })?;
                    }
                }
            }

        }
    }

    Ok(())
}