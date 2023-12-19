use std::sync::Arc;

use log::error;

use crate::key_listeners::{Listener, ListenerHubError};
use crate::key_store::KeyStore;
use crate::parser::Command;

pub fn execute(store: &KeyStore, cmd: Command, channel: Listener) -> Result<(), ListenerHubError> {
    match &cmd {
        Command::SET(key, value) => {
            store.add_key(key.as_str(), value.value.to_vec(), value.duration)
        }
        Command::GET(key) => {
            let store_item = store.get_key(key.as_str());
            channel.send(Arc::new(store_item))
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
        Command::RESUME(stream_key,  stream_opts) => {
            let (_, updates) = store.resume_stream_for_key(stream_key.as_str(),
                                                               stream_opts.checkpoint.map(|v| v as i64),
                                                               stream_opts.limit.map(|v| v as i64));

            let last_time = updates.last()
                .map(|v| v.timestamp.timestamp() as u64);

            if updates.is_empty() {
                return Ok(());
            }

            for event in updates {
                channel.send(Arc::new(Some(event)))
                    .map_err(|err| {
                        error!("Unable to send reply for stream key: {} Error: {:?}", stream_key.as_str(), err);
                        ListenerHubError::ReplyFailedError
                    })?;
            }

            let mut next_stream = stream_opts.clone();
            next_stream.checkpoint = last_time;
            let cmd = Command::RESUME(stream_key.to_string(), next_stream);
            execute(store, cmd, channel)?;
        }
    }

    Ok(())
}