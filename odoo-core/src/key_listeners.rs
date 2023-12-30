use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::{Arc, LockResult, RwLock};

use log::{debug, error, trace};
use tokio::sync;
use uuid::Uuid;

use crate::helpers::hash_key_to_unsigned_int;
use crate::key_store::{KeyStoreEvent, StoreItem, StoreValue};

#[derive(Debug, Clone, thiserror::Error)]
pub enum ListenerHubError {
    #[error("Unable to register listener. {0}")]
    RegisterError(String),
    #[error("Unable to register event for listener. {0}")]
    EventRegisterError(String),
    #[error("Unable to notify listener. {0}")]
    NotificationError(String),
    #[error("Key expired")]
    KeyExpired,
    #[error("Stream replay failed")]
    ReplyFailedError,
}

pub type Listener = sync::broadcast::Sender<Arc<Option<StoreItem>>>;
pub struct ListenerHub {
    /// This is a map of listeners that are interested in a particular key.
    listeners_map: Arc<RwLock<BTreeMap<u64, Listener>>>,

    /// This is a map of the index of listeners that are interested in a particular event .
    event_indexes: Arc<RwLock<BTreeMap<u64, BTreeSet<u64>>>>,

    /// close all connections
    close_connections: bool,
}

impl ListenerHub {
    pub fn new() -> ListenerHub {
        ListenerHub {
            listeners_map: Arc::new(RwLock::new(BTreeMap::new())),
            event_indexes: Arc::new(RwLock::new(BTreeMap::new())),
            close_connections: false,
        }
    }

    pub fn register_listener(&self, tx: Listener) -> Result<Uuid, ListenerHubError> {
        let uuid = Uuid::now_v7();
        match self.listeners_map.write() {
            Ok(mut store) => {
                let key = hash_key_to_unsigned_int(uuid.as_bytes());
                store.insert(key, tx);
            }
            Err(err) => {
                error!("Error registering listener {:?}", &err);
            }
        }
        Ok(uuid)
    }

    pub fn unregister_listener(&self, key: &Uuid) -> bool {
        match self.listeners_map.write() {
            Ok(mut store) => {
                let listener_key = hash_key_to_unsigned_int(key.as_bytes());
                let is_removed = store.remove(&listener_key).is_some();
                if is_removed {
                    let events = self.event_indexes.clone();
                    let key = key.clone();
                    tokio::spawn(async move {
                        match ListenerHub::_unregister_all_events(events, &key) {
                            Ok(_) => {}
                            Err(err) => {
                                error!("Error registering listener events {:?}", &err);
                            }
                        }
                    });
                }
                return is_removed;
            }
            Err(err) => {
                error!("Error registering listener {:?}", &err);
            }
        }
        false
    }

    pub fn register_event(&self, key: &Uuid, event_key: u64) -> Result<(), ListenerHubError> {
        let listener_key = hash_key_to_unsigned_int(key.as_bytes());
        match self.event_indexes.write() {
            Ok(mut event) => {
                if event.contains_key(&event_key) {
                    event.get_mut(&event_key).unwrap().insert(listener_key);
                } else {
                    let mut events = BTreeSet::new();
                    events.insert(listener_key);
                    event.insert(event_key, events);
                }
            }
            Err(err) => {
                error!("Error registering channel on the event listener {:?}", &err);
                return Err(ListenerHubError::EventRegisterError("Error registering key for channel".to_string()));
            }
        }

        Ok(())
    }

    pub fn unregister_event(&self, key: &Uuid, event_key: u64) -> Result<(), ListenerHubError> {
        let listener_key = hash_key_to_unsigned_int(key.as_bytes());
        match self.event_indexes.write() {
            Ok(mut event) => {
                if event.contains_key(&event_key) {
                    event.get_mut(&event_key).unwrap().remove(&listener_key);
                }
            }
            Err(err) => {
                error!("Error unregistering channel on the event listener {:?}", &err);
                return Err(ListenerHubError::EventRegisterError("Error unregistering key for channel".to_string()));
            }
        }

        Ok(())
    }

    pub fn unregister_all_events(&self, key: &Uuid) -> Result<(), ListenerHubError> {
        ListenerHub::_unregister_all_events(self.event_indexes.clone(), key)
    }

    fn _unregister_all_events(event_indexes: Arc<RwLock<BTreeMap<u64, BTreeSet<u64>>>>, key: &Uuid) -> Result<(), ListenerHubError> {
        let listener_key = hash_key_to_unsigned_int(key.as_bytes());
        match event_indexes.write() {
            Ok(mut event) => {
                event.values_mut().for_each(|v| {
                    v.remove(&listener_key);
                });
            }
            Err(err) => {
                error!("Error unregistering channel on the all event listener {:?}", &err);
                return Err(ListenerHubError::EventRegisterError("Error unregistering key for all events channel".to_string()));
            }
        }

        Ok(())
    }

    pub fn notify(&self, event: Arc<KeyStoreEvent>) -> Result<(), ListenerHubError> {
       ListenerHub::_send_to_listeners(self.listeners_map.clone(), self.event_indexes.clone(), event)
    }

    fn _send_to_listeners(listeners_map: Arc<RwLock<BTreeMap<u64, Listener>>>,
                          event_indexes: Arc<RwLock<BTreeMap<u64, BTreeSet<u64>>>>,
                          event: Arc<KeyStoreEvent>) -> Result<(), ListenerHubError> {
        return match event.clone().as_ref().clone() {
            KeyStoreEvent::KeyAdded(store_value) => {
                ListenerHub::_notify_listeners(listeners_map, event_indexes, store_value)
            },
            KeyStoreEvent::StreamUpdate(store_value) => {
                ListenerHub::_notify_listeners(listeners_map, event_indexes, store_value)
            }
            _ => {
                Ok(())
            }
        };
    }


    fn _notify_listeners(listeners_map: Arc<RwLock<BTreeMap<u64, Listener>>>,
                         event_indexes: Arc<RwLock<BTreeMap<u64, BTreeSet<u64>>>>,
                         event: Arc<StoreItem>) -> Result<(), ListenerHubError> {
        let selected_listeners = ListenerHub::_select_listeners(event_indexes.clone(), event.clone())?;
        debug!("notifying listeners");
        let listeners_to_remove = Arc::new(RwLock::new(BTreeSet::new()));
        {
            match listeners_map.read() {
                Ok(listener_store) => {
                    // potential optimization...parallelize or async the listeners
                    for listener_index in &selected_listeners {
                        let listeners_to_remove = listeners_to_remove.clone();
                        if let Some(channel) = listener_store.get(listener_index) {
                            match channel.send(Arc::new(Some(event.clone().as_ref().clone()))) {
                                Err(err) => {
                                    error!("Unable to send to listeners channel: {:?}", err);
                                    ListenerHub::_update_listener_to_remove(listeners_to_remove, listener_index.clone());
                                }
                                _ => {
                                    trace!("sent event to listener");
                                }
                            }
                        } else {
                            ListenerHub::_update_listener_to_remove(listeners_to_remove, listener_index.clone());
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to read listeners: {:?}", err);
                    return Err(ListenerHubError::NotificationError("Unable to read listeners".to_string()));
                }
            }
        }

        if let Ok(mut map) = listeners_map.write() {
            if let Ok(removals) = listeners_to_remove.read() {
                for l in removals.iter() {
                    if let None = map.remove(l) {
                        trace!("Listener not present: {}", l);
                    }
                }
            }
        }

        Ok(())
    }

    fn _select_listeners(event_indexes: Arc<RwLock<BTreeMap<u64, BTreeSet<u64>>>>, event: Arc<StoreItem>) -> Result<BTreeSet<u64>, ListenerHubError> {
        debug!("selecting listeners");
        let event_key = hash_key_to_unsigned_int(event.key.as_bytes());
        return match event_indexes.read() {
            Ok(index) => {
                Ok(match index.get(&event_key) {
                    None => BTreeSet::new(),
                    Some(listeners) => listeners.clone()
                })
            }
            Err(err) => {
                error!("Error notifying all listening channels {:?}", &err);
                Err(ListenerHubError::EventRegisterError("Error notifying all listening channels".to_string()))
            }
        };
    }

    fn _update_listener_to_remove(listeners_to_remove: Arc<RwLock<BTreeSet<u64>>>, index_to_remove: u64) {
        match listeners_to_remove.write() {
            Ok(mut listener) => {
                listener.insert(index_to_remove);
                trace!("removing listeners: {}", index_to_remove);
            }
            Err(err) => {
                error!("Unable mark channel for removals {:?}", &err);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn should_insert_values_with_multiple_threads() {}
}