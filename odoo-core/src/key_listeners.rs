use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use crossbeam::channel;
use uuid::Uuid;

use crate::helpers::hash_key_to_unsigned_int;

#[derive(Debug, Clone)]
pub struct StoreItem {
    timestamp: chrono::DateTime<chrono::Utc>,
    value: Vec<u8>,
    expire_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub enum StoreValue {
    Item(StoreItem),
    Stream,
}


#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EventKey(u64);

#[derive(Debug, Clone)]
pub struct EventListenerIndex(Vec<u64>);


pub enum ListenerHubError {
    RegisterError(String),
    KeyNotFound,
    KeyExpired,
}

pub struct ListenerHub {
    /// This is a map of listeners that are interested in a particular key.
    listeners_map: Arc<RwLock<BTreeMap<u64, channel::Sender<Arc<KeyStoreEvent>>>>>,

    /// This is a map of the index of listeners that are interested in a particular event .
    event_indexes: Arc<RwLock<BTreeMap<u64, Vec<u64>>>>,
}

#[derive(Debug, Clone)]
pub enum KeyStoreEvent {
    KeyAdded(Arc<StoreValue>),
    KeyDeleted(Arc<StoreValue>),
}

impl ListenerHub {
    pub fn new() -> ListenerHub {
        ListenerHub {
            listeners_map: Arc::new(RwLock::new(BTreeMap::new())),
            event_indexes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn register_listener(&self, tx: channel::Sender<Arc<KeyStoreEvent>>) -> Result<Uuid, ListenerHubError> {
        let uuid = Uuid::now_v7();
        match self.listeners_map.write() {
            Ok(mut store) => {
                let key = hash_key_to_unsigned_int(uuid.as_bytes());
                store.insert(key, tx);
            }
            Err(_) => {}
        }
        Ok(uuid)
    }

    pub fn unregister_listener(&self, key: Uuid) -> bool {
        self.listeners_map.remove(&key).is_some()
    }


    // fn notify_listeners(&self, value: KeyStoreEvent) -> Result<(), KeyStoreError> {
    //     let value = Arc::new(value);
    //     self.notify.iter().for_each(|notifier| {
    //         if let Err(notification_error) = notifier.send(value.clone()) {
    //             if let SendError(_) = notification_error {
    //                 error!("Error sending notification to {} listener. EX: {:?}",  notifier.key().as_str(), notification_error);
    //                 self.notify.remove(notifier.key().as_str());
    //             }
    //         }
    //     });
    //     Ok(())
    // }
}


#[cfg(test)]
mod tests {
    #[test]
    fn should_insert_values_with_multiple_threads() {}
}