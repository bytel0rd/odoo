use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel;
use crossbeam::channel::SendError;
use dashmap::DashMap;
use log::error;
use crate::helpers::hash_key_to_unsigned_int;

#[derive(Debug, Clone)]
pub struct StoreItem {
    timestamp: chrono::DateTime<chrono::Utc>,
    value: Vec<u8>,
    expire_at: Option<chrono::DateTime<chrono::Utc>>,
    key: String
}

#[derive(Debug, Clone)]
pub enum StoreValue {
    Item(StoreItem),
    Stream,
}

impl StoreValue {
    pub fn get_key(&self) -> String {
        match self {
            StoreValue::Item(store_value) => {
                return store_value.key.clone();
            }
            _ => {"".to_string()}
        }
    }
}

#[derive(Clone)]
pub struct KeyStore {
    store: DashMap<u64, Arc<StoreValue>>,
    notify: DashMap<String, channel::Sender<Arc<KeyStoreEvent>>>,
}

pub enum KeyStoreError {
    KeyNotFound,
    KeyExpired,
}

#[derive(Debug, Clone)]
pub enum KeyStoreEvent {
    KeyAdded(Arc<StoreValue>),
    KeyDeleted(Arc<StoreValue>),
}

impl KeyStore {
    pub fn new() -> KeyStore {
        KeyStore {
            store: DashMap::new(),
            notify: DashMap::new(),
        }
    }

    pub fn add_key(&self, key: &str, value: Vec<u8>, duration: Option<Duration>) {
        let item = Arc::new(StoreValue::Item(
            StoreItem {
                expire_at: None,
                timestamp: chrono::Utc::now(),
                value,
                key: key.to_string()
            }
        ));
        let store_key = hash_key_to_unsigned_int(key.as_bytes());
        self.store.insert(store_key, item.clone());
        KeyStore::notify_listeners(self.notify.clone(), KeyStoreEvent::KeyAdded(item));
        match duration {
            Some(timeout) => {
                let async_store = self.store.clone();
                let async_listeners = self.notify.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(timeout).await;
                    if let Some((key, value)) = async_store.remove(&store_key) {
                        KeyStore::notify_listeners(async_listeners, KeyStoreEvent::KeyDeleted(value));
                    }
                });
            }
            _ => {}
        }

    }

    pub fn get_key(&self, key: &str) -> Option<StoreItem> {
        if let Some(value) = self.store.get(&hash_key_to_unsigned_int(key.as_bytes())) {
            let stored_value = value.value().clone().as_ref().clone();
            if let StoreValue::Item(item) = stored_value {
                return Some(item.clone());
            }
        }
        None
    }

    pub fn delete_key(&self, key: &str) -> bool {
        self.store.remove(&hash_key_to_unsigned_int(key.as_bytes())).is_some()
    }

    pub fn register_listener(&self, listener_name: String, tx: channel::Sender<Arc<KeyStoreEvent>>) {
        self.notify.insert(listener_name, tx);
    }

    fn notify_listeners(notify: DashMap<String, channel::Sender<Arc<KeyStoreEvent>>>, value: KeyStoreEvent) -> () {
        let value = Arc::new(value);
        notify.iter().for_each(|notifier| {
            if let Err(notification_error) =  notifier.send(value.clone()) {
                if let SendError(_) = notification_error {
                    error!("Error sending notification to {} listener. EX: {:?}",  notifier.key().as_str(), notification_error);
                    notify.remove(notifier.key().as_str());
                }
            }
        });
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn should_insert_values_with_multiple_threads() {
        let store = Arc::new(KeyStore::new());
        for thread_id in 1..100 {
            let store = store.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(5));
                for j in 0..200000 {
                    store.add_key((j * thread_id).to_string().as_str(), format!("{}-{}", thread_id, j).as_bytes().to_vec(), None);
                }
            });
        }
    }

    #[test]
    fn should_notify_new_values() {
        let (tx, rx) = channel::unbounded();
        let store = Arc::new(KeyStore::new());

        store.register_listener("test_listener".to_string(), tx);
        std::thread::spawn(move || {
            store.add_key(10.to_string().as_str(), format!("{}-{}", 19, "test").as_bytes().to_vec(), None);
        });

        std::thread::sleep(Duration::from_secs(5));
        while let Ok(a) = rx.recv() {
            println!("{:?}", a);
        }
    }
}