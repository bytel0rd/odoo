use std::fmt::Debug;
use std::sync::Arc;

use crossbeam::channel;
use crossbeam::channel::SendError;
use dashmap::DashMap;
use log::error;

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

    pub fn add_key(&self, key: u64, value: Vec<u8>) {
        let item = Arc::new(StoreValue::Item(
            StoreItem {
                expire_at: None,
                timestamp: chrono::Utc::now(),
                value,
            }
        ));
        self.store.insert(key, item.clone());
        self.notify_listeners(KeyStoreEvent::KeyAdded(item));
    }

    pub fn get_key(&self, key: u64) -> Option<StoreItem> {
        if let Some(value) = self.store.get(&key) {
            let stored_value = value.value().clone().as_ref().clone();
            if let StoreValue::Item(item) = stored_value {
                return Some(item.clone());
            }
        }
        None
    }

    pub fn delete_key(&self, key: u64) -> bool {
        self.store.remove(&key).is_some()
    }

    pub fn register_listener(&self, listener_name: String, tx: channel::Sender<Arc<KeyStoreEvent>>) {
        self.notify.insert(listener_name, tx);
    }

    fn notify_listeners(&self, value: KeyStoreEvent) -> Result<(), KeyStoreError> {
        let value = Arc::new(value);
        self.notify.iter().for_each(|notifier| {
            if let Err(notification_error) =  notifier.send(value.clone()) {
                if let SendError(_) = notification_error {
                    error!("Error sending notification to {} listener. EX: {:?}",  notifier.key().as_str(), notification_error);
                    self.notify.remove(notifier.key().as_str());
                }
            }
        });
        Ok(())
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
                    store.add_key(j * thread_id, format!("{}-{}", thread_id, j).as_bytes().to_vec());
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
            store.add_key(10, format!("{}-{}", 19, "test").as_bytes().to_vec());
        });

        std::thread::sleep(Duration::from_secs(5));
        while let Ok(a) = rx.recv() {
            println!("{:?}", a);
        }
    }
}