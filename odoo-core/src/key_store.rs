use std::fmt::Debug;
use std::ops::AddAssign;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crossbeam::channel;
use crossbeam::channel::SendError;
use dashmap::DashMap;
use log::{debug, error};

use crate::helpers::hash_key_to_unsigned_int;

#[derive(Debug, Clone)]
pub struct StoreItem {
    timestamp: chrono::DateTime<chrono::Utc>,
    value: Vec<u8>,
    expire_at: Option<chrono::DateTime<chrono::Utc>>,
    key: String,
}

#[derive(Debug, Clone)]
pub struct StoreStream {
    timestamp: chrono::DateTime<chrono::Utc>,
    value: Vec<(i64, Vec<u8>)>,
    expire_at: Option<chrono::DateTime<chrono::Utc>>,
    key: String,
}

#[derive(Debug, Clone)]
pub enum StoreValue {
    Item(StoreItem),
    Stream(Arc<RwLock<StoreStream>>),
}

impl StoreValue {
    pub fn get_key(&self) -> String {
        return match self {
            StoreValue::Item(store_value) => {
                store_value.key.clone()
            }
            StoreValue::Stream(stream) => {
                match stream.read() {
                    Ok(stream) => stream.key.clone(),
                    Err(err) => {
                        error!("Error getting lock to read stream key: {:?}", err);
                        "".to_string()
                    }
                }
            }
        };
    }

    pub fn get_hash_key(&self) -> String {
        return match self {
            StoreValue::Item(store_value) => {
                store_value.key.clone()
            }
            StoreValue::Stream(stream) => {
                match stream.read() {
                    Ok(stream) => {
                        if stream.value.is_empty() {
                            stream.key.clone()
                        } else {
                            let (key, _) = stream.value.last().unwrap();
                            format!("{}-{}", stream.key.clone(), key)
                        }
                    }
                    Err(err) => {
                        error!("Error getting lock to read stream hash key: {:?}", err);
                        "".to_string()
                    }
                }
            }
        };
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
    StreamUpdate(Arc<StoreValue>),
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
                key: key.to_string(),
            }
        ));
        let store_key = hash_key_to_unsigned_int(key.as_bytes());
        self.store.insert(store_key, item.clone());
        KeyStore::notify_listeners(self.notify.clone(), KeyStoreEvent::KeyAdded(item.clone()));
        match duration {
            Some(timeout) => {
                KeyStore::schedule_key_clearing(self.store.clone(), self.notify.clone(), item.clone(), timeout);
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
            if let Err(notification_error) = notifier.send(value.clone()) {
                if let SendError(_) = notification_error {
                    error!("Error sending notification to {} listener. EX: {:?}",  notifier.key().as_str(), notification_error);
                    notify.remove(notifier.key().as_str());
                }
            }
        });
    }


    /// duration: Duration to keep the entire stream for post-last stream in memory for
    pub fn append_stream(&self, key: &str, value: Vec<u8>, duration: Option<Duration>) {
        let store_key = hash_key_to_unsigned_int(key.as_bytes());
        let time = chrono::Utc::now();
        let timestamp = time.timestamp();
        let expire_at = duration.map(|d| {
            let mut time = time.clone();
            time = time + d;
            return time;
        });

        {
            let mut stream = Arc::new(StoreValue::Stream(
                Arc::new(RwLock::new(StoreStream {
                    expire_at: expire_at.clone(),
                    timestamp: time.clone(),
                    value: vec![(timestamp, value.clone())],
                    key: key.to_string(),
                }))
            ));
            if let Some(item) = self.store.get(&store_key) {
                let value = item.value().clone();
                match item.value().as_ref() {
                    StoreValue::Stream(_) => {
                        stream = value;
                    }
                    _ => {
                        self.store.insert(store_key, stream.clone());
                    }
                }
            } else {
                self.store.insert(store_key, stream.clone());
            }
            if let StoreValue::Stream(mut stream_lock) = stream.as_ref().clone() {
                match stream_lock.write() {
                    Ok(mut stream) => {
                        stream.value.push((timestamp, value.clone()));
                        stream.timestamp = time.clone();
                        stream.expire_at = expire_at.clone();
                    }
                    Err(_) => {}
                }
            }
        }

        let stream_update = Arc::new(StoreValue::Item(StoreItem {
            timestamp: time,
            value,
            expire_at,
            key: key.to_string(),
        }));

        KeyStore::notify_listeners(self.notify.clone(), KeyStoreEvent::StreamUpdate(stream_update.clone()));
        match duration {
            Some(timeout) => {
                KeyStore::schedule_key_clearing(self.store.clone(), self.notify.clone(), stream_update.clone(), timeout);
            }
            _ => {}
        }
    }

    pub fn resume_stream_for_key(&self, key: &str, last_timestamp: i64, limit: Option<i64>) -> Vec<StoreItem> {
        let mut pending_streams = vec![];
        if let Some(value) = self.store.get(&hash_key_to_unsigned_int(key.as_bytes())) {
            let stored_value = value.value().as_ref();
            if let StoreValue::Stream(item) = stored_value {
                if let Ok(store) = item.read() {
                    for (key, value) in store.value.iter().rev() {
                        if key.clone() > last_timestamp {
                            let stream_update = StoreItem {
                                timestamp: store.timestamp.clone(),
                                value: value.clone(),
                                expire_at: store.expire_at.clone(),
                                key: key.to_string(),
                            };
                            pending_streams.push(stream_update);
                            if limit.map(|v| v == pending_streams.len() as i64).unwrap_or(false) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        let reversed = pending_streams.iter();
        reversed.rev().map(|f| f.to_owned()).collect::<Vec<StoreItem>>()
    }

    fn schedule_key_clearing(store: DashMap<u64, Arc<StoreValue>>,
                             notify: DashMap<String, channel::Sender<Arc<KeyStoreEvent>>>,
                             item: Arc<StoreValue>,
                             timeout: Duration) {
        let store_key = hash_key_to_unsigned_int(item.get_key().as_bytes());
        let idempotency_key = item.clone().get_hash_key();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if let Some(item) = store.get(&store_key) {
                if idempotency_key.eq(item.get_hash_key().as_str()) {
                    if let Some((_, value)) = store.remove(&store_key) {
                        KeyStore::notify_listeners(notify, KeyStoreEvent::KeyDeleted(value));
                        debug!("Deleted key hash_key: {}", idempotency_key);
                    }
                } else {
                    debug!("Clearing scheduled skipped, hash not a match. expected_hash_key: {} hash_key: {}", idempotency_key, item.get_hash_key());
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