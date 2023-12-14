use dashmap::DashMap;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;
use xxhash_rust::xxh3::xxh3_64;

pub trait EventItem {}

pub enum StoreValue {
    Item(Box<dyn EventItem>),
    Stream,
}

pub struct KeyStore {
    store: DashMap<u64, StoreValue>,
}

impl KeyStore {

    pub fn hash_key(key: &[u8]) -> u64 {
        const_xxh3(key)
    }

    pub


}