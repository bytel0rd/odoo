use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

pub fn hash_key_to_unsigned_int(key: &[u8]) -> u64 {
    const_xxh3(key)
}