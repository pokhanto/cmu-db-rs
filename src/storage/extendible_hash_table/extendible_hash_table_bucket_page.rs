use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    sync::{Arc, RwLockReadGuard, RwLockWriteGuard},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::page::Page;

#[derive(Serialize, Clone, Deserialize, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct ExtendibleHTableBucketPage<K, V>
where
    K: Clone + Hash + Eq + Debug,
    V: Clone + Debug,
{
    max_size: usize,
    data: HashMap<K, V>,
}

impl<K, V> ExtendibleHTableBucketPage<K, V>
where
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned,
    V: Clone + Debug + Serialize + DeserializeOwned,
{
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            data: HashMap::default(),
        }
    }

    pub fn lookup(&self, key: K, value: V) -> bool {
        false
    }

    // TODO: to result
    pub fn insert(&mut self, key: K, value: V) -> bool {
        self.data.insert(key, value);
        true
    }

    pub fn get(&self, key: K) -> Option<&V> {
        self.data.get(&key)
    }

    pub fn delete(&mut self, key: K) -> Option<V> {
        self.data.remove(&key)
    }

    pub fn get_entries(&mut self) -> Vec<(K, V)> {
        self.data.drain().collect::<Vec<(K, V)>>()
    }

    pub fn is_full(&self) -> bool {
        self.data.len() == self.max_size
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn get_max_size(&self) -> usize {
        self.max_size
    }

    pub fn get_size(&self) -> usize {
        self.data.len()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl<K, V> From<&RwLockWriteGuard<'_, Page>> for ExtendibleHTableBucketPage<K, V>
where
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned,
    V: Clone + Debug + Serialize + DeserializeOwned,
{
    fn from(page: &RwLockWriteGuard<'_, Page>) -> Self {
        let data = page.get_data();
        bincode::deserialize(data).unwrap()
    }
}

impl<K, V> From<&RwLockReadGuard<'_, Page>> for ExtendibleHTableBucketPage<K, V>
where
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned,
    V: Clone + Debug + Serialize + DeserializeOwned,
{
    fn from(page: &RwLockReadGuard<'_, Page>) -> Self {
        let data = page.get_data();
        bincode::deserialize(data).unwrap()
    }
}
