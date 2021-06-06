use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize, Debug, Serialize)]
pub struct StoreEntry {
    pub value: String,
    pub millis_since_leader_init: u64,
}

#[derive(Default, Deserialize, Debug, Serialize)]
pub struct Store {
    inner: HashMap<String, StoreEntry>,
}

impl Store {
    pub fn get(&self, key: &str) -> Option<&StoreEntry> {
        self.inner.get(key)
    }

    pub fn set(&mut self, key: String, value: String, millis_since_leader_init: u64) {
        self.inner
            .entry(key)
            // If the key exists, keep the entry with the greater timestamp
            .and_modify(|entry| {
                if entry.millis_since_leader_init < millis_since_leader_init {
                    *entry = StoreEntry {
                        value: value.clone(),
                        millis_since_leader_init,
                    };
                }
            })
            // If the key doesn't exist, add a new entry
            .or_insert_with(|| StoreEntry {
                value,
                millis_since_leader_init,
            });
    }

    pub fn delete(&mut self, key: &str) -> Option<StoreEntry> {
        self.inner.remove(key)
    }
}
