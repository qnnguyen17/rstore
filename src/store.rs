use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize, Debug, Serialize)]
pub struct StoreEntry {
    pub value: String,
    pub sequence_number: u64,
}

#[derive(Default, Deserialize, Debug, Serialize)]
pub struct Store {
    inner: HashMap<String, StoreEntry>,
}

impl Store {
    pub fn get(&self, key: &str) -> Option<&StoreEntry> {
        self.inner.get(key)
    }

    pub fn set(&mut self, key: String, value: String, sequence_number: u64) {
        self.inner
            .entry(key)
            // If the key exists, keep the entry with the greater timestamp
            .and_modify(|entry| {
                if entry.sequence_number < sequence_number {
                    *entry = StoreEntry {
                        value: value.clone(),
                        sequence_number,
                    };
                }
            })
            // If the key doesn't exist, add a new entry
            .or_insert_with(|| StoreEntry {
                value,
                sequence_number,
            });
    }

    pub fn delete(&mut self, key: &str) -> Option<StoreEntry> {
        self.inner.remove(key)
    }
}
