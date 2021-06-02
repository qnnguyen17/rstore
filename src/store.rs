use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Debug, Serialize)]
pub struct Store {
    store: HashMap<String, String>,
}

// TODO: store timestamps in entries to handle out-of-order writes
impl Store {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.store.get(key)
    }

    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.store.remove(key)
    }
}
