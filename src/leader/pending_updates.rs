use std::ops::{Deref, DerefMut};

use crate::proto::replication::replicate_request::Operation;

#[derive(Debug)]
pub(super) struct PendingUpdates(Vec<Operation>);

impl PendingUpdates {
    pub fn new() -> Self {
        Self(Vec::new())
    }
}

impl Deref for PendingUpdates {
    type Target = Vec<Operation>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PendingUpdates {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
