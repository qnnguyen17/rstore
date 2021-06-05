use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TrySendError;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::{transport, Request, Status};

use crate::proto::replication::replica_service_client::ReplicaServiceClient;
use crate::proto::replication::replicate_request;
use crate::proto::replication::replicate_request::operation::Value;
use crate::proto::replication::ReplicateRequest;
use crate::store::Store;

#[derive(Debug, Error)]
pub(super) enum FollowerClientError {
    #[error("failed to connect to follower node")]
    FollowerConnection(#[from] transport::Error),

    #[error("failed RPC call")]
    RpcCall(#[from] Status),
}

#[derive(Clone, Debug)]
pub(super) struct FollowerClient {
    pub(super) address: SocketAddr,
    pending_updates: Arc<RwLock<Store>>,
    retry_thread: Arc<JoinHandle<()>>,
    retry_sender: Arc<SyncSender<()>>,
}

impl FollowerClient {
    pub(super) fn new(address: SocketAddr, runtime: Arc<Runtime>) -> Self {
        let pending_updates = Arc::new(RwLock::new(Store::default()));
        let pending_updates_ = pending_updates.clone();

        // We only need a channel buffer with space for one message, because the retry
        // thread attempts to re-send all pending requests on a loop until successful.
        // Therefore all requests that are added to the queue during the retry attempt
        // will be handled either on the next message or during the current retry loop.
        let (sender, receiver) = mpsc::sync_channel(1);

        let retry_thread = thread::spawn(move || {
            receiver.into_iter().for_each(|()| {
                loop {
                    let pending_updates_read = pending_updates_
                        .read()
                        .expect("failed to acquire read lock on pending updates map");

                    if pending_updates_read.is_empty() {
                        break;
                    }

                    println!("pending updates: {:#?}", pending_updates_read);
                    let endpoint = format!("https://{}", address.to_string());
                    let pending_updates_to_send: Vec<_> = pending_updates_read
                        .iter()
                        .map(|(key, entry)| replicate_request::Operation {
                            key: key.to_owned(),
                            value: Some(Value::StringValue(entry.value.clone())),
                            millis_since_leader_init: entry.millis_since_leader_init,
                        })
                        .collect();

                    drop(pending_updates_read);

                    match runtime.block_on(Self::attempt_replicate(
                        endpoint,
                        pending_updates_to_send.clone(),
                    )) {
                        Ok(()) => {
                            println!("Successfully sent pending replication requests.");
                            let mut stored_pending_updates = pending_updates_
                                .write()
                                .expect("failed to acquire write lock on pending updates map");
                            for sent_update in pending_updates_to_send {
                                // Only delete the stored pending update if the stored record timestamp is <= the sent timestamp
                                // If a newer pending update was stored while the previous one was in flight, we shouldn't delete it
                                let stored_update_is_newer = stored_pending_updates
                                    .get(&sent_update.key)
                                    .map(|stored_entry| {
                                        stored_entry.millis_since_leader_init
                                            > sent_update.millis_since_leader_init
                                    })
                                    .unwrap_or_default();

                                if !stored_update_is_newer {
                                    stored_pending_updates.remove(&sent_update.key);
                                }
                            }
                        }
                        Err(err) => {
                            println!("error while retrying replication requests: {:#?}", err);
                        }
                    }

                    thread::sleep(Duration::from_secs(3));
                }
            });
        });

        Self {
            address,
            pending_updates,
            retry_thread: Arc::new(retry_thread),
            retry_sender: Arc::new(sender),
        }
    }

    pub(super) async fn replicate(
        &self,
        key: String,
        value: String,
        millis_since_leader_init: u64,
    ) {
        let endpoint = format!("https://{}", self.address.to_string());

        // TODO: append request to pending list
        // TODO: signal the replication thread

        let replicate_result = Self::attempt_replicate(
            endpoint,
            vec![replicate_request::Operation {
                key: key.clone(),
                value: Some(Value::StringValue(value.clone())),
                millis_since_leader_init,
            }],
        )
        .await;

        match replicate_result {
            Ok(()) => {}
            Err(err) => {
                println!("replica SET operation failed with error: {:#?}", err);
                let mut pending_updates = self
                    .pending_updates
                    .write()
                    .expect("failed to acquire write lock on pending updates map");

                pending_updates.set(key, value, millis_since_leader_init);

                match self.retry_sender.try_send(()) {
                    Ok(()) => {
                        println!("Successfully sent message to retry channel");
                    }
                    Err(TrySendError::Disconnected(_)) => panic!(),
                    // Full send buffer means the retry thread is guaranteed to see all the outstanding
                    // requests
                    Err(TrySendError::Full(_)) => {}
                }
            }
        }
    }

    async fn attempt_replicate(
        endpoint: String,
        operations: Vec<replicate_request::Operation>,
    ) -> Result<(), FollowerClientError> {
        let mut client = ReplicaServiceClient::connect(endpoint).await?;
        let set_request = ReplicateRequest { operations };

        client.replicate(Request::new(set_request)).await?;

        Ok(())
    }
}
