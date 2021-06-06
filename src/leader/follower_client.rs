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

use super::pending_updates::PendingUpdates;
use crate::proto::replication::replica_service_client::ReplicaServiceClient;
use crate::proto::replication::replicate_request;
use crate::proto::replication::replicate_request::operation::Value;
use crate::proto::replication::ReplicateRequest;

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
    pending_updates: Arc<RwLock<PendingUpdates>>,
    update_thread: Arc<JoinHandle<()>>,
    update_sender: Arc<SyncSender<()>>,
}

impl FollowerClient {
    pub(super) fn new(address: SocketAddr, runtime: Arc<Runtime>) -> Self {
        let pending_updates = Arc::new(RwLock::new(PendingUpdates::new()));
        let pending_updates_ = pending_updates.clone();

        // We only need a channel buffer with space for one message, because the retry
        // thread attempts to re-send all pending requests on a loop until successful.
        // Therefore all requests that are added to the queue during the retry attempt
        // will be handled either on the next message or during the current retry loop.
        let (sender, receiver) = mpsc::sync_channel(1);

        let update_thread = thread::spawn(move || {
            receiver.into_iter().for_each(|()| {
                loop {
                    let pending_updates_to_send = pending_updates_
                        .read()
                        .expect("failed to acquire read lock on pending updates list")
                        .clone();

                    if pending_updates_to_send.is_empty() {
                        break;
                    }

                    let num_updates = pending_updates_to_send.len();

                    let endpoint = format!("https://{}", address.to_string());

                    match runtime
                        .block_on(Self::attempt_replicate(endpoint, pending_updates_to_send))
                    {
                        Ok(()) => {
                            let mut stored_pending_updates = pending_updates_
                                .write()
                                .expect("failed to acquire write lock on pending updates list");

                            // The leader only appends updates to the end of the `pending_updates`.
                            // Therefore the first N operations are guaranteed to stay the same
                            // until we drop them in this line.
                            stored_pending_updates.drain(0..num_updates);
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
            update_thread: Arc::new(update_thread),
            update_sender: Arc::new(sender),
        }
    }

    pub(super) async fn replicate(&self, key: String, value: Option<String>, sequence_number: u64) {
        self.pending_updates
            .write()
            .expect("failed to acquire write lock on pending updates list")
            .push(replicate_request::Operation {
                key,
                value: value.map(Value::StringValue),
                sequence_number,
            });

        match self.update_sender.try_send(()) {
            Ok(()) => {
                println!("Successfully sent message to update thread");
            }
            Err(TrySendError::Disconnected(_)) => panic!(),
            // Full send buffer means the retry thread is guaranteed to see all the outstanding
            // requests
            Err(TrySendError::Full(_)) => {}
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
