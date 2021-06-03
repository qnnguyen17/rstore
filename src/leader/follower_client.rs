use std::collections::HashMap;
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
use crate::proto::replication::set_replica_request;
use crate::proto::replication::SetReplicaRequest;

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
    outstanding_requests: Arc<RwLock<HashMap<String, String>>>,
    retry_thread: Arc<JoinHandle<()>>,
    retry_sender: Arc<SyncSender<()>>,
}

impl FollowerClient {
    pub(super) fn new(address: SocketAddr, runtime: Arc<Runtime>) -> Self {
        let outstanding_requests = Arc::new(RwLock::new(HashMap::<String, String>::new()));
        let requests = outstanding_requests.clone();

        // We only need a channel buffer with space for one message, because the retry
        // thread attempts to re-send all pending requests on a loop until successful.
        // Therefore all requests that are added to the queue during the retry attempt
        // will be handled either on the next message or during the current retry loop.
        let (sender, receiver) = mpsc::sync_channel(1);

        let retry_thread = thread::spawn(move || {
            receiver.into_iter().for_each(|()| {
                loop {
                    let requests_read = requests
                        .read()
                        .expect("failed to acquire read lock on outstanding requests map");

                    if requests_read.is_empty() {
                        break;
                    }

                    println!("outstanding reqs: {:#?}", requests_read);
                    let endpoint = format!("https://{}", address.to_string());
                    let records: Vec<_> = requests_read
                        .iter()
                        .map(|(key, value)| set_replica_request::Record {
                            key: key.to_owned(),
                            value: value.to_owned(),
                        })
                        .collect();

                    drop(requests_read);

                    match runtime.block_on(Self::attempt_replica_set(endpoint, records.clone())) {
                        Ok(()) => {
                            println!("Successfully sent outstanding replication requests.");
                            let mut requests_write = requests
                                .write()
                                .expect("failed to acquire write lock on outstanding requests map");
                            for rec in records {
                                // TODO: need timestamps to correctly handle records that were updated
                                // while request was in flight
                                if requests_write.contains_key(&rec.key) {
                                    // TODO: only perform this if the sent record timestamp is >= the stored record timestamp
                                    // If a newer outstanding request was stored while the previous one was in flight, we
                                    // shouldn't delete it
                                    requests_write.remove(&rec.key);
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
            outstanding_requests,
            retry_thread: Arc::new(retry_thread),
            retry_sender: Arc::new(sender),
        }
    }

    pub(super) async fn replicate_set(&self, key: String, value: String) {
        let endpoint = format!("https://{}", self.address.to_string());

        let set_result = Self::attempt_replica_set(
            endpoint,
            vec![set_replica_request::Record {
                key: key.clone(),
                value: value.clone(),
            }],
        )
        .await;

        match set_result {
            Ok(()) => {}
            Err(err) => {
                println!("replica SET operation failed with error: {:#?}", err);
                let mut outstanding = self
                    .outstanding_requests
                    .write()
                    .expect("failed to acquire write lock on outstanding requests map");
                outstanding.insert(key, value);

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

    async fn attempt_replica_set(
        endpoint: String,
        records: Vec<set_replica_request::Record>,
    ) -> Result<(), FollowerClientError> {
        let mut client = ReplicaServiceClient::connect(endpoint).await?;
        let set_request = SetReplicaRequest { records };

        client.set_replica(Request::new(set_request)).await?;

        Ok(())
    }
}
