use std::sync::{Arc, RwLock};

use thiserror::Error;
use tonic::transport;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};

use crate::proto::replication::leader_service_client::LeaderServiceClient;
use crate::proto::replication::replica_service_server::ReplicaService;
use crate::proto::replication::replicate_request::operation;
use crate::proto::replication::{RegisterFollowerRequest, ReplicateRequest, ReplicateResponse};
use crate::proto::store::get_response::Value;
use crate::proto::store::store_service_server::StoreService;
use crate::proto::store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use crate::store::Store;

#[derive(Debug, Error)]
pub enum FollowerError {
    #[error("failed to deserialize snapshot")]
    SnapshotDeserialization(#[from] serde_json::Error),

    #[error("failed to connect to leader node")]
    LeaderConnection(#[from] transport::Error),

    #[error("failed RPC call")]
    RpcCall(#[from] Status),
}

#[derive(Clone, Debug)]
pub struct Follower {
    store: Arc<RwLock<Store>>,
}

impl Follower {
    pub async fn initialize(leader_endpoint: Endpoint, port: u32) -> Result<Self, FollowerError> {
        let mut client = LeaderServiceClient::connect(leader_endpoint).await?;

        let snapshot_response = client
            .register_follower(Request::new(RegisterFollowerRequest { host_port: port }))
            .await?;

        let serialized_snapshot = snapshot_response.into_inner().serialized_snapshot;

        let snapshot = serde_json::from_str(&serialized_snapshot)?;

        Ok(Follower {
            store: Arc::new(RwLock::new(snapshot)),
        })
    }
}

#[tonic::async_trait]
impl ReplicaService for Follower {
    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let ReplicateRequest { operations } = request.into_inner();

        let mut store = self
            .store
            .write()
            .expect("failed to acquire write lock on data store");

        for o in operations {
            match o.value {
                Some(operation::Value::StringValue(value)) => {
                    store.set(o.key, value, o.millis_since_leader_init);
                }
                None => {
                    todo!("perform deletion");
                }
            }
        }

        Ok(Response::new(ReplicateResponse {}))
    }
}

#[tonic::async_trait]
impl StoreService for Follower {
    async fn set(&self, _request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        unimplemented!()
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = self
            .store
            .read()
            .expect("failed to acquire read lock")
            .get(&key)
            .map(|entry| Value::StringValue(entry.value.clone()));

        Ok(Response::new(GetResponse { value }))
    }

    async fn delete(
        &self,
        _request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        unimplemented!()
    }
}
