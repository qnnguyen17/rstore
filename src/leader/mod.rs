use std::collections::HashMap;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::runtime::Runtime;
use tonic::{Request, Response, Status};

use self::follower_client::FollowerClient;
use crate::proto::replication::leader_service_server::LeaderService;
use crate::proto::replication::{RegisterFollowerRequest, RegisterFollowerResponse};
use crate::proto::store::get_response::Value;
use crate::proto::store::store_service_server::StoreService;
use crate::proto::store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use crate::store::Store;

mod follower_client;

#[derive(Clone, Debug)]
pub struct Leader {
    store: Arc<RwLock<Store>>,
    followers: Arc<RwLock<HashMap<SocketAddr, FollowerClient>>>,
    runtime: Arc<Runtime>,
    init_instant: Instant,
}

impl Leader {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to initialize tokio runtime");

        Self {
            store: Default::default(),
            followers: Default::default(),
            runtime: Arc::new(runtime),
            init_instant: Instant::now(),
        }
    }
}

#[tonic::async_trait]
impl StoreService for Leader {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;

        let value = self
            .store
            .read()
            .expect("failed to acquire read lock on data store")
            .get(&key)
            .map(|entry| Value::StringValue(entry.value.clone()));

        Ok(Response::new(GetResponse { value }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;
        let millis_since_leader_init =
            self.init_instant
                .elapsed()
                .as_millis()
                .try_into()
                .map_err(|err| {
                    Status::internal(format!("failed converting leader elapsed time {:#?}", err))
                })?;

        self.store
            .write()
            .expect("failed to acquire write lock on data store")
            .set(key.clone(), value.clone(), millis_since_leader_init);

        let followers = (*self
            .followers
            .read()
            .expect("failed to acquire read lock on follower clients"))
        .clone();

        for (_, f) in followers {
            let key = key.clone();
            let value = value.clone();
            tokio::spawn(async move {
                f.replicate_set(key, value, millis_since_leader_init).await;
            });
        }

        Ok(Response::new(SetResponse {}))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;

        self.store
            .write()
            .expect("failed to acquire write lock on data store")
            .delete(&key);

        Ok(Response::new(DeleteResponse {}))
    }
}

#[tonic::async_trait]
impl LeaderService for Leader {
    async fn register_follower(
        &self,
        request: Request<RegisterFollowerRequest>,
    ) -> Result<Response<RegisterFollowerResponse>, Status> {
        let mut follower_address = request
            .remote_addr()
            .ok_or_else(|| Status::invalid_argument("no remote address in request"))?;

        let follower_port =
            request.into_inner().host_port.try_into().map_err(|err| {
                Status::invalid_argument(format!("invalid host port: {:#?}", err))
            })?;

        follower_address.set_port(follower_port);

        // This operation will drop any previous oustanding requests (i.e. if the follower
        // died and is reinitializing). This is okay, because the snapshot is generated
        // _after_ the follower has been registered, so we don't lose any data.
        self.followers
            .write()
            .expect("failed to acquire write lock on follower clients")
            .insert(
                follower_address,
                FollowerClient::new(follower_address, self.runtime.clone()),
            );

        let lock_guard = self
            .store
            .read()
            .expect("failed to acquire read lock on data store");

        let snapshot = serde_json::to_string(&(*lock_guard))
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(RegisterFollowerResponse {
            serialized_snapshot: snapshot,
        }))
    }
}
