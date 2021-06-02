use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};

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

#[derive(Clone, Debug, Default)]
pub struct Leader {
    store: Arc<RwLock<Store>>,
    followers: Arc<RwLock<HashSet<FollowerClient>>>,
    // TODO: add a writeahead log
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
            .cloned()
            .map(Value::StringValue);

        Ok(Response::new(GetResponse { value }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;

        self.store
            .write()
            .expect("failed to acquire write lock on data store")
            .set(key.clone(), value.clone());

        let followers = (*self
            .followers
            .read()
            .expect("failed to acquire read lock on follower clients"))
        .clone();

        for f in followers {
            let key = key.clone();
            let value = value.clone();
            tokio::spawn(async move {
                // TODO: error handling
                f.replicate_set(key, value).await.unwrap();
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

        self.followers
            .write()
            .expect("failed to acquire write lock on follower clients")
            .insert(FollowerClient {
                address: follower_address,
            });

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
