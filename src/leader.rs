use std::sync::{Arc, RwLock};

use tonic::{Request, Response, Status};

use crate::proto::replication::leader_service_server::LeaderService;
use crate::proto::replication::{GetLatestSnapshotRequest, GetLatestSnapshotResponse};
use crate::proto::store::get_response::Value;
use crate::proto::store::store_service_server::StoreService;
use crate::proto::store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use crate::store::Store;

#[derive(Clone, Debug, Default)]
pub struct Leader {
    store: Arc<RwLock<Store>>,
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
            .set(key, value);

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
    async fn get_latest_snapshot(
        &self,
        _request: Request<GetLatestSnapshotRequest>,
    ) -> Result<Response<GetLatestSnapshotResponse>, Status> {
        let lock_guard = self
            .store
            .read()
            .expect("failed to acquire read lock on data store");

        let snapshot = serde_json::to_string(&(*lock_guard))
            .map_err(|err| Status::internal(err.to_string()))?;

        drop(lock_guard);

        Ok(Response::new(GetLatestSnapshotResponse {
            serialized: snapshot,
        }))
    }
}
