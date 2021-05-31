use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::proto::store::get_response::Value;
use crate::proto::store::store_service_server::StoreService;
use crate::proto::store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use crate::store::Store;

#[derive(Debug, Default)]
pub struct LeaderStoreService {
    pub store: Arc<Mutex<Store>>,
}

#[tonic::async_trait]
impl StoreService for LeaderStoreService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;

        let value = self
            .store
            .lock()
            .expect("failed to acquire lock on data store")
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
            .lock()
            .expect("failed to acquire lock on data store")
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
            .lock()
            .expect("failed to acquire lock on data store")
            .delete(&key);

        Ok(Response::new(DeleteResponse {}))
    }
}
