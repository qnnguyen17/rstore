use tonic::transport::Server;

use crate::leader::LeaderStoreService;

use proto::store::store_service_server::StoreServiceServer;

mod leader;
mod proto;
mod store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let store_service = LeaderStoreService::default();

    Server::builder()
        .add_service(StoreServiceServer::new(store_service))
        .serve(addr)
        .await?;

    Ok(())
}
