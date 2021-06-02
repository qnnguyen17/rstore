use std::net::SocketAddr;

use thiserror::Error;
use tonic::{transport, Request, Status};

use crate::proto::replication::replica_service_client::ReplicaServiceClient;
use crate::proto::replication::SetReplicaRequest;

#[derive(Debug, Error)]
pub(super) enum FollowerClientError {
    #[error("failed to connect to follower node")]
    FollowerConnection(#[from] transport::Error),

    #[error("failed RPC call")]
    RpcCall(#[from] Status),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct FollowerClient {
    pub(super) address: SocketAddr,
    // TODO: keep track of failed / outstanding replicated operations
}

impl FollowerClient {
    pub(super) async fn replicate_set(
        &self,
        key: String,
        value: String,
    ) -> Result<(), FollowerClientError> {
        let endpoint = format!("https://{}", self.address.to_string());

        let mut client = ReplicaServiceClient::connect(endpoint).await?;
        let set_request = SetReplicaRequest { key, value };

        client.set_replica(Request::new(set_request)).await?;

        Ok(())
    }
}
