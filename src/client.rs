use self::proto::store::store_service_client::StoreServiceClient;
use self::proto::store::{DeleteRequest, GetRequest, SetRequest};

pub mod proto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StoreServiceClient::connect("http://[::1]:50051").await?;
    let set_request = tonic::Request::new(SetRequest {
        key: "foo".into(),
        value: "bar".into(),
    });
    let response = client.set(set_request).await?;
    println!("Response for set: {:?}", response);

    let get_request = tonic::Request::new(GetRequest { key: "foo".into() });
    let response = client.get(get_request).await?;
    println!("Response for get: {:?}", response.into_inner().value);

    let delete_request = tonic::Request::new(DeleteRequest { key: "foo".into() });
    let response = client.delete(delete_request).await?;
    println!("Response for delete: {:?}", response);

    let get_request = tonic::Request::new(GetRequest { key: "foo".into() });
    let response = client.get(get_request).await?;
    println!("Response for get: {:?}", response.into_inner().value);

    Ok(())
}
