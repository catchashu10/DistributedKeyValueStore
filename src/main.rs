use tonic::{transport::Server, Request, Response, Status};

use datastore::data_store_server::{DataStore, DataStoreServer};
use datastore::{GetRequest, GetResponse};

mod datastore;

#[derive(Default)]
pub struct KeyValueServer {}

#[tonic::async_trait]
impl DataStore for KeyValueServer {
    async fn get(
        &self, 
        request: Request<GetRequest>
    ) -> Result<Response<GetResponse>, Status> {
        println!("Got a request from: {:?}", request.remote_addr());
        println!("Request: {:?}", request);
        Ok(Response::new(GetResponse {
            value: format!("Hello, {}!", request.into_inner().key),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let store = KeyValueServer::default();
    let server = DataStoreServer::new(store);
    println!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}
