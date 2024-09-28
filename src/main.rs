use clap::Parser;

use tonic::{transport::Server, Request, Response, Status};

use keyvaluestore::key_value_store_server::{KeyValueStore, KeyValueStoreServer};
use keyvaluestore::{InitRequest, InitResponse, GetRequest, GetResponse, PutRequest, PutResponse, ShutdownResponse};

use sqlite::KeyValueDataStore;

mod keyvaluestore;
mod sqlite;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 50051)]
    port: u32,
}

pub struct KeyValueServer {
    db: KeyValueDataStore,
}

#[tonic::async_trait]
impl KeyValueStore for KeyValueServer {
    async fn init(
        &self,
        _request: Request<InitRequest>
    ) -> Result<Response<InitResponse>, Status> {
        println!("Hello world server: ");
        Ok(Response::new(InitResponse {success: true}))
    }

    async fn shutdown(
        &self,
        _request: Request<()>
    ) -> Result<Response<ShutdownResponse>, Status> {
        Ok(Response::new(ShutdownResponse {success: true}))
    }

    async fn get(
        &self, 
        request: Request<GetRequest>
    ) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;

        match self.db.get(&key) {
            Ok(Some(value)) => Ok(Response::new(GetResponse { value, found_key: true })),
            Ok(None) => Ok(Response::new(GetResponse { value: "".to_string(), found_key: false })),
            Err(e) => Err(Status::internal(format!("DB error: {}", e))),
        }
    }

    async fn put(
        &self, 
        request: Request<PutRequest>
    ) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value } = request.into_inner();

        match self.db.put(&key, &value) {
            Ok(old_value) => Ok(Response::new(PutResponse { old_value, found_key: true })),
            Err(e) => Err(Status::internal(format!("DB error: {}", e))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("[::]:{}", args.port).parse().unwrap();
    let db = KeyValueDataStore::new("kv_store.db").expect("Failed to create datastore");

    let store = KeyValueServer { db };
    let server = KeyValueStoreServer::new(store);
    println!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}
