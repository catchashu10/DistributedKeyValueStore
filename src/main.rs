use tonic::{transport::Server, Request, Response, Status};

use moka::future::Cache;

use datastore::data_store_server::{DataStore, DataStoreServer};
use datastore::{InitRequest, InitResponse, GetRequest, GetResponse, PutRequest, PutResponse, ShutdownResponse};

use sqlite::KeyValueDataStore;

mod datastore;
mod sqlite;

const CACHE_SIZE: usize = 10_000;

pub struct KeyValueServer {
    db: KeyValueDataStore,
    cache: Cache<String, String>,
}

#[tonic::async_trait]
impl DataStore for KeyValueServer {
    async fn init(
        &self,
        _request: Request<InitRequest>
    ) -> Result<Response<InitResponse>, Status> {
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

        // Check the cache if the key exists
        if let Some(cached_value) = self.cache.get(&key.clone()).await {
            return Ok(Response::new(GetResponse { value: cached_value, found: true }));
        }

        match self.db.get(&key) {
            Ok(Some(value)) => {
                // Cache the value
                self.cache.insert(key.clone(), value.clone()).await;
                Ok(Response::new(GetResponse { value, found: true }))
            },
            Ok(None) => Ok(Response::new(GetResponse { value: "".to_string(), found: false })),
            Err(e) => Err(Status::internal(format!("DB error: {}", e))),
        }
    }

    async fn put(
        &self, 
        request: Request<PutRequest>
    ) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value } = request.into_inner();

        match self.db.put(&key, &value) {
            Ok(old_value) => {
                // Cache the value
                self.cache.insert(key.clone(), value.clone()).await;
                Ok(Response::new(PutResponse { value: old_value }))
            },
            Err(e) => Err(Status::internal(format!("DB error: {}", e))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let db = KeyValueDataStore::new("kv_store.db").expect("Failed to create datastore");

    let cache = Cache::builder()
        .max_capacity(CACHE_SIZE as u64)
        .build();

    let store = KeyValueServer { db, cache };
    let server = DataStoreServer::new(store);
    println!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}
