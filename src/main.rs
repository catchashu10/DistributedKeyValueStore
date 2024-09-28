use clap::Parser;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};

use keyvaluestore::key_value_store_server::{KeyValueStore, KeyValueStoreServer};
use keyvaluestore::{ClientRequest, ServerResponse};

use keyvaluestore::server_response::Response::{InitResponse, ShutdownResponse, GetResponse, PutResponse};
use keyvaluestore::client_request::Request::{InitRequest, ShutdownRequest, GetRequest, PutRequest};

use sqlite::KeyValueDataStore;

mod keyvaluestore;
mod sqlite;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 50051)]
    port: u32,
}

pub struct KeyValueServer {
    db: Arc<KeyValueDataStore>,
}

#[tonic::async_trait]
#[allow(non_camel_case_types)]
impl KeyValueStore for KeyValueServer {
    type manage_sessionStream = mpsc::Receiver<Result<ServerResponse, Status>>;
    async fn manage_session(
        &self,
        request: Request<tonic::Streaming<ClientRequest>>,
    ) -> Result<Response<Self::manage_sessionStream>, Status> {
        let mut streamer = request.into_inner();
        // creating queue
        let (mut tx, rx) = mpsc::channel(4);
        let db = self.db.clone(); // cloning db to move into the stream

        tokio::spawn(async move {
            let mut session_active = false;
            // listening on request stream
            while let Some(req) = streamer.message().await.unwrap(){
                match req.request.unwrap() {
                    InitRequest(_init_req) => {
                        session_active = true;

                        // Send InitResponse
                        let response = ServerResponse {
                            response: Some(InitResponse(keyvaluestore::InitResponse {
                                success: true,
                            })),
                        };

                        if let Err(e) = tx.send(Ok(response)).await {
                            eprintln!("Error sending init response: {:?}", e);
                        }
                    }

                    GetRequest(get_req) => {
                        if session_active {
                            let value = db.get(&get_req.key).unwrap_or(None);

                            let response = match value {
                                Some(v) => ServerResponse {
                                    response: Some(GetResponse(keyvaluestore::GetResponse {
                                        value: v,
                                        key_found: true,
                                    })),
                                },
                                None => ServerResponse {
                                    response: Some(GetResponse(keyvaluestore::GetResponse {
                                        value: String::new(),
                                        key_found: false,
                                    })),
                                },
                            };

                            if let Err(e) = tx.send(Ok(response)).await {
                                eprintln!("Error sending get response: {:?}", e);
                            }
                        }
                    }

                    PutRequest(put_req) => {
                        if session_active {
                            let old_value = db.put(&put_req.key.clone(), &put_req.value.clone());
                            let key_found = old_value.is_ok();
                            let response = ServerResponse {
                                response: Some(PutResponse(keyvaluestore::PutResponse {
                                    old_value: old_value.unwrap_or(String::new()),
                                    key_found,
                                })),
                            };

                            if let Err(e) = tx.send(Ok(response)).await {
                                eprintln!("Error sending put response: {:?}", e);
                            }
                        }
                    }

                    ShutdownRequest(_) => {
                        session_active = false;

                        let response = ServerResponse {
                            response: Some(ShutdownResponse(keyvaluestore::ShutdownResponse {
                                success: true,
                            })),
                        };

                        if let Err(e) = tx.send(Ok(response)).await {
                            eprintln!("Error sending shutdown response: {:?}", e);
                        }

                        break;
                    }
                }
            }
        });
        // returning stream as receiver
        Ok(Response::new(rx))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("[::]:{}", args.port).parse().unwrap();
    let db = Arc::new(KeyValueDataStore::new("kv_store.db").expect("Failed to create datastore"));

    let store = KeyValueServer { db };
    let server = KeyValueStoreServer::new(store);
    println!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}
