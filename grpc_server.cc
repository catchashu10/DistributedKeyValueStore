#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"
#include "database_ops.h" // Assuming this file has the DB logic for get and put

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;
using keyvaluestore::KeyValueStore;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::ShutdownResponse;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;

// Logic and data behind the server's behavior.
class KeyValueStoreServiceImpl final : public KeyValueStore::Service {
public:
    // Initialize the server
    Status init(ServerContext* context, const InitRequest* request, InitResponse* reply) override {
        std::string server_name = request->server_name();
        // Assuming some initialization logic here
        std::cout << "Initializing server: " << server_name << std::endl;
        initDB(); // Initialize your database or other necessary components
        reply->set_success(true); // Set success to true after initialization
        return Status::OK;
    }

    // Shut down the server
    Status shutdown(ServerContext* context, const Empty* request, ShutdownResponse* reply) override {
        // Assuming some shutdown logic here
        std::cout << "Shutting down the server." << std::endl;
        // closeDB(); // Shutdown/close your database
        reply->set_success(true); // Set success to true after shutdown
        return Status::OK;
    }

    // Handle Get requests
    Status get(ServerContext* context, const GetRequest* request, GetResponse* reply) override {
        std::string key = request->key();
        std::string value = getKeyValue(key); // getKeyValue from your database logic
        if (!value.empty()) {
            reply->set_value(value);
            reply->set_key_found(true);
            std::cout << "Get key: " << key << ", value: " << value << std::endl;
        } else {
            std::cout << "Get key: " << key << " Not found!" << std::endl;
            reply->set_key_found(false);
        }
        return Status::OK;
    }

    // Handle Put requests
    Status put(ServerContext* context, const PutRequest* request, PutResponse* reply) override {
        std::string key = request->key();
        std::string value = request->value();
        std::string old_value = getKeyValue(key); // get old value first
        if (!old_value.empty()) {
            reply->set_old_value(old_value);
            reply->set_key_found(true);
            std::cout << "Put key: " << key << ", value: " << value <<", old_value: " << old_value << std::endl;
        } else {
            std::cout << "Get key: " << key << " Not found!" << std::endl;
            reply->set_key_found(false);
        }
        setKeyValue(key, value); // set the new key-value pair in your database
        return Status::OK;
    }
};


void RunServer()
{
    initDB();
    std::string server_address("0.0.0.0:50051");
    KeyValueStoreServiceImpl  service;

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case, it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally, assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown.
    server->Wait();
}

int main(int argc, char **argv)
{
    RunServer();

    return 0;
}
