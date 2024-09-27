#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "database_ops.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::KeyRequest;
using kvstore::KeyValuePair;
using kvstore::SetResponse;
using kvstore::ValueResponse;
using kvstore::KeyValue;

// Logic and data behind the server's behavior.
class KeyValueServiceImpl final : public KeyValue::Service {
    Status Set(ServerContext* context, const KeyValuePair* request, SetResponse* reply) override {
        // Call your existing setKeyValue function
        setKeyValue(request->key(), request->value());
        reply->set_success(true);
        return Status::OK;
    }

    Status Get(ServerContext* context, const KeyRequest* request, ValueResponse* reply) override {
        // Call your existing getKeyValue function
        std::string value = getKeyValue(request->key());
        reply->set_value(value);
        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    KeyValueServiceImpl service;

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

int main(int argc, char** argv) {
    RunServer();

    return 0;
}
