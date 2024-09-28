#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "keyvaluestore.grpc.pb.h"
#include "database_ops.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using keyvaluestore::ClientRequest;
using keyvaluestore::GetRequest;
using keyvaluestore::GetResponse;
using keyvaluestore::InitRequest;
using keyvaluestore::InitResponse;
using keyvaluestore::KeyValueStore;
using keyvaluestore::PutRequest;
using keyvaluestore::PutResponse;
using keyvaluestore::ServerResponse;
using keyvaluestore::ShutdownRequest;
using keyvaluestore::ShutdownResponse;

// Logic and data behind the server's behavior.
class KeyValueStoreServiceImpl final : public KeyValueStore::Service
{
public:
    // Manage bidirectional streaming sessions
    Status manage_session(ServerContext *context, ServerReaderWriter<ServerResponse, ClientRequest> *stream) override
    {
        ClientRequest client_request;

        while (stream->Read(&client_request))
        {
            // Prepare the server response
            ServerResponse server_response;

            // Handle init request
            if (client_request.has_init_request())
            {
                InitRequest init_req = client_request.init_request();
                std::cout << "Received InitRequest for server: " << init_req.server_name() << std::endl;
                InitResponse init_resp;
                initDB(); // Initialize database or other logic
                init_resp.set_success(true);
                *server_response.mutable_init_response() = init_resp;
            }

            // Handle get request
            else if (client_request.has_get_request())
            {
                GetRequest get_req = client_request.get_request();
                std::cout << "Received GetRequest for key: " << get_req.key() << std::endl;
                GetResponse get_resp;
                std::string value = getKeyValue(get_req.key()); // Fetch value from the database
                if (!value.empty())
                {
                    get_resp.set_value(value);
                    get_resp.set_key_found(true);
                }
                else
                {
                    get_resp.set_key_found(false);
                }
                *server_response.mutable_get_response() = get_resp;
            }

            // Handle put request
            else if (client_request.has_put_request())
            {
                PutRequest put_req = client_request.put_request();
                std::cout << "Received PutRequest for key: " << put_req.key() << std::endl;
                PutResponse put_resp;
                std::string old_value = getKeyValue(put_req.key()); // Fetch old value if it exists
                if (!old_value.empty())
                {
                    put_resp.set_old_value(old_value);
                    put_resp.set_key_found(true);
                }
                else
                {
                    put_resp.set_key_found(false);
                }
                setKeyValue(put_req.key(), put_req.value()); // Set new key-value pair in database
                *server_response.mutable_put_response() = put_resp;
            }

            // Handle shutdown request
            else if (client_request.has_shutdown_request())
            {
                std::cout << "Received ShutdownRequest" << std::endl;
                ShutdownResponse shutdown_resp;
                // closeDB(); // Clean up or close the database
                shutdown_resp.set_success(true);
                *server_response.mutable_shutdown_response() = shutdown_resp;
                stream->Write(server_response); // Send shutdown response before closing
                break;                          // Exit the loop after shutdown
            }

            // Write response to the client
            stream->Write(server_response);
        }

        return Status::OK;
    }
};

void RunServer()
{
    std::string server_address("0.0.0.0:50051");
    KeyValueStoreServiceImpl service;

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
