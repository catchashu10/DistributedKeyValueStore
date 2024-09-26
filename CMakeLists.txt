cmake_minimum_required(VERSION 3.8)

project(KeyValueStoreServer C CXX)

# Proto file
get_filename_component(project_path "../ExampleSqlite" ABSOLUTE)
get_filename_component(kv_proto_path "${project_path}/kvstore.proto" PATH)

# Generated sources
set(kv_proto_srcs "${project_path}/kvstore.pb.cc")
set(kv_proto_hdrs "${project_path}/kvstore.pb.h")
set(kv_grpc_srcs "${project_path}/kvstore.grpc.pb.cc")
set(kv_grpc_hdrs "${project_path}/kvstore.grpc.pb.h")


# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# kv_grpc_proto library
add_library(kv_grpc_proto
  ${kv_grpc_srcs}
  ${kv_grpc_hdrs}
  ${kv_proto_srcs}
  ${kv_proto_hdrs})
target_link_libraries(kv_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# SQLite3 library
add_library(sqlite3 "sqlite3.c")

# Target server
add_executable(kv_server "grpc_server.cpp" "database_ops.cpp")
target_link_libraries(kv_server
  kv_grpc_proto
  sqlite3
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
