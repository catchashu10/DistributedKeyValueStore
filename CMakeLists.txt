cmake_minimum_required(VERSION 3.8)

project(KeyValueStoreServer C CXX)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include and link directories for Abseil
include_directories($HOME/.local/include)
link_directories($HOME/.local/lib)

# Find required packages
find_package(SQLite3 REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(absl REQUIRED)

# Proto file
get_filename_component(project_path "../DistributedKeyValueStore" ABSOLUTE)
get_filename_component(kv_proto "${project_path}/kvstore.proto" ABSOLUTE)

# Generated sources
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_DIR})
include_directories(${GENERATED_DIR})

set(GRPC_GENERATED_SOURCES
    ${GENERATED_DIR}/kvstore.pb.cc
    ${GENERATED_DIR}/kvstore.pb.h
    ${GENERATED_DIR}/kvstore.grpc.pb.cc
    ${GENERATED_DIR}/kvstore.grpc.pb.h
)

# Generate source files
add_custom_command(
    OUTPUT ${GRPC_GENERATED_SOURCES}
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS --grpc_out ${GENERATED_DIR}
         --cpp_out ${GENERATED_DIR}
         -I ${project_path}
         --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
         ${kv_proto}
    DEPENDS ${kv_proto}
    COMMENT "Generating gRPC sources"
)

# Include generated *.pb.h files
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# gRPC and protobuf libraries
set(_REFLECTION gRPC::grpc++_reflection)
set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_GRPC_GRPCPP gRPC::grpc++)

# kv_grpc_proto library
add_library(kv_grpc_proto ${GRPC_GENERATED_SOURCES})
target_link_libraries(kv_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  SQLite::SQLite3
  absl::strings
  absl::synchronization
  absl::time
  absl::check
)

# Target server
add_executable(kv_server "grpc_server.cpp" "database_ops.cpp")
target_link_libraries(kv_server
  kv_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  SQLite::SQLite3
  absl::strings
  absl::synchronization
  absl::time
  absl::check
)
