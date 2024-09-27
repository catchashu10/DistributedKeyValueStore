# TODO(jtattermusch): Remove the hack to workaround protobuf bug. See https://github.com/protocolbuffers/protobuf/issues/12439
# Hack: protobuf currently doesn't declare it's absl dependencies when protobuf.pc pkgconfig file is used.
PROTOBUF_ABSL_DEPS = absl_absl_check absl_absl_log absl_algorithm absl_base absl_bind_front absl_bits absl_btree absl_cleanup absl_cord absl_core_headers absl_debugging absl_die_if_null absl_dynamic_annotations absl_flags absl_flat_hash_map absl_flat_hash_set absl_function_ref absl_hash absl_layout absl_log_initialize absl_log_severity absl_memory absl_node_hash_map absl_node_hash_set absl_optional absl_span absl_status absl_statusor absl_strings absl_synchronization absl_time absl_type_traits absl_utility absl_variant
# TODO(jtattermusch): Remove the hack to workaround protobuf/utf8_range bug. See https://github.com/protocolbuffers/utf8_range/issues/20
# Hack: utf8_range (which is protobuf's dependency) currently doesn't have a pkgconfig file, so we need to explicitly
# tweak the list of libraries to link against to fix the build.
PROTOBUF_UTF8_RANGE_LINK_LIBS = -lutf8_validity

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc absl_flags absl_flags_parse`
CXXFLAGS += -std=c++14
ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)`\
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread\
           -lgrpc++_reflection\
           -ldl
else
LDFLAGS += -L/usr/local/lib `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)`\
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
endif
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = ../../protos

vpath %.proto $(PROTOS_PATH)

all: kv_server kv_client

kv_server: kvstore.pb.o kvstore.grpc.pb.o kv_server.o
	$(CXX) $^ $(LDFLAGS) -o $@

kv_client: kvstore.pb.o kvstore.grpc.pb.o kv_client.o
	$(CXX) $^ $(LDFLAGS) -o $@

.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h kv_server kv_client
