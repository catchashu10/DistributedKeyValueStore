CXX = g++
CXXFLAGS = -std=c++17 -I/usr/local/include -pthread -I.
LDFLAGS = -I/usr/local/include -lgrpc++ -lgrpc -lgrpc++_reflection -lprotobuf -lpthread -ldl -lsqlite3

all: grpc_server

grpc_server: grpc_server.o kvstore.pb.o kvstore.grpc.pb.o database_ops.o sqlite3.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

sqlite3.o: sqlite3.c
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f *.o grpc_server

.PHONY: all clean
