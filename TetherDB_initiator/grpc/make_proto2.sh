protoc -I . --cpp_out=. --experimental_allow_proto3_optional compaction_data.proto
protoc -I . --grpc_out=. --experimental_allow_proto3_optional --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` compaction_data.proto
protoc -I . --cpp_out=. --experimental_allow_proto3_optional prefetch.proto
protoc -I . --grpc_out=. --experimental_allow_proto3_optional --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` prefetch.proto
g++ -std=c++11 -isystem /home/lemma/.local/include -c -o compaction_data.pb.cc.o compaction_data.pb.cc
g++ -std=c++11 -isystem /home/lemma/.local/include -c -o compaction_data.grpc.pb.cc.o compaction_data.grpc.pb.cc
g++ -std=c++11 -isystem /home/lemma/.local/include -c -o prefetch.pb.cc.o prefetch.pb.cc
g++ -std=c++11 -isystem /home/lemma/.local/include -c -o prefetch.grpc.pb.cc.o prefetch.grpc.pb.cc
