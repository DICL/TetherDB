protoc -I . --cpp_out=. --experimental_allow_proto3_optional compaction_data.proto
protoc -I . --grpc_out=. --experimental_allow_proto3_optional --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` compaction_data.proto
