#!/bin/bash

sed -i -e '/gogoproto/d' protos/rpc.proto
sed -i -e 's/etcd\/mvcc\/mvccpb\/kv.proto/kv.proto/g' protos/rpc.proto
sed -i -e 's/etcd\/auth\/authpb\/auth.proto/auth.proto/g' protos/rpc.proto
sed -i -e '/google\/api\/annotations.proto/d' protos/rpc.proto
sed -i -e '/option (google.api.http)/,+3d' protos/rpc.proto

sed -i -e '/gogoproto/d' protos/kv.proto

sed -i -e '/gogoproto/d' protos/auth.proto

python3 -m grpc.tools.protoc -Iprotos --python_out=rpc --grpc_python_out=rpc protos/rpc.proto protos/auth.proto protos/kv.proto


sed -i -e 's/import auth_pb2/from aioetcd3.rpc import auth_pb2/g' rpc/rpc_pb2.py
sed -i -e 's/import kv_pb2/from aioetcd3.rpc import kv_pb2/g' rpc/rpc_pb2.py


