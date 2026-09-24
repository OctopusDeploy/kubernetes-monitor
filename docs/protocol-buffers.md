# Protocol Buffers

Protocol buffers are used for gRPC communications with Octopus server.

The proto files contained in this repository are manually duplicated to the Octopus Server codebase as well.

## Updating generated code

Generated protocol buffer code is committed to the repository

1. Ensure `protoc` is installed on your system - [guide](https://grpc.io/docs/protoc-installation/)
2. Run `make generate-proto` 
