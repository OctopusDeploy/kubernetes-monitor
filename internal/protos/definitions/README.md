# Protos 
This directory contains the Protobuf definitions for the resource monitoring messages and services used to communicate with [Octopus Server](https://octopus.com/docs).

## Guidelines
- All Protobuf definitions should be placed in this directory.
- Updates to existing definitions should be backwards compatible.
- Follow the [1-1-1 rule](https://protobuf.dev/best-practices/1-1-1/): All proto definitions should have one top-level element and build target per file. 
