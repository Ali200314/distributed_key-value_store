#!/bin/bash

go run ./cmd/node/main.go --id 1 --grpc-port 8081 --http-port 8071 --latency-threshold 0 &
go run ./cmd/node/main.go --id 2 --grpc-port 8082 --http-port 8072 --latency-threshold 0 &
go run ./cmd/node/main.go --id 3 --grpc-port 8083 --http-port 8073 --latency-threshold 0 &

wait
