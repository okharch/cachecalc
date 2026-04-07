SHELL := /bin/bash

GO ?= go
PROTOC ?= protoc
PROTOC_GEN_GO ?= $(shell $(GO) env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC ?= $(shell $(GO) env GOPATH)/bin/protoc-gen-go-grpc

PROTO_DIR := internal/cluster/cachepb
PROTO_FILES := $(PROTO_DIR)/cache.proto
PROTO_GEN := $(PROTO_DIR)/cache.pb.go $(PROTO_DIR)/cache_grpc.pb.go

.PHONY: all proto build test test-unit test-integration test-cluster test-k8s fmt clean clean-proto

all: build

$(PROTO_GEN): $(PROTO_FILES)
	PATH="$(dir $(PROTOC_GEN_GO)):$(PATH)" $(PROTOC) \
		--go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		$(PROTO_FILES)

proto: $(PROTO_GEN)

build: proto
	$(GO) build ./...

test: test-unit

test-unit: proto
	$(GO) test ./... -skip 'Test(PostgresCache|ExternalCachePostgres|RemotePostgres|RemoteConcurrentPostgres|RedisExtCache|ExternalCacheRedis|RemoteRedis|RemoteConcurrentRedis)'

test-integration: proto
	$(GO) test ./...

test-cluster: proto
	$(GO) test ./internal/cluster/... -count=1

test-k8s: proto
	$(GO) test -tags k8s ./internal/cluster/... -run '^$$'

fmt:
	gofmt -w \
		cache.go \
		cachedcalculations_external_adapter.go \
		examples/cluster/main.go \
		examples/cluster_calc/main.go \
		external.go \
		internal/cluster/*.go \
		internal/cluster/cachepb/*.go

clean: clean-proto

clean-proto:
	rm -f $(PROTO_GEN)
