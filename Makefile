SHELL := /bin/bash

GO ?= go
PROTOC ?= protoc
PROTOC_GEN_GO ?= $(shell $(GO) env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC ?= $(shell $(GO) env GOPATH)/bin/protoc-gen-go-grpc

PROTO_DIR := cluster/cachepb
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
	$(GO) test ./smartcache ./cluster ./distlock/... ./valuestore/... ./providers/... -count=1

test-integration: proto
	$(GO) test ./... -count=1

test-cluster: proto
	$(GO) test ./cluster ./providers/cluster -count=1

test-k8s: proto
	$(GO) test -tags k8s ./cluster -run '^$$'

fmt:
	gofmt -w \
		smartcache/*.go \
		distlock/*.go \
		distlock/memory/*.go \
		valuestore/*.go \
		valuestore/memory/*.go \
		cluster/*.go \
		cluster/cachepb/*.go \
		providers/redis/*.go \
		providers/postgres/*.go \
		providers/sqlite/*.go \
		providers/cluster/*.go \
		examples/v3_local/*.go \
		examples/v3_cluster/*.go

clean: clean-proto

clean-proto:
	rm -f $(PROTO_GEN)
