# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`github.com/okharch/cachecalc/v4` — a Go library for cache-with-calculation: deduplicates concurrent computations for the same key, caches results with two-tier TTL (fresh vs usable/stale-while-revalidate), and optionally coordinates across instances via distributed locks and shared value stores.

## Build & Test

```bash
make build          # generates protobuf, then builds all packages
make test           # alias for test-unit (no external services needed)
make test-unit      # runs smartcache, cluster, distlock/..., valuestore/..., providers/...
make test-integration  # runs all tests including integration
make proto          # regenerates gRPC protobuf files (cluster/cachepb/)
make fmt            # gofmt all source files
```

Run a single test:
```bash
go test ./smartcache -run TestName -count=1
```

Protobuf `.pb.go` files are gitignored and must be regenerated after editing `cluster/cachepb/cache.proto` (`make proto`). Requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`.

## Architecture

Three core interfaces that backends implement:

- **`valuestore.Store`** — `Get`/`Put`/`Delete` for `EntrySnapshot` (serialized cached values with TTL metadata). Backends: `memory`, `redis`, `postgres`, `sqlite`, `mongo`.
- **`distlock.Backend`** — `TryAcquire`/`Renew`/`Release` with fencing tokens. Wrapped by `distlock.NewProvider()` which adds auto-renewal and loss detection via `Lease`. Backends: `memory`, `redis`, `postgres`, `sqlite`, `mongo`.
- **`distlock.Provider`** — higher-level interface returning auto-renewing `Lease` objects. Created from a `Backend` via `distlock.NewProvider()`.

**`smartcache.Cache`** is the main coordination layer. It combines local singleflight, TTL-based freshness (`EntrySnapshot.Fresh` / `Usable`), optional shared value store, and optional distributed locks. Entry point is `smartcache.Get[T]()` or `smartcache.GetWithTTL[T]()` — generic functions that take a `*Cache`, key, and calculator function.

**`cluster.Service`** provides multi-instance coordination: leader election (local or K8s), gRPC transport for proxying lock and value-store operations to the leader. Configuration via `cluster.Config` or env vars (`CLUSTER_MODE`, `GRPC_PORT`, `LEADER_ADDR`, etc.).

**Provider backends** (`providers/redis`, `providers/postgres`, `providers/sqlite`, `providers/mongo`) each implement both `valuestore.Store` and `distlock.Backend`. The Redis backend uses Lua scripts for atomic lock operations.

**`internal/contracttest`** contains shared conformance test suites (`RunSmartcacheContract`, `RunValueStoreContract`, `RunDistlockContract`) that validate any backend implementation against the expected behavior.

## Key Design Details

- Values are serialized with `encoding/gob` — cached types must be gob-encodable.
- `EntrySnapshot` has two TTL boundaries: `RefreshAt` (triggers background recalculation while serving stale) and `ExpireAt` (hard expiry, blocks until fresh value).
- `MinTTL` is auto-adjusted to at least `2 × CalcTime` to prevent refresh storms.
- `Config.MaxWorkers` limits local concurrency; `Config.GlobalMaxWorkers` limits cluster-wide concurrent calculations via distributed lock slots.
- `PublishMode` controls whether shared-store publication is best-effort or required for the calculation to succeed.
- `Cache.SetShared()` allows hot-swapping lock/value providers (used during leader promotion/demotion).
