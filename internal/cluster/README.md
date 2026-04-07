# Cluster Package

This package implements a distributed `cachecalc.ExternalCache` adapter.

The intent is to let multiple application instances share one authoritative
in-memory cache without changing the existing `ExternalCache` interface.

## Build Notes

The protobuf-generated gRPC files in `internal/cluster/cachepb/` are generated
at build/test time and are not intended to be committed.

Useful targets from the repository root:

1. `make proto`
2. `make build`
3. `make test`
4. `make test-cluster`
5. `make test-k8s`

## Review First

If you want the shortest review path, start here:

1. `distributed_external_cache.go`
2. `clustered_cached_calculations.go`
2. `local_elector.go`
3. `grpc_client.go`
4. `grpc_server.go`
5. `cachepb/cache.proto`
6. `memory_external_cache.go`

## What Was Implemented

`DistributedExternalCache` is the main entry point.

- When this node is the leader, calls go directly to the local in-memory
  `ExternalCache`.
- When this node is a follower, calls are proxied to the leader over gRPC.
- The switch between leader and follower is driven by a `LeaderElector`
  implementation.

`NewClusteredCachedCalculations` is the higher-level helper for the intended
smart-calculation topology.

- It creates a `CachedCalculations` instance and a `DistributedExternalCache`
  together.
- On the leader, remote L2 serving is backed directly by that instance's live
  `CachedCalculations` state via `cachecalc.CachedCalculationsExternalAdapter`.
- This avoids duplicating leader value storage and lets a promoted instance
  expose its already-warm local cache entries to followers.

## Package Layout

### `distributed_external_cache.go`

Main coordinator for role-based routing.

- Holds:
  - `local` authoritative cache
  - `client` follower-side gRPC proxy
  - `elector`
  - leader gRPC server
- Uses `atomic.Bool` plus a mutex to switch roles safely.
- Starts the elector and reacts to:
  - `onStartLeading`: starts gRPC server and routes traffic locally
  - `onStopLeading`: stops gRPC server and routes traffic remotely

### `clustered_cached_calculations.go`

Convenience constructor for the refactored design.

- Creates `CachedCalculations`
- Wraps it in a leader-side `ExternalCache` adapter
- Starts `DistributedExternalCache`
- Injects the distributed cache back into `CachedCalculations` as its external
  coordination backend

This is the path that makes leader L2 serving use the leader's real local smart
cache state rather than a separate copy.

### `elector.go`

Defines the abstraction:

```go
type LeaderElector interface {
	Start(ctx context.Context, onStartLeading func(), onStopLeading func()) error
	IsLeader() bool
	LeaderAddress() string
}
```

### `local_elector.go`

Default local-mode leader election.

- Tries to `Listen` on `LEADER_LOCK_PORT`
- If bind succeeds:
  - this instance becomes leader
- If bind fails:
  - this instance stays follower
  - retries election periodically
- Leader identity is the configured gRPC address

This is intentionally simple and best-effort. It is not consensus.

### `k8s_elector.go`

Optional Kubernetes implementation, enabled with `-tags k8s`.

- Uses `client-go` leader election with `LeaseLock`
- Publishes the leader gRPC address as the leader identity

Without the build tag, `k8s_elector_stub.go` returns a clear error instead.

### `grpc_server.go`

Leader-side transport.

- Exposes the `ExternalCache` methods over gRPC
- Registers `CacheService`
- Uses the local authoritative cache implementation underneath
- In the clustered helper wiring, this means followers read the leader's live
  `CachedCalculations` state through the adapter

### `grpc_client.go`

Follower-side transport.

- Implements `ExternalCache`
- Dials the current leader address from the elector
- Forwards all methods over gRPC
- Invalidates local L2 entries on writes
- Optionally keeps a small follower-side L2 cache for `Get`

### `memory_external_cache.go`

Fallback leader-side authoritative in-memory cache implementation.

- Implements the existing `ExternalCache` interface directly
- Tracks TTL in memory
- Also exposes remaining TTL internally so the gRPC `Get` response can bound
  follower L2 cache lifetime

This type is still used when `DistributedExternalCache` is constructed directly
without the clustered helper. It is no longer the preferred path for
`CachedCalculations`-based clustering.

### `cachecalc.CachedCalculationsExternalAdapter`

Leader-side adapter implemented in the root package.

- Exposes a leader's live `CachedCalculations` entries as remote L2 state
- Keeps distributed lock ownership in a separate lightweight map
- Avoids duplicating leader value storage in a second cache structure

### `cachepb/cache.proto`

Defines the gRPC API matching `ExternalCache` semantics:

- `Set`
- `SetNX`
- `Get`
- `ExtendIfValue`
- `DelIfValue`
- `SetIfLockOwned`
- `Del`
- `Health`

## Request Flow

### Leader path

Direct-construction fallback:

`DistributedExternalCache` -> local `MemoryExternalCache`

Preferred clustered path:

`DistributedExternalCache` -> `CachedCalculationsExternalAdapter` -> live leader `CachedCalculations` entries

### Follower path

`DistributedExternalCache` -> `grpcExternalCacheClient` -> leader gRPC server -> leader local cache

## Failover Behavior

In local mode:

- exactly one instance should hold the TCP lock port
- that instance starts the gRPC server and becomes leader
- when it exits or loses the lock listener, followers retry election
- one follower should eventually bind the lock port and become the new leader

The tests cover:

- 3 instances, one leader
- follower write/read through leader
- leader shutdown and follower promotion

## Important Limitation

Failover does **not** preserve the old leader process itself or its dedicated
lock map, but the new design does improve warm promotion behavior.

When leadership moves, the promoted instance can immediately expose its already
warm local `CachedCalculations` entries as L2 to other nodes. This is a major
improvement over the earlier design where the new leader's remote cache started
empty.

However:

- only values already present in the promoted instance's local cache survive
- local caches are still not replicated to every node
- lock ownership state is not replicated across leadership changes
- this is still a simple design, not replicated consensus storage

That means:

- API compatibility is preserved
- leader/follower routing works
- failover works
- promoted leaders can reuse their own warm local values as L2
- cache contents are still not fully replicated across all leaders

## Configuration

Environment-driven config is parsed in `config.go`.

- `CLUSTER_MODE=local|k8s`
- `GRPC_PORT=50051`
- `LEADER_LOCK_PORT=9000`
- `LEADER_ADDR=...`

Defaults are aimed at local multi-process testing.

## Suggested Review Questions

- Is the local elector behavior acceptable for the intended deployment model?
- Is the follower reconnection behavior sufficient on leader loss?
- Is the lack of cache-state replication acceptable for the intended use case?
- Should the example be promoted into top-level documentation later?
