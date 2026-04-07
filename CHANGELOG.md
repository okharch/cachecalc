# Changelog

All notable changes to this project should be documented in this file.

This history is derived from existing git tags plus the current unreleased
`v2.0.0` work on top of `v1.5.0`.

## v2.0.1

### Fixes

- Fixed an atomicity bug in `CachedCalculationsExternalAdapter.SetIfLockOwned`.
  The leader-side adapter now keeps lock ownership validation and value
  publication in the same critical section, so a stale owner cannot publish a
  value after losing the distributed lock.
- Fixed a lock-order inversion in leader-side expired-entry cleanup.
  Adapter cleanup now follows the same lock order as
  `CachedCalculations.RemoveEntries`, preventing a deadlock between expiry
  cleanup and cache maintenance.

### Tests

- Added a regression test for stale publication after lock loss.
- Added a regression test for the adapter cleanup deadlock scenario.

## v2.0.0

### Highlights

- Added a built-in distributed cluster cache in `internal/cluster`.
- Added leader/follower `ExternalCache` implementation over gRPC.
- Added local leader election using TCP bind ownership.
- Added optional Kubernetes leader election using Lease objects behind `-tags k8s`.
- Added `cluster.NewClusteredCachedCalculations(...)` as the preferred wiring for
  distributed smart calculations without Redis/PostgreSQL/SQLite.
- Added `CachedCalculationsExternalAdapter`, allowing the leader to expose its
  live local `CachedCalculations` entries as shared L2 cache.
- Improved leader re-election behavior:
  - a promoted leader can immediately reuse its own warm local L1 entries as L2
  - leader value storage no longer needs to be duplicated when using the new
    clustered helper
- Added `examples/cluster_calc` demo for multi-instance behavior and failover.
- Added `Makefile` targets for protobuf generation, build, and test workflows.

### Other Changes

- PostgreSQL tests now skip cleanly when PostgreSQL is unavailable.
- SQLite cache initialization was hardened for concurrent test access:
  - single DB connection per handle
  - WAL mode
  - busy timeout
  - normal synchronous mode
- Cluster and top-level documentation were expanded.

### Migration Notes

- Existing Redis/PostgreSQL/SQLite `ExternalCache` backends remain supported.
- If you already construct `CachedCalculations` with one of those providers,
  that usage does not need to change.
- If you want to replace an external provider with the built-in cluster cache,
  prefer:

```go
cfg, err := cluster.ConfigFromEnv()
if err != nil {
    panic(err)
}

cc, dist, err := cluster.NewClusteredCachedCalculations(ctx, 4, cfg)
if err != nil {
    panic(err)
}
defer cc.Close()
defer dist.Close()
```

- For local multi-process deployments:
  - use `CLUSTER_MODE=local`
  - use `LEADER_LOCK_PORT` and `GRPC_PORT`
- For Kubernetes deployments:
  - build with `-tags k8s`
  - use `CLUSTER_MODE=k8s`
  - rely on Lease-based election
- Cluster mode is a simpler operational option when you do not want to depend on
  Redis/PostgreSQL/SQLite for distributed coordination.
- Cluster mode is not replicated durable storage:
  - leader lock state is not preserved across leader death
  - only values already warm in the promoted instance's local L1 survive into
    the new shared L2 view

## v1.5.0

Derived from tags `v1.5.0` and `1.5.0`.

- Fixed data race conditions.

## v1.4.2

- Improved testing by using randomized keys to avoid clashes in the same
  external cache environment.

## v1.4.1

- Implementation and enhancement of a caching system.

## v1.4.0

- Generalized cache key type in cached calculations.

## v1.3.0

- Added public `DeserializeValue`.
- Added tests for deserializing entry values.

## v1.2.1

- Added `CachedCalculations.RemoveEntries(filter)`.

## v1.1.2

- Fixed test synchronization by making the mutex global.

## v1.1.1

- Added `GetCachedCalcOptX`.
- Added `GetCachedCalcOpt`.
- Enabled calculations to return `MaxTTL` and `MinTTL` dynamically.

## v1.0.1

- Improved README formatting and code highlighting.
