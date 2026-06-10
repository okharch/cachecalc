# Changelog

All notable changes to this project are documented here.

## v4.4.0

### Features

- **Cache warm-up on leader re-election.** When a cluster leader dies and a new
  leader is elected, every surviving follower streams its local cache entries to
  the new leader via a bidirectional gRPC `WarmUp` RPC. The leader accepts
  entries only when they are newer (by `CreatedAt`) than what it already has,
  preventing stale overwrites. This dramatically reduces cold-start
  recomputations after failover.
  - New protobuf messages: `WarmUpEntry`, `WarmUpSummary`
    (`cluster/cachepb/cache.proto`).
  - New server-side handler: `rpcServer.WarmUp` (`cluster/grpc_server.go`).
  - New client-side streaming: `remoteValueStore.doWarmUp` with automatic
    re-streaming on gRPC reconnect (`cluster/grpc_client.go`).
  - New `Cache.RangeLocal()` method to iterate usable local entries
    (`smartcache/cache.go`).
  - New `Service.SetWarmUpSource()` to wire the local cache iteration into the
    cluster service (`cluster/service.go`).
  - `providers/cluster.Bind()` now calls `SetWarmUpSource` automatically.

- **Leader skips self-warm-up.** A newly elected leader no longer sends a
  warm-up stream to itself. Previously this produced a harmless but noisy
  rejected-entry log line on every election.

- **Configurable gRPC reconnect backoff.** Added `Config.ReconnectBaseDelay`
  (default 1 s) to control the base delay for gRPC client reconnection backoff.
  Both the value-store and lock-backend clients respect this setting.

- **Node-labeled cluster logging.** Added `Config.Name` field. When set, the
  cluster logger prefix becomes `cluster[<name>]:` instead of `cluster:`,
  making multi-node test and production logs easy to distinguish. Log lines
  also include source file and line number (`log.Lshortfile`) for IDE
  navigation.

### Bug Fixes

- Fixed warm-up initialization order: the service context and warm-up source
  are now wired before the elector starts, preventing a race where an early
  leader promotion could miss the warm-up callback.
- Fixed gRPC client address tracking: `ensureClient` now detects leader address
  changes and reconnects instead of reusing a stale connection to the old
  leader.

### Tests

- Added `TestWarmUpAllFollowers` — a 4-node cluster test that verifies all
  surviving followers push their entries to the new leader after failover.
- Updated existing cluster tests to use named node configs for clearer log
  output.

## v4.3.0

### Behavior Change

- Background refresh errors no longer replace the cached value with an error
  snapshot. Previously, when a calculator function returned an error during
  stale-while-revalidate background refresh, the error was cached with full
  TTL, causing all subsequent readers to receive the error until it expired.
  Now the stale-but-usable value is preserved, and the next request triggers
  another background refresh attempt. Once `ExpireAt` is reached, the entry
  expires naturally and a foreground calculation runs — if that also fails,
  the caller receives the error directly.

### Features

- Added `Config.OnRefreshError` callback for observability. Called with the
  cache key and error whenever a background refresh calculation fails and the
  stale value is preserved. Use it to wire up metrics, logging, or alerting
  without changing the `Get` return type.

### Tests

- Added `TestBackgroundRefreshErrorPreservesStaleValue` covering the
  stale-preservation behavior and `OnRefreshError` callback invocation.

## v4.2.0

### Features

- Added MongoDB backend (`providers/mongo`) implementing both
  `valuestore.Store` and `distlock.Backend`.
- The backend uses MongoDB TTL indexes for automatic expiry cleanup of
  both cached values and distributed locks.

### Tests

- Added contract test coverage for MongoDB:
  - `valuestore.Store`
  - `distlock.Backend`
  - composed `smartcache` (cross-instance deduplication, stale refresh)

## v4.1.0

### Features

- Added `smartcache.Config.GlobalMaxWorkers` to limit concurrent calculations
  across caches that share the same distributed lock provider.
- Kept the public `smartcache.Get(...)` and `smartcache.GetWithTTL(...)`
  interfaces unchanged.

### Semantics

- `GlobalMaxWorkers > 0` enforces a cluster-wide calculation budget.
- `GlobalMaxWorkers <= 0` disables the global limit.
- The global limit is applied after per-key ownership is decided, so:
  - distributed locking still selects who computes a given key
  - the global worker budget limits how many winning calculations run at once

### Tests

- Added regression coverage for:
  - cluster-wide throttling across different keys
  - disabled global throttling when `GlobalMaxWorkers` is non-positive

## v4.0.0

### Breaking Changes

- Changed the Go module path to `github.com/okharch/cachecalc/v4`.
- Removed the `limitWorkers` parameter from `smartcache.Get(...)` and
  `smartcache.GetWithTTL(...)`.
- Moved worker limiting to `smartcache.Config.MaxWorkers` only:
  - `MaxWorkers > 0` limits concurrent calculations
  - `MaxWorkers <= 0` disables worker limiting

### Why

- Worker-pool behavior is now a cache-level policy instead of a per-call
  choice.
- Call sites no longer need to thread the same boolean through every request.
- Refresh and foreground calculations now share one consistent concurrency
  model.

### Migration Notes

- Update imports from `github.com/okharch/cachecalc/...` to
  `github.com/okharch/cachecalc/v4/...`.
- Remove the extra boolean argument from `smartcache.Get(...)` and
  `smartcache.GetWithTTL(...)`.
- Configure worker limiting through `smartcache.Config.MaxWorkers`:
  - set a positive number to cap concurrent calculations
  - set zero or a negative value for unlimited concurrency

## v3.0.0

### Architecture

- Rebuilt the module around explicit package boundaries:
  - `smartcache`
  - `distlock`
  - `valuestore`
  - `cluster`
  - `providers/redis`
  - `providers/postgres`
  - `providers/sqlite`
  - `providers/cluster`
- Removed the legacy root-level `ExternalCache` architecture.
- Split distributed locking from shared value storage so hybrid provider
  combinations are first-class.
- Added `smartcache.Policy.PublishMode` with:
  - `PublishBestEffort` for cache-style shared publication
  - `PublishRequired` for token/session-like values that are not valid until
    shared publication succeeds
- Added `EntrySnapshot.CreatedAt` to track real snapshot recency independently
  from TTL windows.

### Cluster

- Replaced the old internal cluster package with a top-level `cluster` package.
- Kept the leader/follower model with gRPC transport.
- Preserved local TCP election.
- Preserved optional Kubernetes Lease election behind `-tags k8s`.
- Changed cluster integration to expose separate `valuestore.Store` and
  `distlock.Provider` roles instead of a single mixed cache interface.
- Added warm leader promotion through `smartcache.Cache.LocalValues()`, so a
  newly promoted leader can immediately serve entries it already had warm
  locally.
- Added readiness-aware provider helpers so:
  - low-level `cluster.New(...)` preserves follower startup/retry behavior
  - `providers/cluster.Bind(...)` and `providers/cluster.NewWithLocal(...)`
    wait for a usable leader or fail initialization cleanly
- Added rollback in `providers/cluster.Bind(...)` so failed readiness does not
  leave the passed `smartcache.Cache` wired to a dead cluster service.
- Hardened leader/follower transitions:
  - mutating leader-local operations are fenced across demotion
  - reads retry across role changes instead of returning old leader-local data
  - shutdown uses a dedicated path so `Service.Close()` does not block
    indefinitely on slow custom leader-local mutations
- Fixed follower read-through cache snapshot aliasing by cloning `[]byte` values
  on cache store and cache read.

### Providers

- Added Redis backend implementing both `valuestore.Store` and
  `distlock.Backend`.
- Added PostgreSQL backend implementing both `valuestore.Store` and
  `distlock.Backend`.
- Added SQLite backend implementing both `valuestore.Store` and
  `distlock.Backend`.
- Added cluster provider binding helper in `providers/cluster`.

### Tests

- Added local stale-while-refresh coverage for `smartcache`.
- Added cross-instance deduplication coverage with separate lock/value
  providers.
- Added cluster failover coverage proving that a promoted leader can serve a
  value it already had warm locally.
- Added shared contract-style test suites for:
  - `valuestore.Store`
  - `distlock.Backend`
  - composed `smartcache`
- Added backend-specific coverage for:
  - memory
  - Redis
  - PostgreSQL
  - SQLite
- Added regression coverage for edge cases fixed during the v3 hardening pass,
  including:
  - stale background refresh not recomputing shared values
  - lock reacquisition after owner loss
  - foreground calculation goroutine leaks
  - startup readiness contracts for cluster constructors
  - required-publication commit semantics
  - read-through cache snapshot immutability
  - bind rollback on readiness failure
  - demotion fencing for leader-local mutations
  - shutdown behavior with slow custom local backends
  - shared/local reconciliation with mixed TTL policies
  - legacy zero-`CreatedAt` snapshot compatibility

### Migration Notes

v3 is a deliberate clean break.

- Old root-package APIs such as `GetCachedCalc`, `CachedCalculations`, and the
  mixed `ExternalCache` contract were removed.
- Replace previous usage with:
  - `smartcache.New(...)`
  - `smartcache.Get(...)` or `smartcache.GetWithTTL(...)`
  - explicit `distlock.Provider`
  - explicit `valuestore.Store`
- If you previously used the built-in cluster cache, move to:
  - `cluster.ConfigFromEnv()`
  - `providers/cluster.Bind(...)`
- If you previously used Redis/PostgreSQL/SQLite through `ExternalCache`,
  create the new backend and pass:
  - `backend.LockProvider()` to `smartcache.Config.Locks`
  - `backend` to `smartcache.Config.Values`
- If your values must be visible cluster-wide before they are considered valid,
  use `smartcache.Get(...)` with `smartcache.Policy{PublishMode:
  smartcache.PublishRequired}` instead of `GetWithTTL(...)`.

## v2.0.1

- Fixed an atomicity bug in `CachedCalculationsExternalAdapter.SetIfLockOwned`.
- Fixed a lock-order inversion in leader-side expired-entry cleanup.

## v2.0.0

- Added a built-in distributed cluster cache in `internal/cluster`.
- Added leader/follower `ExternalCache` implementation over gRPC.
- Added local leader election using TCP bind ownership.
- Added optional Kubernetes leader election using Lease objects behind
  `-tags k8s`.

## v1.5.0

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
