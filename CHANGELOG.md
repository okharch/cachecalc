# Changelog

All notable changes to this project are documented here.

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
