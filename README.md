# SmartCacheCalc v3

`cachecalc` v3 is a composable smart-calculation cache for Go.

The architecture is intentionally split into small, explicit packages:

- `smartcache/`
  Local cache, stale-while-refresh logic, and orchestration.
- `distlock/`
  Distributed lock contracts and lock-provider helpers.
- `valuestore/`
  Shared cache snapshot contracts.
- `cluster/`
  Leader election and gRPC-backed lock/value transport.
- `providers/redis`
- `providers/postgres`
- `providers/sqlite`
- `providers/cluster`

## Why v3

Previous versions centered everything around one mixed external-cache contract.
That made leader election, distributed locks, and shared value publication too
tightly coupled.

v3 separates those responsibilities:

- `smartcache` owns cache policy
- `distlock` owns exclusive calculation rights
- `valuestore` owns shared snapshots
- providers can be mixed freely

That means hybrid setups are first-class:

- cluster locks + Redis values
- PostgreSQL locks + PostgreSQL values
- SQLite locks + SQLite values
- cluster locks + cluster values
- memory locks + memory values

## Core Concepts

`smartcache` uses two time boundaries:

- `MinTTL`
  If the value is newer than this, return it immediately.
- `MaxTTL`
  If the value is older than this, force a fresh calculation.

Between those boundaries, the cache returns the current value immediately and
refreshes it in the background.

## Basic Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    lockmem "github.com/okharch/cachecalc/distlock/memory"
    "github.com/okharch/cachecalc/smartcache"
    vmemory "github.com/okharch/cachecalc/valuestore/memory"
)

func main() {
    cache := smartcache.New(smartcache.Config{
        MaxWorkers: 8,
        Locks:      lockmem.NewProvider(),
        Values:     vmemory.New(),
    })
    defer cache.Close()

    value, err := smartcache.GetWithTTL(
        context.Background(),
        cache,
        "catalog:v1",
        5*time.Second,
        30*time.Second,
        true,
        func(ctx context.Context) (string, error) {
            time.Sleep(500 * time.Millisecond)
            return "calculated at " + time.Now().Format(time.RFC3339Nano), nil
        },
    )
    if err != nil {
        panic(err)
    }

    fmt.Println(value)
}
```

## Hybrid Provider Example

```go
redisBackend, err := redis.New(ctx, "")
if err != nil {
    panic(err)
}
defer redisBackend.Close()

cache := smartcache.New(smartcache.Config{
    MaxWorkers: 8,
    Locks:      redisBackend.LockProvider(),
    Values:     redisBackend,
})
```

Or split different providers:

```go
cache := smartcache.New(smartcache.Config{
    MaxWorkers: 8,
    Locks:      clusterService.LockProvider(),
    Values:     redisBackend,
})
```

## Cluster Provider

The cluster provider runs one elected leader and proxies followers to it over
gRPC.

Common wiring:

```go
cache := smartcache.New(smartcache.Config{MaxWorkers: 8})

cfg, err := cluster.ConfigFromEnv()
if err != nil {
    panic(err)
}

service, err := providerscluster.Bind(ctx, cache, cfg)
if err != nil {
    panic(err)
}
defer service.Close()
defer cache.Close()
```

Advantages of the cluster provider:

- no separate Redis/PostgreSQL/SQLite dependency for small clusters
- clean leader/follower transport boundary
- leader re-election can stay warm

When a new leader is promoted, it can expose its own already-warm
`cache.LocalValues()` entries as shared L2 values immediately. That means a
promoted leader does not necessarily start with an empty shared cache view.

Tradeoff:

- cluster state is not durably replicated
- lock ownership is still lost with the old leader
- only values already warm on the promoted node survive into the new shared
  view

See:

- `cluster/README.md`
- `examples/v3_local`
- `examples/v3_cluster`

## Build

Generate cluster gRPC code:

```bash
make proto
```

Run tests:

```bash
make test
```
