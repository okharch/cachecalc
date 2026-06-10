# Providers

Each provider in this directory implements both `valuestore.Store` and `distlock.Backend` on top of a specific storage system. This means a single backend instance gives you shared cached values **and** distributed locks — no separate setup required.

## Available Providers

| Provider | Import Path | Best For |
|----------|-------------|----------|
| **Redis** | `providers/redis` | Production clusters needing low-latency shared cache and locks. Atomic lock operations via Lua scripts. |
| **PostgreSQL** | `providers/postgres` | Teams already running Postgres — avoids adding another dependency. Uses row-level locking. |
| **SQLite** | `providers/sqlite` | Single-node deployments, local development, and testing. No external services needed. |
| **MongoDB** | `providers/mongo` | Document-oriented stacks. Uses TTL indexes for automatic expiry of values and locks. |
| **Cluster** | `providers/cluster` | Multi-instance coordination via gRPC. Leader/follower model with automatic cache warm-up on failover. |

## Choosing a Provider

**Single process, no shared state needed?**
Skip providers entirely — `smartcache.New(smartcache.Config{})` works out of the box with in-memory storage.

**Multiple processes, same datacenter?**
Use **Redis** for the best latency, or **PostgreSQL** / **MongoDB** if you already run one and want to avoid adding infrastructure.

**Single binary, persistent cache across restarts?**
Use **SQLite** — it stores values and locks in a local file with WAL mode for safe concurrent access.

**Multiple instances that should coordinate without an external database?**
Use **Cluster** — it elects a leader via TCP (or Kubernetes Lease) and proxies lock/value operations over gRPC. Followers automatically warm up the new leader's cache on failover.

## Usage

Every storage backend (Redis, PostgreSQL, SQLite, MongoDB) follows the same pattern:

```go
import "github.com/okharch/cachecalc/v4/providers/redis"

backend, err := redis.New(ctx, "redis://localhost:6379")
if err != nil { ... }
defer backend.Close()

cache := smartcache.New(smartcache.Config{
    Locks:  backend.LockProvider(),
    Values: backend,
})
```

### Mixing providers

Locks and values are independent interfaces — you can use different backends for each:

```go
redisBackend, _ := redis.New(ctx, redisURL)
pgBackend, _ := postgres.New(ctx, pgDSN)

cache := smartcache.New(smartcache.Config{
    Locks:  redisBackend.LockProvider(),  // Redis for low-latency locks
    Values: pgBackend,                    // Postgres for durable value storage
})
```

### Cluster provider

The cluster provider works differently — it wraps a `smartcache.Cache` and adds multi-instance coordination:

```go
import (
    "github.com/okharch/cachecalc/v4/cluster"
    pcluster "github.com/okharch/cachecalc/v4/providers/cluster"
)

cache := smartcache.New(smartcache.Config{MaxWorkers: 4})

cfg := cluster.DefaultConfig()
cfg.Name = "node-1"  // shows up in log lines as cluster[node-1]:

service, err := pcluster.Bind(ctx, cache, cfg)
if err != nil { ... }
defer service.Close()

// Use cache normally — cluster coordination is transparent.
val, err := smartcache.Get(ctx, cache, "key", calcFn)
```

`Bind()` automatically wires up warm-up so that when a new leader is elected, all followers stream their local cache entries to it.

For lower-level control, use `cluster.New()` + `cache.SetShared()` directly instead of `Bind()`.

### Constructor reference

```go
redis.New(ctx, "redis://localhost:6379")          // *Backend, error
postgres.New(ctx, "postgres://user:pass@host/db") // *Backend, error
sqlite.New("/path/to/cache.db")                   // *Backend, error
mongo.New(ctx, "mongodb://localhost:27017")        // *Backend, error
pcluster.Bind(ctx, cache, clusterConfig)          // *cluster.Service, error
```

All storage backends implement `Close() error` for cleanup.
