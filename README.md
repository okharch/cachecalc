# ⚡️ SmartCacheCalc

🧠 **A smarter alternative to Go’s `singleflight.Group`**  
With **TTL-based caching**, **background refresh**, and **distributed coordination** via Redis, PostgreSQL, SQLite, or the built-in cluster cache.

---

## 🚀 Overview

**SmartCacheCalc** helps backend systems **avoid duplicate expensive calculations** by:

- ❌ Eliminating **concurrent recomputation** (like Go’s `singleflight`)
- 📦 **Caching results** with MinTTL / MaxTTL rules
- 🔄 **Refreshing stale cache entries in the background**
- 🌍 Coordinating across multiple processes using **external locks/cache backends** (Redis, Postgres, SQLite, built-in cluster mode)
- 🧩 Drop-in usage via a single function call

---

## 🔧 Problem It Solves

In backend systems, it’s common to:
- Fetch a heavy list (e.g. countries, product catalog, warehouse prices)
- Repeat that operation dozens of times per minute
- Serve essentially **the same result**, wasting CPU and DB resources

**SmartCacheCalc** stops that waste.

✅ Ensures only **one calculation per key** runs at a time  
✅ Returns the **cached result immediately** if it’s still valid  
✅ Starts a **background refresh** if it’s slightly stale  
✅ Recalculates from scratch only when the cache is **fully expired**

---

## 🛠️ How It Works

### 🔒 1. Distributed Locking (Optional)

If an **external cache** (like Redis/PostgreSQL/SQLite/cluster mode) is provided, it ensures:
- Only one instance performs the calculation
- Others wait until the cache is filled

Without external coordination, it works **in-memory only** (faster, but not distributed).

---

### 📆 2. Smart Expiration Rules

- **MinTTL**: If recent enough, serve cached value immediately
- **Stale-but-OK**: Still return cached value, but **trigger refresh in background**
- **MaxTTL**: If fully expired, force recalculation on next request

---

### 💻 3. Simple API

Wrap your existing backend logic like this:

```go
slowGet := func(ctx context.Context) (any, error) {
    return fetchExpensiveResult(), nil
}

result, err := cachecalc.GetCachedCalc(
    ctx, "my-key",
    30*time.Second,  // MaxTTL
    10*time.Second,  // MinTTL
    true,            // allow background refresh
    slowGet,
)
```

That's it — no boilerplate, no infrastructure gymnastics.

---

## 🧭 Built-In Cluster Cache

You can now run distributed smart calculations without depending on Redis,
PostgreSQL, or SQLite.

The `internal/cluster` package provides:

- leader election
- gRPC transport between instances
- a distributed `ExternalCache`
- a helper that wires `CachedCalculations` directly into the cluster layer

Supported deployment modes:

- `CLUSTER_MODE=local`
  Uses simple TCP bind ownership for local multi-process deployments.
- `CLUSTER_MODE=k8s`
  Uses Kubernetes Lease-based leader election through `client-go`.
  Build this mode with `-tags k8s`.

### Why use cluster cache instead of Redis/Postgres/SQLite?

- No separate cache service to provision for local deployments or simple clusters
- The leader can expose its already-warm local smart-cache entries as shared L2
- On leader re-election, the new leader can immediately reuse its own local L1
  entries as remote L2 entries for other nodes
- That reduces duplicate memory on the leader compared to keeping a second
  leader-only cache copy

Tradeoff:

- This is a simple leader/follower design, not durable replicated storage
- A promoted leader can reuse only the entries already warm in its own local L1
- Cluster state is not fully replicated to every node

### Clustered `CachedCalculations` Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/okharch/cachecalc"
    "github.com/okharch/cachecalc/internal/cluster"
)

func main() {
    ctx := context.Background()

    cfg, err := cluster.ConfigFromEnv()
    if err != nil {
        panic(err)
    }

    cc, distributedCache, err := cluster.NewClusteredCachedCalculations(ctx, 4, cfg)
    if err != nil {
        panic(err)
    }
    defer cc.Close()
    defer distributedCache.Close()

    value, err := cachecalc.GetCachedCalcX(
        cc,
        ctx,
        "commodities-v1",
        30*time.Second,
        2*time.Minute,
        true,
        func(ctx context.Context) (string, error) {
            time.Sleep(2 * time.Second)
            return fmt.Sprintf("calculated at %s", time.Now().Format(time.RFC3339)), nil
        },
    )
    if err != nil {
        panic(err)
    }

    fmt.Println(value, distributedCache.IsLeader(), distributedCache.LeaderAddress())
}
```

### What happens during leader re-election?

- Exactly one instance is leader at a time
- Followers proxy `ExternalCache` traffic to the leader over gRPC
- When the leader dies, another instance can become leader
- In the new clustered design, the promoted leader can expose its own already
  warm local `CachedCalculations` entries as L2 immediately

That means leader re-election is warmer than a cold restart:

- old leader-specific lock state is lost
- but the new leader does not necessarily start with an empty shared cache view
- if it already had the value in local L1, other instances can fetch that value
  from the new leader right away

For a runnable demo, see:

- `examples/cluster_calc`
- `internal/cluster/README.md`

For Kubernetes deployments:

- build with `go build -tags k8s ./...`
- set `CLUSTER_MODE=k8s`
- provide the usual in-cluster Kubernetes environment
- optionally set `LEADER_ADDR`, or let the k8s elector derive it from pod info

---

## 🔍 Real-World Use Cases

- 🔁 Token introspection with returned TTL
- 🌐 Caching third-party API responses
- 🏪 Product catalog or stock availability
- 🌍 Country lists or static dictionaries
- 💱 Exchange rates or slow aggregations

---

## 📦 Installation

```bash
go get github.com/okharch/cachecalc
```

Or import directly:

```go
import "github.com/okharch/cachecalc"
```

---

## 🧪 Example

```go
func getCommodities(ctx context.Context) (any, error) {
    return fetchFromDatabase(), nil
}

result, err := cachecalc.GetCachedCalc(
    ctx,
    "commodities-v1",
    10*time.Minute,  // MaxTTL
    9*time.Minute,  // MinTTL
    true,
    getCommodities,
)
```

---

## 📝 Notes & Design Philosophy

- ✅ Works **with or without external cache**
- ✅ Can use the built-in cluster cache instead of Redis/PostgreSQL/SQLite
- ⏱️ Prefers **latency reduction** over strict freshness
- 🧠 Encourages re-use and simplicity
- 🛠️ Optimized for **high-frequency, low-variance data**

---

## 🔮 Planned Improvements

- [ ] Let `slowGet` optionally return custom TTLs (e.g. for token expirations)
- [ ] Add built-in adapters for Redis/PostgreSQL/SQLite
- [ ] CLI demo tool for experimenting

---

## 🏷️ GitHub Topics (for discovery)

```
go, golang, cache, caching, ttl, redis, token-cache, singleflight, distributed, background-refresh, backend, lock, deduplication, concurrency
```

---

## 💬 Feedback Welcome

This tool was built to solve real bottlenecks in backend systems.  
If you find it useful or want to contribute — pull requests are welcome!
