# ⚡️ SmartCacheCalc

🧠 **A smarter alternative to Go’s `singleflight.Group`**  
With **TTL-based caching**, **background refresh**, and **distributed coordination** via Redis, PostgreSQL, or SQLite.

---

## 🚀 Overview

**SmartCacheCalc** helps backend systems **avoid duplicate expensive calculations** by:

- ❌ Eliminating **concurrent recomputation** (like Go’s `singleflight`)
- 📦 **Caching results** with MinTTL / MaxTTL rules
- 🔄 **Refreshing stale cache entries in the background**
- 🌍 Coordinating across multiple processes using **external locks** (Redis, Postgres, SQLite)
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

If an **external cache** (like Redis/PostgreSQL) is provided, it ensures:
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
