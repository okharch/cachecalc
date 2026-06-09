# cachecalc

`cachecalc` is a distributed computation coordination layer with caching for Go. It exists for the cases where "just add a cache" stops working: expensive requests get duplicated under load, goroutines race to compute the same value, multiple service instances stampede the same dependency, and the result can become inconsistent across the fleet. These failures show up as thundering herd incidents, wasted CPU, duplicate upstream traffic, and subtle correctness bugs.

`cachecalc` coordinates the computation itself, not only the stored value. For a given key, it can deduplicate work inside one process, coordinate ownership across processes, publish the result for reuse, and refresh stale entries in the background with explicit TTL rules. If you need to prevent duplicate requests in Go, build a cluster-wide singleflight flow, or add a thundering herd problem solution around expensive backend calls, this library is the layer that ties those concerns together.

## What This Library Provides

At a high level, `cachecalc` combines four concerns that are often implemented separately and incompletely:

- Per-key single-flight deduplication inside one process
- Coordination across goroutines and across service instances
- Shared value publication so other nodes can reuse a finished result
- A TTL model that serves hot values immediately, refreshes stale ones in the background, and forces recomputation only when the value is truly expired

That makes it useful as a:

- `golang singleflight alternative` when you also need caching and TTL behavior
- `distributed cache coordination` layer for shared computations
- `golang TTL cache with background refresh` for expensive values that should stay warm
- `distributed locking for computation` when only one node should refresh a key at a time

The mental model is simple:

1. Ask for a value by key.
2. Reuse it immediately if it is still fresh.
3. If it is stale but still usable, return it now and refresh in the background.
4. If it is expired, let exactly one worker compute it and let everyone else wait or reuse the published result.

## Key Capabilities

### Single-flight per key

Inside one process, `cachecalc` deduplicates identical computations by key. If 200 goroutines ask for the same expensive value at once, only one calculation runs. The rest either reuse the cached snapshot or wait for the in-flight result. This is the first line of defense against duplicate expensive work.

### Cluster-wide coordination

Local single-flight is not enough once the service runs on multiple instances. `cachecalc` can use a distributed lock provider so only one process in the cluster becomes the active calculator for a key. Other processes can observe the shared value store and reuse the published result instead of recomputing it. In practice, this gives you cluster-wide singleflight behavior without forcing the entire cache design into one storage backend.

### Smart TTL with background refresh

Each value uses two TTL boundaries:

- `MinTTL`: before this boundary, return the value immediately with no refresh
- `MaxTTL`: after this boundary, the value is expired and must be recomputed

Between `MinTTL` and `MaxTTL`, the value is still usable, but it is no longer fresh. `cachecalc` serves it immediately and refreshes it in the background. This is the core stale-while-refresh behavior:

- hot reads stay fast
- stale values do not trigger a stampede
- expired values still get strict recomputation semantics

If a background refresh calculation fails, the stale value is preserved — readers continue getting the cached result while the cache retries on subsequent requests. The error is logged and reported via the optional `Config.OnRefreshError` callback. Only when `MaxTTL` expires does a foreground calculation run, and only then does the caller see the error directly.

This is the main reason the library is more than a cache wrapper. It coordinates when to reuse, when to refresh, and when to block.

### Controlled publication semantics

For some values, a locally computed result is still useful even if shared publication fails. For others, divergence between nodes is dangerous. `cachecalc` supports both cases:

- `PublishBestEffort`: keep the newly computed local value even if publishing to shared storage fails
- `PublishRequired`: only commit the new value after the shared publish succeeds

`PublishRequired` matters for externally invalidating values such as rotating credentials or login tokens, where one node quietly moving ahead of the rest can break the system.

## Architecture & Flexibility

`cachecalc` is intentionally split into separate concerns:

- `smartcache`: local orchestration, per-key coordination, TTL policy, background refresh
- `distlock`: distributed lock contract used to decide who computes
- `valuestore`: shared snapshot contract used to publish and reuse results
- `cluster` and `providers/*`: concrete ways to wire distributed coordination

That separation gives you real deployment flexibility.

### Run local-only

You can use in-memory locks and an in-memory value store for a single process or a single node service:

```go
values := vmemory.New()
locks := lockmem.NewProvider()

cache := smartcache.New(smartcache.Config{
    MaxWorkers: 4,
    Locks:      locks,
    Values:     values,
})
defer cache.Close()
```

This is enough when duplicate work only happens inside one process and you do not need cross-instance coordination.

### Run hybrid

You can split coordination from storage. For example:

- use one backend for distributed locks
- use another backend for shared cached values
- keep a separate fast local cache in each process

That means you can tune consistency and performance independently instead of accepting whatever one monolithic cache product happens to provide.

### Scale to multi-instance clusters

`cachecalc` supports pluggable providers such as Redis, PostgreSQL, SQLite, and the built-in cluster transport. You can run it:

- as a local-only library
- as a hybrid local-plus-distributed system
- as a multi-instance service with shared coordination and value reuse

The important point is that computation, locking, and value publication are separate knobs. That is what makes the library adaptable to different operational environments.

## Hybrid Cache Patterns

One of the strongest patterns in `cachecalc` is the L1/L2 split:

- L1: fast local in-memory snapshots inside each process
- L2: shared coordination and optional shared value publication across processes

This lets you build systems where one node computes and other nodes reuse the result safely.

Typical pattern:

1. Every node keeps hot values in memory for low-latency reads.
2. A distributed lock decides which node is allowed to recompute a key.
3. The winning node publishes the refreshed snapshot.
4. Other nodes adopt that shared value instead of recomputing.

This is the practical balance between consistency and throughput:

- local reads stay cheap
- duplicate refreshes are suppressed
- upstream dependencies see fewer bursts
- cross-node correctness is better than a naive local cache

If you need a `distributed cache coordination` pattern rather than a simple distributed key/value store, this is the core design to pay attention to.

The built-in cluster provider follows the same model. One elected leader exposes shared state over gRPC, while followers proxy lock and value operations to it. That is useful when you want cluster-wide coordination without adding Redis or a database just to serialize computations.

## Real-World Example: Global Token Refresh

Token refresh is a good example because it combines concurrency, external invalidation, and correctness pressure.

Imagine a service that talks to an external API with a short-lived access token:

- logging in invalidates the previous token
- several requests notice the token is near expiry at the same time
- each request tries to refresh it
- the upstream system invalidates earlier refresh results
- some requests now hold a token that another request has already made obsolete

A naive cache does not solve this. A local mutex does not solve it across replicas. Plain singleflight does not give you reuse after the refresh finishes.

With `cachecalc`, the flow becomes:

1. Use a shared key such as `login-token`.
2. Let only one goroutine or one service instance perform the refresh globally.
3. Publish the new token so other requests reuse the exact same result.
4. Derive TTL from the token expiration.
5. Use `PublishRequired` so a new token is not accepted locally unless it is also published successfully for the rest of the cluster.

Example:

```go
token, err := smartcache.Get(ctx, cache, "login-token", func(ctx context.Context) (string, smartcache.Policy, error) {
    token, err := refreshToken(ctx)
    if err != nil {
        return "", smartcache.Policy{}, err
    }

    return token.AccessToken, smartcache.Policy{
        MinTTL:      time.Until(token.ExpiresAt.Add(-30 * time.Second)),
        MaxTTL:      time.Until(token.ExpiresAt),
        PublishMode: smartcache.PublishRequired,
    }, nil
})
```

In practice this means:

- only one refresh runs globally
- concurrent callers across goroutines and services reuse the same token
- refresh happens before hard expiry when possible
- once the token is too old, callers block for a real refresh instead of serving a broken credential

This is exactly the kind of scenario where `cachecalc` is not "just a cache". It is coordination around a value whose lifecycle has correctness implications.

## Comparison With Alternatives

### `sync.Mutex`

`sync.Mutex` is process-local and manually scoped. It can serialize code inside one instance, but it does not deduplicate by key, does not cache results, does not provide TTL behavior, and does not help in a distributed deployment.

### `singleflight`

`singleflight` suppresses duplicate work for concurrent callers in one process, but it stops there. It does not retain values, does not model freshness, does not do background refresh, and does not provide cluster-wide coordination. If you are looking for a `golang singleflight alternative` because you need reuse after the first calculation finishes, `cachecalc` addresses that gap.

### Naive cache

A basic cache can store values, but it usually does not coordinate recomputation. Under load, multiple callers discover the same miss or the same stale entry and stampede the backend. That is how you get race conditions and cache stampedes even though "a cache" exists. `cachecalc` is designed specifically to prevent duplicate requests in Go and provide a real thundering herd problem solution.

### `groupcache`

`groupcache` is more caching-focused. It is strong when you want peer-assisted caching, but it does not give you the same explicit separation between computation coordination, locking, and storage, and it does not center the same min-TTL/max-TTL refresh model. If your main requirement is a `golang TTL cache with background refresh` plus cluster-wide computation ownership, `cachecalc` is aimed more directly at that use case.

## When To Use / When Not To Use

Use `cachecalc` when:

- the computation is expensive enough that duplicate work is harmful
- multiple goroutines or multiple service instances can ask for the same key
- you need controlled freshness instead of binary cache hit or miss behavior
- recomputation should be coordinated, not merely stored afterward
- correctness depends on one shared result being reused consistently

Do not use `cachecalc` when:

- values are cheap to compute and duplicate work does not matter
- you only need a plain in-memory map with expiration
- there is no concurrency pressure and no multi-instance deployment
- a simple lock around one local code path is sufficient

## Summary

`cachecalc` should be understood as a distributed computation coordination layer with caching. It combines local single-flight, optional cross-process coordination, shared value publication, and a stale-while-refresh TTL model in one composable design.

If you need to prevent duplicate requests in Go, build a cluster-wide singleflight path, add distributed locking for computation, or keep expensive values warm without causing refresh stampedes, `cachecalc` is the layer to put in front of that work.
