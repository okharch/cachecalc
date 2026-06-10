# FAQ

## Can I use different providers for locks and storage?

Yes. `smartcache.Config` accepts `Locks` (a `distlock.Provider`) and `Values` (a `valuestore.Store`) as independent interfaces, so you can mix any combination. Each backend in `providers/` implements both interfaces, but you only need to use one side from each.

```go
redisBackend, _ := redis.New(ctx, redisURL)
mongoBackend, _ := mongo.New(ctx, mongoURI)

cache := smartcache.New(smartcache.Config{
    Locks:  distlock.NewProvider(redisBackend),  // Redis for locks
    Values: mongoBackend,                        // MongoDB for storage
})
```

## What happens when a background refresh fails?

The stale value is preserved and continues to be served. The next request will trigger another background refresh attempt. This repeats naturally until the calculation succeeds or `MaxTTL` (ExpireAt) is reached — at that point the entry expires, the next request does a foreground calculation, and if that also fails the caller gets the error directly.

To observe refresh failures without changing the `Get` return type, use the `OnRefreshError` callback:

```go
cache := smartcache.New(smartcache.Config{
    OnRefreshError: func(key string, err error) {
        log.Printf("background refresh failed for %q: %v", key, err)
        metrics.IncrCounter("cache.refresh_error", 1)
    },
})
```

This was changed in v4.3.0. Prior versions replaced the stale entry with an error snapshot, causing all subsequent readers to fail until the error TTL expired.

## On a single instance, does cachecalc use a simple mutex for goroutine coordination?

No. It uses a **channel-based singleflight** pattern, not a plain mutex. Each `localEntry` holds a `wait chan struct{}` field. When the first goroutine requests a key that needs (re)calculation, it creates the channel and starts computing. All subsequent goroutines for the same key see that channel and block on `<-wait`. When the calculation finishes, the channel is closed — waking every waiter at once so they all read the fresh result.

This is more efficient than a mutex: waiters don't compete for a lock, and there's zero re-calculation regardless of how many goroutines pile up.

```go
// Simplified flow inside smartcache.serve():
entry := cache.entryFor(key)
if entry.wait != nil {
    // Another goroutine is already calculating — just wait
    <-entry.wait
    return entry.snapshot
}
// First arrival — create channel, start calculation
entry.wait = make(chan struct{})
go func() {
    entry.snapshot = calculate(key)
    close(entry.wait) // wake all waiters
}()
<-entry.wait
return entry.snapshot
```

## How does cache warm-up work when a new cluster leader is elected?

When the current leader dies, a new leader is elected via the configured elector (local TCP or Kubernetes Lease). Every surviving follower detects the new leader through gRPC reconnection and automatically streams its local cache entries to the new leader using the `WarmUp` RPC. The leader accepts each entry only if it is newer (by `CreatedAt`) than any existing entry for the same key, so stale values never overwrite fresh ones.

This means the new leader's shared store is populated almost immediately after election — without triggering a wave of cache-miss recomputations. The warm-up happens automatically when you use `providers/cluster.Bind()`; no additional configuration is needed.

The leader itself never sends a warm-up stream to itself — it already has its own local entries.

## Where does the MongoDB provider store data, and how do I protect it?

The MongoDB provider uses a hardcoded database `smartcache` with two collections: `values` (cached entries) and `locks` (distributed locks). Both have TTL indexes on the `expire_at` field for automatic cleanup.

The names are not currently configurable, but you can protect the data at the MongoDB level:

- **RBAC** — create a dedicated MongoDB user with `readWrite` access limited to the `smartcache` database. Avoid granting `dbAdmin` or `drop` privileges so that manual or accidental deletions cannot remove the collections.
- **Network isolation** — bind MongoDB to internal interfaces, enable TLS, and restrict access with firewall rules so only your application nodes can connect.
- **Client-side field-level encryption** — if the cached data is sensitive, MongoDB supports encrypting individual fields before they reach the server.

```js
// Example: create a restricted user in the mongo shell
use smartcache
db.createUser({
  user: "cacheapp",
  pwd: "...",
  roles: [{ role: "readWrite", db: "smartcache" }]
})
```

## Does the in-memory backend store Go values directly without serialization?

No. **All values are gob-serialized to `[]byte`**, even with the in-memory backend. The `EntrySnapshot.Value` field is always `[]byte`, and the memory store holds a `map[string]EntrySnapshot` of these serialized snapshots — the same format used by Redis, Postgres, and every other backend.

This means cached types must be gob-encodable (exported fields, registered interfaces). The upside is a uniform `EntrySnapshot` format across all backends, so you can swap between memory and distributed stores without any code changes. It also means the in-memory store faithfully reproduces the same serialization behavior you'll see in production with a distributed backend.
