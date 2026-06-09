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
