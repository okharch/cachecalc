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
