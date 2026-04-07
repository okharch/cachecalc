# Cluster Provider

`cluster` implements a simple leader/follower transport for `smartcache`.

## Design

- Exactly one leader owns the authoritative shared state.
- Followers proxy `valuestore.Store` and `distlock.Backend` calls to the
  leader over gRPC.
- Local mode elects the leader by TCP bind ownership.
- Kubernetes mode elects the leader with a `coordination.k8s.io/v1` `Lease`
  when built with `-tags k8s`.

The cluster package does not implement cache orchestration by itself. It only
provides distributed lock and shared value primitives that fit the v3 package
architecture.

## Warm Promotion

The intended cluster wiring is:

1. create a `smartcache.Cache`
2. expose `cache.LocalValues()` as the leader-local shared store
3. bind the cache to a `cluster.Service`

When leadership moves, the promoted node can immediately expose the entries it
already had warm in its own local cache as shared L2 values for the rest of the
cluster. That makes failover warmer than a separate leader-only cache copy.

## Wiring

Use `providers/cluster.Bind` for the common case:

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

This sets:

- `cache.LocalValues()` as the leader-local `valuestore.Store`
- a memory lock backend on the leader
- the cluster service itself as the shared `valuestore.Store`
- `service.LockProvider()` as the distributed lock provider
