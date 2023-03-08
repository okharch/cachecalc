# CachedCalculations
Coordinated and Cached Calculations (Golang)

# CachedCalculation

CachedCalculation is a small and simple implementation for coordinated calculations on data processing and data transformations at the backend.

## Requirements

For backend methods, it is a frequent situation when a REST API method is requested many times a minute and takes a few seconds to return the result. Most of the time, it will be the same as the underlying data is not changed that often. Still, all of them, if not properly coordinated, will be making the same queries to the database, combine results, and return the same JSON data. The right approach is to:
1. Avoid concurrent calculation of the same request to decrease the load of the database, etc.
2. Cache the response result based on the request and return subsequent responses from the cache rather than calculating them again.

I created CachedCalculations because I couldn't find any such implementations from Google or AI that met my specific requirements as a backend developer.

## Implementation

1. Provided that some of the calculation is "cached", it guarantees that when it does not hit the cache, it automatically performs necessary calculation, caches response, and returns it to the client. It coordinates all such calculations, so the same calculation is never completed concurrently, either on the same machine or distributed.

2. It provides the facility for cache entry expiration:
- If the data isn't too old (MinTTL), let's say 1-10 seconds, it's better to save the response to the cache. When the next request with the same parameters arrives, return the data from the cache instead of recalculating it.
- Even if the data is slightly unfresh (MinTTL expired), it can be returned to the client from the cache for the sake of speed. It can be immediately recalculated when the backend is idle and has new fresh data ready for the next request that arrives (assuming the client pressed F5 on slightly unfresh data).
- When the data has expired totally (MaxTTL), it's removed completely and will be recalculated on demand.

There are some interesting features based on Min/Max TTL parameters, like precooking long-calculated but frequently used data, although not much has been implemented just yet.

3. CachedCalculation provides a simple method of refactoring existing backend methods to use its infrastructure. The idea is to wrap all the existing methods that handle some request to a function of type `CalculateValue func(context.Context) (any, error)`, where `any` in most cases will be the string with JSON. To do this, wrap your implementation into a closure called e.g. `slowGet`, which sees all the parameters of your method and use it like this: `err := smartcache.Get(ctx, key, slowGet, &result, true, maxTTL, minTTL)`. It's a straightforward refactoring process!

4. CachedCalculation uses an in-memory cache (even without serialization) for the local process but also sends the results and takes lock on the calculation using external cache if provided. This allows to avoid concurrency issues mentioned in point #1. When the calculated value is ready, all interested clients are informed and take it from external cache into internal memory cache. If you know that you have only a single instance of the server, you don't need an external cache. It will work significantly faster. Although it works with an external cache asynchronously, it still takes time for coordination between processes. My rule of thumb in this case: if you have a simple REST API handler, give it more RAM and CPU, and Golang will handle your request much faster in a single-server case than in Kubernetes or other distributed systems. Don't use a cannon to
