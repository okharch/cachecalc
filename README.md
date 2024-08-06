# CachedCalculations
Coordinated and Cached Calculations (Golang)

# CachedCalculation

CachedCalculation is a small and simple implementation to coordinate and cache calculations.

## Requirements

For backend, it is a frequent situation when some resource is requested many times a minute
and takes a few seconds to compose and return the result. 
Most of the time the result will be the same as the underlying data is not changed that often.
For example it could be the list of countries, the list of commodities and their prices at warehouse, etc.
Still, all of this calculation's tasks, if not properly coordinated, 
will be making the same queries to the database, 
combine the results, and return the same JSON data. 
The situation could be improved by :
1. Avoiding concurrent calculation of the same resource which decrease the load of the database, etc.
2. Cache the response result based on the request and return subsequent responses from the cache 
rather than calculating them again.

Meet the CachedCalculations, which was created to implement these specific requirements for development of efficient backend services.

## Implementation

1. Provided, that some calculation is "cached", it guarantees that when it does not hit the cache, 
it automatically performs necessary calculation, caches response, and returns it to the client. 
All the requests for the same resource will be waiting for the result if the calculation for the resource has been started,
so the same calculation is never completed concurrently, either within the same service instance or distributed like k8 or even different machines.
2. It provides these facilities for cache entry expiration:
- `MinTTL timeout`: If the data isn't too old, let's say 1-10 seconds, for MinTTL time just cached response is returned for the subsequent requests of the same resource. When MinTTL for the data expired, it still returns cached response to the client but immediately starts the calculation of the resource in background (refresh). Until value refreshed it continues to return the cached response. When refresh finished, it stores the response to the cache, and will return the refreshed response for subsequent calls.
- `MaxTTL timeout` When the data has expired totally, it's forcefully removed and will be recalculated on demand. Next value is returned upon completion of the calculation.
- `ExpireEntry channel` channel is provided to expire cache entry immediately. It acts like MaxTTL - on next call the calculation will be performed and upon completion the new value will be returned to the client. 
If some watcher sends a message to this channel, the cached value for the key is immediately expired. 
It will be recalculated next time when function for the key is called.
3. CachedCalculation provides a simple method `GetCachedCalc` of refactoring existing backend methods to use its infrastructure. 
The idea is to wrap all the existing methods that handle some request to a function of type 
```
CalculateValue func(context.Context) (any, error)
``` 
where `any` in the type of the result, in most cases will be the string with JSON. 
To do this, wrap your implementation into a closure called e.g. `slowGet`, 
which sees all the parameters of your method and use it like this: 
```
result, err := cachecalc.GetCachedCalc(ctx, key, maxTTL, minTTL, true, slowGet)
```
It's a straightforward refactoring process which will drastically improves the performance of your backend!

4. CachedCalculation takes lock before calculation using external cache if provided.
Those who were unable to take lock patiently wait when the result appears in the external cache.
This allows to avoid concurrency issues mentioned in point #1. 
When the calculated value is ready, all clients are provided with it from the cache. 
If you know that you have only a single instance of the server, you don't need an external cache. 
In this case it will work significantly faster due the absence of networking delays. 
Although it works with an external cache asynchronously, it still takes time for coordination between processes. 
My rule of thumb in this case: if you have a simple REST API handler without heavy load, give it more RAM and CPU, 
and single Golang server will handle your request much faster than in Kubernetes or other distributed systems. 
Don't use a sledgehammer to crack a nut!

## TODO
- Make TTL returned by calculation. It might know better when returned value expires. For example ttl for token returned by calculation.
- Provide a way to expire cache entry by goroutine. It might be useful for the case when the data is changed in the database.

