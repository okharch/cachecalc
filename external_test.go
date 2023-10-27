package cachecalc

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func init2Caches(t *testing.T, ctx context.Context, initExternalCache func(ctx context.Context) ExternalCache) (sc1, sc2 *CachedCalculations) {
	logger.Println("init 2(two) cached calculations")
	ec1 := initExternalCache(ctx)
	err := ec1.Del(ctx, "key")
	require.NoError(t, err)
	err = ec1.Del(ctx, "key.lock")
	require.NoError(t, err)
	sc1 = NewCachedCalculations(3, ec1)
	sc2 = NewCachedCalculations(3, initExternalCache(ctx))
	require.NotNil(t, sc1)
	require.NotNil(t, sc2)
	return
}

func testRemote(t *testing.T, ctx context.Context, initExternalCache func(ctx context.Context) ExternalCache) {
	cc1, cc2 := init2Caches(t, ctx, initExternalCache)
	defer cc1.Close()
	defer cc2.Close()
	var d1, d2, d3 int
	var wg sync.WaitGroup
	GetI1 := initCalcTest(t, &wg, cc1)
	GetI2 := initCalcTest(t, &wg, cc2)
	// make sure we remove the lock - could be left from previous sessions
	err := cc1.externalCache.Del(ctx, "key.Lock")
	require.NoError(t, err)
	wg.Add(3)
	GetI1(&d1, 1)
	GetI2(&d2, 2)
	GetI2(&d3, 3)
	wg.Wait()
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
	// force refresh
	wg.Add(3)
	time.Sleep(refresh + tick)
	GetI2(&d1, 4) // should still return old value but run refresh calculations
	require.Equal(t, 1, d1)
	GetI1(&d2, 5) // still old value as calculations take time
	require.Equal(t, 1, d2)
	time.Sleep(tick * 3) // now it should be ready
	GetI2(&d3, 6)
	require.Equal(t, 2, d3)
	wg.Wait()
	DefaultCCs.Wait()
}

func testRemoteConcurrent(t *testing.T, ctx context.Context, initExternalCache func(ctx context.Context) ExternalCache) {
	cc1, cc2 := init2Caches(t, ctx, initExternalCache)
	defer cc1.Close()
	defer cc2.Close()
	var d1, d2, d3 int
	var wg sync.WaitGroup
	GetI1 := initCalcTest(t, &wg, cc1)
	GetI2 := initCalcTest(t, &wg, cc2)
	wg.Add(3)
	go GetI1(&d1, 1)
	go GetI2(&d2, 2)
	go GetI2(&d3, 3)
	wg.Wait()
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
	time.Sleep(refresh + tick)
	logger.Printf("force refresh by sleep for %v", refresh)
	wg.Add(3)
	go GetI2(&d1, 4)
	require.Equal(t, 1, d1)
	go GetI1(&d2, 5)
	require.Equal(t, 1, d2)
	logger.Printf("hold thread 6 so refreshed value will be available")
	time.Sleep(tick * 3) // give time to be refreshed
	go GetI1(&d3, 6)
	wg.Wait()
	// still previous call might return local value instead of refreshed remote, so lets wait and ask again, it had to refresh
	time.Sleep(tick * 3) // give time to be refreshed
	wg.Add(1)
	// previous time
	GetI1(&d3, 7)
	wg.Wait()
	require.Equal(t, 2, d3)
	DefaultCCs.Wait()
}

func testExternalCache(t *testing.T, ctx context.Context, initExternalCache func(ctx context.Context) ExternalCache) {
	cc1, cc2 := init2Caches(t, ctx, initExternalCache)
	defer cc1.Close()
	defer cc2.Close()
	var entry CacheEntry
	var err error
	v, err := serialize(1)
	require.NoError(t, err)
	require.NotNil(t, v)
	entry.Value = v
	lockKey := key + ".lock"
	created, err := cc1.externalCache.SetNX(ctx, lockKey, v, expire)
	require.NoError(t, err)
	require.True(t, created)
	created, err = cc2.externalCache.SetNX(ctx, lockKey, v, expire)
	require.NoError(t, err)
	require.False(t, created)
	se, err := serializeEntry(&entry)
	require.NoError(t, err)
	require.NotNil(t, se)
	require.NotZero(t, len(se))
	err = cc1.externalCache.Set(ctx, key, se, expire)
	require.NoError(t, err)
	val, exists, err := cc2.externalCache.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.NotZero(t, len(val))
	require.Equal(t, se, val)
	time.Sleep(expire + tick)
	val2, exists, err := cc2.externalCache.Get(ctx, key)
	require.NoError(t, err)
	require.False(t, exists) // key expired
	require.Zero(t, len(val2))
}
