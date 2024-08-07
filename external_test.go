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
	sc1 = NewCachedCalculations(ctx, ec1, 3)
	sc2 = NewCachedCalculations(ctx, initExternalCache(ctx), 3)
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
	key := getRandomKey(t)
	GetI1 := initCalcTest(t, &wg, cc1, key)
	GetI2 := initCalcTest(t, &wg, cc2, key)
	// make sure we remove the lock - could be left from previous sessions
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
	time.Sleep(tick) // need to wait here to give time for 4 for taking lock and starting calculations
	GetI1(&d2, 5)    // still old value as calculations take time
	require.Equal(t, 1, d2)
	time.Sleep(tick) // now it should be ready
	GetI2(&d3, 6)
	require.Equal(t, 2, d3)
	wg.Wait()
	DefaultCCs.Wait()
}

// testKeyExpiration is a test helper function designed to test the
// ExpireEntries functionality for any implementation of the ExternalCache
// interface. It sets a key in the cache with a long expiration time, deletes
// the key, and verifies that a deletion notification is received.
//
// Parameters:
//   - t: The testing framework instance used for managing the test state and
//     reporting errors.
//   - initCache: A function that takes a context and returns an instance of
//     ExternalCache. This function initializes the cache to be tested.
//
// The function performs the following steps:
//  1. Creates a cancellable context and initializes the cache using the
//     provided initCache function.
//  2. Sets a key-value pair in the cache.
//  3. Waits for a key deletion notification on the ExpireEntries channel.
//  4. Sets a key with a long expiration time and deletes it.
//  5. Verifies that the deletion notification for the key is received.
//  6. Cancels the context and waits for the ExpireEntries channel to close.
func testKeyExpiration(t *testing.T, initCache func(context.Context) ExternalCache) {
	ctxCancel, cancel := context.WithCancel(context.TODO())
	ctx := context.WithValue(ctxCancel, "thread", 1)
	cc := initCache(ctx)
	defer func() {
		_ = cc.Close()
	}()
	// set some key with some value
	key := getRandomKey(t)
	val := []byte("test")
	// wait for delete key message
	exp := cc.ExpireEntries(ctx)
	// now let's set key with long expiration and try to apply Del method to check whether that channel will trigger the message
	key = getRandomKey(t)
	err := cc.Set(ctx, key, val, tick*1000)
	require.NoError(t, err)
	// delete the key
	logger.Println("deleting key", key)
	require.NoError(t, cc.Del(ctx, key))
	logger.Println("wait for deletion message", key)
	select {
	case k := <-exp:
		require.Equal(t, key, k)
		logger.Println("key deletion notification received", key)
	case <-time.After(time.Second * 10):
		cancel()
		t.Log("key deletion notification not received")
	}
	cancel()
	// wait until channel is closed on context cancel
	<-exp
}

func testRemoteConcurrent(t *testing.T, ctx context.Context, initExternalCache func(ctx context.Context) ExternalCache) {
	cc1, cc2 := init2Caches(t, ctx, initExternalCache)
	defer cc1.Close()
	defer cc2.Close()
	var d1, d2, d3 int
	var wg sync.WaitGroup
	key := getRandomKey(t)
	GetI1 := initCalcTest(t, &wg, cc1, key)
	GetI2 := initCalcTest(t, &wg, cc2, key)
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
	defer func() {
		cc1.Close()
		cc2.Close()
	}()
	var entry CacheEntry
	var err error
	v, err := serialize(1)
	require.NoError(t, err)
	require.NotNil(t, v)
	entry.Value = v
	key := getRandomKey(t)
	lockKey := getKeyLock(key)
	logger.Println("cc1.SetNX", lockKey)
	created, err := cc1.externalCache.SetNX(ctx, lockKey, v, expire)
	require.NoError(t, err)
	require.True(t, created)
	logger.Println("cc2.SetNX", lockKey)
	created, err = cc2.externalCache.SetNX(ctx, lockKey, v, expire)
	require.NoError(t, err)
	require.False(t, created)
	se, err := serializeEntry(&entry)
	require.NoError(t, err)
	require.NotNil(t, se)
	require.NotZero(t, len(se))
	logger.Println("cc1.Set", key)
	err = cc1.externalCache.Set(ctx, key, se, expire)
	require.NoError(t, err)
	logger.Println("cc2.Get", key)
	val, exists, err := cc2.externalCache.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.NotZero(t, len(val))
	require.Equal(t, se, val)
	time.Sleep(expire + tick*10)
	logger.Println("cc2.Get after tick", key)
	val2, exists, err := cc2.externalCache.Get(ctx, key)
	require.NoError(t, err)
	require.False(t, exists) // key expired
	require.Zero(t, len(val2))
	logger.Println("testExternalCache:finishing", key)
}

func TestExternalExpire(t *testing.T) {
	type calcFunc func(ctx context.Context) (int, CachedCalcOpts, error)
	returnValue := 0
	var mu sync.RWMutex
	var wg sync.WaitGroup
	getCalc := func(ctx context.Context) (calc calcFunc, expireCh chan struct{}) {
		expireCh = make(chan struct{}, 1)
		calc = func(ctx context.Context) (int, CachedCalcOpts, error) {
			thread := getThread(ctx)
			mu.Lock()
			returnValue++
			logger.Printf("thread %v calc will return %v\n", thread, returnValue)
			mu.Unlock()
			return returnValue, CachedCalcOpts{
				MaxTTL:      time.Hour,
				MinTTL:      time.Hour,
				ExpireEntry: expireCh,
			}, nil
		}
		return
	}
	key := getRandomKey(t)
	ctx1, cancel1 := context.WithCancel(context.WithValue(context.TODO(), "thread", 1))
	ctx2, cancel2 := context.WithCancel(context.WithValue(context.TODO(), "thread", 2))
	// create two different cc with external cache
	cc1, cc2 := init2Caches(t, ctx1, initRedisCache(t))
	var v1, v2 int
	// get it for the first time
	wg.Add(2)
	calc1, expireCh1 := getCalc(ctx1)
	calc2, expireCh2 := getCalc(ctx2)
	get := func(cc *CachedCalculations, ctx context.Context, calc calcFunc, v *int) {
		defer wg.Done()
		var err error
		*v, err = GetCachedCalcOptX(cc, ctx, key, calc, true)
		require.NoError(t, err)
	}
	go get(cc1, ctx1, calc1, &v1)
	go get(cc2, ctx2, calc2, &v2)
	wg.Wait()
	require.Equal(t, 1, v1)
	require.Equal(t, 1, v2)
	// force refresh
	logger.Println("expiration signal sent to v1")
	expireCh1 <- struct{}{}
	logger.Println("expiration signal sent to v2")
	expireCh2 <- struct{}{}
	// now it should be recalculated
	wg.Add(2)
	time.Sleep(time.Millisecond * 100)
	go get(cc1, ctx1, calc1, &v1)
	go get(cc2, ctx2, calc2, &v2)
	wg.Wait()
	require.Equal(t, 2, v1)
	require.Equal(t, 2, v2)
	cancel1()
	cancel2()
}

func testExpiration(t *testing.T, ctx context.Context, initExternalCache func(ctx context.Context) ExternalCache) {
	cc1, cc2 := init2Caches(t, ctx, initExternalCache)
	defer cc1.Close()
	defer cc2.Close()
	var d1, d2, d3 int
	var wg sync.WaitGroup
	key := getRandomKey(t)
	GetI1 := initCalcTest(t, &wg, cc1, key)
	GetI2 := initCalcTest(t, &wg, cc2, key)
	wg.Add(3)
	GetI1(&d1, 1)
	GetI2(&d2, 2)
	GetI2(&d3, 3)
	wg.Wait()
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
	time.Sleep(refresh + tick)
	wg.Add(3)
	GetI2(&d1, 4)
	require.Equal(t, 1, d1)
	GetI1(&d2, 5)
	require.Equal(t, 1, d2)
	time.Sleep(tick * 3)
	GetI1(&d3, 6)
	wg.Wait()
	require.Equal(t, 2, d3)
	DefaultCCs.Wait()
}
