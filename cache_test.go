package cachecalc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() {
	logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
}

const refresh = tick * 20 // calc gets 2 ticks, minTTL is aligned to 4 ticks according to that
const expire = tick * 50

var counter int
var mu sync.Mutex

/*
initCalcTest initializes a test function for performing cached calculations. It resets the external cache, if any, and provides a function to perform calculations in a concurrent environment.

Parameters:
- t: The testing framework.
- wg: A WaitGroup to synchronize the completion of goroutines.
- cc: A pointer to CachedCalculations, which handles the caching mechanism.
- key: A string used as a key for the cache.

Returns:
A function that performs the cached calculation. The returned function takes:
- result: A pointer to an integer where the result of the calculation will be stored.
- thread: An integer representing the thread ID.

The returned function does the following:
1. Logs the initialization of calculations.
2. If `cc` and `cc.externalCache` are non-nil, it deletes the specified key and its lock from the cache.
3. Defines a helper function `getI` to simulate a calculation by incrementing a counter.
4. Defines the main function `GetI` which performs the cached calculation and updates the result.
5. `GetI` also uses the WaitGroup to signal the completion of the goroutine.
*/
func initCalcTest(t *testing.T, wg *sync.WaitGroup, cc *CachedCalculations, key string, tick, refresh, expire time.Duration) func(result *int, thread int) {
	counter = 0
	ctx := context.TODO()
	logger.Printf("init calculations")
	if cc != nil && cc.externalCache != nil {
		err := cc.externalCache.Del(ctx, key)
		require.NoError(t, err)
		err = cc.externalCache.Del(ctx, key+".lock")
		require.NoError(t, err)
	}
	getI := func(ctx context.Context) (int, error) {
		logger.Println("calculation in thread ", ctx.Value("thread"))
		time.Sleep(tick)
		mu.Lock()
		counter++
		mu.Unlock()
		logger.Println("calculation completed in thread ", ctx.Value("thread"), "result ", counter)
		return counter, nil
	}
	GetI := func(result *int, thread int) {
		defer wg.Done()
		ctx := context.WithValue(ctx, "thread", thread)
		r, err := GetCachedCalcX(cc, ctx, key, refresh, expire, true, getI)
		require.NoError(t, err)
		*result = r
	}
	return GetI
}

func randomHexString(length int) (string, error) {
	// Calculate the number of bytes needed for the given length
	byteLength := length / 2
	if length%2 != 0 {
		byteLength++
	}

	// Generate random bytes
	bytes := make([]byte, byteLength)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	// Convert the random bytes to a hexadecimal string
	hexString := hex.EncodeToString(bytes)

	// Trim the string to the desired length
	if len(hexString) > length {
		hexString = hexString[:length]
	}

	return hexString, nil
}

func getRandomKey(t *testing.T) string {
	s, err := randomHexString(4)
	require.NoError(t, err)
	return s
}

func TestLocalSimple(t *testing.T) {
	const nThreads = 10
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.TODO())
	cc := NewCachedCalculations(ctx, nil, 4)
	GetI := initCalcTest(t, &wg, cc, getRandomKey(t), tick, refresh, expire)
	wg.Add(nThreads)
	dest := make([]int, nThreads)
	for i := 0; i < nThreads; i++ {
		go func(i int) {
			GetI(&dest[i], i+1)
		}(i)
	}
	wg.Wait()
	logger.Println("waiting for calc coordinator to finish operations")
	cc.Wait()
	for i := 0; i < nThreads; i++ {
		require.Equal(t, 1, dest[i])
	}
	cc.Close()
	cancel()
}

func TestLocal(t *testing.T) {
	var d1, d2, d3 int
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.TODO())
	cc := NewCachedCalculations(ctx, nil, 4)
	GetI := initCalcTest(t, &wg, cc, getRandomKey(t), tick, refresh, expire)
	wg.Add(4)
	GetI(&d1, 1)
	require.Equal(t, 1, d1)
	time.Sleep(refresh + tick)
	logger.Printf("waiting for %v enough to start refresh", refresh+tick)
	GetI(&d2, 2) //
	require.Equal(t, 1, d2)
	GetI(&d3, 3)
	require.Equal(t, 1, d3)
	logger.Printf("waiting for %v enough that refresh will be completed", tick*2)
	time.Sleep(tick * 2) // its enough time to refresh value
	GetI(&d3, 3)
	require.Equal(t, 2, d3)
	wg.Wait()
	cc.Close()
	cancel()
}

// TestLocalExpireChannel tests the expiration mechanism of the CachedCalculations utility.
// It ensures that after expiration, the next request gets a fresh value, and verifies the behavior on cancellation.
func TestLocalExpireChannel(t *testing.T) {
	returnValue := 0
	var mu sync.Mutex
	var expireEntry chan struct{}
	calc := func(ctx context.Context) (int, CachedCalcOpts, error) {
		// expire entry should be created each time as it's closing is used to signal expiration job jas been completed
		expireEntry = make(chan struct{})
		mu.Lock()
		returnValue++
		mu.Unlock()
		logger.Println("calc will return ", returnValue)
		return returnValue, CachedCalcOpts{ExpireEntry: expireEntry}, nil
	}
	key := getRandomKey(t)
	ctx, cancel := context.WithCancel(context.WithValue(context.TODO(), "thread", 1))
	// expire entry several times and see if it is recalculated
	for i := 1; i < 3; i++ {
		v, err := GetCachedCalcOpt(ctx, key, calc, true)
		require.NoError(t, err)
		require.Equal(t, i, v)
		logger.Println("send expiration signal")
		sendExpirationSignal(ctx, expireEntry)
		v, err = GetCachedCalcOpt(ctx, key, calc, true)
		require.NoError(t, err)
		require.Equal(t, i+1, v)
	}
	logger.Println("TestLocalExpireChannel: testing cancellation")
	cancel()
	_, err := GetCachedCalcOpt(ctx, key, calc, true)
	require.Error(t, err, "context cancelled")
	require.True(t, errors.Is(err, context.Canceled), "context cancelled")
	DefaultCCs.Wait()
}

func TestTimeOuts(t *testing.T) {
	returnValue := 0
	var mu sync.Mutex
	calc := func(ctx context.Context) (int, CachedCalcOpts, error) {
		time.Sleep(tick * 2)
		mu.Lock()
		returnValue++
		mu.Unlock()
		logger.Printf("thread %v:calc will return %v", ctx.Value("thread"), returnValue)
		return returnValue, CachedCalcOpts{
			MaxTTL: expire,
			MinTTL: refresh,
		}, nil
	}
	key := getRandomKey(t)
	ctx := context.WithValue(context.TODO(), "thread", 1)
	v, err := GetCachedCalcOpt(ctx, key, calc, true)
	require.NoError(t, err)
	require.Equal(t, returnValue, v, "first calculation")
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.Equal(t, 1, v, "cached value")
	logger.Printf("waiting for MinTTL, so next call will start refreshing:%v", refresh)
	time.Sleep(refresh)
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.NoError(t, err)
	require.Equal(t, 1, v, "value should be the same calculation is in progress")
	logger.Printf("waiting a tick(%v) to receive the same value, calculation takes 2 ticks(%v)", tick, tick*2)
	time.Sleep(tick)
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.NoError(t, err)
	require.Equal(t, 1, v, "value still should be the same, calculation takes 2 ticks, had been waiting for 1 tick")
	logger.Println("waiting 2 ticks to receive refreshed value")
	time.Sleep(tick * 2)
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.NoError(t, err)
	require.Equal(t, 2, v, "value should be refreshed by now as have been waiting at least 3 ticks")
	logger.Println("waiting for MaxTTL, value should be recalculated one more time")
	time.Sleep(expire)
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.NoError(t, err)
	require.Equal(t, 3, v, "value should be refreshed")
}

func TestGetCachedCalcOptX(t *testing.T) {
	returnValue := 0
	var mu sync.Mutex
	const minTTL = tick * 2
	const maxTTL = tick * 6
	calc := func(ctx context.Context) (int, CachedCalcOpts, error) {
		logger.Println("asking calc for value in thread ", ctx.Value("thread"), "wait ", tick)
		time.Sleep(tick)
		mu.Lock()
		returnValue++
		defer mu.Unlock()
		logger.Println("calc will return ", returnValue)
		return returnValue, CachedCalcOpts{
			MaxTTL: maxTTL,
			MinTTL: minTTL,
		}, nil
	}
	ctx := context.WithValue(context.TODO(), "thread", 1)
	cc := NewCachedCalculations(ctx, nil, 4)
	v, err := GetCachedCalcOptX(cc, ctx, "1", calc, true)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	logger.Println("step 1 completed")
	now := time.Now()
	v, err = GetCachedCalcOptX(cc, ctx, "1", calc, true)
	testImmediate(t, now)
	require.Equal(t, 1, v)
	logger.Println("step 2 completed")
	logger.Println("waiting for key MinTTL expiration, it will return old value and run refresh calc in background so refreshed value will be available in 1 tick")
	time.Sleep(minTTL + tick)
	now = time.Now()
	v, err = GetCachedCalcOptX(cc, ctx, "1", calc, true)
	testImmediate(t, now)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	logger.Println("step 3 completed")
	logger.Println("waiting for 2 ticks, value should be refreshed after that")
	time.Sleep(tick * 2)
	now = time.Now()
	v, err = GetCachedCalcOptX(cc, ctx, "1", calc, true)
	testImmediate(t, now)
	require.NoError(t, err)
	require.Equal(t, 2, v)
	logger.Println("step 4 completed")
	logger.Println("waiting for key MaxTTL expiration, value should be recalculated")
	time.Sleep(maxTTL)
	now = time.Now()
	v, err = GetCachedCalcOptX(cc, ctx, "1", calc, true)
	require.Equal(t, 3, v)
	logger.Println("step 5 completed: value recalculated upon maxTTL expiration: ", time.Since(now))
	require.True(t, time.Since(now) >= tick, "calculation should be refreshed, takes more than 1 tick")
	DefaultCCs.Close()
}

func testImmediate(t *testing.T, now time.Time) {
	require.True(t, time.Since(now) < tick, "calculation should be immediate")
}

// sendExpirationSignal sends an expiration signal to the expireEntry channel and waits until the channel is closed,
// ensuring that the entry is expired before returning. It uses the fact, that server closes the channel after expiration job is done.
//
// Parameters:
// - ctx: The context to control cancellation.
// - expireEntry: The channel used to signal and wait for expiration.
func sendExpirationSignal(ctx context.Context, expireEntry chan struct{}) (result bool) {
	select {
	case expireEntry <- struct{}{}:
		result = true
		logger.Printf("sendExpirationSignal:thread %s:expiration signal sent", ctx.Value("thread"))
	case <-ctx.Done():
		logger.Printf("sendExpirationSignal:thread %s:ctx cancelled", ctx.Value("thread"))
	case <-time.After(tick):
		logger.Printf("sendExpirationSignal:thread %s:timeout", ctx.Value("thread"))
		return false
	}
	// now wait until expireEntry is closed
	select {
	case <-expireEntry:
		logger.Printf("sendExpirationSignal:thread %s:expire entry closed", ctx.Value("thread"))
	case <-ctx.Done():
		logger.Printf("sendExpirationSignal:thread %s:ctx cancelled on waiting expireEntry closed", ctx.Value("thread"))
	}
	return
}
