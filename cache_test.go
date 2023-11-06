package cachecalc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const tick = time.Millisecond * 20
const refresh = tick * 3
const expire = tick * 20

var counter int
var mu sync.Mutex

func initCalcTest(t *testing.T, wg *sync.WaitGroup, cc *CachedCalculations, key string) func(result *int, thread int) {
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
		time.Sleep(tick)
		mu.Lock()
		counter++
		mu.Unlock()
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
	cc := NewCachedCalculations(4, nil)
	GetI := initCalcTest(t, &wg, cc, getRandomKey(t))
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
}

func TestLocal(t *testing.T) {
	var d1, d2, d3 int
	var wg sync.WaitGroup
	cc := NewCachedCalculations(4, nil)
	GetI := initCalcTest(t, &wg, cc, getRandomKey(t))
	wg.Add(4)
	GetI(&d1, 1)
	require.Equal(t, 1, d1)
	time.Sleep(refresh)
	GetI(&d2, 2) // will return the same but will trigger calculation
	require.Equal(t, 1, d2)
	GetI(&d3, 3)
	require.Equal(t, 1, d3)
	time.Sleep(tick * 2) // its enough time to refresh value
	GetI(&d3, 3)
	require.Equal(t, 2, d3)
	wg.Wait()
	cc.Close()
}

func TestSimpleCalc(t *testing.T) {
	returnValue := 0
	var mu sync.Mutex
	calc := func(ctx context.Context) (int, CachedCalcOpts, error) {
		time.Sleep(time.Millisecond * 20)
		mu.Lock()
		returnValue++
		mu.Unlock()
		logger.Println("calc will return ", returnValue)
		return returnValue, CachedCalcOpts{
			MaxTTL: 500 * time.Millisecond,
			MinTTL: 200 * time.Millisecond,
		}, nil
	}
	key := getRandomKey(t)
	ctx := context.WithValue(context.TODO(), "thread", 1)
	v, err := GetCachedCalcOpt(ctx, key, calc, true)
	require.NoError(t, err)
	require.Equal(t, returnValue, v)
	logger.Println("step 1 completed")
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.Equal(t, 1, v)
	logger.Println("step 2 completed")
	logger.Println("waiting for key expiration, so value will be refreshed")
	time.Sleep(500 * time.Millisecond)
	v, err = GetCachedCalcOpt(ctx, key, calc, true)
	require.Equal(t, 2, v)
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
	cc := NewCachedCalculations(4, nil)
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
