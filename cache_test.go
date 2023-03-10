package cachecalc

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Init2Caches(t *testing.T, ctx context.Context) (sc1, sc2 *CachedCalculations) {
	externalCache, err := NewRedisCache(ctx)
	if err != nil {
		t.Skipf("skip test due external cache not available: %s", err)
	}
	require.NotNil(t, externalCache)
	sc1 = NewCachedCalculations(3, externalCache)
	sc2 = NewCachedCalculations(3, externalCache)
	require.NotNil(t, sc1)
	require.NotNil(t, sc2)
	return
}

func TestRemote(t *testing.T) {
	ctx := context.TODO()
	cc1, cc2 := Init2Caches(t, ctx)
	_ = cc1
	_ = cc2
}

func TestLocalSimple(t *testing.T) {
	logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	ctx := context.TODO()
	counter := 0
	const p = time.Millisecond * 30
	const refresh = p * 2
	const expire = p * 3
	var d1, d2, d3 int
	var wg sync.WaitGroup
	getI := func(ctx context.Context) (int, error) { time.Sleep(p); counter++; return counter, nil }
	GetI := func(result *int, thread int) {
		defer wg.Done()
		ctx := context.WithValue(ctx, "thread", thread)
		r, err := GetCachedCalc(ctx, "key", refresh, expire, true, getI)
		require.NoError(t, err)
		*result = r
	}
	wg.Add(3)
	go GetI(&d1, 1)
	go GetI(&d2, 2)
	go GetI(&d3, 3)
	wg.Wait()
	time.Sleep(expire)
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
}

func TestLocal(t *testing.T) {
	logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	ctx := context.TODO()
	counter := 0
	const p = time.Millisecond * 30
	const refresh = p * 2
	const expire = p * 3
	var d1, d2, d3 int
	var wg sync.WaitGroup
	getI := func(ctx context.Context) (int, error) { time.Sleep(p); counter++; return counter, nil }
	GetI := func(result *int, thread int) {
		defer wg.Done()
		ctx := context.WithValue(ctx, "thread", thread)
		r, err := GetCachedCalc(ctx, "key", refresh, expire, true, getI)
		require.NoError(t, err)
		*result = r
	}
	// simple test cache
	wg.Add(3)
	GetI(&d1, 1)
	GetI(&d2, 2)
	GetI(&d3, 3)
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
	wg.Wait()
	// wait value need to be refreshed and d1 should be the same, but d2 different
	time.Sleep(refresh + time.Millisecond)
	wg.Add(1)
	go GetI(&d1, 1) // should trigger refresh
	wg.Wait()
	require.Equal(t, 1, d1)
	time.Sleep(p + time.Millisecond) // which in p should increase counter
	require.Equal(t, 2, counter)
	//require.Equal(t, 1, d1)
	//require.Equal(t, 1, d2)
	//require.Equal(t, 1, d3)
}
