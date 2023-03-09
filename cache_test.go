package cachecalc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init2Caches(t *testing.T, ctx context.Context) (sc1, sc2 *CachedCalculations) {
	externalCache, err := NewRedisCache(ctx)
	if err != nil {
		t.Skipf("skip test due external cache not available: %s", err)
	}
	require.NotNil(t, externalCache)
	sc1 = NewCachedCalculations(ctx, 3, externalCache)
	sc2 = NewCachedCalculations(ctx, 3, externalCache)
	require.NotNil(t, sc1)
	require.NotNil(t, sc2)
	return
}

func TestSimple(t *testing.T) {
	ctx := context.TODO()
	counter := 0
	const p = time.Millisecond * 300
	cc := CachedCalculation{
		Key:         "key",
		Calculate:   func(ctx context.Context) (any, error) { time.Sleep(p); counter++; return counter, nil },
		LimitWorker: false,
		MinTTL:      p * 2,
		MaxTTL:      p * 4,
	}
	ccS := NewCachedCalculations(ctx, 3, nil)
	require.NotNil(t, ccS)
	var d1, d2, d3 int
	var wg sync.WaitGroup
	Get := func(result *int) {
		defer wg.Done()
		r, err := ccS.Get(ctx, cc)
		require.NoError(t, err)
		var ok bool
		*result, ok = r.(int)
		require.True(t, ok)
	}
	wg.Add(3)
	go Get(&d1)
	go Get(&d2)
	go Get(&d3)
	wg.Wait()
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
}
