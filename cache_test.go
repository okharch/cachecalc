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
	sc1 = NewCachedCalculations(3, externalCache)
	sc2 = NewCachedCalculations(3, externalCache)
	require.NotNil(t, sc1)
	require.NotNil(t, sc2)
	return
}

func TestLocalSimple(t *testing.T) {
	ctx := context.TODO()
	counter := 0
	const p = time.Millisecond * 30
	ccS := NewCachedCalculations(3, nil)
	require.NotNil(t, ccS)
	var d1, d2, d3 int
	var wg sync.WaitGroup
	getI := func(ctx context.Context) (int, error) { time.Sleep(p); counter++; return counter, nil }
	GetI := func(result *int) {
		defer wg.Done()
		r, err := GetCachedCalc(ccS, ctx, "key", p*2, p*3, true, getI)
		require.NoError(t, err)
		*result = r
	}
	wg.Add(3)
	go GetI(&d1)
	go GetI(&d2)
	go GetI(&d3)
	wg.Wait()
	require.Equal(t, 1, d1)
	require.Equal(t, 1, d2)
	require.Equal(t, 1, d3)
}
