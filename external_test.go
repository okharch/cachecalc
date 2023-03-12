package cachecalc

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
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

func TestRemote(t *testing.T) {
	ctx := context.TODO()
	cc1, cc2 := init2Caches(t, ctx)
	var d1, d2, d3 int
	var wg sync.WaitGroup
	GetI1 := initCalcTest(t, &wg, cc1)
	GetI2 := initCalcTest(t, &wg, cc2)
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
	time.Sleep(refresh)
	GetI2(&d1, 1)
	require.Equal(t, 1, d1)
	GetI1(&d2, 2)
	require.Equal(t, 1, d1)
	time.Sleep(tick)
	GetI1(&d3, 3)
	wg.Wait()
}
