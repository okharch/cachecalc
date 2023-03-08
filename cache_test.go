package cachecalc

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init2Caches(t *testing.T, ctx context.Context) (sc1, sc2 *CachedCalculation) {
	externalCache, err := NewRedisCache(ctx)
	if err != nil {
		t.Skipf("skip test due external cache not available: %s", err)
	}
	require.NotNil(t, externalCache)
	sc1 = NewCache(ctx, 3, externalCache)
	sc2 = NewCache(ctx, 3, externalCache)
	require.NotNil(t, sc1)
	require.NotNil(t, sc2)
	return
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	sc1, sc2 := init2Caches(t, ctx)
	c := 0
	var i1, i2 int
	key := "test"
	const maxTTL = time.Millisecond * 200
	const minTTL = time.Millisecond * 100
	fget := func(ctx context.Context) (any, error) {
		c++
		return c, nil
	}
	var err error
	getI_1 := func() {
		log.Print("increased cached counter 1")
		err = sc1.Get(ctx, key, fget, &i1, true, maxTTL, minTTL)
	}
	getI_2 := func() {
		log.Print("increased cached counter 2")
		err = sc2.Get(ctx, key, fget, &i2, true, maxTTL, minTTL)
	}
	getI_1()
	require.NoError(t, err)
	require.Equal(t, 1, i1)
	getI_2()
	require.NoError(t, err)
	require.Equal(t, 1, i2)
	time.Sleep(minTTL)
	getI_2()
	require.NoError(t, err)
	require.Equal(t, 1, i2)
	time.Sleep(minTTL)
	getI_2()
	require.NoError(t, err)
	require.Equal(t, 2, i2)
	cancel()
	sc1.Wait()
	sc2.Wait()
}

func TestWaitCalculation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	sc1, sc2 := init2Caches(t, ctx)
	c := 0
	const minTTL = time.Millisecond * 200
	const maxTTL = minTTL * 4
	slowGet := func(ctx context.Context) (any, error) {
		time.Sleep(minTTL)
		c++
		return c, nil
	}
	key := "testSlow"
	var i1, i2 int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := sc1.Get(ctx, key, slowGet, &i1, true, maxTTL, minTTL)
		require.NoError(t, err)
	}()
	time.Sleep(minTTL / 2)
	go func() {
		defer wg.Done()
		err := sc2.Get(ctx, key, slowGet, &i2, true, maxTTL, minTTL)
		require.NoError(t, err)
	}()
	wg.Wait()
	require.Equal(t, 1, i1)
	require.Equal(t, i1, i2)
	cancel()
	sc1.Wait()
	sc2.Wait()
}
