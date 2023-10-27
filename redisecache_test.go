package cachecalc

import (
	"context"
	"crypto/rand"
	"github.com/stretchr/testify/require"
	"log"
	"sync"
	"testing"
	"time"
)

func TestRedisExtCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	logger.Println("TestRedisExtCache...")
	ecache, err := NewRedisCache(ctx)
	if err != nil {
		t.Skipf("Redis not available: %s", err)
	}
	require.NotNil(t, ecache)
	var wg sync.WaitGroup
	test := func() {
		defer wg.Done()
		// try to get some random key with the same value
		keyb := make([]byte, 8)
		_, err = rand.Read(keyb)
		require.NoError(t, err)
		val := make([]byte, 128)
		_, err = rand.Read(val)
		require.NoError(t, err)
		key := string(keyb)
		err = ecache.Set(ctx, key, val, time.Minute)
		require.NoError(t, err)
		// check if it has written the key: exists and value is the same
		valGot, exists, err := ecache.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, val, valGot)
		// remove key
		err = ecache.Del(ctx, key)
		require.NoError(t, err)
		// make sure it does not exist
		val, exists, err = ecache.Get(ctx, key)
		require.NoError(t, err)
		require.False(t, exists)
		// make expiration test
		err = ecache.Set(ctx, key, val, time.Millisecond*200)
		require.NoError(t, err)
		time.Sleep(time.Millisecond * 500)
		_, exists, err = ecache.Get(ctx, key)
		require.NoError(t, err)
		require.False(t, exists)
	}
	// ordinary test
	wg.Add(101)
	test()
	// put some concurrency
	for i := 0; i < 100; i++ {
		go test()
	}
	wg.Wait()
	// cancel context and check whether it returns error
	cancel()
	err = ecache.Set(ctx, "test", []byte("test"), time.Minute)
	require.Error(t, err)
}

func initRedisCache(t *testing.T) func(context.Context) ExternalCache {
	return func(ctx context.Context) ExternalCache {
		externalCache, err := NewRedisCache(ctx)
		if err != nil {
			t.Skipf("skip test due external cache not available: %s", err)
		}
		return externalCache
	}
}

func TestExternalCacheRedis(t *testing.T) {
	log.Println("TestExternalCacheRedis...")
	ctx := context.TODO()
	testExternalCache(t, ctx, initRedisCache(t))

}

func TestRemoteRedis(t *testing.T) {
	log.Println("TestRemoteRedis...")
	ctx := context.TODO()
	testRemote(t, ctx, initRedisCache(t))
}

func TestRemoteConcurrentRedis(t *testing.T) {
	log.Println("TestRemoteConcurrentRedis...")
	ctx := context.TODO()
	testRemoteConcurrent(t, ctx, initRedisCache(t))
}
