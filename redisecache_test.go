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
	//var mu sync.Mutex
	test := func() {
		defer wg.Done()
		// try to get some random key with the same value
		keyb := make([]byte, 8)
		_, err := rand.Read(keyb)
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

func TestExpirationRedis(t *testing.T) {
	log.Println("TestExpirationRedis...")
	// init redis external cache
	f := initRedisCache(t)
	ctxCancel, cancel := context.WithCancel(context.TODO())
	ctx := context.WithValue(ctxCancel, "thread", 1)
	cc := f(ctx)
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
