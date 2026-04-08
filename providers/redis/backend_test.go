package redis_test

import (
	"context"
	"testing"

	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/internal/contracttest"
	"github.com/okharch/cachecalc/v4/providers/redis"
	"github.com/okharch/cachecalc/v4/smartcache"
	"github.com/okharch/cachecalc/v4/valuestore"
)

func TestRedisValueStoreContract(t *testing.T) {
	contracttest.RunValueStoreContract(t, func(t *testing.T) (valuestore.Store, func()) {
		backend := newRedisBackendOrSkip(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestRedisLockBackendContract(t *testing.T) {
	contracttest.RunLockBackendContract(t, func(t *testing.T) (distlock.Backend, func()) {
		backend := newRedisBackendOrSkip(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestRedisSmartcacheContract(t *testing.T) {
	contracttest.RunSmartcacheContract(t, func(t *testing.T) (*smartcache.Cache, *smartcache.Cache, func()) {
		backend := newRedisBackendOrSkip(t)
		cacheA := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		cacheB := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		return cacheA, cacheB, func() {
			cacheA.Close()
			cacheB.Close()
			_ = backend.Close()
		}
	})
}

func newRedisBackendOrSkip(t *testing.T) *redis.Backend {
	t.Helper()
	backend, err := redis.New(context.Background(), "")
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	return backend
}
