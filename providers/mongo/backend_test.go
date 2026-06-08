package mongo_test

import (
	"context"
	"testing"

	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/internal/contracttest"
	"github.com/okharch/cachecalc/v4/providers/mongo"
	"github.com/okharch/cachecalc/v4/smartcache"
	"github.com/okharch/cachecalc/v4/valuestore"
)

func TestMongoValueStoreContract(t *testing.T) {
	contracttest.RunValueStoreContract(t, func(t *testing.T) (valuestore.Store, func()) {
		backend := newMongoBackendOrSkip(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestMongoLockBackendContract(t *testing.T) {
	contracttest.RunLockBackendContract(t, func(t *testing.T) (distlock.Backend, func()) {
		backend := newMongoBackendOrSkip(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestMongoSmartcacheContract(t *testing.T) {
	contracttest.RunSmartcacheContract(t, func(t *testing.T) (*smartcache.Cache, *smartcache.Cache, func()) {
		backend := newMongoBackendOrSkip(t)
		cacheA := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		cacheB := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		return cacheA, cacheB, func() {
			cacheA.Close()
			cacheB.Close()
			_ = backend.Close()
		}
	})
}

func newMongoBackendOrSkip(t *testing.T) *mongo.Backend {
	t.Helper()
	backend, err := mongo.New(context.Background(), "")
	if err != nil {
		t.Skipf("mongo unavailable: %v", err)
	}
	return backend
}
