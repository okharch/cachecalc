package postgres_test

import (
	"context"
	"testing"

	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/internal/contracttest"
	"github.com/okharch/cachecalc/v4/providers/postgres"
	"github.com/okharch/cachecalc/v4/smartcache"
	"github.com/okharch/cachecalc/v4/valuestore"
)

func TestPostgresValueStoreContract(t *testing.T) {
	contracttest.RunValueStoreContract(t, func(t *testing.T) (valuestore.Store, func()) {
		backend := newPostgresBackendOrSkip(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestPostgresLockBackendContract(t *testing.T) {
	contracttest.RunLockBackendContract(t, func(t *testing.T) (distlock.Backend, func()) {
		backend := newPostgresBackendOrSkip(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestPostgresSmartcacheContract(t *testing.T) {
	contracttest.RunSmartcacheContract(t, func(t *testing.T) (*smartcache.Cache, *smartcache.Cache, func()) {
		backend := newPostgresBackendOrSkip(t)
		cacheA := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		cacheB := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		return cacheA, cacheB, func() {
			cacheA.Close()
			cacheB.Close()
			_ = backend.Close()
		}
	})
}

func newPostgresBackendOrSkip(t *testing.T) *postgres.Backend {
	t.Helper()
	backend, err := postgres.New(context.Background(), "")
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	return backend
}
