package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/internal/contracttest"
	"github.com/okharch/cachecalc/v4/providers/sqlite"
	"github.com/okharch/cachecalc/v4/smartcache"
	"github.com/okharch/cachecalc/v4/valuestore"
)

func TestSQLiteValueStoreContract(t *testing.T) {
	contracttest.RunValueStoreContract(t, func(t *testing.T) (valuestore.Store, func()) {
		backend := newSQLiteBackend(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestSQLiteLockBackendContract(t *testing.T) {
	contracttest.RunLockBackendContract(t, func(t *testing.T) (distlock.Backend, func()) {
		backend := newSQLiteBackend(t)
		return backend, func() { _ = backend.Close() }
	})
}

func TestSQLiteSmartcacheContract(t *testing.T) {
	contracttest.RunSmartcacheContract(t, func(t *testing.T) (*smartcache.Cache, *smartcache.Cache, func()) {
		backend := newSQLiteBackend(t)
		cacheA := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		cacheB := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: backend.LockProvider(), Values: backend})
		return cacheA, cacheB, func() {
			cacheA.Close()
			cacheB.Close()
			_ = backend.Close()
		}
	})
}

func newSQLiteBackend(t *testing.T) *sqlite.Backend {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smartcache.sqlite")
	backend, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("new sqlite backend: %v", err)
	}
	return backend
}
