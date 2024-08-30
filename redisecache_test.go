package cachecalc

import (
	"context"
	"testing"
	"time"
)

// type initCacheFunc func(*testing.T) func(context.Context) ExternalCache
func initRedisCache(t *testing.T) func(context.Context) ExternalCache {
	return func(ctx context.Context) ExternalCache {
		externalCache, err := NewRedisCache(ctx)
		if err != nil {
			t.Skipf("skip test due to external cache not being available: %s", err)
		}
		return externalCache
	}
}

// TestSet tests the Set method of the ExternalCache interface.
func TestSet(t *testing.T) {
	testSet(t, initRedisCache)
}

// TestGet tests the Get method of the ExternalCache interface.
func TestGet(t *testing.T) {
	testGet(t, initRedisCache)
}

// TestDel tests the Del method of the ExternalCache interface.
func TestDel(t *testing.T) {
	testDel(t, initRedisCache)
}

// TestClose tests the Close method of the ExternalCache interface.
func TestClose(t *testing.T) {
	testClose(t, initRedisCache)
}

// TestGetLock tests the GetLock method of the ExternalCache interface.
func TestGetLock(t *testing.T) {
	testGetLock(t, initRedisCache, time.Second)
}

// TestEntryUpdates tests the EntryUpdates method of the ExternalCache interface.
func TestEntryUpdates(t *testing.T) {
	testEntryUpdates(t, initRedisCache)
}
