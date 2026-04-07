package cachecalc

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCachedCalculationsExternalAdapterSetIfLockOwnedIsAtomic documents the
// required contract for SetIfLockOwned: once the lock is lost, the value must
// not be published.
//
// The current v2.0.0 implementation is expected to fail this test because it
// checks lock ownership and then releases the lock map mutex before writing the
// value entry, which leaves a race window where the lock can be deleted.
func TestCachedCalculationsExternalAdapterSetIfLockOwnedIsAtomic(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	cc := NewCachedCalculations(1, nil)
	defer cc.Close()
	adapter := NewCachedCalculationsExternalAdapter(cc)

	ctx := context.Background()
	lockKey := "atomic.lock"
	valueKey := "atomic.value"
	lockValue := []byte("owner")
	value := []byte("payload")

	created, err := adapter.SetNX(ctx, lockKey, lockValue, time.Minute)
	require.NoError(t, err)
	require.True(t, created)

	// Pre-create and lock the destination entry so SetIfLockOwned can complete
	// its lock check and then block before publishing the value.
	cc.Lock()
	entry := &CacheEntry{}
	entry.Lock()
	cc.entries[valueKey] = entry
	cc.Unlock()

	done := make(chan struct{})
	var stored bool
	var storeErr error
	go func() {
		stored, storeErr = adapter.SetIfLockOwned(ctx, lockKey, lockValue, valueKey, value, time.Minute)
		close(done)
	}()

	// With one P and the destination entry locked, this yields to the goroutine
	// so it can pass the ownership check and block on entry publication.
	runtime.Gosched()

	removed, err := adapter.DelIfValue(ctx, lockKey, lockValue)
	require.NoError(t, err)
	require.True(t, removed, "lock should be removable before the blocked publish resumes")

	entry.Unlock()
	<-done

	require.NoError(t, storeErr)
	require.False(t, stored, "value publication must fail after lock ownership is lost")

	got, exists, err := adapter.Get(ctx, valueKey)
	require.NoError(t, err)
	require.False(t, exists, "value must not be written after lock loss")
	require.Nil(t, got)
}
