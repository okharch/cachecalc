package cachecalc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestObtainExternalDoesNotDeleteLockReacquiredByAnotherOwner documents a lock-ownership
// bug in the distributed coordination path.
//
// When the bug happens:
// 1. Process A acquires the external lock for a key and starts a long calculation.
// 2. The lock lease expires before process A finishes.
// 3. Process B acquires a fresh lock for the same key.
// 4. Process A finally exits and runs its deferred cleanup, which unconditionally calls
//    Del(lockKey) without checking whether it still owns that lock.
//
// Why that is a bug:
// - Process A can delete process B's valid lock.
// - Once process B's lock is removed, a third process can also enter the same critical
//   section, so the "single calculator per key" guarantee is lost.
//
// This regression test simulates that sequence by replacing the lock value after the
// first calculation has started and then asserting that cleanup from the first owner does
// not remove the lock that now belongs to someone else.
func TestObtainExternalDoesNotDeleteLockReacquiredByAnotherOwner(t *testing.T) {
	cache := newMemoryExternalCache()
	cc := NewCachedCalculations(1, cache)

	calcStarted := make(chan struct{}, 1)
	releaseCalc := make(chan struct{})
	done := make(chan error, 1)
	ready := make(chan error, 1)
	var result int

	go func() {
		done <- cc.obtainExternal(context.Background(), &request{
			ctx:   context.Background(),
			key:   "shared-key",
			dest:  &result,
			ready: ready,
			calculateValue: func(context.Context) (any, CachedCalcOpts, error) {
				calcStarted <- struct{}{}
				<-releaseCalc
				return 1, CachedCalcOpts{
					MinTTL: time.Millisecond,
					MaxTTL: 50 * time.Millisecond,
				}, nil
			},
		})
	}()

	select {
	case <-calcStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for calculation to start")
	}

	cache.mu.Lock()
	cache.values[getKeyLock("shared-key")] = []byte("other-owner")
	cache.mu.Unlock()

	close(releaseCalc)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for obtainExternal to finish")
	}

	lockValue, exists, err := cache.Get(context.Background(), getKeyLock("shared-key"))
	require.NoError(t, err)
	require.True(t, exists, "reacquired lock should still exist")
	require.Equal(t, []byte("other-owner"), lockValue, "current implementation deletes another owner's lock")
}
