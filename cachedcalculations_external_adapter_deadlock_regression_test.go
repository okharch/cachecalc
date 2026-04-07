package cachecalc

import (
	"testing"
	"time"
)

// TestCachedCalculationsExternalAdapterDeleteExpiredEntryDoesNotDeadlockWithRemoveEntries
// documents a lock-ordering requirement between the leader-side adapter and
// CachedCalculations maintenance.
//
// Scenario:
//  1. The adapter observes an expired entry and enters deleteExpiredEntry while
//     already holding entry.Lock.
//  2. At the same time, CachedCalculations.RemoveEntries starts a maintenance
//     pass and holds cc.Lock before trying to lock the same entry.
//  3. If deleteExpiredEntry then tries to take cc.Lock while RemoveEntries is
//     waiting on entry.Lock, both goroutines block each other.
//
// Required behavior:
// deleteExpiredEntry must not acquire cc.Lock while still holding entry.Lock,
// or otherwise the adapter and CachedCalculations cleanup paths can deadlock.
func TestCachedCalculationsExternalAdapterDeleteExpiredEntryDoesNotDeadlockWithRemoveEntries(t *testing.T) {
	cc := NewCachedCalculations(1, nil)
	defer cc.Close()
	adapter := NewCachedCalculationsExternalAdapter(cc)

	key := "deadlock-key"
	entry := &CacheEntry{Expire: time.Now().Add(-time.Second)}
	cc.entries[key] = entry

	entry.Lock()
	defer func() {
		// Cleanup in case the test fails after proving the deadlock.
		select {
		case <-time.After(10 * time.Millisecond):
		default:
		}
		entry.Unlock()
	}()

	ccLocked := make(chan struct{})
	removeDone := make(chan struct{})
	go func() {
		cc.Lock()
		close(ccLocked)
		entry.Lock()
		entry.Unlock()
		cc.Unlock()
		close(removeDone)
	}()

	<-ccLocked

	deleteDone := make(chan struct{})
	go func() {
		adapter.deleteExpiredEntry(key, entry)
		close(deleteDone)
	}()

	select {
	case <-deleteDone:
	case <-removeDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("lock-order inversion reproduced: deleteExpiredEntry waits on cc.Lock while RemoveEntries-style path waits on entry.Lock")
	}
}
