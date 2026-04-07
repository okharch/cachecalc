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
//  1. The adapter observes an expired entry while holding entry.Lock.
//  2. At the same time, CachedCalculations.RemoveEntries starts a maintenance
//     pass and holds cc.Lock before trying to lock the same entry.
//  3. If the adapter cleanup path keeps entry.Lock while trying to take cc.Lock,
//     both goroutines block each other.
//
// Required behavior:
// the adapter must release entry.Lock before trying to acquire cc.Lock for
// expired-entry cleanup, or otherwise the adapter and CachedCalculations
// cleanup paths can deadlock.
func TestCachedCalculationsExternalAdapterDeleteExpiredEntryDoesNotDeadlockWithRemoveEntries(t *testing.T) {
	cc := NewCachedCalculations(1, nil)
	defer cc.Close()
	adapter := NewCachedCalculationsExternalAdapter(cc)

	key := "deadlock-key"
	entry := &CacheEntry{Expire: time.Now().Add(-time.Second)}
	cc.entries[key] = entry

	entry.Lock()

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
		entry.Unlock()
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
