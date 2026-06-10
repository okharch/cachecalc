package cluster

import (
	"testing"
	"time"

	"github.com/okharch/cachecalc/v4/valuestore"
)

// TestRemoteValueStoreReadThroughCacheClonesSnapshots documents the immutability
// contract for follower-side read-through cache entries.
//
// Scenario:
//  1. A follower caches a snapshot fetched from the leader.
//  2. The original snapshot value passed into the cache is mutated by the
//     caller after caching.
//  3. A later Get returns the cached snapshot, and that returned []byte is also
//     mutated by the caller.
//  4. Another Get reads the same key from the read-through cache again.
//
// Required behavior:
// the cached snapshot must be isolated from both the original caller-provided
// slice and any slices returned by prior reads. Otherwise one caller can
// silently corrupt the follower's local cache and affect later readers.
func TestRemoteValueStoreReadThroughCacheClonesSnapshots(t *testing.T) {
	store := newRemoteValueStore(nil, 0, 0, time.Second)
	original := valuestore.EntrySnapshot{
		Value:    []byte("alpha"),
		ExpireAt: time.Now().Add(time.Second),
	}

	store.setCached("item", original)
	original.Value[0] = 'z'

	first, ok := store.getCached("item")
	if !ok {
		t.Fatal("expected cached entry after setCached")
	}
	first.Value[1] = 'y'

	second, ok := store.getCached("item")
	if !ok {
		t.Fatal("expected cached entry on second read")
	}
	if string(second.Value) != "alpha" {
		t.Fatalf("cached value was mutated through aliased slices: got %q, want alpha", string(second.Value))
	}
}
