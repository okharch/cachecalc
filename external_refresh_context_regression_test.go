package cachecalc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetCachedCalcOptXRefreshContinuesAfterCallerCancellation documents a
// stale-while-revalidate regression in the refresh path.
//
// Scenario:
//  1. The cache already has a value for a key.
//  2. That value is stale enough to require refresh (Refresh is in the past),
//     but it is still usable and must be served immediately (Expire is in the future).
//  3. A caller requests the key and receives that stale value right away.
//  4. The library starts a refresh calculation that is meant to continue in background
//     so the cache is improved for the next caller.
//  5. The original caller then cancels its context, for example because the HTTP request
//     finished or the client disconnected after already receiving the stale response.
//
// Current broken behavior:
// - the refresh uses that same caller context,
// - so cancellation of the original request aborts the refresh,
// - and the cache remains stale for future callers.
//
// Correct behavior is for the refresh to outlive the caller once the stale value has
// already been returned and the work has been promoted to background maintenance.
func TestGetCachedCalcOptXRefreshContinuesAfterCallerCancellation(t *testing.T) {
	cache := newMemoryExternalCache()
	cc := NewCachedCalculations(1, cache)

	entry := &CacheEntry{
		Value:   mustSerializeInt(t, 1),
		Refresh: time.Now().Add(-time.Second),
		Expire:  time.Now().Add(time.Second),
	}
	cc.entries["stale-key"] = entry

	serializedEntry, err := serializeEntry(entry)
	require.NoError(t, err)
	require.NoError(t, cache.Set(context.Background(), "stale-key", serializedEntry, time.Minute))

	ctx, cancel := context.WithCancel(context.Background())
	refreshed := make(chan struct{}, 1)

	value, err := GetCachedCalcOptX(cc, ctx, "stale-key", func(ctx context.Context) (int, CachedCalcOpts, error) {
		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
			return 0, CachedCalcOpts{}, ctx.Err()
		}
		refreshed <- struct{}{}
		return 2, CachedCalcOpts{
			MinTTL: time.Minute,
			MaxTTL: time.Minute,
		}, nil
	}, true)
	require.NoError(t, err)
	require.Equal(t, 1, value, "first caller should get stale value immediately")

	cancel()

	select {
	case <-refreshed:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("background refresh should continue after the original caller cancels its context")
	}

	fresh, err := GetCachedCalcOptX(cc, context.Background(), "stale-key", func(context.Context) (int, CachedCalcOpts, error) {
		t.Fatal("refreshed value should already be cached")
		return 0, CachedCalcOpts{}, nil
	}, true)
	require.NoError(t, err)
	require.Equal(t, 2, fresh)
}
