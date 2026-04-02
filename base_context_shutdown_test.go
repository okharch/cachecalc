package cachecalc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCloseCancelsBackgroundRefreshCalculation documents the intended split
// between request lifetime and cache/service lifetime for background refresh work.
//
// Scenario:
// 1. CachedCalculations already has a stale-but-not-expired value for a key.
// 2. A caller requests that key and receives the stale value immediately.
// 3. Because the value is stale, the cache starts a refresh calculation that is
//    expected to continue in background for the benefit of later callers.
// 4. The original caller cancels its request context after already receiving the
//    stale response. That cancellation must not stop the background refresh.
// 5. Later, the service itself shuts down and calls CachedCalculations.Close().
//    At that point all ongoing background calculations and lease renewals must be
//    canceled promptly.
//
// This test verifies both halves of that contract:
// - request cancellation does not kill the background refresh once stale data was served
// - cache/service shutdown does kill the background refresh
func TestCloseCancelsBackgroundRefreshCalculation(t *testing.T) {
	cc := NewCachedCalculations(1, nil)

	entry := &CacheEntry{
		Value:   mustSerializeInt(t, 1),
		Refresh: time.Now().Add(-time.Second),
		Expire:  time.Now().Add(time.Second),
	}
	cc.entries["shutdown-key"] = entry

	refreshStarted := make(chan struct{}, 1)
	refreshCanceled := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	value, err := GetCachedCalcOptX(cc, ctx, "shutdown-key", func(ctx context.Context) (int, CachedCalcOpts, error) {
		refreshStarted <- struct{}{}
		<-ctx.Done()
		refreshCanceled <- ctx.Err()
		return 0, CachedCalcOpts{}, ctx.Err()
	}, true)
	require.NoError(t, err)
	require.Equal(t, 1, value, "caller should receive stale value immediately")

	cancel()

	select {
	case <-refreshStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("background refresh did not start")
	}

	select {
	case err := <-refreshCanceled:
		t.Fatalf("background refresh should not stop on caller cancellation, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cc.Close()

	select {
	case err := <-refreshCanceled:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("background refresh should be canceled when CachedCalculations is closed")
	}
}
