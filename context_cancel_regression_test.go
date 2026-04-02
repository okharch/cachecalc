package cachecalc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObtainLocalReturnsWhenContextIsCanceledWhileWaiting(t *testing.T) {
	cc := NewCachedCalculations(1, nil)
	wait := make(chan struct{})
	cc.entries["key"] = &CacheEntry{
		Expire: time.Now().Add(-time.Second),
		wait:   wait,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var result int
	done := make(chan error, 1)
	go func() {
		done <- cc.obtainLocal(ctx, &request{
			ctx:   ctx,
			key:   "key",
			dest:  &result,
			ready: make(chan error, 1),
			calculateValue: func(context.Context) (any, CachedCalcOpts, error) {
				t.Fatal("calculateValue should not be called while waiting on another computation")
				return 0, CachedCalcOpts{}, nil
			},
		})
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		close(wait)
		<-done
		t.Fatal("waiting caller should stop promptly when its context is canceled")
	}
}
