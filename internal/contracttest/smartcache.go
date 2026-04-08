package contracttest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okharch/cachecalc/smartcache"
)

type CacheFactory func(t *testing.T) (a, b *smartcache.Cache, cleanup func())

func RunSmartcacheContract(t *testing.T, newCaches CacheFactory) {
	t.Helper()

	t.Run("ConcurrentDedupAcrossCaches", func(t *testing.T) {
		cacheA, cacheB, cleanup := newCaches(t)
		defer cleanup()
		key := uniqueSmartcacheKey(t, "shared")

		var calls atomic.Int32
		calc := func(label string) func(context.Context) (string, smartcache.Policy, error) {
			return func(ctx context.Context) (string, smartcache.Policy, error) {
				calls.Add(1)
				time.Sleep(40 * time.Millisecond)
				return label, smartcache.Policy{MinTTL: 200 * time.Millisecond, MaxTTL: 400 * time.Millisecond}, nil
			}
		}

		results := make(chan string, 2)
		errs := make(chan error, 2)
		go func() {
				v, err := smartcache.Get(context.Background(), cacheA, key, calc("a"))
			results <- v
			errs <- err
		}()
		go func() {
				v, err := smartcache.Get(context.Background(), cacheB, key, calc("b"))
			results <- v
			errs <- err
		}()

		firstErr := <-errs
		secondErr := <-errs
		if firstErr != nil || secondErr != nil {
			t.Fatalf("unexpected errors: %v / %v", firstErr, secondErr)
		}
		first := <-results
		second := <-results
		if first != second {
			t.Fatalf("expected same value from both caches, got %q vs %q", first, second)
		}
		if calls.Load() != 1 {
			t.Fatalf("expected one calculation, got %d", calls.Load())
		}
	})

	t.Run("StaleValueTriggersBackgroundRefresh", func(t *testing.T) {
		cacheA, _, cleanup := newCaches(t)
		defer cleanup()
		key := uniqueSmartcacheKey(t, "stale")

		var counter atomic.Int32
		calc := func(ctx context.Context) (string, smartcache.Policy, error) {
			n := counter.Add(1)
			if n > 1 {
				time.Sleep(20 * time.Millisecond)
			}
			return "value-" + string(rune('0'+n)), smartcache.Policy{MinTTL: 30 * time.Millisecond, MaxTTL: 120 * time.Millisecond}, nil
		}

		v1, err := smartcache.Get(context.Background(), cacheA, key, calc)
		if err != nil || v1 != "value-1" {
			t.Fatalf("first value = %q err=%v", v1, err)
		}

		time.Sleep(40 * time.Millisecond)
		v2, err := smartcache.Get(context.Background(), cacheA, key, calc)
		if err != nil || v2 != "value-1" {
			t.Fatalf("stale read = %q err=%v", v2, err)
		}

		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			v3, err := smartcache.Get(context.Background(), cacheA, key, calc)
			if err == nil && v3 == "value-2" {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("background refresh did not publish a newer value")
	})
}

func uniqueSmartcacheKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("%s/%s/%d", t.Name(), suffix, time.Now().UnixNano())
}
