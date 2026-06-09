package smartcache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	lockmem "github.com/okharch/cachecalc/v4/distlock/memory"
	vmemory "github.com/okharch/cachecalc/v4/valuestore/memory"
)

// TestBackgroundRefreshAcrossCachesBacksOffWhenAnotherCacheOwnsRefresh
// reproduces a cluster-wide stale refresh race:
//  1. cacheA seeds a shared value.
//  2. cacheB warms its local snapshot from the shared store.
//  3. The value becomes stale on both caches.
//  4. cacheA starts a background refresh and keeps the distributed lease.
//  5. cacheB also attempts background refresh for the same key.
//
// The second cache must not run its own calculation while another cache already
// owns the refresh. It should return the stale value and back off until the
// winner publishes the fresh shared snapshot.
func TestBackgroundRefreshAcrossCachesBacksOffWhenAnotherCacheOwnsRefresh(t *testing.T) {
	values := vmemory.New()
	locks := lockmem.NewProvider()

	cacheA := New(Config{MaxWorkers: 2, Locks: locks, Values: values})
	cacheB := New(Config{MaxWorkers: 2, Locks: locks, Values: values})
	defer cacheA.Close()
	defer cacheB.Close()

	const key = "refresh-backoff"

	initial, err := Get(context.Background(), cacheA, key, func(ctx context.Context) (string, Policy, error) {
		return "value-1", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("seed cacheA: %v", err)
	}
	if initial != "value-1" {
		t.Fatalf("seed cacheA = %q, want value-1", initial)
	}

	warm, err := Get(context.Background(), cacheB, key, func(ctx context.Context) (string, Policy, error) {
		t.Fatal("cacheB should warm from shared value, not calculate")
		return "", Policy{}, nil
	})
	if err != nil {
		t.Fatalf("warm cacheB: %v", err)
	}
	if warm != "value-1" {
		t.Fatalf("warm cacheB = %q, want value-1", warm)
	}

	time.Sleep(50 * time.Millisecond)

	refreshStarted := make(chan struct{})
	refreshRelease := make(chan struct{})
	stale, err := Get(context.Background(), cacheA, key, func(ctx context.Context) (string, Policy, error) {
		close(refreshStarted)
		<-refreshRelease
		return "value-2", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("trigger cacheA refresh: %v", err)
	}
	if stale != "value-1" {
		t.Fatalf("cacheA stale read = %q, want value-1", stale)
	}

	select {
	case <-refreshStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cacheA background refresh did not start")
	}

	var cacheBCalcCalls atomic.Int32
	stale, err = Get(context.Background(), cacheB, key, func(ctx context.Context) (string, Policy, error) {
		cacheBCalcCalls.Add(1)
		return "value-b", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("trigger cacheB refresh: %v", err)
	}
	if stale != "value-1" {
		t.Fatalf("cacheB stale read = %q, want value-1", stale)
	}

	time.Sleep(50 * time.Millisecond)
	if got := cacheBCalcCalls.Load(); got != 0 {
		t.Fatalf("cacheB background refresh calculated %d times, want 0 while cacheA owns the distributed refresh", got)
	}

	close(refreshRelease)

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		current, ok, err := values.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("shared get: %v", err)
		}
		if ok {
			got, err := decodeSnapshotValue[string](current)
			if err != nil {
				t.Fatalf("decode shared value: %v", err)
			}
			if got == "value-2" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("cacheA background refresh never published the new value")
}

// TestBackgroundRefreshWaitsForWorkerSlotWhenLimited reproduces worker-pool
// saturation during stale background refresh:
//  1. MaxWorkers is set to 1.
//  2. An unrelated foreground calculation occupies the only worker slot.
//  3. Another key becomes stale and triggers background refresh.
//
// The background refresh goroutine may start, but its calculation must not run
// until the worker slot is released.
func TestBackgroundRefreshWaitsForWorkerSlotWhenLimited(t *testing.T) {
	cache := New(Config{MaxWorkers: 1})
	defer cache.Close()

	seed, err := Get(context.Background(), cache, "stale-key", func(ctx context.Context) (string, Policy, error) {
		return "stale-1", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("seed stale-key: %v", err)
	}
	if seed != "stale-1" {
		t.Fatalf("seed stale-key = %q, want stale-1", seed)
	}

	time.Sleep(50 * time.Millisecond)

	busyStarted := make(chan struct{})
	busyRelease := make(chan struct{})
	busyDone := make(chan struct{})
	go func() {
		defer close(busyDone)
		_, _ = Get(context.Background(), cache, "busy-key", func(ctx context.Context) (string, Policy, error) {
			close(busyStarted)
			<-busyRelease
			return "busy", Policy{
				MinTTL: 30 * time.Millisecond,
				MaxTTL: 300 * time.Millisecond,
			}, nil
		})
	}()

	select {
	case <-busyStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("busy calculation did not start")
	}

	backgroundStarted := make(chan struct{})
	stale, err := Get(context.Background(), cache, "stale-key", func(ctx context.Context) (string, Policy, error) {
		close(backgroundStarted)
		return "stale-2", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("trigger background refresh: %v", err)
	}
	if stale != "stale-1" {
		t.Fatalf("stale read = %q, want stale-1", stale)
	}

	select {
	case <-backgroundStarted:
		t.Fatal("background refresh started calculation before a worker slot was free")
	case <-time.After(50 * time.Millisecond):
	}

	close(busyRelease)

	select {
	case <-busyDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("busy calculation did not finish")
	}

	select {
	case <-backgroundStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("background refresh did not start after the worker slot was released")
	}
}

// TestNonPositiveMaxWorkersDisablesWorkerLimiting reproduces a refresh path
// with no worker cap:
//  1. MaxWorkers is set to zero, which now means unlimited workers.
//  2. A foreground calculation is already running.
//  3. A stale key triggers background refresh.
//
// The background refresh calculation should start immediately instead of
// waiting for the unrelated foreground calculation to release a worker slot.
func TestNonPositiveMaxWorkersDisablesWorkerLimiting(t *testing.T) {
	cache := New(Config{MaxWorkers: 0})
	defer cache.Close()

	seed, err := Get(context.Background(), cache, "stale-key", func(ctx context.Context) (string, Policy, error) {
		return "stale-1", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("seed stale-key: %v", err)
	}
	if seed != "stale-1" {
		t.Fatalf("seed stale-key = %q, want stale-1", seed)
	}

	time.Sleep(50 * time.Millisecond)

	busyStarted := make(chan struct{})
	busyRelease := make(chan struct{})
	go func() {
		_, _ = Get(context.Background(), cache, "busy-key", func(ctx context.Context) (string, Policy, error) {
			close(busyStarted)
			<-busyRelease
			return "busy", Policy{
				MinTTL: 30 * time.Millisecond,
				MaxTTL: 300 * time.Millisecond,
			}, nil
		})
	}()

	select {
	case <-busyStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("busy calculation did not start")
	}

	backgroundStarted := make(chan struct{})
	stale, err := Get(context.Background(), cache, "stale-key", func(ctx context.Context) (string, Policy, error) {
		close(backgroundStarted)
		return "stale-2", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 300 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("trigger background refresh: %v", err)
	}
	if stale != "stale-1" {
		t.Fatalf("stale read = %q, want stale-1", stale)
	}

	select {
	case <-backgroundStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("background refresh did not start immediately with MaxWorkers disabled")
	}

	close(busyRelease)
}

// TestGlobalMaxWorkersLimitsCalculationsAcrossCaches reproduces cluster-wide
// capacity throttling across different keys:
//  1. Two caches share the same distributed lock provider.
//  2. GlobalMaxWorkers is set to 1 while local MaxWorkers is disabled.
//  3. cacheA starts a calculation for one key and holds the only global slot.
//  4. cacheB starts a calculation for a different key.
//
// The second calculation must not start until the first calculation releases
// the shared global worker slot.
func TestGlobalMaxWorkersLimitsCalculationsAcrossCaches(t *testing.T) {
	locks := lockmem.NewProvider()
	cacheA := New(Config{MaxWorkers: 0, GlobalMaxWorkers: 1, Locks: locks})
	cacheB := New(Config{MaxWorkers: 0, GlobalMaxWorkers: 1, Locks: locks})
	defer cacheA.Close()
	defer cacheB.Close()

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = Get(context.Background(), cacheA, "key-a", func(ctx context.Context) (string, Policy, error) {
			close(firstStarted)
			<-firstRelease
			return "a", Policy{MinTTL: 30 * time.Millisecond, MaxTTL: 300 * time.Millisecond}, nil
		})
	}()

	select {
	case <-firstStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("first calculation did not start")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		_, _ = Get(context.Background(), cacheB, "key-b", func(ctx context.Context) (string, Policy, error) {
			close(secondStarted)
			return "b", Policy{MinTTL: 30 * time.Millisecond, MaxTTL: 300 * time.Millisecond}, nil
		})
	}()

	select {
	case <-secondStarted:
		t.Fatal("second calculation started before the global worker slot was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(firstRelease)

	select {
	case <-firstDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("first calculation did not finish")
	}

	select {
	case <-secondStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second calculation did not start after the global worker slot was released")
	}

	select {
	case <-secondDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second calculation did not finish")
	}
}

// TestBackgroundRefreshErrorPreservesStaleValue verifies that when a background
// refresh calculation returns an error, the cache keeps serving the previous
// stale-but-usable value instead of replacing it with an error snapshot.
// It also verifies that OnRefreshError is called with the key and error.
func TestBackgroundRefreshErrorPreservesStaleValue(t *testing.T) {
	var refreshErr atomic.Value
	var refreshKey atomic.Value
	cache := New(Config{
		MaxWorkers: 2,
		OnRefreshError: func(key string, err error) {
			refreshKey.Store(key)
			refreshErr.Store(err.Error())
		},
	})
	defer cache.Close()

	const key = "stale-key"

	seed, err := Get(context.Background(), cache, key, func(ctx context.Context) (string, Policy, error) {
		return "good-value", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 500 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if seed != "good-value" {
		t.Fatalf("seed = %q, want good-value", seed)
	}

	time.Sleep(50 * time.Millisecond)

	calcStarted := make(chan struct{})
	stale, err := Get(context.Background(), cache, key, func(ctx context.Context) (string, Policy, error) {
		close(calcStarted)
		return "", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 500 * time.Millisecond,
		}, errors.New("fetch failed")
	})
	if err != nil {
		t.Fatalf("stale read should succeed: %v", err)
	}
	if stale != "good-value" {
		t.Fatalf("stale read = %q, want good-value", stale)
	}

	select {
	case <-calcStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("background refresh did not start")
	}

	time.Sleep(50 * time.Millisecond)

	next, err := Get(context.Background(), cache, key, func(ctx context.Context) (string, Policy, error) {
		return "recovered", Policy{
			MinTTL: 30 * time.Millisecond,
			MaxTTL: 500 * time.Millisecond,
		}, nil
	})
	if err != nil {
		t.Fatalf("post-error read should succeed: %v", err)
	}
	if next != "good-value" && next != "recovered" {
		t.Fatalf("post-error read = %q, want good-value or recovered", next)
	}

	if v, ok := refreshKey.Load().(string); !ok || v != key {
		t.Fatalf("OnRefreshError key = %v, want %q", refreshKey.Load(), key)
	}
	if v, ok := refreshErr.Load().(string); !ok || v != "fetch failed" {
		t.Fatalf("OnRefreshError err = %v, want 'fetch failed'", refreshErr.Load())
	}
}

// TestNonPositiveGlobalMaxWorkersDisablesClusterLimit reproduces a cache setup
// where cluster-wide worker limiting is disabled:
//  1. Two caches share the same distributed lock provider.
//  2. GlobalMaxWorkers is set to zero, which means disabled.
//  3. cacheA starts one calculation and keeps running.
//  4. cacheB starts a calculation for a different key.
//
// The second calculation should start immediately because no cluster-wide
// worker budget is enforced when GlobalMaxWorkers is non-positive.
func TestNonPositiveGlobalMaxWorkersDisablesClusterLimit(t *testing.T) {
	locks := lockmem.NewProvider()
	cacheA := New(Config{MaxWorkers: 0, GlobalMaxWorkers: 0, Locks: locks})
	cacheB := New(Config{MaxWorkers: 0, GlobalMaxWorkers: 0, Locks: locks})
	defer cacheA.Close()
	defer cacheB.Close()

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	go func() {
		_, _ = Get(context.Background(), cacheA, "key-a", func(ctx context.Context) (string, Policy, error) {
			close(firstStarted)
			<-firstRelease
			return "a", Policy{MinTTL: 30 * time.Millisecond, MaxTTL: 300 * time.Millisecond}, nil
		})
	}()

	select {
	case <-firstStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("first calculation did not start")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		_, _ = Get(context.Background(), cacheB, "key-b", func(ctx context.Context) (string, Policy, error) {
			close(secondStarted)
			return "b", Policy{MinTTL: 30 * time.Millisecond, MaxTTL: 300 * time.Millisecond}, nil
		})
	}()

	select {
	case <-secondStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second calculation did not start immediately with GlobalMaxWorkers disabled")
	}

	close(firstRelease)

	select {
	case <-secondDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second calculation did not finish")
	}
}
