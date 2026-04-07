package smartcache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okharch/cachecalc/distlock"
	lockmem "github.com/okharch/cachecalc/distlock/memory"
	"github.com/okharch/cachecalc/valuestore"
	vmemory "github.com/okharch/cachecalc/valuestore/memory"
)

func TestBackgroundRefreshRecomputesWhenSharedValueIsStale(t *testing.T) {
	values := vmemory.New()
	locks := lockmem.NewProvider()

	cacheA := New(Config{MaxWorkers: 2, Locks: locks, Values: values})
	cacheB := New(Config{MaxWorkers: 2, Locks: locks, Values: values})
	defer cacheA.Close()
	defer cacheB.Close()

	var calls atomic.Int32
	calc := func(label string) func(context.Context) (string, Policy, error) {
		return func(ctx context.Context) (string, Policy, error) {
			n := calls.Add(1)
			return label + "-" + string(rune('0'+n)), Policy{
				MinTTL: 30 * time.Millisecond,
				MaxTTL: 300 * time.Millisecond,
			}, nil
		}
	}

	v1, err := Get(context.Background(), cacheA, "shared-stale-refresh", true, calc("value"))
	if err != nil {
		t.Fatalf("initial get: %v", err)
	}
	if v1 != "value-1" {
		t.Fatalf("initial value = %q, want value-1", v1)
	}

	vWarm, err := Get(context.Background(), cacheB, "shared-stale-refresh", true, calc("value"))
	if err != nil {
		t.Fatalf("warm follower get: %v", err)
	}
	if vWarm != "value-1" {
		t.Fatalf("warm follower value = %q, want value-1", vWarm)
	}

	time.Sleep(50 * time.Millisecond)

	v2, err := Get(context.Background(), cacheB, "shared-stale-refresh", true, calc("value"))
	if err != nil {
		t.Fatalf("stale get: %v", err)
	}
	if v2 != "value-1" {
		t.Fatalf("stale read returned %q, want stale value value-1", v2)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		current, ok, err := values.Get(context.Background(), "shared-stale-refresh")
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

	t.Fatalf("background refresh never published a new shared value; calls=%d", calls.Load())
}

func TestLockReacquireDoesNotWaitForHourAfterOwnerLoss(t *testing.T) {
	backend := lockmem.NewBackend()
	provider := distlock.NewProvider(backend)

	cache := New(Config{
		MaxWorkers: 1,
		Locks:      provider,
	})
	defer cache.Close()

	reqCtx, cancel := context.WithCancel(context.Background())
	req := &request{ctx: reqCtx}

	lease, acquired, err := cache.acquire(reqCtx, "lock-ttl-regression", req)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if !acquired {
		t.Fatal("expected initial lease acquisition to succeed")
	}

	cancel()
	select {
	case <-lease.Lost():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("lease was not marked lost after owner context cancellation")
	}

	time.Sleep(150 * time.Millisecond)

	reacquired, err := backend.TryAcquire(context.Background(), "lock-ttl-regression.lock", []byte("new-owner"), 200*time.Millisecond)
	if err != nil {
		t.Fatalf("reacquire directly from backend: %v", err)
	}
	if !reacquired {
		t.Fatal("lock was still held after owner loss; ttl appears much longer than calculation policy")
	}
}

func decodeSnapshotValue[T any](snapshot valuestore.EntrySnapshot) (T, error) {
	var result T
	err := decodeValue(snapshot.Value, &result)
	return result, err
}
