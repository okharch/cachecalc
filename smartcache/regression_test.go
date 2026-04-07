package smartcache

import (
	"context"
	"errors"
	"sync"
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

func TestPublishRequiredKeepsPreviousLocalValueWhenSharedPublishFails(t *testing.T) {
	values := &failingStore{
		Store:   vmemory.New(),
		putErr:  errors.New("shared store unavailable"),
		failPut: true,
	}
	cache := New(Config{
		MaxWorkers: 1,
		Locks:      lockmem.NewProvider(),
		Values:     values,
	})
	defer cache.Close()

	initial, err := Get(context.Background(), cache, "required-publish", false, func(ctx context.Context) (string, Policy, error) {
		return "stable", Policy{
			MinTTL:      20 * time.Millisecond,
			MaxTTL:      100 * time.Millisecond,
			PublishMode: PublishBestEffort,
		}, nil
	})
	if err != nil {
		t.Fatalf("initial get: %v", err)
	}
	if initial != "stable" {
		t.Fatalf("initial = %q, want stable", initial)
	}

	time.Sleep(30 * time.Millisecond)

	stale, err := Get(context.Background(), cache, "required-publish", false, func(ctx context.Context) (string, Policy, error) {
		return "fresh", Policy{
			MinTTL:      20 * time.Millisecond,
			MaxTTL:      100 * time.Millisecond,
			PublishMode: PublishRequired,
		}, nil
	})
	if err != nil {
		t.Fatalf("stale read: %v", err)
	}
	if stale != "stable" {
		t.Fatalf("stale read = %q, want stable", stale)
	}

	time.Sleep(30 * time.Millisecond)

	got, err := Get(context.Background(), cache, "required-publish", false, func(ctx context.Context) (string, Policy, error) {
		return "fresh", Policy{
			MinTTL:      20 * time.Millisecond,
			MaxTTL:      100 * time.Millisecond,
			PublishMode: PublishBestEffort,
		}, nil
	})
	if err != nil {
		t.Fatalf("local value after failed required publish: %v", err)
	}
	if got != "stable" {
		t.Fatalf("local value changed to %q after failed required publish, want stable", got)
	}
}

// TestPublishRequiredDoesNotTreatPostPutLeaseLossAsFailedCommit documents the
// required-publication commit point for a newly calculated snapshot.
//
// Scenario:
//  1. A previous usable local snapshot already exists for the key.
//  2. A new calculation runs with PublishRequired, so the new snapshot should
//     become visible locally only once it has been written to the shared value
//     store.
//  3. The shared Put succeeds, which means the new snapshot is now globally
//     visible to other instances.
//  4. Immediately after that successful Put, the lease is marked lost before
//     the caller returns.
//
// Required behavior:
// once the shared Put succeeds, the calculation must be treated as committed.
// Losing the lease after that point must not roll the local instance back to
// the previous snapshot or return a result that leaves local and shared state
// inconsistent.
func TestPublishRequiredDoesNotTreatPostPutLeaseLossAsFailedCommit(t *testing.T) {
	baseStore := vmemory.New()
	var currentLease *fakeLease
	values := &leaseLossAfterPutStore{
		Store:       baseStore,
		afterNthPut: 2,
		afterPut: func() {
			if currentLease != nil {
				currentLease.markLost()
			}
		},
	}
	cache := New(Config{
		MaxWorkers: 1,
		Locks: fakeProvider{
			acquire: func() distlock.Lease {
				currentLease = &fakeLease{lost: make(chan struct{})}
				return currentLease
			},
		},
		Values: values,
	})
	defer cache.Close()

	initial, err := Get(context.Background(), cache, "publish-required-lease-loss", false, func(ctx context.Context) (string, Policy, error) {
		return "stable", Policy{
			MinTTL:      20 * time.Millisecond,
			MaxTTL:      100 * time.Millisecond,
			PublishMode: PublishBestEffort,
		}, nil
	})
	if err != nil {
		t.Fatalf("initial get: %v", err)
	}
	if initial != "stable" {
		t.Fatalf("initial = %q, want stable", initial)
	}

	time.Sleep(30 * time.Millisecond)

	stale, err := Get(context.Background(), cache, "publish-required-lease-loss", false, func(ctx context.Context) (string, Policy, error) {
		return "fresh", Policy{
			MinTTL:      20 * time.Millisecond,
			MaxTTL:      100 * time.Millisecond,
			PublishMode: PublishRequired,
		}, nil
	})
	if err != nil {
		t.Fatalf("stale read: %v", err)
	}
	if stale != "stable" {
		t.Fatalf("stale read = %q, want stable", stale)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		current, ok, err := baseStore.Get(context.Background(), "publish-required-lease-loss")
		if err != nil {
			t.Fatalf("shared get: %v", err)
		}
		if ok {
			got, err := decodeSnapshotValue[string](current)
			if err != nil {
				t.Fatalf("decode shared value: %v", err)
			}
			if got == "fresh" {
				local, ok, err := cache.LocalValues().Get(context.Background(), "publish-required-lease-loss")
				if err != nil {
					t.Fatalf("local get: %v", err)
				}
				if !ok {
					t.Fatal("local value missing after shared commit")
				}
				localValue, err := decodeSnapshotValue[string](local)
				if err != nil {
					t.Fatalf("decode local value: %v", err)
				}
				if localValue != "fresh" {
					t.Fatalf("shared value committed as fresh while local stayed %q", localValue)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("shared value was never updated to fresh")
}

func decodeSnapshotValue[T any](snapshot valuestore.EntrySnapshot) (T, error) {
	var result T
	err := decodeValue(snapshot.Value, &result)
	return result, err
}

type failingStore struct {
	valuestore.Store
	putErr  error
	failPut bool
}

func (s *failingStore) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	if s.failPut {
		return s.putErr
	}
	return s.Store.Put(ctx, key, entry)
}

type fakeProvider struct {
	acquire func() distlock.Lease
}

func (p fakeProvider) Acquire(context.Context, string, time.Duration) (distlock.Lease, bool, error) {
	return p.acquire(), true, nil
}

type fakeLease struct {
	once sync.Once
	lost chan struct{}
}

func (l *fakeLease) Lost() <-chan struct{} {
	return l.lost
}

func (l *fakeLease) Release(context.Context) error {
	l.markLost()
	return nil
}

func (l *fakeLease) markLost() {
	l.once.Do(func() {
		close(l.lost)
	})
}

type leaseLossAfterPutStore struct {
	valuestore.Store
	puts        atomic.Int32
	afterNthPut int32
	afterPut    func()
}

func (s *leaseLossAfterPutStore) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	if err := s.Store.Put(ctx, key, entry); err != nil {
		return err
	}
	if s.afterPut != nil && s.puts.Add(1) == s.afterNthPut {
		s.afterPut()
	}
	return nil
}
