package contracttest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/okharch/cachecalc/distlock"
)

type LockBackendFactory func(t *testing.T) (distlock.Backend, func())

func RunLockBackendContract(t *testing.T, newBackend LockBackendFactory) {
	t.Helper()

	t.Run("AcquireRenewRelease", func(t *testing.T) {
		backend, cleanup := newBackend(t)
		defer cleanup()

		key := uniqueLockKey(t, "acquire")
		ownerA := []byte("owner-a")
		ownerB := []byte("owner-b")
		ttl := 200 * time.Millisecond

		acquired, err := backend.TryAcquire(context.Background(), key, ownerA, ttl)
		if err != nil {
			t.Fatalf("acquire first: %v", err)
		}
		if !acquired {
			t.Fatal("expected first acquire to succeed")
		}

		acquired, err = backend.TryAcquire(context.Background(), key, ownerB, ttl)
		if err != nil {
			t.Fatalf("acquire second: %v", err)
		}
		if acquired {
			t.Fatal("expected second acquire to fail while held")
		}

		renewed, err := backend.Renew(context.Background(), key, ownerA, ttl)
		if err != nil {
			t.Fatalf("renew owner: %v", err)
		}
		if !renewed {
			t.Fatal("expected owner renewal to succeed")
		}

		renewed, err = backend.Renew(context.Background(), key, ownerB, ttl)
		if err != nil {
			t.Fatalf("renew non-owner: %v", err)
		}
		if renewed {
			t.Fatal("expected non-owner renewal to fail")
		}

		released, err := backend.Release(context.Background(), key, ownerB)
		if err != nil {
			t.Fatalf("release non-owner: %v", err)
		}
		if released {
			t.Fatal("expected non-owner release to fail")
		}

		released, err = backend.Release(context.Background(), key, ownerA)
		if err != nil {
			t.Fatalf("release owner: %v", err)
		}
		if !released {
			t.Fatal("expected owner release to succeed")
		}
	})

	t.Run("ExpiredLockCanBeReacquired", func(t *testing.T) {
		backend, cleanup := newBackend(t)
		defer cleanup()

		key := uniqueLockKey(t, "expiry")
		ownerA := []byte("owner-a")
		ownerB := []byte("owner-b")

		acquired, err := backend.TryAcquire(context.Background(), key, ownerA, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if !acquired {
			t.Fatal("expected acquire to succeed")
		}

		time.Sleep(80 * time.Millisecond)

		acquired, err = backend.TryAcquire(context.Background(), key, ownerB, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("reacquire after expiry: %v", err)
		}
		if !acquired {
			t.Fatal("expected reacquire after expiry to succeed")
		}
	})
}

func uniqueLockKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("%s/%s/%d", t.Name(), suffix, time.Now().UnixNano())
}
