package contracttest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/okharch/cachecalc/v4/valuestore"
)

type ValueStoreFactory func(t *testing.T) (valuestore.Store, func())

func RunValueStoreContract(t *testing.T, newStore ValueStoreFactory) {
	t.Helper()

	t.Run("GetMissOnEmptyStore", func(t *testing.T) {
		store, cleanup := newStore(t)
		defer cleanup()

		_, ok, err := store.Get(context.Background(), "missing")
		if err != nil {
			t.Fatalf("get missing: %v", err)
		}
		if ok {
			t.Fatal("expected missing key")
		}
	})

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		store, cleanup := newStore(t)
		defer cleanup()
		key := uniqueKey(t, "roundtrip")

		now := time.Now()
		want := valuestore.EntrySnapshot{
			Value:        []byte("value-1"),
			RefreshAt:    now.Add(100 * time.Millisecond),
			ExpireAt:     now.Add(500 * time.Millisecond),
			CalcDuration: 25 * time.Millisecond,
			Error:        "",
		}
		if err := store.Put(context.Background(), key, want); err != nil {
			t.Fatalf("put: %v", err)
		}

		got, ok, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok {
			t.Fatal("expected key to exist")
		}
		assertSnapshotEqual(t, got, want)
	})

	t.Run("PutOverwrite", func(t *testing.T) {
		store, cleanup := newStore(t)
		defer cleanup()
		key := uniqueKey(t, "overwrite")

		first := valuestore.EntrySnapshot{
			Value:     []byte("value-1"),
			RefreshAt: time.Now().Add(100 * time.Millisecond),
			ExpireAt:  time.Now().Add(500 * time.Millisecond),
		}
		second := valuestore.EntrySnapshot{
			Value:        []byte("value-2"),
			RefreshAt:    time.Now().Add(200 * time.Millisecond),
			ExpireAt:     time.Now().Add(700 * time.Millisecond),
			CalcDuration: 10 * time.Millisecond,
			Error:        "boom",
		}
		if err := store.Put(context.Background(), key, first); err != nil {
			t.Fatalf("put first: %v", err)
		}
		if err := store.Put(context.Background(), key, second); err != nil {
			t.Fatalf("put second: %v", err)
		}

		got, ok, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok {
			t.Fatal("expected key to exist")
		}
		assertSnapshotEqual(t, got, second)
	})

	t.Run("DeleteRemovesValue", func(t *testing.T) {
		store, cleanup := newStore(t)
		defer cleanup()
		key := uniqueKey(t, "delete")

		entry := valuestore.EntrySnapshot{
			Value:     []byte("value-1"),
			RefreshAt: time.Now().Add(100 * time.Millisecond),
			ExpireAt:  time.Now().Add(500 * time.Millisecond),
		}
		if err := store.Put(context.Background(), key, entry); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := store.Delete(context.Background(), key); err != nil {
			t.Fatalf("delete: %v", err)
		}

		_, ok, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("get after delete: %v", err)
		}
		if ok {
			t.Fatal("expected deleted key to be missing")
		}
	})

	t.Run("ExpiredSnapshotIsNotReturned", func(t *testing.T) {
		store, cleanup := newStore(t)
		defer cleanup()
		key := uniqueKey(t, "expired")

		entry := valuestore.EntrySnapshot{
			Value:     []byte("stale"),
			RefreshAt: time.Now().Add(-2 * time.Second),
			ExpireAt:  time.Now().Add(-time.Second),
		}
		if err := store.Put(context.Background(), key, entry); err != nil {
			t.Fatalf("put expired: %v", err)
		}

		_, ok, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("get expired: %v", err)
		}
		if ok {
			t.Fatal("expected expired key to be missing")
		}
	})
}

func uniqueKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("%s/%s/%d", t.Name(), suffix, time.Now().UnixNano())
}

func assertSnapshotEqual(t *testing.T, got, want valuestore.EntrySnapshot) {
	t.Helper()
	if string(got.Value) != string(want.Value) {
		t.Fatalf("value mismatch: got %q want %q", string(got.Value), string(want.Value))
	}
	if got.Error != want.Error {
		t.Fatalf("error mismatch: got %q want %q", got.Error, want.Error)
	}
	if got.CalcDuration != want.CalcDuration {
		t.Fatalf("calc duration mismatch: got %v want %v", got.CalcDuration, want.CalcDuration)
	}
	if !got.RefreshAt.Equal(want.RefreshAt) {
		t.Fatalf("refresh mismatch: got %v want %v", got.RefreshAt, want.RefreshAt)
	}
	if !got.ExpireAt.Equal(want.ExpireAt) {
		t.Fatalf("expire mismatch: got %v want %v", got.ExpireAt, want.ExpireAt)
	}
}
