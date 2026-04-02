package cachecalc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scriptedExternalCache struct {
	setFn   func(context.Context, string, []byte, time.Duration) error
	setNXFn func(context.Context, string, []byte, time.Duration) (bool, error)
	getFn   func(context.Context, string) ([]byte, bool, error)
	delFn   func(context.Context, string) error
}

func (s *scriptedExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if s.setFn != nil {
		return s.setFn(ctx, key, value, ttl)
	}
	return nil
}

func (s *scriptedExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	if s.setNXFn != nil {
		return s.setNXFn(ctx, key, value, ttl)
	}
	return false, nil
}

func (s *scriptedExternalCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if s.getFn != nil {
		return s.getFn(ctx, key)
	}
	return nil, false, nil
}

func (s *scriptedExternalCache) Del(ctx context.Context, key string) error {
	if s.delFn != nil {
		return s.delFn(ctx, key)
	}
	return nil
}

func (s *scriptedExternalCache) Close() error {
	return nil
}

// TestObtainExternalErrorSignalsCallerAndClearsWait documents a broken external-cache
// failure path in obtainExternal.
//
// Current behavior before the fix:
// 1. obtainExternal creates entry.wait to mark the key as in-flight locally.
// 2. externalCache.Get returns an error before any value is delivered to r.ready.
// 3. obtainExternal returns that error to its direct caller, but it does not clear
//    entry.wait and does not notify the waiting request channel.
//
// Why that is a bug:
// - The top-level GetCachedCalc* path waits on r.ready, so the initiating caller can hang
//   forever even though the backend operation already failed.
// - The stale entry.wait marker makes later callers believe a calculation is still in
//   progress, so they can also block indefinitely on the same key.
//
// This regression test asserts the required recovery behavior for such failures:
// the error must be surfaced to the waiting caller and the local in-flight marker must
// be cleared so future requests can retry normally.
func TestObtainExternalErrorSignalsCallerAndClearsWait(t *testing.T) {
	wantErr := errors.New("external get failed")
	cc := NewCachedCalculations(1, &scriptedExternalCache{
		getFn: func(context.Context, string) ([]byte, bool, error) {
			return nil, false, wantErr
		},
	})

	var result int
	ready := make(chan error, 1)
	err := cc.obtainExternal(context.Background(), &request{
		ctx:   context.Background(),
		key:   "key",
		dest:  &result,
		ready: ready,
		calculateValue: func(context.Context) (any, CachedCalcOpts, error) {
			t.Fatal("calculateValue should not be called when external Get fails")
			return 0, CachedCalcOpts{}, nil
		},
	})
	require.ErrorIs(t, err, wantErr)

	cc.Lock()
	entry := cc.entries["key"]
	cc.Unlock()
	require.NotNil(t, entry)
	require.Nil(t, entry.wait, "error path should clear the in-flight marker")

	select {
	case got := <-ready:
		require.ErrorIs(t, got, wantErr)
	default:
		t.Fatal("error path should signal the waiting caller")
	}
}

type memoryExternalCache struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newMemoryExternalCache() *memoryExternalCache {
	return &memoryExternalCache{values: make(map[string][]byte)}
}

func (m *memoryExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = append([]byte(nil), value...)
	return nil
}

func (m *memoryExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.values[key]; exists {
		return false, nil
	}
	m.values[key] = append([]byte(nil), value...)
	return true, nil
}

func (m *memoryExternalCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.values[key]
	if !exists {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (m *memoryExternalCache) Del(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.values, key)
	return nil
}

func (m *memoryExternalCache) Close() error {
	return nil
}
