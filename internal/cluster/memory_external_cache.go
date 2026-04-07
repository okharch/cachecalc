package cluster

import (
	"context"
	"sync"
	"time"

	cachecalc "github.com/okharch/cachecalc"
)

type memoryEntry struct {
	value    []byte
	deadline time.Time
}

// MemoryExternalCache is the leader-side authoritative cache implementation.
// It satisfies cachecalc.ExternalCache entirely in memory, including TTL
// tracking, and is intentionally simple because replication is not attempted.
type MemoryExternalCache struct {
	mu     sync.Mutex
	values map[string]memoryEntry
}

// NewMemoryExternalCache creates the authoritative leader-local cache used when
// no custom ExternalCache implementation is supplied to the cluster package.
func NewMemoryExternalCache() *MemoryExternalCache {
	return &MemoryExternalCache{values: make(map[string]memoryEntry)}
}

func (m *MemoryExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = memoryEntry{value: cloneBytes(value), deadline: deadlineFromTTL(ttl)}
	return nil
}

func (m *MemoryExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.getEntryLocked(key); ok {
		return false, nil
	}
	m.values[key] = memoryEntry{value: cloneBytes(value), deadline: deadlineFromTTL(ttl)}
	return true, nil
}

func (m *MemoryExternalCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.getEntryLocked(key)
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(entry.value), true, nil
}

func (m *MemoryExternalCache) ExtendIfValue(ctx context.Context, key string, expectedValue []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.getEntryLocked(key)
	if !ok || !equalBytes(entry.value, expectedValue) {
		return false, nil
	}
	entry.deadline = deadlineFromTTL(ttl)
	m.values[key] = entry
	return true, nil
}

func (m *MemoryExternalCache) DelIfValue(ctx context.Context, key string, expectedValue []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.getEntryLocked(key)
	if !ok || !equalBytes(entry.value, expectedValue) {
		return false, nil
	}
	delete(m.values, key)
	return true, nil
}

func (m *MemoryExternalCache) SetIfLockOwned(ctx context.Context, lockKey string, expectedLockValue []byte, key string, value []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lockEntry, ok := m.getEntryLocked(lockKey)
	if !ok || !equalBytes(lockEntry.value, expectedLockValue) {
		return false, nil
	}
	m.values[key] = memoryEntry{value: cloneBytes(value), deadline: deadlineFromTTL(ttl)}
	return true, nil
}

func (m *MemoryExternalCache) Del(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.values, key)
	return nil
}

func (m *MemoryExternalCache) Close() error {
	return nil
}

// RemainingTTL reports the leader-side remaining TTL for a key so follower L2
// entries can be bounded to no longer than the authoritative cache entry.
func (m *MemoryExternalCache) RemainingTTL(key string) (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.getEntryLocked(key)
	if !ok {
		return 0, false
	}
	if entry.deadline.IsZero() {
		return 0, true
	}
	ttl := time.Until(entry.deadline)
	if ttl < 0 {
		delete(m.values, key)
		return 0, false
	}
	return ttl, true
}

func (m *MemoryExternalCache) getEntryLocked(key string) (memoryEntry, bool) {
	entry, ok := m.values[key]
	if !ok {
		return memoryEntry{}, false
	}
	if !entry.deadline.IsZero() && time.Now().After(entry.deadline) {
		delete(m.values, key)
		return memoryEntry{}, false
	}
	return entry, true
}

func deadlineFromTTL(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Now()
	}
	return time.Now().Add(ttl)
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return append([]byte(nil), value...)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ cachecalc.ExternalCache = (*MemoryExternalCache)(nil)
