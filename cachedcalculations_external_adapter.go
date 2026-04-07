package cachecalc

import (
	"bytes"
	"context"
	"sync"
	"time"
)

type leaderLockEntry struct {
	value    []byte
	deadline time.Time
}

// CachedCalculationsExternalAdapter exposes a CachedCalculations instance as an
// ExternalCache implementation for leader-side cluster serving. Value entries
// are backed directly by the live CachedCalculations state, while distributed
// lock ownership is kept in a separate lightweight map.
type CachedCalculationsExternalAdapter struct {
	cc *CachedCalculations

	metaMu  sync.Mutex
	rawKeys map[string]struct{}
	locks   map[string]leaderLockEntry
}

// NewCachedCalculationsExternalAdapter returns an ExternalCache view over the
// provided CachedCalculations instance. It is intended for leader-side L2
// serving so the leader can expose its warm local cache entries to followers.
func NewCachedCalculationsExternalAdapter(cc *CachedCalculations) *CachedCalculationsExternalAdapter {
	return &CachedCalculationsExternalAdapter{
		cc:      cc,
		rawKeys: make(map[string]struct{}),
		locks:   make(map[string]leaderLockEntry),
	}
}

func (a *CachedCalculationsExternalAdapter) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	entry := &CacheEntry{}
	rawValue := false
	if err := deserializeEntry(value, entry); err != nil {
		rawValue = true
		now := time.Now()
		entry.Value = append([]byte(nil), value...)
		entry.Refresh = now.Add(ttl)
		entry.Expire = now.Add(ttl)
		entry.Err = nil
	} else {
		clampEntryExpiry(entry, ttl)
	}
	target := a.obtainOrCreateEntry(key)
	defer target.Unlock()
	applyEntrySnapshot(target, entry)
	if target.wait != nil {
		close(target.wait)
		target.wait = nil
	}
	a.setRawKey(key, rawValue)
	return nil
}

func (a *CachedCalculationsExternalAdapter) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	if lock, ok := a.getLockLocked(key); ok && !isExpired(lock.deadline) {
		return false, nil
	}
	a.locks[key] = leaderLockEntry{value: append([]byte(nil), value...), deadline: lockDeadlineFromTTL(ttl)}
	return true, nil
}

func (a *CachedCalculationsExternalAdapter) Get(ctx context.Context, key string) ([]byte, bool, error) {
	entry, exists := a.obtainEntry(key)
	if !exists {
		return nil, false, nil
	}
	if entry.Expire.IsZero() || entry.Expire.Before(time.Now()) {
		entry.Unlock()
		a.deleteExpiredEntry(key, entry)
		return nil, false, nil
	}
	if a.isRawKey(key) {
		value := append([]byte(nil), entry.Value...)
		entry.Unlock()
		return value, true, nil
	}
	buf, err := serializeEntry(entry)
	entry.Unlock()
	if err != nil {
		return nil, false, err
	}
	return buf, true, nil
}

func (a *CachedCalculationsExternalAdapter) ExtendIfValue(ctx context.Context, key string, expectedValue []byte, ttl time.Duration) (bool, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	lock, ok := a.getLockLocked(key)
	if !ok || isExpired(lock.deadline) || !equalBytes(lock.value, expectedValue) {
		if ok && isExpired(lock.deadline) {
			delete(a.locks, key)
		}
		return false, nil
	}
	lock.deadline = lockDeadlineFromTTL(ttl)
	a.locks[key] = lock
	return true, nil
}

func (a *CachedCalculationsExternalAdapter) DelIfValue(ctx context.Context, key string, expectedValue []byte) (bool, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	lock, ok := a.getLockLocked(key)
	if !ok || isExpired(lock.deadline) || !equalBytes(lock.value, expectedValue) {
		if ok && isExpired(lock.deadline) {
			delete(a.locks, key)
		}
		return false, nil
	}
	delete(a.locks, key)
	return true, nil
}

func (a *CachedCalculationsExternalAdapter) SetIfLockOwned(ctx context.Context, lockKey string, expectedLockValue []byte, key string, value []byte, ttl time.Duration) (bool, error) {
	entry := &CacheEntry{}
	rawValue := false
	if err := deserializeEntry(value, entry); err != nil {
		rawValue = true
		now := time.Now()
		entry.Value = append([]byte(nil), value...)
		entry.Refresh = now.Add(ttl)
		entry.Expire = now.Add(ttl)
		entry.Err = nil
	} else {
		clampEntryExpiry(entry, ttl)
	}

	target := a.obtainOrCreateEntry(key)
	defer target.Unlock()

	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	lock, ok := a.getLockLocked(lockKey)
	if !ok || isExpired(lock.deadline) || !equalBytes(lock.value, expectedLockValue) {
		if ok && isExpired(lock.deadline) {
			delete(a.locks, lockKey)
		}
		return false, nil
	}

	applyEntrySnapshot(target, entry)
	if target.wait != nil {
		close(target.wait)
		target.wait = nil
	}
	a.setRawKeyLocked(key, rawValue)
	return true, nil
}

func (a *CachedCalculationsExternalAdapter) Del(ctx context.Context, key string) error {
	a.metaMu.Lock()
	delete(a.rawKeys, key)
	delete(a.locks, key)
	a.metaMu.Unlock()

	a.cc.Lock()
	entry, exists := a.cc.entries[key]
	if !exists {
		a.cc.Unlock()
		return nil
	}
	entry.Lock()
	delete(a.cc.entries, key)
	a.cc.Unlock()
	entry.Unlock()
	return nil
}

func (a *CachedCalculationsExternalAdapter) Close() error {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	a.rawKeys = make(map[string]struct{})
	a.locks = make(map[string]leaderLockEntry)
	return nil
}

// RemainingTTL reports the TTL of the leader-backed value entry so follower
// read-through caches can avoid outliving the authoritative entry.
func (a *CachedCalculationsExternalAdapter) RemainingTTL(key string) (time.Duration, bool) {
	entry, exists := a.obtainEntry(key)
	if !exists {
		return 0, false
	}
	if entry.Expire.IsZero() || entry.Expire.Before(time.Now()) {
		entry.Unlock()
		a.deleteExpiredEntry(key, entry)
		return 0, false
	}
	ttl := time.Until(entry.Expire)
	entry.Unlock()
	return ttl, true
}

func (a *CachedCalculationsExternalAdapter) obtainEntry(key string) (*CacheEntry, bool) {
	a.cc.Lock()
	entry, exists := a.cc.entries[key]
	if !exists {
		a.cc.Unlock()
		return nil, false
	}
	entry.Lock()
	a.cc.Unlock()
	return entry, true
}

func (a *CachedCalculationsExternalAdapter) obtainOrCreateEntry(key string) *CacheEntry {
	a.cc.Lock()
	entry, exists := a.cc.entries[key]
	if !exists {
		entry = &CacheEntry{}
		a.cc.entries[key] = entry
	}
	entry.Lock()
	a.cc.Unlock()
	return entry
}

func (a *CachedCalculationsExternalAdapter) deleteExpiredEntry(key string, entry *CacheEntry) {
	a.cc.Lock()
	defer a.cc.Unlock()
	current, exists := a.cc.entries[key]
	if !exists || current != entry {
		return
	}
	current.Lock()
	defer current.Unlock()
	if current.wait != nil || current.Expire.IsZero() || current.Expire.After(time.Now()) {
		return
	}
	delete(a.cc.entries, key)
	a.metaMu.Lock()
	delete(a.rawKeys, key)
	a.metaMu.Unlock()
}

func (a *CachedCalculationsExternalAdapter) isRawKey(key string) bool {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	_, ok := a.rawKeys[key]
	return ok
}

func (a *CachedCalculationsExternalAdapter) setRawKey(key string, raw bool) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	a.setRawKeyLocked(key, raw)
}

func (a *CachedCalculationsExternalAdapter) setRawKeyLocked(key string, raw bool) {
	if raw {
		a.rawKeys[key] = struct{}{}
		return
	}
	delete(a.rawKeys, key)
}

func (a *CachedCalculationsExternalAdapter) getLockLocked(key string) (leaderLockEntry, bool) {
	lock, ok := a.locks[key]
	return lock, ok
}

func applyEntrySnapshot(dst, src *CacheEntry) {
	dst.Expire = src.Expire
	dst.Refresh = src.Refresh
	dst.CalcDuration = src.CalcDuration
	dst.Err = src.Err
	dst.Value = append([]byte(nil), src.Value...)
}

func clampEntryExpiry(entry *CacheEntry, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	deadline := time.Now().Add(ttl)
	if entry.Expire.IsZero() || entry.Expire.After(deadline) {
		entry.Expire = deadline
	}
	if entry.Refresh.IsZero() || entry.Refresh.After(entry.Expire) {
		entry.Refresh = entry.Expire
	}
}

func isExpired(deadline time.Time) bool {
	return !deadline.IsZero() && time.Now().After(deadline)
}

func lockDeadlineFromTTL(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Now()
	}
	return time.Now().Add(ttl)
}

func equalBytes(a, b []byte) bool {
	return bytes.Equal(a, b)
}

var _ ExternalCache = (*CachedCalculationsExternalAdapter)(nil)
