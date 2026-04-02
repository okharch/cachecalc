package cachecalc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestObtainExternalDoesNotOverwriteNewerValueAfterOwnershipCheck documents a
// race in the publish path:
// 1. worker A finishes a calculation and verifies that it still owns the lock;
// 2. before worker A publishes the external value, the lease expires and worker B
//    publishes a newer result;
// 3. worker A still executes an unconditional Set and overwrites B's value.
//
// Correct behaviour is to make "still own the lock" and "publish the result"
// a single atomic backend operation, or otherwise refuse to publish once that
// guarantee cannot be made.
func TestObtainExternalDoesNotOverwriteNewerValueAfterOwnershipCheck(t *testing.T) {
	cache := newPublishRaceExternalCache(t, "race-key")
	cc := NewCachedCalculations(1, cache)

	var result int
	err := cc.obtainExternal(context.Background(), &request{
		ctx:   context.Background(),
		key:   "race-key",
		dest:  &result,
		ready: make(chan error, 1),
		calculateValue: func(context.Context) (any, CachedCalcOpts, error) {
			return 1, CachedCalcOpts{
				MinTTL: 10 * time.Millisecond,
				MaxTTL: 100 * time.Millisecond,
			}, nil
		},
	})
	require.NoError(t, err)

	var entry CacheEntry
	serialized, exists, err := cache.Get(context.Background(), "race-key")
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, deserializeEntry(serialized, &entry))

	var published int
	require.NoError(t, DeserializeValue(entry.Value, &published))
	require.Equal(t, 2, published, "older owner overwrote a newer external value after a non-atomic ownership check")
}

type publishRaceExternalCache struct {
	mu             sync.Mutex
	values         map[string][]byte
	raceKey        string
	lockKey        string
	armPublishRace bool
	t              *testing.T
}

func newPublishRaceExternalCache(t *testing.T, key string) *publishRaceExternalCache {
	return &publishRaceExternalCache{
		values:  make(map[string][]byte),
		raceKey: key,
		lockKey: getKeyLock(key),
		t:       t,
	}
}

func (p *publishRaceExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if key == p.raceKey && p.armPublishRace {
		p.armPublishRace = false
		newer, err := serializeEntry(&CacheEntry{
			Value:   mustSerializeInt(p.t, 2),
			Refresh: time.Now().Add(time.Minute),
			Expire:  time.Now().Add(time.Minute),
		})
		require.NoError(p.t, err)
		p.values[p.raceKey] = newer
		p.values[p.lockKey] = []byte("owner-b")
	}
	p.values[key] = append([]byte(nil), value...)
	return nil
}

func (p *publishRaceExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.values[key]; exists {
		return false, nil
	}
	p.values[key] = append([]byte(nil), value...)
	return true, nil
}

func (p *publishRaceExternalCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, exists := p.values[key]
	if !exists {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (p *publishRaceExternalCache) ExtendIfValue(ctx context.Context, key string, expectedValue []byte, ttl time.Duration) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, exists := p.values[key]
	if !exists || string(value) != string(expectedValue) {
		return false, nil
	}
	if key == p.lockKey {
		p.armPublishRace = true
	}
	return true, nil
}

func (p *publishRaceExternalCache) DelIfValue(ctx context.Context, key string, expectedValue []byte) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, exists := p.values[key]
	if !exists || string(value) != string(expectedValue) {
		return false, nil
	}
	delete(p.values, key)
	return true, nil
}

func (p *publishRaceExternalCache) Del(ctx context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.values, key)
	return nil
}

func (p *publishRaceExternalCache) Close() error {
	return nil
}

func mustSerializeInt(t *testing.T, value int) []byte {
	t.Helper()
	serialized, err := serialize(value)
	require.NoError(t, err)
	return serialized
}
