package smartcache

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/okharch/cachecalc/distlock"
	"github.com/okharch/cachecalc/valuestore"
)

type PublishMode int

const (
	PublishBestEffort PublishMode = iota
	PublishRequired
)

// Policy controls when a value is served, refreshed, and fully expired.
type Policy struct {
	MinTTL      time.Duration
	MaxTTL      time.Duration
	CalcTime    time.Duration
	PublishMode PublishMode
}

type CalculateValue[T any] func(context.Context) (T, error)
type CalculateValueWithPolicy[T any] func(context.Context) (T, Policy, error)

type request struct {
	ctx          context.Context
	key          string
	limitWorkers bool
	ready        chan error
	dest         any
	calc         func(context.Context) (any, Policy, error)
}

type localEntry struct {
	mu       sync.Mutex
	snapshot valuestore.EntrySnapshot
	wait     chan struct{}
}

type Config struct {
	MaxWorkers int
	LockTTL    time.Duration
	Locks      distlock.Provider
	Values     valuestore.Store
	Logger     *log.Logger
}

// Cache coordinates local singleflight, TTL decisions, and optional shared
// lock/value providers.
type Cache struct {
	mu          sync.Mutex
	entries     map[string]*localEntry
	locks       distlock.Provider
	values      valuestore.Store
	baseCtx     context.Context
	cancel      context.CancelFunc
	workerSem   chan struct{}
	workers     sync.WaitGroup
	logger      *log.Logger
	localValues valuestore.Store
	lockTTL     time.Duration
}

func New(cfg Config) *Cache {
	maxWorkers := cfg.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	c := &Cache{
		entries:   make(map[string]*localEntry, 1024),
		locks:     cfg.Locks,
		values:    cfg.Values,
		baseCtx:   baseCtx,
		cancel:    cancel,
		workerSem: make(chan struct{}, maxWorkers),
		logger:    cfg.Logger,
		lockTTL:   normalizeLockTTL(cfg.LockTTL),
	}
	c.localValues = &localValueStore{cache: c}
	return c
}

func (c *Cache) Close() {
	c.cancel()
	c.workers.Wait()
}

func (c *Cache) SetShared(locks distlock.Provider, values valuestore.Store) {
	c.mu.Lock()
	c.locks = locks
	c.values = values
	c.mu.Unlock()
}

func (c *Cache) Shared() (distlock.Provider, valuestore.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.locks, c.values
}

func (c *Cache) LocalValues() valuestore.Store {
	return c.localValues
}

func Get[T any](ctx context.Context, cache *Cache, key string, limitWorkers bool, calc CalculateValueWithPolicy[T]) (result T, err error) {
	req := &request{
		ctx:          ctx,
		key:          key,
		limitWorkers: limitWorkers,
		ready:        make(chan error, 1),
		dest:         &result,
		calc: func(ctx context.Context) (any, Policy, error) {
			return calc(ctx)
		},
	}
	cache.serve(req)
	err = <-req.ready
	return
}

func GetWithTTL[T any](ctx context.Context, cache *Cache, key string, minTTL, maxTTL time.Duration, limitWorkers bool, calc CalculateValue[T]) (result T, err error) {
	wrapped := func(ctx context.Context) (T, Policy, error) {
		started := time.Now()
		value, err := calc(ctx)
		return value, Policy{MinTTL: minTTL, MaxTTL: maxTTL, CalcTime: time.Since(started)}, err
	}
	return Get(ctx, cache, key, limitWorkers, wrapped)
}

func (c *Cache) serve(req *request) {
	entry := c.entryFor(req.key)
	for {
		entry.mu.Lock()
		now := time.Now()
		if entry.wait != nil {
			if entry.snapshot.Usable(now) {
				pushSnapshot(req, entry.snapshot)
				entry.mu.Unlock()
				return
			}
			wait := entry.wait
			entry.mu.Unlock()
			select {
			case <-wait:
			case <-req.ctx.Done():
				req.ready <- req.ctx.Err()
				return
			}
			continue
		}
		if entry.snapshot.Fresh(now) {
			pushSnapshot(req, entry.snapshot)
			entry.mu.Unlock()
			return
		}
		if entry.snapshot.Usable(now) {
			snapshot := entry.snapshot
			entry.wait = make(chan struct{})
			wait := entry.wait
			entry.mu.Unlock()
			pushSnapshot(req, snapshot)
			c.workers.Add(1)
			go func() {
				defer c.workers.Done()
				c.refresh(req.key, entry, wait, req, true)
			}()
			return
		}
		entry.wait = make(chan struct{})
		wait := entry.wait
		entry.mu.Unlock()
		c.workers.Add(1)
		go func() {
			defer c.workers.Done()
			c.refresh(req.key, entry, wait, req, false)
		}()
		select {
		case <-wait:
		case <-req.ctx.Done():
			req.ready <- req.ctx.Err()
			return
		}
	}
}

func (c *Cache) refresh(key string, entry *localEntry, wait chan struct{}, req *request, background bool) {
	defer func() {
		entry.mu.Lock()
		if entry.wait == wait {
			close(entry.wait)
			entry.wait = nil
		}
		entry.mu.Unlock()
	}()

	current := c.localSnapshot(entry)
	if snapshot, ok, err := c.getShared(req.ctx, key); err == nil && ok {
		if shouldAdoptSharedSnapshot(current, snapshot) {
			c.storeLocal(entry, snapshot)
			current = snapshot
		}
		if current.Fresh(time.Now()) {
			return
		}
	} else if err != nil && !background {
		c.storeLocal(entry, errorSnapshot(err))
		return
	}

	lease, acquired, err := c.acquire(req.ctx, key, req)
	if err != nil {
		if background {
			return
		}
		c.storeLocal(entry, errorSnapshot(err))
		return
	}
	if !acquired {
		if background {
			return
		}
		snapshot, ok, err := c.waitForShared(req.ctx, key)
		if err != nil {
			c.storeLocal(entry, errorSnapshot(err))
			return
		}
		if ok {
			c.storeLocal(entry, snapshot)
			return
		}
		c.storeLocal(entry, errorSnapshot(errors.New("shared value not published before context cancellation")))
		return
	}
	defer lease.Release(context.Background())

	if req.limitWorkers {
		c.workerSem <- struct{}{}
		defer func() { <-c.workerSem }()
	}
	calcCtx, cancel := c.calcContext(req.ctx, background)
	defer cancel()
	started := time.Now()
	value, policy, err := req.calc(calcCtx)
	calcDuration := time.Since(started)
	if policy.CalcTime <= 0 {
		policy.CalcTime = calcDuration
	}
	snapshot, snapErr := snapshotFromResult(value, err, policy, calcDuration)
	if snapErr != nil {
		c.storeLocal(entry, errorSnapshot(snapErr))
		return
	}

	previous := c.localSnapshot(entry)
	if policy.PublishMode == PublishRequired {
		if err := c.publishRequired(req.ctx, key, snapshot, lease); err != nil {
			if background && previous.Usable(time.Now()) {
				c.storeLocal(entry, previous)
				return
			}
			c.storeLocal(entry, errorSnapshot(err))
			return
		}
		c.storeLocal(entry, snapshot)
		return
	}

	c.storeLocal(entry, snapshot)
	if c.values != nil && !leaseLost(lease) {
		if err := c.values.Put(req.ctx, key, snapshot); err != nil {
			c.logf("publish shared snapshot for %q: %v", key, err)
			return
		}
	}
}

func (c *Cache) calcContext(reqCtx context.Context, background bool) (context.Context, context.CancelFunc) {
	if background {
		return c.baseCtx, func() {}
	}
	ctx, cancel := context.WithCancel(reqCtx)
	go func() {
		select {
		case <-c.baseCtx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (c *Cache) getShared(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	c.mu.Lock()
	values := c.values
	c.mu.Unlock()
	if values == nil {
		return valuestore.EntrySnapshot{}, false, nil
	}
	return values.Get(ctx, key)
}

func (c *Cache) acquire(ctx context.Context, key string, req *request) (distlock.Lease, bool, error) {
	c.mu.Lock()
	locks := c.locks
	c.mu.Unlock()
	if locks == nil {
		return noopLease{}, true, nil
	}
	return locks.Acquire(ctx, key+".lock", c.lockTTL)
}

func (c *Cache) waitForShared(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, ok, err := c.getShared(ctx, key)
		if err != nil {
			return valuestore.EntrySnapshot{}, false, err
		}
		if ok && snapshot.Usable(time.Now()) {
			return snapshot, true, nil
		}
		select {
		case <-ctx.Done():
			return valuestore.EntrySnapshot{}, false, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Cache) storeLocal(entry *localEntry, snapshot valuestore.EntrySnapshot) {
	entry.mu.Lock()
	entry.snapshot = cloneSnapshot(snapshot)
	entry.mu.Unlock()
}

func (c *Cache) localSnapshot(entry *localEntry) valuestore.EntrySnapshot {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return cloneSnapshot(entry.snapshot)
}

func (c *Cache) publishRequired(ctx context.Context, key string, snapshot valuestore.EntrySnapshot, lease distlock.Lease) error {
	if c.values == nil {
		return errors.New("shared value store is required for PublishRequired")
	}
	if leaseLost(lease) {
		return errors.New("lease lost before shared publication")
	}
	if err := c.values.Put(ctx, key, snapshot); err != nil {
		return err
	}
	return nil
}

func shouldAdoptSharedSnapshot(local, shared valuestore.EntrySnapshot) bool {
	if !local.Usable(time.Now()) {
		return true
	}
	if !local.CreatedAt.IsZero() && !shared.CreatedAt.IsZero() {
		return shared.CreatedAt.After(local.CreatedAt)
	}
	if local.Error != "" && shared.Error == "" {
		return true
	}
	return false
}

func (c *Cache) entryFor(key string) *localEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		entry = &localEntry{}
		c.entries[key] = entry
	}
	return entry
}

func normalizeLockTTL(ttl time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}
	return time.Second
}

func (c *Cache) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

func snapshotFromResult(value any, calcErr error, policy Policy, calcDuration time.Duration) (valuestore.EntrySnapshot, error) {
	now := time.Now()
	minTTL := policy.MinTTL
	if minTTL < calcDuration*2 {
		minTTL = calcDuration * 2
	}
	maxTTL := policy.MaxTTL
	if maxTTL <= 0 {
		maxTTL = time.Hour
	}
	if minTTL > maxTTL {
		minTTL = maxTTL
	}
	snapshot := valuestore.EntrySnapshot{
		CreatedAt:    now,
		RefreshAt:    now.Add(minTTL),
		ExpireAt:     now.Add(maxTTL),
		CalcDuration: calcDuration,
	}
	if calcErr != nil {
		snapshot.Error = calcErr.Error()
		return snapshot, nil
	}
	buf, err := encodeValue(value)
	if err != nil {
		return valuestore.EntrySnapshot{}, err
	}
	snapshot.Value = buf
	return snapshot, nil
}

func pushSnapshot(req *request, snapshot valuestore.EntrySnapshot) {
	if snapshot.Error != "" {
		req.ready <- errors.New(snapshot.Error)
		return
	}
	req.ready <- decodeValue(snapshot.Value, req.dest)
}

func encodeValue(value any) ([]byte, error) {
	return marshalValue(value)
}

func decodeValue(buf []byte, dest any) error {
	return unmarshalValue(buf, dest)
}

func cloneSnapshot(snapshot valuestore.EntrySnapshot) valuestore.EntrySnapshot {
	snapshot.Value = append([]byte(nil), snapshot.Value...)
	return snapshot
}

func errorSnapshot(err error) valuestore.EntrySnapshot {
	now := time.Now()
	return valuestore.EntrySnapshot{
		CreatedAt: now,
		Error:     err.Error(),
		RefreshAt: now.Add(50 * time.Millisecond),
		ExpireAt:  now.Add(50 * time.Millisecond),
	}
}

type noopLease struct{}

func (noopLease) Lost() <-chan struct{} {
	ch := make(chan struct{})
	return ch
}

func (noopLease) Release(context.Context) error { return nil }

func leaseLost(lease distlock.Lease) bool {
	select {
	case <-lease.Lost():
		return true
	default:
		return false
	}
}

type localValueStore struct {
	cache *Cache
}

func (s *localValueStore) Get(_ context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	s.cache.mu.Lock()
	entry, ok := s.cache.entries[key]
	s.cache.mu.Unlock()
	if !ok {
		return valuestore.EntrySnapshot{}, false, nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !entry.snapshot.Usable(time.Now()) {
		entry.snapshot = valuestore.EntrySnapshot{}
		return valuestore.EntrySnapshot{}, false, nil
	}
	return cloneSnapshot(entry.snapshot), true, nil
}

func (s *localValueStore) Put(_ context.Context, key string, snapshot valuestore.EntrySnapshot) error {
	entry := s.cache.entryFor(key)
	entry.mu.Lock()
	entry.snapshot = cloneSnapshot(snapshot)
	entry.mu.Unlock()
	return nil
}

func (s *localValueStore) Delete(_ context.Context, key string) error {
	s.cache.mu.Lock()
	delete(s.cache.entries, key)
	s.cache.mu.Unlock()
	return nil
}
