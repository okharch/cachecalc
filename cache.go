package cachecalc

import (
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"time"
)

type CachedCalcOpts struct {
	MaxTTL, MinTTL time.Duration
	CalcTime       time.Duration // the duration of last calculation
	// ExpireEntry is a channel to send signal on entry expiration
	// it should also watch for ctx.Done() and return false in that case
	ExpireEntry chan struct{}
}

// CalculateValue this is type of function which returns interface{} type
type (
	CalculateValue       func(context.Context) (any, error)
	CalculateValueAndOpt func(context.Context) (any, CachedCalcOpts, error)
)

// DefaultCCs default cached calculations cache used by GetCachedCalc
// It does not use external cache for coordinating between multiple distributed
var cancelDefaultCtx, CancelDefaultCCs = context.WithCancel(context.Background())
var DefaultCCs = NewCachedCalculations(cancelDefaultCtx, nil, 4)

type request struct {
	calculateValue CalculateValueAndOpt
	key            any
	ready          chan error // error message, empty if no error
	dest           any        // but provide pointer to the result!!!
	limitWorkers   bool
	CachedCalcOpts // request struct
}

type CacheEntry struct {
	Expire       time.Time     // time of expiration of this entry
	Refresh      time.Time     // time when this value should be refreshed
	CalcDuration time.Duration // how much time it took to calculate the value
	Err          error         // stores the last error status of calculations
	Value        []byte        // stores the serialized value of last calculations
	// wait is channel which, if not nil, signals about ongoing calculation on the item.
	// It is closed by issuer to inform interested clients on end of calculations
	wait   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	sync.WaitGroup
	sync.RWMutex
}

// CachedCalculations has the only method: GetCachedCalc. It is used for easy refactoring of slow/long calculating backend methods. See examples
type CachedCalculations struct {
	entries       map[any]*CacheEntry
	externalCache ExternalCache
	workers       sync.WaitGroup // housekeeping of goroutines which calculate values for specific keys
	limitWorkers  chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	sync.Mutex
	sync.WaitGroup
}

func getKeyLock(key string) string {
	return key + ".lock"
}

var logger *log.Logger

func init() {
	logger = log.New(io.Discard, "", log.LstdFlags)
}

// NewCachedCalculations is used to create app's instance of CachedCalculations.
// It creates two threads which handle and coordinate cached backend calculations
// Graceful exit from app should include expiring ctx context and then smartCacheInstance.Wait()
// This will gracefully finish the job of those threads
func NewCachedCalculations(ctx context.Context, externalCache ExternalCache, maxWorkers int) *CachedCalculations {
	var cc CachedCalculations
	cc.entries = make(map[any]*CacheEntry, 1024*16)
	cc.limitWorkers = make(chan struct{}, maxWorkers+1)
	cc.ctx, cc.cancel = context.WithCancel(ctx)
	cc.externalCache = externalCache
	if externalCache != nil {
		// remove internal cache entries which expired externally
		cc.Add(1)
		go func() {
			thread := getThread(cc.ctx)
			ch := externalCache.ExpireEntries(cc.ctx)
			for key := range ch {
				logger.Printf("thread %v: external cache expired key %s", thread, key)
				cc.removeEntry(ctx, key, false)
			}
			cc.Done()
		}()
	}
	return &cc
}

// GetCachedCalc uses default cached calculations cache as GetCachedCalcX(DefaultCCs,...) for convenience
// it is created with default for no external cache, but that can be redefined by app
func GetCachedCalcOpt[T any](ctx context.Context, key any,
	calculateValueAndOpt func(ctx context.Context) (T, CachedCalcOpts, error), limitWorkers bool) (T, error) {
	return GetCachedCalcOptX(DefaultCCs, ctx, key, calculateValueAndOpt, limitWorkers) // GetCachedCalcOpt() uses DefaultCCs for cc parameter
}

func GetCachedCalcOptX[T any](cc *CachedCalculations, ctx context.Context, key any,
	calculateValueAndOpt func(ctx context.Context) (T, CachedCalcOpts, error), limitWorkers bool) (result T, err error) {
	ready := make(chan error)
	// cast calculateValueAndOpt to func(ctx context.Context) (any, CachedCalcOpts, error)
	calcValue := func(ctx context.Context) (any, CachedCalcOpts, error) {
		return calculateValueAndOpt(ctx)
	}
	cc.removeExpired()
	//return calculateValue(ctx)
	// put request to channel for handling
	cc.Lock()
	cc.Add(2)
	cc.Unlock()
	go func() {
		cc.handleRequest(ctx, &request{
			calculateValue: calcValue,
			key:            key,
			dest:           &result,
			ready:          ready,
			limitWorkers:   limitWorkers,
		})
		cc.Lock()
		cc.Done()
		cc.Unlock()
	}()
	// then wait for the result
	logger.Printf("thread %v, waiting for ready channel\n", getThread(ctx))
	err = <-ready
	logger.Printf("thread %v, ready channel received\n", getThread(ctx))
	cc.Done()
	if err != nil {
		logger.Printf("thread %v, GetCachedCalcOptX returns error: %v\n", getThread(ctx), err)
		return
	}
	logger.Printf("thread %v, GetCachedCalcOptX returns: %v\n", getThread(ctx), result)
	return
}

// GetCachedCalc uses default cached calculations cache as GetCachedCalcX(DefaultCCs,...) for convenience
// it is created with default for no external cache, but that can be redefined by app
func GetCachedCalc[T any](ctx context.Context, key any, minTTL, maxTTL time.Duration, limitWorker bool,
	calculateValue func(ctx context.Context) (T, error)) (result T, err error) {
	return GetCachedCalcX(DefaultCCs, ctx, key, minTTL, maxTTL, limitWorker, calculateValue)
}

// GetCachedCalcX is used wherever you need to perform cached and coordinated calculation instead of regular and uncoordinated
//
// ctx is a parent context for calculation
// if parent context is cancelled then all child context are cancelled as well
//
// params of cachedCalculation - see description of how cachedCalculation defined
func GetCachedCalcX[T any](cc *CachedCalculations, ctx context.Context, key any, minTTL, maxTTL time.Duration, limitWorker bool,
	calculateValue func(ctx context.Context) (T, error)) (T, error) {
	// cast calculateValueAndOpt to func(ctx context.Context) (any, CachedCalcOpts, error)
	calcValue := func(ctx context.Context) (T, CachedCalcOpts, error) {
		started := time.Now()
		result, err := calculateValue(ctx)
		opt := CachedCalcOpts{MaxTTL: maxTTL, MinTTL: minTTL, CalcTime: time.Since(started)}
		return result, opt, err
	}
	return GetCachedCalcOptX(cc, ctx, key, calcValue, limitWorker) // GetCachedCalcX() which provides calculateValue func(ctx context.Context) (T, error) without options
}

// Close is automatically called on expired context. It is safe to call it multiple times
// it tries to gracefully interrupt all ongoing calculations using their context
// when it succeeds in this it removes the record about job
func (cc *CachedCalculations) Close() {
	cc.cancel()
	cc.Wait()
	cc.Lock()
	cc.workers.Wait()
	defer cc.Unlock()
	for k, entry := range cc.entries {
		wait := entry.wait // CachedCalculations.Close()
		if wait != nil {
			<-wait // CachedCalculations.Close()
		}
		entry.cancel()        // cancel context to avoid context leak and shutdown expiration goroutine
		delete(cc.entries, k) // Close(), cancel+
	}
	if cc.externalCache != nil {
		_ = cc.externalCache.Close()
	}
}

// for request r obtains value from cache/calculation and pushes status of the operation to r.wait
// then checks whether value need to be refreshed in cache
func (cc *CachedCalculations) obtainValue(ctx context.Context, r *request) (err error) {
	entry := cc.obtainEntry(ctx, r) // entry is locked after call
	if valueReady, err := cc.valueReady(ctx, entry, r); valueReady {
		// entry is unlocked here
		return err
	}
	// entry is locked here
	if cc.externalCache == nil {
		return cc.calculateValue(ctx, r, entry, true) // unlocks entry
	} else {
		return cc.obtainExternal(ctx, entry, r)
	}
}

func (cc *CachedCalculations) valueReady(ctx context.Context, entry *CacheEntry, r *request) (result bool, err error) {
	thread := getThread(ctx)
	wait := entry.wait // before starting calculation check whether someone else is not performing it already
	if wait == nil {
		return
	}
	// non-active thread can just return whatever value is there and be it
	// unless entry expired
	if entry.Expire.Before(time.Now()) {
		// must not continue lock on entry until entry is being calculated!
		entry.Unlock()
		logger.Printf("thread %v:%s,waiting while other thread calculating\n", thread, r.key)
		<-wait // wait until it was closed
		entry.RLock()
		defer entry.RUnlock()
	} else {
		defer entry.Unlock()
		logger.Printf("thread %v, key %s already being updated by someone else but value still not expired", thread, r.key)
	}
	cc.pushValue(entry, r, false) // obtainLocal() pushes value to cache when it is ready or not expired
	return true, entry.Err
}

// obtainEntry locks local cache and checks whether entry exist.
// if it is not it adds new entry to the local cache
// it locks the returned entry so the caller must release it
func (cc *CachedCalculations) obtainEntry(ctx context.Context, r *request) *CacheEntry {
	cc.Lock()
	defer cc.Unlock()
	entry, exists := cc.entries[r.key]
	if !exists {
		// create new entry for internal memory as entry does not exist
		entry = &CacheEntry{}
		// set context for entry
		entry.ctx, entry.cancel = context.WithCancel(ctx)
		cc.entries[r.key] = entry
	}
	entry.Lock()
	return entry
}

// pushValue pushes a value from the cache to the request's ready channel. If the entry has an error, it sends the error instead.
// If startExpiration is true and ExpireEntry is not nil, it starts a goroutine to handle the expiration of the entry.
// if it receives expiration signal, it removes the entry from the cache and then broadcasts the `expiration job done` signal back to the client by closing the channel.
//
// Parameters:
// - entry: The cache entry containing the value or error.
// - r: The request containing the ready channel and expiration channel.
// - startExpiration: A flag indicating whether to start the expiration process.
func (cc *CachedCalculations) pushValue(entry *CacheEntry, r *request, startExpiration bool) {
	ctx := entry.ctx
	thread := getThread(ctx)
	err := entry.Err
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		r.ready <- err
		logger.Printf("thread %v, pushing error %s = %v to ready channel", thread, r.key, err)
	} else {
		// store value to the destination variable
		err := deserialize(entry.Value, r.dest)
		v := reflect.ValueOf(r.dest).Elem()
		logger.Printf("thread %v, pushing value %v of %s : %v to ready channel", thread, getEntryValue(entry, r), r.key, v)
		r.ready <- err
		if startExpiration && r.ExpireEntry != nil {
			// run goroutine to expire entry
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				logger.Printf("thread %v, listening expiration channel for entry %s\n", thread, r.key)
				// wait for expiration signal or context done
				wg.Done()
				select {
				case <-r.ExpireEntry:
					logger.Printf("thread %v, expiration signal received: entry %s is being removed\n", thread, r.key)
					cc.removeEntry(entry.ctx, r.key, true)
					logger.Printf("thread %v, broadcast the `removing of entry %s completed` signal back to the client", thread, r.key)
					close(r.ExpireEntry)
				case <-ctx.Done():
					logger.Printf("thread %v, entry %s context done\n", thread, r.key)
				}
			}()
			wg.Wait()
			time.Sleep(time.Millisecond) // give time for goroutine to start
		}
	}
}

func (cc *CachedCalculations) removeEntry(ctx context.Context, key any, removeExternal bool) {
	cc.Lock()
	defer cc.Unlock()
	logger.Printf("thread %v, removing entry %s:locked\n", getThread(ctx), key)
	entry, ok := cc.entries[key]
	if !ok {
		thread := getThread(ctx)
		logger.Printf("thread %v entry %s not found in cache upon expiration", thread, key)
		return
	}
	delete(cc.entries, key) // remove entry from cache upon expiration : pushValue(), cancel+
	defer entry.cancel()    // cancel context to avoid context leak
	if removeExternal && cc.externalCache != nil {
		key := fmt.Sprint(key)
		err := cc.externalCache.Del(entry.ctx, key)
		if err != nil {
			logger.Printf("failed to remove key %s from external cache: %s", key, err)
			return
		}
		thread := getThread(entry.ctx)
		logger.Printf("thread %v key %s removed from external cache", thread, key)
	}
	logger.Printf("thread %v, removeEntry: entry %s was removed\n", getThread(ctx), key)
}

// getThread returns the thread id from the context, this is a utility for debugging
func getThread(ctx context.Context) any {
	v := ctx.Value("thread")
	return v
}

// calculateValue expects entry to be locked with .Lock and will unlock it before exit
func (cc *CachedCalculations) calculateValue(ctx context.Context, r *request, entry *CacheEntry, unlockEntry bool) (err error) {
	if unlockEntry {
		defer entry.Unlock() // entry locked before calculateValue
	}
	reason := "entry expired"
	thread := ctx.Value("thread")
	hadValue := entry.Expire.After(time.Now())
	if entry.Expire.IsZero() {
		reason = "entry init"
	}
	if hadValue {
		cc.pushValue(entry, r, true) // calculateValue() replaces value to cache if it is not expired, checks whether need to be refreshed below
		logger.Printf("thread %v,entry %s checking refresh %v", thread, r.key, entry.Refresh.Sub(time.Now()))
		if entry.Refresh.After(time.Now()) {
			err = entry.Err
			return err
		}
		reason = "entry refresh"
	}
	if entry.ctx.Err() != nil {
		err = entry.ctx.Err()
		return err
	}
	// will be calculating/refreshing value
	entry.wait = make(chan struct{}) // mark that calculation is being performed for this entry
	entry.Unlock()                   // it was locked before calculateValue
	logger.Printf("thread %v,lock was released for entry %s\n", thread, r.key)
	if r.limitWorkers {
		// push new worker
		logger.Printf("thread %v, entry %s, taking worker...\n", thread, r.key)
		cc.limitWorkers <- struct{}{}
	}
	started := time.Now()
	logger.Printf("thread %v:%s, reason %s, calculating value...", thread, r.key, reason)
	v, opt, err := r.calculateValue(context.WithValue(ctx, "reason", reason))
	// set default for timeouts: if set to zero, set them to an hour
	opt.MaxTTL = nzDuration(opt.MaxTTL, opt.MinTTL)
	opt.MinTTL = nzDuration(opt.MinTTL, opt.MaxTTL)
	logger.Printf("thread %v,value %s calculated to %v, reason: %s", thread, r.key, v, reason)
	if r.limitWorkers {
		// pop worker
		logger.Printf("thread %v, entry %s, releasing worker...\n", thread, r.key)
		<-cc.limitWorkers
	}
	r.CachedCalcOpts = opt
	calcDuration := time.Since(started)
	now := time.Now()
	minTTL := calcDuration * 2
	if r.MinTTL < minTTL {
		r.MinTTL = minTTL
	}
	if r.MinTTL > r.MaxTTL {
		r.MinTTL = r.MaxTTL
	}
	logger.Printf("thread %v,waiting to lock entry %s for updating && broadcasting value is ready\n", thread, r.key)
	entry.Lock() // lock entry before updating
	if err == nil {
		entry.Value, err = serialize(v)
	}
	entry.Err = err
	entry.CalcDuration = calcDuration
	w := entry.wait
	entry.wait = nil // calculations complete
	close(w)         // broadcast result of local calculation to clients
	logger.Printf("thread %v,entry %s broadcast value %v is ready, setting cache entry, refresh %v expire %v\n", thread, r.key, v, r.MinTTL, r.MaxTTL)
	// update refresh and expire
	entry.Refresh = now.Add(r.MinTTL)
	entry.Expire = now.Add(r.MaxTTL)
	logger.Printf("thread %v,entry %s refresh +%v:%v, expire +%v:%v\n", thread, r.key, r.MinTTL, now.Add(r.MinTTL), r.MaxTTL, now.Add(r.MaxTTL))
	if !hadValue {
		cc.pushValue(entry, r, true) // calculateValue() pushes the completely new value to cache
		logger.Printf("thread %v,entry %s value %v has been pushed to ready channel\n", thread, r.key, v)
	}
	logger.Printf("thread %v,entry %s %v was unlocked\n", thread, r.key, v)
	return
}

func (cc *CachedCalculations) handleRequest(ctx context.Context, r *request) {
	obtain := func() {
		defer cc.workers.Done()
		if err := cc.obtainValue(ctx, r); err != nil {
			logger.Printf("failed to obtain value: %s", err)
		}
	}
	cc.workers.Add(1)
	go obtain()
}

func entryNonEmpty(e *CacheEntry) bool {
	return !e.Expire.IsZero()
}

func (cc *CachedCalculations) removeExpired() {
	expired := func(_ any, e *CacheEntry) bool {
		return e.wait == nil && !e.Expire.IsZero() && e.Expire.Before(time.Now())
	}
	cc.RemoveEntries(expired)
}

func (cc *CachedCalculations) RemoveEntries(filter func(key any, entry *CacheEntry) bool) {
	cc.Lock()
	defer cc.Unlock()
	for k, e := range cc.entries {
		e.Lock()
		if filter(k, e) {
			logger.Printf("remove entry %s from cache upon expiration", k)
			e.cancel()            // cancel context to avoid context leak and shutdown expiration goroutine
			delete(cc.entries, k) // remove entry from cache upon expiration : RemoveEntries(), cancel+
		}
		e.Unlock()
	}
}

/*
nzDuration returns the first non-zero duration from the provided list of durations.
If all provided durations are zero, it returns a default duration of 100 years.

Parameters:
- durations: A variadic parameter of type `time.Duration`. This represents the list of durations to check.

Returns:
- time.Duration: The first non-zero duration from the provided list, or a default duration of 100 years if all are zero.

Usage:

This function is useful when you want to provide a list of potential durations and select the first valid (non-zero) one. If none are valid, a default duration is used.
*/
func nzDuration(durations ...time.Duration) time.Duration {
	for _, d := range durations {
		if d != 0 {
			return d
		}
	}
	return time.Hour * 24 * 365 * 100
}
