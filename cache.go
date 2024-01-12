package cachecalc

import (
	"context"
	"io"
	"log"
	"reflect"
	"sync"
	"time"
)

type CachedCalcOpts struct {
	MaxTTL, MinTTL time.Duration
	CalcTime       time.Duration // the duration of last calculation
}

// CalculateValue this is type of function which returns interface{} type
type (
	CalculateValue       func(context.Context) (any, error)
	CalculateValueAndOpt func(context.Context) (any, CachedCalcOpts, error)
)

// DefaultCCs default cached calculations cache used by GetCachedCalc
// It does not use external cache for coordinating between multiple distributed
var DefaultCCs = NewCachedCalculations(4, nil)

type request struct {
	ctx            context.Context
	calculateValue CalculateValueAndOpt
	key            any
	ready          chan error // error message, empty if no error
	dest           any        // but provide pointer to the result!!!
	limitWorkers   bool
	CachedCalcOpts
}

type CacheEntry struct {
	Expire       time.Time     // time of expiration of this entry
	Refresh      time.Time     // time when this value should be refreshed
	CalcDuration time.Duration // how much time it took to calculate the value
	Err          error         // stores the last error status of calculations
	Value        []byte        // stores the serialized value of last calculations
	// wait is channel which, if not nil, signals about ongoing calculation on the item.
	// It is closed by issuer to inform interested clients on end of calculations
	wait chan struct{}
	sync.WaitGroup
	sync.RWMutex
}

// CachedCalculations has the only method: GetCachedCalc. It is used for easy refactoring of slow/long calculating backend methods. See examples
type CachedCalculations struct {
	entries       map[any]*CacheEntry
	externalCache ExternalCache
	workers       sync.WaitGroup
	limitWorkers  chan struct{}
	sync.Mutex
	sync.WaitGroup
}

func getKeyLock(key string) string {
	return key + ".lock"
}

var logger *log.Logger

func init() {
	logger = log.New(io.Discard, "", log.LstdFlags)
	//logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
}

// NewCachedCalculations is used to create app's instance of CachedCalculations.
// It creates two threads which handle and coordinate cached backend calculations
// Graceful exit from app should include expiring ctx context and then smartCacheInstance.Wait()
// This will gracefully finish the job of those threads
func NewCachedCalculations(maxWorkers int, externalCache ExternalCache) *CachedCalculations {
	var cc CachedCalculations
	cc.entries = make(map[any]*CacheEntry, 1024*16)
	cc.externalCache = externalCache
	cc.limitWorkers = make(chan struct{}, maxWorkers+1)
	//cc.buf = new(bytes.Buffer)
	return &cc
}

// GetCachedCalc uses default cached calculations cache as GetCachedCalcX(DefaultCCs,...) for convenience
// it is created with default for no external cache, but that can be redefined by app
func GetCachedCalcOpt[T any](ctx context.Context, key any,
	calculateValueAndOpt func(ctx context.Context) (T, CachedCalcOpts, error), limitWorkers bool) (T, error) {
	return GetCachedCalcOptX(DefaultCCs, ctx, key, calculateValueAndOpt, limitWorkers)
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
		cc.handleRequest(&request{
			calculateValue: calcValue,
			ctx:            ctx,
			key:            key,
			dest:           &result,
			ready:          ready,
			limitWorkers:   limitWorkers,
		})
		cc.Lock()
		cc.Done()
		cc.Unlock()
	}()
	// then wait for the result to be wait
	err = <-ready
	cc.Lock()
	cc.Done()
	cc.Unlock()
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
	return GetCachedCalcOptX(cc, ctx, key, calcValue, limitWorker)
}

// Close is automatically called on expired context. It is safe to call it multiple times
// it tries to gracefully interrupt all ongoing calculations using their context
// when it succeeds in this it removes the record about job
func (cc *CachedCalculations) Close() {
	cc.Wait()
	cc.Lock()
	cc.workers.Wait()
	defer cc.Unlock()
	for k, entry := range cc.entries {
		wait := entry.wait // CachedCalculations.Close()
		if wait != nil {
			<-wait // CachedCalculations.Close()
		}
		delete(cc.entries, k) // Close()
	}
	if cc.externalCache != nil {
		cc.externalCache.Close()
	}
}

// for request r obtains value from cache/calculation and pushes status of the operation to r.wait
// then checks whether value need to be refreshed in cache
func (cc *CachedCalculations) obtainValue(ctx context.Context, r *request) (err error) {
	if cc.externalCache == nil {
		return cc.obtainLocal(ctx, r)
	} else {
		return cc.obtainExternal(ctx, r)
	}
}

// simple case for single CachedCalculations instance.
func (cc *CachedCalculations) obtainLocal(ctx context.Context, r *request) (err error) {
	entry := cc.obtainEntry(r) // entry is locked after call
	thread := getThread(ctx)
	wait := entry.wait // before starting calculation check whether someone else is not performing it already
	if wait != nil {
		// non-active thread can just return whatever value is there and be it
		// unless entry expired
		if entry.Expire.Before(time.Now()) {
			// must not continue lock on entry until entry is being calculated!
			entry.Unlock()
			logger.Printf("thread %v:%s,waiting while other thread calculating\n", thread, r.key)
			<-wait // wait until it was closed
			// read Lock is enough to return value
			entry.Lock()
		} else {
			logger.Printf("thread %v, key %s already being updated by someone else but value still not expired", thread, r.key)
		}
		cc.pushValue(ctx, entry, r)
		err = entry.Err
		entry.Unlock()
		return err
	}
	// entry is locked here
	return cc.calculateValue(ctx, r, entry, true) // unlocks entry
}

// obtainEntry locks local cache and checks whether entry exist.
// if it is not it adds new entry to the local cache
// it locks the returned entry so the caller must release it
func (cc *CachedCalculations) obtainEntry(r *request) *CacheEntry {
	cc.Lock()
	defer cc.Unlock()
	entry, exists := cc.entries[r.key]
	if !exists {
		// create new entry for internal memory as entry does not exist
		entry = &CacheEntry{}
		cc.entries[r.key] = entry
	}
	entry.Lock()
	return entry
}

func (cc *CachedCalculations) pushValue(ctx context.Context, entry *CacheEntry, r *request) {
	thread := getThread(ctx)
	if entry.Err != nil {
		r.ready <- entry.Err
		logger.Printf("thread %v, pushing error %s = %v to ready channel", thread, r.key, entry.Err)
	} else {
		// store value to the destination variable
		err := deserialize(entry.Value, r.dest)
		v := reflect.ValueOf(r.dest).Elem()
		logger.Printf("thread %v, pushing value %v of %s : %v to ready channel", thread, getEntryValue(entry, r), r.key, v)
		r.ready <- err
	}
}

func getThread(ctx context.Context) any {
	v := ctx.Value("thread")
	return v
}

// calculateValue expects entry to be locked with .Lock and will unlock it before exit
func (cc *CachedCalculations) calculateValue(ctx context.Context, r *request, entry *CacheEntry, pushValue bool) (err error) {
	reason := "entry expired"
	thread := ctx.Value("thread")
	hadValue := entry.Expire.After(time.Now())
	if entry.Expire.IsZero() {
		reason = "entry init"
	}
	if hadValue {
		if pushValue {
			cc.pushValue(ctx, entry, r)
		}
		if entry.Refresh.After(time.Now()) {
			err = entry.Err
			entry.Unlock()
			return err
		}
		reason = "entry refresh"
	}
	// will be calculating/refreshing value
	if entry.wait == nil {
		entry.wait = make(chan struct{}) // mark that calculation is being performed for this entry
	}
	entry.Unlock() // it was locked before calculateValue
	logger.Printf("thread %v,lock was released for entry %s\n", thread, r.key)
	if r.limitWorkers {
		// add new worker
		logger.Printf("thread %v, entry %s, taking worker...\n", thread, r.key)
		cc.limitWorkers <- struct{}{}
	}
	started := time.Now()
	logger.Printf("thread %v:%s, reason %s, calculating value...", thread, r.key, reason)
	v, opt, err := r.calculateValue(context.WithValue(ctx, "reason", reason))
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
	entry.Lock()
	if err == nil {
		entry.Value, err = serialize(v)
	}
	entry.Err = err
	entry.CalcDuration = calcDuration
	close(entry.wait) // broadcast result of local calculation to clients
	entry.wait = nil  // calculations complete
	logger.Printf("thread %v,entry %s broadcast value %v is ready, setting cache entry\n", thread, r.key, v)
	// update refresh and expire
	entry.Refresh = now.Add(r.MinTTL)
	entry.Expire = now.Add(r.MaxTTL)
	logger.Printf("thread %v,entry %s refresh +%v:%v, expire +%v:%v\n", thread, r.key, r.MinTTL, now.Add(r.MinTTL), r.MaxTTL, now.Add(r.MaxTTL))
	if !hadValue {
		cc.pushValue(ctx, entry, r)
		logger.Printf("thread %v,entry %s value %v has been pushed to ready channel\n", thread, r.key, v)
	}
	entry.Unlock()
	logger.Printf("thread %v,entry %s %v was unlocked\n", thread, r.key, v)
	return
}

func (cc *CachedCalculations) handleRequest(r *request) {
	obtain := func() {
		defer cc.workers.Done()
		if err := cc.obtainValue(r.ctx, r); err != nil {
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
			delete(cc.entries, k) // remove entry from cache upon expiration : RemoveEntries()
		}
		e.Unlock()
	}
}

func nzDuration(durations ...time.Duration) time.Duration {
	for _, d := range durations {
		if d != 0 {
			return d
		}
	}
	return time.Hour
}
