package cachecalc

import (
	"context"
	"io"
	"log"
	"sync"
	"time"
)

// CalculateValue this is type of function which returns interface{} type
type CalculateValue func(context.Context) (any, error)

// DefaultCCs default cached calculations cache used by GetCachedCalc
// It does not use external cache for coordinating between multiple distributed
var DefaultCCs = NewCachedCalculations(4, nil)

type request struct {
	ctx            context.Context
	calculateValue CalculateValue
	key            string
	ready          chan error // error message, empty if no error
	dest           any        // but provide pointer to result!!!
	limitWorker    bool       // if you know calculation puts heavy load on infrastructure, then set it to true!
	maxTTL, minTTL time.Duration
}

type cacheEntry struct {
	Expire  time.Time // time of expiration of this entry
	Refresh time.Time // time when this value should be refreshed
	Request time.Time // when this Value was last time requested. If it is not requested recently it will not be refreshed
	Err     error     // stores the last error status of calculations
	Value   []byte    // stores the serialized value of last calculations
	// wait is channel which, if not nil, signals about ongoing calculation on the item.
	// It is closed by issuer to inform interested clients on end of calculations
	wait chan struct{}
	sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// CachedCalculation defines parameters of coordinated calculation.
// Parameter of this type is used for CachedCalculations.Get method
type CachedCalculation struct {
	Key         string         // key to store this calculation into cache
	Calculate   CalculateValue // function called to create/refresh the value for the Key
	LimitWorker bool           // true if it is heavy load and need to be limited concurrent workers like this
	MinTTL      time.Duration  // until MinTTL value is returned from cache and is not being refreshed
	MaxTTL      time.Duration  // when value expires completely and needs to be recalculated
}

// CachedCalculations has the only method: GetCachedCalc. It is used for easy refactoring of slow/long calculating backend methods. See examples
type CachedCalculations struct {
	entries       map[string]*cacheEntry
	externalCache ExternalCache
	workers       sync.WaitGroup
	limitWorkers  chan struct{}
	sync.Mutex
	sync.WaitGroup
}

var logger *log.Logger

func init() {
	logger = log.New(io.Discard, "", log.LstdFlags)
}

// NewCachedCalculations is used to create app's instance of CachedCalculations.
// It creates two threads which handle and coordinate cached backend calculations
// Graceful exit from app should include expiring ctx context and then smartCacheInstance.Wait()
// This will gracefully finish the job of those threads
func NewCachedCalculations(maxWorkers int, externalCache ExternalCache) *CachedCalculations {
	var cc CachedCalculations
	cc.entries = make(map[string]*cacheEntry, 1024*16)
	cc.externalCache = externalCache
	cc.limitWorkers = make(chan struct{}, maxWorkers+1)
	//cc.buf = new(bytes.Buffer)
	return &cc
}

// GetCachedCalc uses default cached calculations cache as GetCachedCalcX(DefaultCCs,...) for convenience
// it is created with default for no external cache, but that can be redefined by app
func GetCachedCalc[T any](ctx context.Context, key string, minTTL, maxTTL time.Duration, limitWorker bool,
	calculateValue func(ctx context.Context) (T, error)) (result T, err error) {
	return GetCachedCalcX(DefaultCCs, ctx, key, minTTL, maxTTL, limitWorker, calculateValue)
}

// GetCachedCalcX is used wherever you need to perform cached and coordinated calculation instead of regular and uncoordinated
//
// ctx is a parent context for calculation
// if parent context is cancelled then all child context are cancelled as well
//
// params of CachedCalculation - see description of how CachedCalculation defined
func GetCachedCalcX[T any](cc *CachedCalculations, ctx context.Context, key string, minTTL, maxTTL time.Duration, limitWorker bool,
	calculateValue func(ctx context.Context) (T, error)) (result T, err error) {
	ready := make(chan error)
	calcValue := func(ctx context.Context) (any, error) {
		return calculateValue(ctx)
	}
	cc.removeExpired()
	//return calculateValue(ctx)
	// put request to channel for handling
	go cc.handleRequest(&request{
		calculateValue: calcValue,
		ctx:            ctx,
		key:            key,
		dest:           &result,
		ready:          ready,
		maxTTL:         maxTTL,
		minTTL:         minTTL,
		limitWorker:    limitWorker,
	})
	// then wait for the result to be wait
	err = <-ready
	return
}

// Close is automatically called on expired context. It is safe to call it multiple times
// it tries to gracefully interrupt all ongoing calculations using their context
// when it succeeds in this it removes the record about job
func (cc *CachedCalculations) Close() {
	cc.Lock()
	defer cc.Unlock()
	for k, v := range cc.entries {
		v.cancel()     // Close(): cancel calculations if any, release context as well
		wait := v.wait // CachedCalculations.Close()
		if wait != nil {
			<-v.wait // CachedCalculations.Close()
		}
		delete(cc.entries, k)
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
	entry := cc.obtainEntry(ctx, r)
	return cc.calculateValue(ctx, r, entry, true)
}

func (cc *CachedCalculations) obtainEntry(ctx context.Context, r *request) *cacheEntry {
	cc.Lock()
	defer cc.Unlock()
	entry, exists := cc.entries[r.key]
	if !exists {
		// create new entry for internal memory as entry does not exist
		entry = &cacheEntry{}
		entry.ctx, entry.cancel = context.WithCancel(ctx)
		cc.entries[r.key] = entry
	}
	entry.Lock()
	return entry
}

func (cc *CachedCalculations) pushValue(entry *cacheEntry, r *request) {
	if entry.Err != nil {
		r.ready <- entry.Err
	} else {
		// store value to the destination variable
		r.ready <- deserialize(entry.Value, r.dest)
	}
}

func getThread(ctx context.Context) any {
	v := ctx.Value("thread")
	return v
}

func (cc *CachedCalculations) calculateValue(ctx context.Context, r *request, entry *cacheEntry, pushValue bool) (err error) {
	reason := "init entry"
	thread := ctx.Value("thread")
	wait := entry.wait    // before starting calculation check whether someone else is not performing it already
	active := wait == nil // only active thread provides real calculations, others are waiting for it
	if !active {
		// non-active thread can just return whatever value is there and be it
		// unless it entry is still empty
		if entry.Expire.IsZero() {
			// must not continue lock on entry until entry is being calculated!
			entry.Unlock()
			logger.Printf("thread %v,waiting while other thread calculating entry %s\n", thread, r.key)
			<-wait // wait until it was closed
			logger.Printf("thread %v,waiting for other thread completed: %s\n", thread, r.key)
			// read Lock is enough to return value
			entry.RLock()
			defer entry.RUnlock()
		} else {
			defer entry.Unlock()
			logger.Printf("thread %v, key %s already being updated by someone else", thread, r.key)
		}
		cc.pushValue(entry, r)
		return entry.Err
	}
	hadValue := entry.Expire.After(time.Now())
	if hadValue {
		if pushValue {
			cc.pushValue(entry, r)
		}
		if entry.Refresh.After(time.Now()) {
			defer entry.Unlock()
			return entry.Err
		}
		reason = "refresh"
	}
	// will be calculating/refreshing value
	entry.wait = make(chan struct{}) // mark that calculation is being performed for this entry
	entry.Unlock()                   // should not
	logger.Printf("thread %v,lock was released for entry %s\n", thread, r.key)
	if r.limitWorker {
		// add new worker
		logger.Printf("thread %v, entry %s, taking worker...\n", thread, r.key)
		cc.limitWorkers <- struct{}{}
	}
	started := time.Now()
	var v any
	logger.Printf("thread %v,key %s, reason %s calculating value...", thread, r.key, reason)
	v, err = r.calculateValue(context.WithValue(entry.ctx, "reason", reason))
	logger.Printf("thread %v,value %s recalculated to %v", thread, r.key, v)
	calcDuration := time.Since(started)
	now := time.Now()
	minTTL := calcDuration * 2
	if r.minTTL < minTTL {
		r.minTTL = minTTL
	}
	if r.minTTL > r.maxTTL {
		r.minTTL = r.maxTTL
	}
	if r.limitWorker {
		// pop worker
		logger.Printf("thread %v, entry %s, releasing worker...\n", thread, r.key)
		<-cc.limitWorkers
	}
	logger.Printf("thread %v,waiting to lock entry %s for updating && broadcasting value is ready\n", thread, r.key)
	entry.Lock()
	if err == nil {
		entry.Value, err = serialize(v)
	}
	entry.Err = err
	close(entry.wait) // broadcast result of local calculation to clients
	logger.Printf("thread %v,entry %s broadcast value %v is ready, setting cache entry\n", thread, r.key, v)
	entry.wait = nil // calculations complete
	// update refresh and expire
	entry.Refresh = now.Add(r.minTTL)
	entry.Expire = now.Add(r.maxTTL)
	entry.Unlock()
	logger.Printf("thread %v,entry %s %v was unlocked\n", thread, r.key, v)
	cc.Lock()
	cc.entries[r.key] = entry
	cc.Unlock()
	logger.Printf("thread %v,entry %s %v is set to cache entry\n", thread, r.key, v)
	if !hadValue {
		cc.pushValue(entry, r)
		logger.Printf("thread %v,entry %s value %v has been pushed to ready channel\n", thread, r.key, v)
	}
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

func entryNonEmpty(e *cacheEntry) bool {
	return e.Expire.IsZero()
}

func (cc *CachedCalculations) removeExpired() {
	cc.Lock()
	defer cc.Unlock()
	now := time.Now()
	for k, e := range cc.entries {
		e.Lock()
		if e.wait == nil && !e.Expire.IsZero() && e.Expire.Before(now) {
			delete(cc.entries, k)
		}
		e.Unlock()
	}
}
