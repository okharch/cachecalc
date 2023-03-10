package cachecalc

import (
	"context"
	"log"
	"os"
	"sync"
	"time"
)

// CalculateValue this is type of function which returns interface{} type
type CalculateValue func(context.Context) (any, error)

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
	Expire  time.Time     // time of expiration of this entry
	Refresh time.Time     // time when this value should be refreshed
	Request time.Time     // when this Value was last time requested. If it is not requested recently it will not be refreshed
	wait    chan struct{} // closed when entry is wait
	Err     error
	Value   []byte
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
	//logger = log.New(io.Discard, "", log.LstdFlags)
	logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
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

// GetCachedCalc is used wherever you need to perform cached and coordinated calculation instead of regular and uncoordinated
//
// ctx is a parent context for calculation
// if parent context is cancelled then all child context are cancelled as well
//
// params of CachedCalculation - see description of how CachedCalculation defined
func GetCachedCalc[T any](cc *CachedCalculations, ctx context.Context, key string, minTTL, maxTTL time.Duration, limitWorker bool,
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
	entry, entryExists, entryNeedRefresh := cc.obtainEntry(ctx, r)
	logger.Printf("lock was obtained for entry %s\n", r.key)
	if !entryExists {
		return cc.calculateValue(r, entry, "not exists", true)
	}
	cc.pushValue(entry, r)
	if entryNeedRefresh { // item can be refreshed
		return cc.calculateValue(r, entry, "refresh", false)
	}
	entry.Unlock() // this release will happen only if calculateValue has not been called, otherwise Unlock is done there
	logger.Printf("lock was released for entry %s\n", r.key)
	return nil
}

func (cc *CachedCalculations) obtainEntry(ctx context.Context, r *request) (entry *cacheEntry, exists bool, needRefresh bool) {
	cc.Lock()
	defer cc.Unlock()
	entry, exists = cc.entries[r.key]
	if !exists {
		// create new entry for internal memory as entry does not exist
		entry = &cacheEntry{}
		entry.ctx, entry.cancel = context.WithCancel(ctx)
		cc.entries[r.key] = entry
	}
	entry.Lock()
	if exists {
		now := time.Now()
		needRefresh = entry.Refresh.Before(now)
	}
	return
}

func (cc *CachedCalculations) pushValue(entry *cacheEntry, r *request) {
	if entry.Err != nil {
		r.ready <- entry.Err
	} else {
		// store value to the destination variable
		r.ready <- deserialize(entry.Value, r.dest)
	}
}

func (cc *CachedCalculations) obtainExternal(ctx context.Context, r *request) (err error) {
	lockKey := r.key + ".Lock"
	gotLock := false
	// release lock on exit if it was obtained
	defer func() {
		if gotLock {
			if err := cc.externalCache.Del(ctx, lockKey); err != nil {
				logger.Printf("failed to remove lock %s: %s", lockKey, err)
			} else {
				logger.Printf("lock %s removed", lockKey)
			}
		}
	}()
	// try to obtain the value from local cache
	entry, entryExists, entryNeedRefresh := cc.obtainEntry(ctx, r)
	if entryExists && !entryNeedRefresh {
		entry.Unlock()
		cc.pushValue(entry, r)
		return entry.Err
	}
	// try to get lock on external
	panic("not implemented")
	//for {
	//	ts := intbytes(time.Now().UnixMilli())
	//	if gotLock, err = cc.externalCache.SetNX(ctx, lockKey, ts, r.maxTTL); err != nil {
	//		return
	//	}
	//	value, external, err := cc.externalCache.GetCachedCalc(entry.ctx, r.key)
	//	if external {
	//		// we keep at external cache whole entry. entry is pointer to cache entry
	//		err := deserialize(value, entry)
	//		if err == nil {
	//			logger.Printf("got value %v from external cc", ts(entry.Refresh))
	//			if entry.Refresh.After(time.Now()) {
	//				logger.Printf("no need to refresh value %v from external cc", ts(entry.Refresh))
	//				return nil
	//			}
	//		}
	//	}
	//	if !gotLock {
	//		time.Sleep(time.Millisecond)
	//		continue
	//	}
	//	if entry.ctx.Err() != nil {
	//		return entry.ctx.Err()
	//	}
	//	if err != nil {
	//		err = fmt.Errorf("external cc: %s", err)
	//		return err
	//	}
	//}
	//	// try getting lock to refresh value
	//	if !gotLock {
	//		lockCounter++
	//		logger.Printf("obtaining lock %d to refresh value", lockCounter)
	//		gotLock, err = cc.externalCache.SetNX(ctx, lockKey, uin32bytes(lockCounter), r.maxTTL)
	//		if err != nil {
	//			return fmt.Errorf("set lock error: %w", err)
	//		}
	//	}
	//	logger.Printf("gotLock %d: %v", lockCounter, gotLock)
	//	if gotLock {
	//		err2, done := cc.calculateValue(r, err, e, reason, pushValue)
	//		// set newly calculated item to external cc
	//		buf, err := serializeEntry(e)
	//		if err != nil {
	//			return fmt.Errorf("error marshalling cc entry: %s", err)
	//		}
	//		err = cc.externalCache.Set(e.ctx, r.key, buf, r.maxTTL)
	//		if err != nil {
	//			return fmt.Errorf("failed to set external cc %s: %s", r.key, err)
	//		}
	//		return nil
	//	} else {
	//		// wait until the one with lock calculates new value.
	//		logger.Print("waiting the result of calculation from redis")
	//		time.Sleep(time.Millisecond * 20)
	//	}
	//}
}

func (cc *CachedCalculations) calculateValue(r *request, entry *cacheEntry, reason string, pushValue bool) (err error) {
	wait := entry.wait // before starting calculation check whether someone else is not performing it already
	if wait != nil {
		entry.Unlock()
		logger.Printf("lock was released for entry %s\n", r.key)
		if pushValue {
			logger.Printf("waiting while other thread calculating entry %s\n", r.key)
			<-wait // wait until it was closed
			logger.Printf("waiting for other thread completed: %s\n", r.key)
			cc.pushValue(entry, r)
			logger.Printf("entry %s value has been pushed to ready channel from cache\n", r.key)
		}
		return entry.Err
	}

	entry.wait = make(chan struct{}) // mark that calculation is being performed for this entry
	entry.Unlock()
	logger.Printf("lock was released for entry %s\n", r.key)
	if r.limitWorker {
		// add new worker
		cc.limitWorkers <- struct{}{}
	}
	started := time.Now()
	var v any
	v, err = r.calculateValue(context.WithValue(entry.ctx, "reason", reason))
	logger.Printf("value %s recalculated to %v", r.key, v)
	calcDuration := time.Since(started)
	if err == nil {
		entry.Value, err = serialize(v)
	}
	entry.Err = err
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
		<-cc.limitWorkers
	}
	logger.Printf("waiting to lock entry %s for broadcasting value is ready\n", r.key)
	entry.Lock()
	close(entry.wait) // broadcast to calculation who waits
	logger.Printf("entry %s broadcast value %v is ready, setting cache entry\n", r.key, v)
	entry.wait = nil // calculations complete
	// update refresh and expire
	entry.Refresh = now.Add(r.minTTL)
	entry.Expire = now.Add(r.maxTTL)
	entry.Unlock()
	logger.Printf("entry %s %v was unlocked\n", r.key, v)
	cc.Lock()
	cc.entries[r.key] = entry
	cc.Unlock()
	logger.Printf("entry %s %v is set to cache entry\n", r.key, v)
	if pushValue {
		cc.pushValue(entry, r)
		logger.Printf("entry %s value %v has been pushed to ready channel\n", r.key, v)
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

func (cc *CachedCalculations) removeExpired() {
	cc.Lock()
	defer cc.Unlock()
	now := time.Now()
	for k, e := range cc.entries {
		e.Lock()
		if e.wait == nil && e.Expire.Before(now) {
			delete(cc.entries, k)
		}
		e.Unlock()
	}
}
