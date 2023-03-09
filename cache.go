package cachecalc

import (
	"context"
	"io"
	"log"
	"strconv"
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
	limitWorker    bool       // if you know calculation puts heavy load on infrastructure, than set it to true!
	maxTTL, minTTL time.Duration
}

type cacheEntry struct {
	Expire       time.Time     // time of expiration of this entry
	Refresh      time.Time     // time when this value should be refreshed
	Request      time.Time     // when this Value was last time requested. If it is not requested recently it will not be refreshed
	wait         chan struct{} // closed when entry is wait
	Err          error
	Value        []byte
	sync.RWMutex // lock entry until it's value is wait
	ctx          context.Context
	cancel       context.CancelFunc
}

// CachedCalculation defines parameters of coordinated calculation.
// Parameter of this type is used for CachedCalculations.Get method
type CachedCalculation struct {
	Key         string         // key to store this calculation into cache
	Calculate   CalculateValue // function called to create/refresh the value for the Key
	LimitWorker bool           // true if it is heavy load and need to be limited cocnurrent workers like this
	MinTTL      time.Duration  // until MinTTL value is returned from cache and is not being refreshed
	MaxTTL      time.Duration  // when value expires completely and needs to be recalculated
}

// CachedCalculations has the only method: Get. It is used for easy refactoring of slow/long calculating backend methods. See examples
type CachedCalculations struct {
	entries       map[string]*cacheEntry
	requests      chan *request
	externalCache ExternalCache
	workers       sync.WaitGroup
	limitWorkers  chan struct{}
	idle          chan struct{}
	sync.Mutex
	sync.WaitGroup
}

// this is a global object for CachedCalculations,
// can be obtained using cachecalc.GetCachedCalculations() method
var singletonCC *CachedCalculations

// returns singleton CachedCalculations object
func GetCachedCalculations() *CachedCalculations {
	return singletonCC
}

// sets singleton CachedCalculations object.
// In most cases you only need the single instance of cached calculations.
// If that is the case, create instance using NewCachedCalculations and pass the instance to this function to set singleton.
// You then have access to it anywhere in application using cachecalc.GetCachedCalculations()
func SetCachedCalculations(cc *CachedCalculations) {
	singletonCC = cc
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
func NewCachedCalculations(ctx context.Context, maxWorkers int, externalCache ExternalCache) *CachedCalculations {
	var cc CachedCalculations
	cc.entries = make(map[string]*cacheEntry, 1024*16)
	cc.requests = make(chan *request)
	cc.externalCache = externalCache
	cc.limitWorkers = make(chan struct{}, maxWorkers+1)
	cc.idle = make(chan struct{}, 1)
	//cc.buf = new(bytes.Buffer)
	cc.Add(2)
	go cc.serve(ctx)     // serve requests to calculate
	go cc.checkIdle(ctx) // cleanup expired entries
	return &cc
}

// Get is used wherever you need to perform cached and coordinated calculation instead of regular and uncoordinated
//
// ctx is a parent context for calculation
// if parent context is cancelled then all child context are cancelled as well
//
// params of CachedCalculation - see description of how CachedCalculation defined
func (cc *CachedCalculations) Get(ctx context.Context, params CachedCalculation) (result any, err error) {
	ready := make(chan error)
	// put request to channel for handling
	cc.requests <- &request{
		calculateValue: params.Calculate,
		ctx:            ctx,
		key:            params.Key,
		dest:           &result,
		ready:          ready,
		maxTTL:         params.MaxTTL,
		minTTL:         params.MinTTL,
		limitWorker:    params.LimitWorker,
	}
	// then wait for the result to be wait
	err = <-ready
	return
}

func (cc *CachedCalculations) serve(ctx context.Context) {
	defer cc.Done()
	for {
		select {
		case <-ctx.Done():
			cc.Close()
			return
		case aRequest := <-cc.requests:
			cc.handleRequest(aRequest)
		case <-cc.idle:
			cc.runIdle(ctx)
		}
	}
}

func (cc *CachedCalculations) checkIdle(ctx context.Context) {
	defer cc.Done()
	for {
		// check if any workers active
		cc.workers.Wait()
		// if context expired then exit checkIdle thread
		if ctx.Err() != nil {
			return
		}
		cc.idle <- struct{}{}
		time.Sleep(time.Millisecond)
	}
}

// Close is automatically called on expired context. It is safe to call it multiple times
// it tries to gracefully interrupt all ongoing calculations using their context
// when it succeeds in this it removes the record about job
func (cc *CachedCalculations) Close() {
	cc.Lock()
	defer cc.Unlock()
	for k, v := range cc.entries {
		v.cancel() // Close(): cancel calculations if any, release context as well
		wait := v.wait
		if wait != nil {
			<-v.wait
		}
		delete(cc.entries, k)
	}
}

var lockCounter uint32

func intbytes(u int64) []byte {
	b := [8]byte{
		byte(0xff & u),
		byte(0xff & (u >> 8)),
		byte(0xff & (u >> 16)),
		byte(0xff & (u >> 24)),
		byte(0xff & (u >> 32)),
		byte(0xff & (u >> 40)),
		byte(0xff & (u >> 48)),
		byte(0xff & (u >> 56)),
	}
	return b[0:]
}

func ts(t time.Time) string {
	d := time.Since(t)
	return strconv.Itoa(int(d.Milliseconds()))
}

func ErrMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
	entry, entryExists, entryExpired, entryNeedRefresh := cc.obtainEntry(ctx, r)
	if !entryExists {
		return cc.calculateValue(r, entry, "not exists", true)
	}
	if entryExpired { // item expired, recalculate it
		return cc.calculateValue(r, entry, "expired", true)
	}
	cc.pushValue(entry, r)
	if entryNeedRefresh { // item can be refreshed
		return cc.calculateValue(r, entry, "refresh", false)
	}
	entry.Unlock() // release lock obtained at obtainEntry
	return nil
}

func (cc *CachedCalculations) obtainEntry(ctx context.Context, r *request) (entry *cacheEntry, exists bool, expired bool, needRefresh bool) {
	cc.Lock()
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
		expired = entry.Expire.Before(now)
		needRefresh = entry.Refresh.Before(now)
	}
	cc.Unlock() // we need to unlock whole cache before doing any calculations in order to not hold everybody
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
	entry, entryExists, entryExpired, entryNeedRefresh := cc.obtainEntry(ctx, r)
	if entryExists && !entryExpired && !entryNeedRefresh {
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
	//	value, external, err := cc.externalCache.Get(entry.ctx, r.key)
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

func (cc *CachedCalculations) calculateValue(r *request, e *cacheEntry, reason string, pushValue bool) (err error) {
	wait := e.wait
	if wait != nil {
		e.Unlock()
		<-wait // wait until it was closed
		return e.Err
	}

	e.wait = make(chan struct{})
	e.Unlock()
	if r.limitWorker {
		cc.limitWorkers <- struct{}{}
	}
	started := time.Now()
	var v any
	v, err = r.calculateValue(context.WithValue(e.ctx, "reason", reason))
	logger.Printf("value %s recalculated to %v", r.key, v)
	calcDuration := time.Since(started)
	if err == nil {
		e.Value, err = serialize(v)
	}
	e.Err = err
	now := time.Now()
	minTTL := calcDuration * 2
	if r.minTTL < minTTL {
		r.minTTL = minTTL
	}
	if r.minTTL > r.maxTTL {
		r.minTTL = r.maxTTL
	}
	// update refresh and expire
	e.Refresh = now.Add(r.minTTL)
	e.Expire = now.Add(r.maxTTL)
	// wait for next worker
	if r.limitWorker {
		<-cc.limitWorkers
	}
	e.Lock()
	close(e.wait) // broadcast calculation is wait
	e.wait = nil
	e.Unlock()
	cc.Lock()
	cc.entries[r.key] = e
	cc.Unlock()
	if pushValue {
		cc.pushValue(e, r)
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

func (cc *CachedCalculations) runIdle(_ context.Context) {
	cc.Lock()
	defer cc.Unlock()
	for k, e := range cc.entries {
		if time.Now().After(e.Expire) {
			delete(cc.entries, k)
		}
	}
}
