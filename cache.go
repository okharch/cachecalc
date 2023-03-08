package cachecalc

import (
	"context"
	"errors"
	"fmt"
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
	ready          chan string // error message, empty if no error
	dest           any         // but provide pointer to result!!!
	limitWorker    bool        // if you know calculation puts heavy load on infrastructure, than set it to true!
	maxTTL, minTTL time.Duration
}

type cacheEntry struct {
	Expire  time.Time
	Refresh time.Time
	Err     string
	Value   []byte
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// CachedCalculation has the only method: Get. It is used for easy refactoring of slow/long calculating backend methods. See examples
type CachedCalculation struct {
	entries       map[string]*cacheEntry
	requests      chan *request
	externalCache ExternalCache
	workers       sync.WaitGroup
	limitWorkers  chan struct{}
	idle          chan struct{}
	sync.Mutex
	sync.WaitGroup
}

var logger *log.Logger

func init() {
	logger = log.New(io.Discard, "", log.LstdFlags)
	//logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
}

// NewCache is used to create app's instance of SmartCache.
// It creates two threads which handle and coordinate cached backend calculations
// Graceful exit from app should include expiring ctx context and then smartCacheInstance.Wait()
// This will gracefully finish the job of those threads
func NewCache(ctx context.Context, maxWorkers int, externalCache ExternalCache) *CachedCalculation {
	var cc CachedCalculation
	cc.entries = make(map[string]*cacheEntry, 1024*16)
	cc.requests = make(chan *request)
	cc.externalCache = externalCache
	cc.limitWorkers = make(chan struct{}, maxWorkers+1)
	cc.idle = make(chan struct{}, 1)
	//cc.buf = new(bytes.Buffer)
	cc.Add(2)
	go cc.serve(ctx)
	go cc.checkIdle(ctx)
	return &cc
}

// Get is used wherever you need cached and coordinated CalculateValue instead of slow and uncoordinated
// key: you should compose some string key based on request parameters,
// calculate: you should wrap your calculation into closure,
// dest: provide pointer to the variable where you are expecting the result of calculations
// limitWorker: set it to true if you know that calculation puts heavy load on infrastructure when used concurrently with other similar calculations.
// Then when it occures that the limit of workers is reached it will be put to queue and waiting for some of them to complete
// maxTTL - time of CachedCalculation entry expiration. When it is reached the entry is removed from cc
// minTTL - time when it needs to be refreshed in background. Still cached entry is returned to the client
func (cc *CachedCalculation) Get(ctx context.Context, key string, calculate CalculateValue, dest any, limitWorker bool, maxTTL, minTTL time.Duration) error {
	ready := make(chan string)
	cc.requests <- &request{
		calculateValue: calculate,
		ctx:            ctx,
		key:            key,
		dest:           dest,
		ready:          ready,
		maxTTL:         maxTTL,
		minTTL:         minTTL,
		limitWorker:    limitWorker,
	}
	errMsg := <-ready
	if errMsg == "" {
		return nil
	}
	return errors.New(errMsg)
}

func (cc *CachedCalculation) serve(ctx context.Context) {
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

func (cc *CachedCalculation) checkIdle(ctx context.Context) {
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
func (cc *CachedCalculation) Close() {
	cc.Lock()
	defer cc.Unlock()
	for k, v := range cc.entries {
		v.cancel()
		v.wg.Wait()
		delete(cc.entries, k)
	}
}

var lockCounter uint32

func uin32bytes(u uint32) []byte {
	b := [4]byte{
		byte(0xff & u),
		byte(0xff & (u >> 8)),
		byte(0xff & (u >> 16)),
		byte(0xff & (u >> 24))}
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

func (cc *CachedCalculation) refreshValue(ctx context.Context, r *request, pushReady bool) (err error) {
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
	logger.Printf("refreshing value %s, pushing: %v", r.key, pushReady)
	cc.Lock()
	e, ok := cc.entries[r.key]
	if !ok {
		e = &cacheEntry{}
		cc.entries[r.key] = e
	}
	cc.Unlock()
	e.wg.Wait()
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Add(1)
	defer e.wg.Done()
	pushValue := func() {
		if !pushReady {
			return
		}
		if e.Err != "" {
			r.ready <- e.Err
		} else {
			err := deserialize(e.Value, r.dest)
			if err != nil {
				r.ready <- fmt.Sprintf("deserialize value error: %s", err)
			} else {
				r.ready <- ""
			}
		}
		pushReady = false
	}
	for {
		value, exists, err := cc.externalCache.Get(e.ctx, r.key)
		if e.ctx.Err() != nil {
			return e.ctx.Err()
		}
		if exists {
			err := deserialize(value, &e)
			if err == nil {
				logger.Printf("got value %v from external cc", ts(e.Refresh))
				cc.Lock()
				cc.entries[r.key] = e
				cc.Unlock()
				pushValue()
				if e.Refresh.After(time.Now()) {
					logger.Printf("no need to refresh value %v from external cc", ts(e.Refresh))
					return nil
				}
			}
		}
		if err != nil {
			err = fmt.Errorf("external cc: %s", err)
			return err
		}
		// try to get lock to refresh value
		if !gotLock {
			lockCounter++
			logger.Printf("obtaining lock %d to refresh value", lockCounter)
			gotLock, err = cc.externalCache.SetNX(ctx, lockKey, uin32bytes(lockCounter), r.maxTTL)
			if err != nil {
				return fmt.Errorf("set lock error: %w", err)
			}
		}
		logger.Printf("gotLock %d: %v", lockCounter, gotLock)
		if gotLock {
			// we got the lock, let's recalculate value
			if r.limitWorker {
				cc.limitWorkers <- struct{}{}
			}
			started := time.Now()
			v, err := r.calculateValue(e.ctx)
			logger.Printf("value %s recalculated to %v", r.key, v)
			calcDuration := time.Since(started)
			if err == nil {
				e.Value, err = serialize(v)
				e.Err = ErrMsg(err)
				err = deserialize(e.Value, r.dest)
				if err != nil {
					logger.Printf("deserialization error: %s", err)
				}
			} else {
				e.Err = err.Error()
			}
			now := time.Now()
			if r.minTTL.Microseconds() != 0 {
				if calcDuration > r.minTTL {
					logger.Printf("time for calculation(%v) of %s is greater than minTTL(%v), fixed", calcDuration, r.key, r.minTTL)
				}
			}
			minTTL := calcDuration * 2
			if r.minTTL < minTTL {
				r.minTTL = minTTL
			}
			if r.minTTL > r.maxTTL {
				r.minTTL = r.maxTTL
			}
			e.Refresh = now.Add(r.minTTL)
			e.Expire = now.Add(r.maxTTL)
			cc.Lock()
			cc.entries[r.key] = e
			cc.Unlock()
			if r.limitWorker {
				<-cc.limitWorkers
			}
			pushValue()
			// set newly calculated item to external cc
			buf, err := serialize_entry(e)
			if err != nil {
				err = fmt.Errorf("error marshalling cc entry: %s", err)
				return err
			}
			err = cc.externalCache.Set(e.ctx, r.key, buf, r.maxTTL)
			if err != nil {
				return fmt.Errorf("failed to set external cc %s: %s", r.key, err)
			}
			return nil
		} else {
			// wait until the one with lock calculates new value.
			logger.Print("waiting the result of calculation from redis")
			time.Sleep(r.minTTL / 8)
		}
	}
}

func (cc *CachedCalculation) handleRequest(r *request) {
	// check if entry already exists in local cc
	refresh := func(pushReady bool) {
		defer cc.workers.Done()
		if err := cc.refreshValue(r.ctx, r, pushReady); err != nil {
			logger.Printf("failed to refresh value: %s", err)
		}
	}
	cc.Lock()
	e, ok := cc.entries[r.key]
	cc.Unlock()
	if ok {
		now := time.Now()
		logger.Printf("got locally cached value, expire: %v, refresh %v", ts(e.Expire), ts(e.Refresh))
		if e.Expire.After(now) {
			if e.Err != "" {
				r.ready <- e.Err
			} else {
				r.ready <- ErrMsg(deserialize(e.Value, r.dest))
			}
			if e.Refresh.Before(now) {
				cc.workers.Add(1)
				go refresh(false)
			}
			return
		}
	}
	cc.workers.Add(1)
	go refresh(true)
}

func (cc *CachedCalculation) runIdle(_ context.Context) {
	cc.Lock()
	defer cc.Unlock()
	for k, e := range cc.entries {
		if time.Now().After(e.Expire) {
			delete(cc.entries, k)
		}
	}
}
