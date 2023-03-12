package cachecalc

import (
	"context"
	"fmt"
	"time"
)

func (cc *CachedCalculations) obtainExternal(ctx context.Context, r *request) (err error) {
	lockKey := r.key + ".Lock"
	thread := getThread(ctx)
	// try to obtain the value from local cache
	entry := cc.obtainEntry(ctx, r)
	// there should be the only thread that communicates with external/calculates value from the single cc instance
	// activeThread flag marks current thread as the one
	activeThread := entry.wait == nil
	if !activeThread {
		if entryNonEmpty(entry) {
			cc.pushValue(entry, r)
			defer entry.Unlock()
			return entry.Err
		}
		entry.Unlock()
		<-entry.wait
		entry.RLock()
		defer entry.RUnlock()
		cc.pushValue(entry, r)
		return entry.Err
	}
	// release lock on exit if it was obtained
	var localLock bool
	var externalLock bool // we obtained lock from external
	defer func() {
		if externalLock {
			if err := cc.externalCache.Del(ctx, lockKey); err != nil {
				logger.Printf("failed to remove lock %s: %s", lockKey, err)
			} else {
				logger.Printf("lock %s removed", lockKey)
			}
		}
		if !localLock {
			entry.Lock()
			entry.wait = nil
		}
		entry.Unlock()
	}()
	entry.wait = make(chan struct{})
	entry.Unlock()
	// this loop tries to obtain either the freshest value from external or lock to calculate its own version
	pushValue := true // need to push obtained value only once
	for {
		// check entrySerialized for key
		entrySerialized, externalExists, err := cc.externalCache.Get(ctx, r.key)
		if err != nil {
			return fmt.Errorf("thread %v, key %s:failed to obtain entrySerialized from external cache: %w", thread, r.key, err)
		}
		if externalExists {
			localLock = true
			entry.Lock()
			err = deserializeEntry(entrySerialized, entry)
			if err != nil {
				return fmt.Errorf("thread %v, key %s:failed to obtain entrySerialized from external cache: %w", thread, r.key, err)
			}
			close(entry.wait) // broadcast value obtained from external cache to the local clients
			if entry.Err != nil {
				return entry.Err
			}
			if pushValue {
				cc.pushValue(entry, r)
				pushValue = false // no more need to push value, do it only once
			}
			if entry.Refresh.Before(time.Now()) {
				return entry.Err
			} // continue only if needed to refresh value
		} // continue if not external value exists
		// try to set lock
		if !externalLock {
			externalLock, err = cc.externalCache.SetNX(ctx, r.key, []byte{1}, r.maxTTL)
			if err != nil {
				return fmt.Errorf("thread %v, key %s:failed to set external lock : %w", thread, r.key, err)
			}
			if externalLock {
				break
			}
		} // continue if got external lock
		time.Sleep(time.Millisecond * 10) // sleep 10ms and try again
	} // looping until we get external Lock
	// as we got external lock, we can calculate value locally, set it to external cache
	entry.Lock()
	wait := entry.wait
	entry.wait = nil                                // need for entry inside calculateValue consider itself "active"
	_ = cc.calculateValue(ctx, r, entry, pushValue) // ignore returned value, it is stored to entry
	entry.wait = wait                               // restore broadcast line for pre-calculation's clients
	close(entry.wait)                               // broadcast those who waited before calculations
	// serialize and set external cache to the latest value
	se, err := serializeEntry(entry)
	if err != nil {
		return err
	}
	err = cc.externalCache.Set(ctx, r.key, se, r.maxTTL)
	if err != nil {
		return err
	}
	return entry.Err
}
