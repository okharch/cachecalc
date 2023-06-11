package cachecalc

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

func (cc *CachedCalculations) obtainExternal(ctx context.Context, r *request) (err error) {
	lockKey := r.key + ".Lock"
	thread := getThread(ctx)
	logger.Printf("thread %v obtain %s", thread, r.key)
	// try to obtain the value from local cache
	entry := cc.obtainEntry(r)
	// there should be the only thread that communicates with external/calculates value from the single cc instance
	// activeThread flag marks current thread as the one
	pushValue := true
	reason := "no value found"
	if entryNonEmpty(entry) {
		cc.pushValue(ctx, entry, r)
		err = entry.Err
		if entry.Refresh.Before(time.Now()) {
			logger.Printf("thread %v:%s leaving no need to refresh for %v", thread, r.key, time.Now().Sub(entry.Refresh))
			entry.Unlock()
			return
		}
		reason = "refresh"
		pushValue = false
	}
	activeThread := entry.wait == nil
	if !activeThread {
		entry.Unlock()
		if !pushValue {
			logger.Printf("thread %v, key %s - leaving with cached entry %v", thread, r.key, getEntryValue(entry, r))
			return
		}
		<-entry.wait
		entry.RLock()
		defer entry.RUnlock()
		cc.pushValue(ctx, entry, r)
		return entry.Err
	}
	// release external lock on exit if it was obtained
	var externalLock bool // we obtained lock from external
	defer func() {
		if externalLock {
			if err := cc.externalCache.Del(ctx, lockKey); err != nil {
				logger.Printf("thread %v, key %s failed to remove lock %s: %s", thread, r.key, lockKey, err)
			} else {
				logger.Printf("thread %v, key %s lock %s removed", thread, r.key, lockKey)
			}
		}
	}()
	entry.wait = make(chan struct{})
	entry.Unlock()
	// this loop tries to obtain either the freshest value from external or lock to calculate its own version
	var externalExists bool
	var entrySerialized []byte
	unlockEntry := func() {
		close(entry.wait)
		entry.wait = nil
		entry.Unlock()
	}
	// loop until either got lock or entry value from external cache
	for !externalExists {
		// check entrySerialized for key
		logger.Printf("thread %v:%s getting external value", thread, r.key)
		entrySerialized, externalExists, err = cc.externalCache.Get(ctx, r.key)
		if err != nil {
			return fmt.Errorf("thread %v, key %s:failed to obtain entrySerialized from external cache: %w", thread, r.key, err)
		}
		if externalExists {
			//logger.Printf("thread %v key %s : got external entry length %d", thread, r.key, len(entrySerialized))
			entry.Lock()
			err = deserializeEntry(entrySerialized, entry)
			if err != nil {
				unlockEntry()
				return fmt.Errorf("thread %v, key %s:failed to obtain entrySerialized from external cache: %w", thread, r.key, err)
			}
			logger.Printf("thread %v broadcast %s value obtained from external cache to the local clients", thread, r.key)
			if entry.Err != nil {
				unlockEntry()
				return entry.Err
			}
			// broadcast obtained value
			if pushValue {
				cc.pushValue(ctx, entry, r)
				pushValue = false // no more need to push value, do it only once
			}
			if entry.Refresh.After(time.Now()) {
				unlockEntry()
				logger.Printf("thread %v no need to refresh %s, exiting", thread, r.key)
				return entry.Err
			} // continue only if needed to refresh value
			close(entry.wait)
			entry.wait = make(chan struct{})
			entry.Unlock()
		}
		externalLock, err = cc.externalCache.SetNX(ctx, lockKey, []byte{1}, r.MaxTTL)
		if err != nil {
			return fmt.Errorf("thread %v, key %s:failed to set external lock : %w", thread, r.key, err)
		}
		if externalLock {
			break
		} else {
			if externalExists {
				logger.Printf("thread %v exiting as was not able to take lock %s", thread, r.key)
				entry.Lock()
				err = entry.Err
				unlockEntry() // close wait
				return
			}
		}
		time.Sleep(time.Millisecond * 10) // sleep 10ms and try again
	}
	// as we got external lock, we can calculate value locally, set it to external cache
	entry.Lock()
	logger.Printf("thread %v:%s recalculating value: %s", thread, r.key, reason)
	_ = cc.calculateValue(ctx, r, entry, pushValue) // ignore returned value, it is stored to entry
	// serialize and set external cache to the latest value
	entry.RLock()
	defer entry.RUnlock()
	se, err := serializeEntry(entry)
	if err != nil {
		return err
	}
	err = cc.externalCache.Set(ctx, r.key, se, r.MaxTTL)
	if err != nil {
		return err
	}
	logger.Printf("thread %v:%s SET external cache to %v", thread, r.key, getEntryValue(entry, r))
	return entry.Err
}

func getEntryValue(entry *cacheEntry, r *request) any {
	entry.RLock()
	defer entry.RUnlock()
	deserialize(entry.Value, r.dest)
	return reflect.ValueOf(r.dest).Elem()
}
