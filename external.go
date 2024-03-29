package cachecalc

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

func (cc *CachedCalculations) obtainExternal(ctx context.Context, r *request) (err error) {
	key := fmt.Sprintf("%s", r.key)
	lockKey := getKeyLock(key)
	thread := getThread(ctx)
	logger.Printf("thread %v obtain local entry %s", thread, key)
	// try to obtain the value from local cache
	entry := cc.obtainEntry(r) // this will lock entry
	// there should be the only thread that communicates with external/calculates value from the single cc instance
	// activeThread flag marks current thread as the one
	pushValue := true
	reason := "no value found"
	err = entry.Err
	if entryNonEmpty(entry) {
		cc.pushValue(ctx, entry, r)
		if entry.Refresh.After(time.Now()) {
			logger.Printf("thread %v:%s leaving no need to refresh for %v", thread, key, entry.Refresh.Sub(time.Now()))
			entry.Unlock()
			return err
		}
		reason = "refresh"
		pushValue = false
	}
	// if thread is not the one that calculates value or is obtaining it from external cache it should wait for the result
	wait := entry.wait
	if wait != nil {
		if !pushValue {
			logger.Printf("thread %v, %s - leaving with cached entry %v", thread, key, getEntryValue(entry, r))
			entry.Unlock()
			return err
		}
		logger.Printf("thread %v, %s - waiting entry to be calculated", thread, key)
		entry.Unlock()
		<-wait
		entry.RLock()
		cc.pushValue(ctx, entry, r)
		err = entry.Err
		entry.RUnlock()
		return err
	}
	entry.wait = make(chan struct{}) // make all other threads wait
	entry.Unlock()
	unlockEntry := func() {
		close(entry.wait)
		entry.wait = nil
		entry.Unlock()
	}
	externalLock := false
	defer func() {
		// release external lock on exit if it was obtained
		if !externalLock {
			return
		}
		// as we got external lock, we can calculate value locally, set it to external cache
		if err = cc.externalCache.Del(ctx, lockKey); err != nil {
			logger.Printf("thread %v, failed to remove lock %s: %s", thread, lockKey, err)
		} else {
			logger.Printf("thread %v, external lock %s removed", thread, lockKey)
		}
	}()
	// this loop tries to obtain either the freshest value from external
	// or lock to calculate its own version
	ttl := nzDuration(r.MaxTTL)
	for {
		// check entrySerialized for key
		logger.Printf("thread %v:%s getting external value", thread, key)
		entrySerialized, externalExists, err := cc.externalCache.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err)
		}
		if externalExists {
			logger.Printf("thread %v:%s external value exists", thread, key)
			entry.Lock()
			err = deserializeEntry(entrySerialized, entry)
			if err != nil {
				unlockEntry() // deserializeEntry, will close wait
				return fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err)
			}
			if entry.Err != nil {
				err = entry.Err
				unlockEntry() // deserializeEntry, entry.Err != nil, will close wait
				return err
			}
			logger.Printf("thread %v: broadcast %s external value: %v", thread, key, pushValue)
			// broadcast obtained value
			if pushValue {
				cc.pushValue(ctx, entry, r)
				pushValue = false // no more need to push value, do it only once
			}
			if entry.Refresh.After(time.Now()) {
				logger.Printf("thread %v no need to refresh %s (%v), exiting", thread, r.key, time.Since(entry.Refresh))
				err := entry.Err
				unlockEntry()
				return err
			}
			// continue only if needed to refresh value
			close(entry.wait)                // release other threads, they would use value obtained from external cache
			entry.wait = make(chan struct{}) // and this thread will recalculate new value
			logger.Printf("thread %v will be refreshing value %s", thread, r.key)
			entry.Unlock()
		} // leaves with entry.Unlock()
		if externalLock {
			logger.Printf("thread %v got external lock %s", thread, r.key)
			break // to calculate value after external lock is obtained
		}
		logger.Printf("thread %v: trying to set external lock %s", thread, lockKey)
		externalLock, err = cc.externalCache.SetNX(ctx, lockKey, []byte(fmt.Sprint(thread)), ttl)
		if err != nil {
			return fmt.Errorf("thread %v: failed to set external lock %s: %w", thread, lockKey, err)
		}
		if externalLock {
			logger.Printf("thread %v: got external lock %s, checking latest value...", thread, lockKey)
			continue
		}
		if externalExists {
			logger.Printf("thread %v leaving with old value as was not able to take lock %s", thread, lockKey)
			entry.Lock()
			err = entry.Err
			unlockEntry() // close wait
			return err
		}
		ttc := nzDuration(entry.CalcDuration, r.CalcTime, time.Millisecond*20)
		time.Sleep(ttc)
	}
	logger.Printf("thread %v:%s calculating value: %s", thread, key, reason)
	entry.Lock()                                    // this will be unlocked by calculateValue
	_ = cc.calculateValue(ctx, r, entry, pushValue) // ignore returned value, it is stored to entry, also makes entry.Unlock()
	// serialize and set external cache to the latest value
	entry.Lock()
	defer entry.Unlock()
	se, err := serializeEntry(entry)
	if err != nil {
		return err
	}
	err = cc.externalCache.Set(ctx, key, se, ttl)
	if err != nil {
		return err
	}
	//logger.Printf("thread %v:%s SET external cache to %v, expires in %dms", thread, r.key, getEntryValue(entry, r), ttl.Milliseconds())
	err = entry.Err
	return err
}

func getEntryValue(entry *CacheEntry, r *request) any {
	//entry.RLock()
	//defer entry.RUnlock()
	deserialize(entry.Value, r.dest)
	v := reflect.ValueOf(r.dest).Elem()
	return v
}
