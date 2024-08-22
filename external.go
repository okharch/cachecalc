package cachecalc

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

func (cc *CachedCalculations) obtainExternal(ctx context.Context, entry *CacheEntry, r *request) (err error) {
	key := fmt.Sprint(r.key)
	lockKey := getKeyLock(key)
	thread := getThread(ctx)
	externalLock := false
	unlockEntry := true // entry is locked here
	//entry.wait = make(chan struct{}) // make all other threads wait
	defer func() {
		//close(entry.wait) // release other threads
		//entry.wait = nil
		// release external lock on exit if it was obtained
		if externalLock {
			logger.Printf("thread %v: remove external lock %s", thread, lockKey)
			// with fresh context as we might be exiting with expired context
			if err = cc.externalCache.Del(context.TODO(), lockKey); err != nil { // remove lock before leaving obtainExternal()
				logger.Printf("thread %v, failed to remove lock %s: %s", thread, lockKey, err)
			} else {
				logger.Printf("thread %v, external lock %s removed", thread, lockKey)
			}
		}
		if entry != nil && unlockEntry {
			logger.Printf("thread %v: unlock entry %s, broadcast value", thread, key)
			entry.Unlock()
		}
	}()
	//reason := "init value for local cache"
	// this loop tries to obtain either lock to external cache for the key to calculate its own version
	ttl := nzDuration(r.MaxTTL)
	logger.Printf("thread %v: trying to set external lock %s", thread, lockKey)
	for {
		// check for context cancellation
		if err = ctx.Err(); err != nil {
			return err
		}
		// try to set lock before any operation:read or write
		externalLock, err = cc.externalCache.SetNX(ctx, lockKey, []byte(fmt.Sprint(thread)), ttl)
		if err != nil {
			return fmt.Errorf("thread %v: failed to set external lock %s: %w", thread, lockKey, err)
		}
		if externalLock {
			break
		}
		// wait sometime before next try
		time.Sleep(time.Millisecond * 20)
	}
	logger.Printf("thread %v: got external lock %s, fetching latest value...", thread, lockKey)
	// externalLock obtained, check entrySerialized for key
	logger.Printf("thread %v:[%s] getting external value", thread, key)
	entrySerialized, externalExists, err := cc.externalCache.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err)
	}
	if externalExists {
		logger.Printf("thread %v:%s external value exists", thread, key)
		err = deserializeEntry(entrySerialized, entry)
		if err != nil {
			return fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err)
		}
		if entry.Err != nil {
			return entry.Err
		}
		logger.Printf("thread %v: broadcast %s external value", thread, key)
		unlockEntry = false // entry will be unlocked by calculateValue
		err = cc.calculateValue(ctx, r, entry, true)
	} else {
		logger.Printf("thread %v:%s external value does not exist", thread, key)
		unlockEntry = false // entry will be unlocked by calculateValue
		err = cc.calculateValue(ctx, r, entry, false)
		defer entry.Unlock()
		se, err := serializeEntry(entry)
		if err == nil {
			err = cc.externalCache.Set(ctx, key, se, ttl)
		}
		if err != nil {
			return err
		}
	}
	logger.Printf("thread %v:%s SET external cache to %v, expires in %dms", thread, r.key, getEntryValue(entry, r), ttl.Milliseconds())
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
