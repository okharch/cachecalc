package cachecalc

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

func getKeyLock(key string) string {
	return key + ".lock"
}

// external cache minimum delay, it can be aligned during execution
var ecMinDelay = time.Millisecond * 100

func (cc *CachedCalculations) obtainExternal(ctx context.Context, entry *CacheEntry, r *request) (err error) {
	key := fmt.Sprint(r.key)
	lockKey := getKeyLock(key)
	thread := getThread(ctx)
	//entry.wait = make(chan struct{}) // make all other threads wait
	defer func() {
		if entry != nil {
			logger.Printf("thread %v: unlock entry %s, broadcast value: %v", thread, key, getEntryValue(entry, r))
			entry.Unlock()
		}
	}()
	//reason := "init value for local cache"
	// this loop tries to obtain either lock to external cache for the key to calculate its own version
	//entryLocked := make(chan struct{})
	//lockTTL := time.Millisecond * 20
	ttl := nzDuration(r.MaxTTL)
	logger.Printf("thread %v: trying to set external lock %s", thread, lockKey)
	ec := cc.externalCache
	lockRelease, err := ec.GetLock(ctx, lockKey, r.MinTTL)
	if err != nil {
		return fmt.Errorf("thread %v: failed to obtain external lock %s: %w", thread, lockKey, err)
	}
	defer func() {
		if errRelease := lockRelease(); errRelease != nil && err == nil {
			err = fmt.Errorf("failed to release external lock %s: %w", lockKey, errRelease)
			logger.Printf("thread %v: %s", thread, err)
		}
	}()
	logger.Printf("thread %v: got external lock %s, fetching latest value...", thread, lockKey)
	// externalLock obtained, check entrySerialized for key
	logger.Printf("thread %v:[%s] getting external value", thread, key)
	entrySerialized, externalExists, err := ec.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err)
	}
	var valueUpdated bool
	if externalExists {
		err = deserializeEntry(entrySerialized, entry)
		logger.Printf("thread %v:%s external value exists: %v", thread, key, getEntryValue(entry, r))
		if err != nil {
			return fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err)
		}
		if entry.Err != nil {
			return entry.Err
		}
		valueUpdated, err = cc.calculateValue(ctx, r, entry, false)
		logger.Printf("thread %v: broadcast %s external value:%v", thread, key, getEntryValue(entry, r))
	} else {
		logger.Printf("thread %v:%s external value does not exist", thread, key)
		_, err = cc.calculateValue(ctx, r, entry, false)
		valueUpdated = true
	}
	if err != nil {
		return err
	}
	if valueUpdated {
		se, err := serializeEntry(entry)
		if err == nil {
			err = cc.externalCache.Set(ctx, key, se, ttl)
		}
		logger.Printf("thread %v:%s SET external cache to %v, expires in %dms", thread, r.key, getEntryValue(entry, r), ttl.Milliseconds())
	}
	err = entry.Err
	return err
}

// this is used for debug only
func getEntryValue(entry *CacheEntry, r *request) any {
	err := deserialize(entry.Value, r.dest)
	if err != nil {
		return fmt.Errorf("failed to deserialize entry.Value: %w", err)
	}
	v := reflect.ValueOf(r.dest).Elem()
	return v
}

func releaseLockOnContextCancel(ctx context.Context, releaseLock func() error) {
	<-ctx.Done()
	_ = releaseLock()
}
