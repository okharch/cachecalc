package cachecalc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

type lockLease struct {
	lost   atomic.Bool
	stopCh chan struct{}
	doneCh chan struct{}
}

func newLockOwnerToken() ([]byte, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	token := make([]byte, hex.EncodedLen(len(buf)))
	hex.Encode(token, buf)
	return token, nil
}

func leaseRenewInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval <= 0 {
		interval = time.Millisecond * 10
	}
	return interval
}

func (cc *CachedCalculations) startLockLease(ctx context.Context, key string, ownerToken []byte, ttl time.Duration) *lockLease {
	lease := &lockLease{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go func() {
		defer close(lease.doneCh)
		ticker := time.NewTicker(leaseRenewInterval(ttl))
		defer ticker.Stop()
		for {
			select {
			case <-lease.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewed, err := cc.externalCache.ExtendIfValue(ctx, key, ownerToken, ttl)
				if err != nil {
					logger.Printf("warning: failed to renew external lock %s: %v", key, err)
					lease.lost.Store(true)
					return
				}
				if !renewed {
					logger.Printf("warning: external lock %s was lost before calculation completed", key)
					lease.lost.Store(true)
					return
				}
			}
		}
	}()
	return lease
}

func (l *lockLease) stop() {
	if l == nil {
		return
	}
	close(l.stopCh)
	<-l.doneCh
}

func (cc *CachedCalculations) obtainExternal(ctx context.Context, r *request) (err error) {
	key := r.key
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
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		entry.RLock()
		cc.pushValue(ctx, entry, r)
		err = entry.Err
		entry.RUnlock()
		return err
	}
	entry.wait = make(chan struct{}) // make all other threads wait
	entry.Unlock()
	var replySent atomic.Bool
	replySent.Store(!pushValue)
	var unlockEntry func()
	fail := func(err error, overwriteEntryErr bool) error {
		entry.Lock()
		if overwriteEntryErr {
			entry.Err = err
		}
		unlockEntry()
		if !replySent.Load() {
			replySent.Store(true)
			r.ready <- err
		}
		return err
	}
	unlockEntry = func() {
		close(entry.wait)
		entry.wait = nil
		entry.Unlock()
	}
	externalLock := false
	var ownerToken []byte
	defer func() {
		// release external lock on exit if it was obtained
		if !externalLock {
			return
		}
		removed, delErr := cc.externalCache.DelIfValue(ctx, lockKey, ownerToken)
		if delErr != nil {
			logger.Printf("thread %v, failed to remove lock %s: %s", thread, lockKey, delErr)
		} else if removed {
			logger.Printf("thread %v, external lock %s removed", thread, lockKey)
		} else {
			logger.Printf("thread %v, external lock %s already moved to another owner", thread, lockKey)
		}
	}()
	// this loop tries to obtain either the freshest value from external
	// or lock to calculate its own version
	lockTTL := nzDuration(r.MaxTTL)
	ownerToken, err = newLockOwnerToken()
	if err != nil {
		return fail(err, pushValue)
	}
	var lease *lockLease
	defer lease.stop()
	for {
		// check entrySerialized for key
		logger.Printf("thread %v:%s getting external value", thread, key)
		entrySerialized, externalExists, err := cc.externalCache.Get(ctx, key)
		if err != nil {
			return fail(fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err), pushValue)
		}
		if externalExists {
			logger.Printf("thread %v:%s external value exists", thread, key)
			entry.Lock()
			err = deserializeEntry(entrySerialized, entry)
			if err != nil {
				entry.Unlock()
				return fail(fmt.Errorf("thread %v: %s failed to obtain entrySerialized from external cache: %w", thread, key, err), pushValue)
			}
			if entry.Err != nil {
				err = entry.Err
				entry.Unlock()
				return fail(err, pushValue)
			}
			logger.Printf("thread %v: broadcast %s external value: %v", thread, key, pushValue)
			// broadcast obtained value
			if pushValue {
				cc.pushValue(ctx, entry, r)
				pushValue = false // no more need to push value, do it only once
				replySent.Store(true)
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
		externalLock, err = cc.externalCache.SetNX(ctx, lockKey, ownerToken, lockTTL)
		if err != nil {
			return fail(fmt.Errorf("thread %v: failed to set external lock %s: %w", thread, lockKey, err), pushValue)
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
		select {
		case <-time.After(ttc):
		case <-ctx.Done():
			return fail(ctx.Err(), pushValue)
		}
	}
	backgroundCalc := !pushValue
	calcCtx, cancelCalc := cc.calculationContext(ctx, backgroundCalc)
	defer cancelCalc()
	lease = cc.startLockLease(calcCtx, lockKey, ownerToken, lockTTL)
	logger.Printf("thread %v:%s calculating value: %s", thread, key, reason)
	entry.Lock()                                    // this will be unlocked by calculateValue
	_ = cc.calculateValue(calcCtx, r, entry, false) // ignore returned value, it is stored to entry, also makes entry.Unlock()
	lease.stop()
	lease = nil
	if entry.CalcDuration > lockTTL {
		logger.Printf("warning: calculation for %s took %v which exceeded external lock TTL %v; consider increasing MaxTTL/expiry", key, entry.CalcDuration, lockTTL)
	}
	// serialize and set external cache to the latest value
	entry.Lock()
	defer entry.Unlock()
	cacheTTL := time.Until(entry.Expire)
	if cacheTTL <= 0 {
		cacheTTL = lockTTL
	}
	se, err := serializeEntry(entry)
	if err != nil {
		return err
	}
	stored, err := cc.externalCache.SetIfLockOwned(calcCtx, lockKey, ownerToken, key, se, cacheTTL)
	if err != nil {
		if !replySent.Load() {
			replySent.Store(true)
			r.ready <- err
		}
		return err
	}
	if !stored {
		logger.Printf("warning: external lock %s was lost before publishing result; skipping external cache update", lockKey)
		if !replySent.Load() {
			cc.pushValue(calcCtx, entry, r)
			replySent.Store(true)
		}
		return entry.Err
	}
	if !replySent.Load() {
		cc.pushValue(calcCtx, entry, r)
		replySent.Store(true)
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
