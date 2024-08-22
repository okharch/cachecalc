package cachecalc

import (
	"context"
	"crypto/rand"
	"errors"
	"time"
)

var ErrNoLockFound = errors.New("no lock found")

// GetExternalLock tries to set lock using SetNX func of ExternalCache instance
// it starts a background process that automatically renews the TTL of a lock.
// it returns an error if the lock is not obtained, and releaseLock function.
// You must ensure releaseLock is called exactly once in order to release the lock. defer is recommended
func GetExternalLock(ctx context.Context, ec ExternalCache, key string) (releaseLock func() error, err error) {
	// set lock
	lockKey := getKeyLock(key)
	minTTL := time.Second * 10
	releaseLock = func() error { return nil }
	// generate some random value for lock
	lockValue := make([]byte, 20)
	_, err = rand.Read(lockValue)
	if err != nil {
		return
	}
	// try to set lock until it is obtained or context is cancelled
	for {
		externalLock, err := ec.SetNX(ctx, lockKey, lockValue, minTTL)
		if err != nil {
			return releaseLock, err
		}
		if externalLock {
			break
		}
		select {
		case <-time.After(ecMinDelay):
			continue
		case <-ctx.Done():
			return releaseLock, context.Canceled
		}
	}

	ctxRelease, cancel := context.WithCancel(ctx)

	// renew lock
	go func() {
		for {
			select {
			case <-time.After(minTTL / 2):
				err := ec.Set(ctxRelease, lockKey, lockValue, minTTL)
				if ctxRelease.Err() != nil {
					return
				}
				if err != nil {
					logger.Printf("Failed to renew TTL for lock %s: %v", lockKey, err)
				}
			case <-ctxRelease.Done():
				return
			}
		}
	}()

	releaseLock = func() error {
		// let renew lock goroutine exit
		cancel()
		return ec.DelValue(ctx, lockKey, lockValue)
	}

	return releaseLock, nil
}
