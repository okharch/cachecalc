package cachecalc

import (
	"context"
	"crypto/rand"
	"errors"
	"time"
)

var ErrNoLockFound = errors.New("no lock found")

// GetExternalLock attempts to acquire a distributed lock using the provided ExternalCache.
// It generates a random lock value and continuously attempts to set the lock in the external cache
// until the lock is obtained or the context is canceled. Once the lock is acquired, a goroutine is
// started to periodically renew the lock's TTL until the lock is released or the context is canceled.
//
// Parameters:
//   - ctx: The context to manage cancellation and timeouts for acquiring the lock.
//   - ec: The external cache interface that supports setting and renewing locks.
//   - key: The key in the external cache that identifies the resource to be locked.
//
// Returns:
//   - releaseLock: A function to release the lock. If called, it will stop the lock renewal goroutine
//     and attempt to delete the lock from the cache. If the lock is not found during deletion, it
//     returns an error.
//   - err: An error if the lock could not be acquired or if there was a failure in generating the lock value.
func GetExternalLock(ctx context.Context, ec ExternalCache, key string) (releaseLock func() error, err error) {
	// Generate the lock key based on the provided key.
	lockKey := getKeyLock(key)
	minTTL := time.Second * 10
	releaseLock = func() error { return nil }

	// Generate a random value for the lock.
	lockValue := make([]byte, 20)
	_, err = rand.Read(lockValue)
	if err != nil {
		return
	}

	// Attempt to set the lock until it is obtained or the context is canceled.
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

	// Goroutine to periodically renew the lock's TTL.
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

	// Set the release function to cancel the lock renewal and delete the lock from the cache.
	releaseLock = func() error {
		cancel()
		return ec.DelValue(ctx, lockKey, lockValue)
	}

	return releaseLock, nil
}
