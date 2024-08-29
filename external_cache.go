package cachecalc

import (
	"context"
	"time"
)

// ExternalCache interface defines a set of required methods to provide the service of an external cache
// which SmartCache can use to coordinate several of its distributed instances.
type ExternalCache interface {
	// Set sets key to hold the string value.
	// If key already holds a value, it is overwritten, regardless of its type.
	// Any previous time to live associated with the key is discarded on a successful SET operation.
	// Should return nil if successful.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// GetLock attempts to acquire a distributed lock using the provided ExternalCache.
	// should return a function that releases the lock and an error if the lock could not be acquired.
	// if no error but release function is nil, the lock is not acquired, probably because it is already locked.
	GetLock(ctx context.Context, key string) (releaseLock func() error, err error)

	// Get gets the value of key.
	// exists will be false if the key does not exist.
	// An error is returned if the implementor wants to signal any errors.
	Get(ctx context.Context, key string) (value []byte, exists bool, err error)

	// Del removes the specified key. A key is ignored if it does not exist.
	Del(ctx context.Context, key string) error

	// Close closes the connection to the external cache.
	Close() error

	// EntryUpdates is a channel which will be used to refresh the entry
	// subscriber provides the key and the channel will return the fresh value
	EntryUpdates(ctx context.Context, key string) (chan []byte, error)
}
