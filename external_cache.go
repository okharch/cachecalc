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

	// InitLock initializes the lock for the key, need to reset lock so it becomes available
	InitLock(ctx context.Context, key string) error

	// GetLock attempts to acquire a distributed lock using the provided ExternalCache.
	GetLock(ctx context.Context, key string) (releaseLock func() error, err error)

	// Get gets the value of key.
	// exists will be false if the key does not exist.
	// An error is returned if the implementor wants to signal any errors.
	Get(ctx context.Context, key string) (value []byte, exists bool, err error)

	// Del removes the specified key. A key is ignored if it does not exist.
	Del(ctx context.Context, key string) error

	// Close closes the connection to the external cache.
	Close() error

	// ExpireEntries returns a channel of keys that have been deleted.
	ExpireEntries(ctx context.Context) chan string
}
