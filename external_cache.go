package cachecalc

import (
	"context"
	"time"
)

// ExternalCache interface defines a set of required method to provide service of external cache
// which SmartCache can use to coordinate several of its distributed instances.
type ExternalCache interface {
	// Set sets key to hold the string value.
	// If key already holds a value, it is overwritten, regardless of its type.
	// Any previous time to live associated with the key is discarded on successful SET operation.
	// Should return nil if successful
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// SetNX sets key to hold string value if key does not exist.
	// In that case, it is equal to SET.
	// When key already holds a value, no operation is performed.
	// SETNX is short for "SET if Not eXists".
	// keyCreated must return true if value is set
	SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (keyCreated bool, err error)
	// GetCachedCalc gets the value of key.
	// exists will be false If the key does not exist.
	// An error is returned if the implementor want to signal any errors.
	Get(ctx context.Context, key string) (value []byte, exists bool, err error)
	// Del Removes the specified key. A key is ignored if it does not exist.
	Del(ctx context.Context, key string) error
	Close() error // closes the connection
}
