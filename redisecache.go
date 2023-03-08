package cachecalc

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
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
	// Get gets the value of key.
	// exists will be false If the key does not exist.
	// An error is returned if the implementor want to signal any errors.
	Get(ctx context.Context, key string) (value []byte, exists bool, err error)
	// Del Removes the specified key. A key is ignored if it does not exist.
	Del(ctx context.Context, key string) error
}

// GetRedis returns redis client which is used internally in this module but can be used otherwise
// when env variable REDIS_URL is set it uses to connect to redis. Otherwise it tries redis://127.0.0.1.
// returns redis.Client instance and nil for error on success
func GetRedis(ctx context.Context) (*redis.Client, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1" // dev env
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to init redis at %s: %s", redisURL, err)
	}
	redisCache := redis.NewClient(opt)
	pong, err := redisCache.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("Redis init, pong: %v, err: %w", pong, err)
	}
	return redisCache, nil
}

type RedisExternalCache struct {
	client *redis.Client
}

func (r *RedisExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, string(value), ttl).Err()
}

func (r *RedisExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, value, ttl).Result()
}

func (r *RedisExternalCache) Get(ctx context.Context, key string) (value []byte, exists bool, err error) {
	cmd := r.client.Get(ctx, key)
	err = cmd.Err()
	if err != nil {
		// When doing a key lookup the Redis client will return an error if the key does not exist.
		// You can check if that error is equal to redis. Nil ,
		// which indicates that the key was not found
		if err.Error() == "redis: nil" {
			return nil, false, nil
		}
		return
	}
	exists = true
	value, err = cmd.Bytes()
	return
}

func (r *RedisExternalCache) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// NewRedisCache creates an instance of ExternalCache which is connected to ENV{REDIS_URL} or local redis server if not specified.
func NewRedisCache(ctx context.Context) (ExternalCache, error) {
	client, err := GetRedis(ctx)
	if err != nil {
		return nil, err
	}
	rec := &RedisExternalCache{client}
	return rec, nil
}
