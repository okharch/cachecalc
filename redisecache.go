package cachecalc

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"time"
)

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

func (r *RedisExternalCache) Close() error {
	return r.client.Close()
}
