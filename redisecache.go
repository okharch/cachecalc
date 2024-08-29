package cachecalc

import (
	"context"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"os"
	"sync"
	"time"
)

// GetRedis returns a Redis client which is used internally in this module but can be used otherwise.
// When the environment variable REDIS_URL is set, it uses it to connect to Redis. Otherwise, it tries redis://127.0.0.1.
// Returns redis.Client instance and nil for error on success.
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
	client         *redis.Client
	locker         *redislock.Client
	subscriptions  map[string]*subscription
	subscriptionsM sync.Mutex
}

type subscription struct {
	pubSub    *redis.PubSub
	channels  []chan []byte
	closeChan chan struct{}
}

// NewRedisCache creates an instance of ExternalCache connected to ENV{REDIS_URL} or a local Redis server if not specified.
func NewRedisCache(ctx context.Context) (ExternalCache, error) {
	client, err := GetRedis(ctx)
	if err != nil {
		return nil, err
	}
	return &RedisExternalCache{
		client:        client,
		subscriptions: make(map[string]*subscription),
	}, nil
}

func (r *RedisExternalCache) EntryUpdates(ctx context.Context, key string) (chan []byte, error) {
	r.subscriptionsM.Lock()
	defer r.subscriptionsM.Unlock()

	// Check if there is already a subscription for this key
	sub, exists := r.subscriptions[key]
	if !exists {
		// Create a new subscription
		pubSub := r.client.PSubscribe(ctx, fmt.Sprintf("__keyspace@0__:%s", key))
		sub = &subscription{
			pubSub:    pubSub,
			channels:  []chan []byte{},
			closeChan: make(chan struct{}),
		}
		r.subscriptions[key] = sub

		// Start listening for updates
		go r.listenForUpdates(ctx, key, sub)
	}

	// Create a new channel for this subscriber
	ch := make(chan []byte)
	sub.channels = append(sub.channels, ch)

	// Return the channel to the caller
	return ch, nil
}

func (r *RedisExternalCache) listenForUpdates(ctx context.Context, key string, sub *subscription) {
	defer func() {
		r.subscriptionsM.Lock()
		defer r.subscriptionsM.Unlock()

		// Clean up the subscription
		delete(r.subscriptions, key)
		sub.pubSub.Close()
		for _, ch := range sub.channels {
			close(ch)
		}
	}()

	for {
		select {
		case <-sub.pubSub.Channel():
			// On receiving a message, get the updated value and send it to all subscribers
			value, exists, err := r.Get(ctx, key)
			if err != nil {
				return
			}

			// if key does not exist send nil to all subscribers
			if !exists {
				value = nil
			}

			for _, ch := range sub.channels {
				ch <- value
			}
		case <-ctx.Done():
			return
		case <-sub.closeChan:
			return
		}
	}
}

func (r *RedisExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

func (r *RedisExternalCache) Get(ctx context.Context, key string) (value []byte, exists bool, err error) {
	cmd := r.client.Get(ctx, key)
	err = cmd.Err()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, err
	}
	exists = true
	value, err = cmd.Bytes()
	return value, exists, err
}

func (r *RedisExternalCache) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *RedisExternalCache) Close() error {
	return r.client.Close()
}

func (r *RedisExternalCache) GetLock(ctx context.Context, key string) (releaseLock func() error, err error) {
	// Attempt to obtain the lock immediately, non-blocking
	lock, err := r.locker.Obtain(ctx, key, 0, nil) // Set duration to 0 for immediate failure if lock is held
	if errors.Is(err, redislock.ErrNotObtained) {
		// Lock is not obtained by the current process, return the error
		return nil, err
	} else if err != nil {
		// An error occurred while trying to obtain the lock
		return nil, fmt.Errorf("failed to acquire lock for key %s: %w", key, err)
	}

	// Lock was successfully obtained, create the release function
	releaseLock = func() error {
		// Release the lock
		if err := lock.Release(ctx); err != nil {
			return fmt.Errorf("failed to release lock for key %s: %w", key, err)
		}
		return nil
	}

	return releaseLock, nil
}
