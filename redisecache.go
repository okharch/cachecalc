package cachecalc

import (
	"context"
	"errors"
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

func (r *RedisExternalCache) ExpireEntries(ctx context.Context) chan string {
	err := r.client.ConfigSet(ctx, "notify-keyspace-events", "gE").Err()
	if err != nil {
		logger.Printf("failed to set notify-keyspace-events: %s", err)
		return nil
	}

	thread := getThread(ctx)
	pubSub := r.client.PSubscribe(ctx, "__keyevent@0__:del")
	logger.Printf("Thread %v:ExpireEntries subscribed to __keyevent@0__:del", thread)
	msgCh := pubSub.Channel()
	ch := make(chan string)

	go func() {
		defer close(ch)
		defer func() {
			_ = pubSub.Close()
			logger.Printf("Thread %v:ExpireEntries unsubscribed", thread)
		}()

		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				logger.Printf("Thread %v: redis: msg %s channel %s", thread, msg.Payload, msg.Channel)
				key := msg.Payload
				logger.Printf("Thread %v:ExpireEntries received key %s", thread, key)
				ch <- key
			case <-ctx.Done():
				logger.Printf("Thread %v:ExpireEntries context done", thread)
				return
			}
		}
	}()

	return ch
}

// NewRedisCache creates an instance of ExternalCache connected to ENV{REDIS_URL} or local redis server if not specified.
func NewRedisCache(ctx context.Context) (ExternalCache, error) {
	client, err := GetRedis(ctx)
	if err != nil {
		return nil, err
	}
	rec := &RedisExternalCache{
		client: client,
	}
	return rec, nil
}

const lockToken = "token" // The token value used in the semaphore pattern

// GetLock attempts to acquire a distributed lock using Redis BLPOP/BRPOP with a semaphore pattern.
func (r *RedisExternalCache) GetLock(ctx context.Context, key string) (releaseLock func() error, err error) {
	// Define the lock queue key based on the input key
	lockQueueKey := fmt.Sprintf("lock_queue:%s", key)

	// Attempt to acquire the lock by popping from the lock queue. This will block until a token is available.
	for {
		result, err := r.client.BLPop(ctx, time.Second*10, lockQueueKey).Result()
		if errors.Is(err, redis.Nil) {
			logger.Printf("Attempt to block pop from the empty list. Will wait for 10s timeout")
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to acquire lock for key %s: %w", key, err)
		}
		// Ensure the token popped is the expected value (this is a sanity check).
		if len(result) < 2 || result[1] != lockToken {
			return nil, fmt.Errorf("unexpected value when acquiring lock for key %s: %v", key, result)
		}
		break
	}
	logger.Printf("redis lock %s acquired", key)

	ctxUnlock, cancel := context.WithCancel(ctx)
	// Define the function to release the lock.
	var lockReleased bool
	releaseLock = func() error {
		if lockReleased {
			return nil
		}
		// Push the token back into the lock queue to release the lock.
		// use timeout context to execute the query
		ctx, cancelUnlock := context.WithTimeout(context.TODO(), 5*time.Second)
		defer cancelUnlock()
		err := r.client.RPush(ctx, lockQueueKey, lockToken).Err()
		logger.Printf("redis lock %s released", key)
		lockReleased = true
		cancel() // cancel the context to stop the goroutine
		if err != nil {
			return fmt.Errorf("failed to release lock for key %s: %w", key, err)
		}
		return nil
	}
	go releaseLockOnContextCancel(ctxUnlock, releaseLock)

	return releaseLock, nil
}

// Initialize the lock by pushing the initial token into the lock queue.
func (r *RedisExternalCache) InitLock(ctx context.Context, key string) error {
	lockQueueKey := fmt.Sprintf("lock_queue:%s", key)
	exists, err := r.client.Exists(ctx, lockQueueKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check if lock queue exists for key %s: %w", key, err)
	}

	if exists == 0 {
		// Initialize the lock queue if it doesn't exist.
		err := r.client.RPush(ctx, lockQueueKey, lockToken).Err()
		if err != nil {
			return fmt.Errorf("failed to initialize lock queue for key %s: %w", key, err)
		}
	} else {
		// Optional: Ensure the queue has the correct token.
		tokenCount, err := r.client.LLen(ctx, lockQueueKey).Result()
		if err != nil {
			return fmt.Errorf("failed to check lock queue length for key %s: %w", key, err)
		}

		if tokenCount == 0 {
			// Reinitialize the lock queue if it's empty.
			err := r.client.RPush(ctx, lockQueueKey, lockToken).Err()
			if err != nil {
				return fmt.Errorf("failed to reinitialize lock queue for key %s: %w", key, err)
			}
		}
	}

	return nil
}
