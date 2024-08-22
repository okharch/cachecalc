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

func (r *RedisExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, value, ttl).Result()
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

	time.Sleep(time.Millisecond * 20)

	return ch
}

func (r *RedisExternalCache) DelValue(ctx context.Context, key string, value []byte) error {
	script := redis.NewScript(`
		local current = redis.call('GET', KEYS[1])
		if current == ARGV[1] then
			return redis.call('DEL', KEYS[1])
		else
			return 0
		end
	`)
	ret, err := script.Run(ctx, r.client, []string{key}, value).Result()
	if err != nil {
		return err
	}
	if ret == int64(0) {
		return ErrNoLockFound
	}
	return nil
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
