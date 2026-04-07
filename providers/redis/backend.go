package redis

import (
	"context"
	"fmt"
	"os"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/okharch/cachecalc/distlock"
	"github.com/okharch/cachecalc/valuestore"
)

var renewScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

var releaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// Backend provides both shared value storage and distributed locks on Redis.
type Backend struct {
	client *goredis.Client
}

func New(ctx context.Context, redisURL string) (*Backend, error) {
	if redisURL == "" {
		redisURL = os.Getenv("REDIS_URL")
	}
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}
	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := goredis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Backend{client: client}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	cmd := b.client.Get(ctx, key)
	buf, err := cmd.Bytes()
	if err == goredis.Nil {
		return valuestore.EntrySnapshot{}, false, nil
	}
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	entry, err := valuestore.Unmarshal(buf)
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	if !entry.Usable(time.Now()) {
		_ = b.Delete(ctx, key)
		return valuestore.EntrySnapshot{}, false, nil
	}
	return entry, true, nil
}

func (b *Backend) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	buf, err := valuestore.Marshal(entry)
	if err != nil {
		return err
	}
	return b.client.Set(ctx, key, buf, ttl(entry)).Err()
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	return b.client.Del(ctx, key).Err()
}

func (b *Backend) TryAcquire(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	return b.client.SetNX(ctx, key, token, lockTTL).Result()
}

func (b *Backend) Renew(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	n, err := renewScript.Run(ctx, b.client, []string{key}, string(token), lockTTL.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (b *Backend) Release(ctx context.Context, key string, token []byte) (bool, error) {
	n, err := releaseScript.Run(ctx, b.client, []string{key}, string(token)).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (b *Backend) LockProvider() distlock.Provider {
	return distlock.NewProvider(b)
}

func (b *Backend) Close() error {
	return b.client.Close()
}

func ttl(entry valuestore.EntrySnapshot) time.Duration {
	ttl := time.Until(entry.ExpireAt)
	if ttl <= 0 {
		return time.Millisecond
	}
	return ttl
}
