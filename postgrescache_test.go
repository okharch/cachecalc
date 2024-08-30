package cachecalc

import (
	"context"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func PostgreUrl() string {
	url := os.Getenv("POSTGRE_URL")
	if url == "" {
		url = "postgres://dev:dev@localhost:29510/dev?sslmode=disable"
	}
	return url
}

// TestPostgresCache tests the PostgresCache implementation
func TestPostgresCache(t *testing.T) {
	// Adjust these variables to match your PostgreSQL configuration
	ctx := context.Background()
	// Initialize the cache
	dbURL := PostgreUrl()
	logger.Println("TestPostgresCache...")
	cache, err := NewPostgresCache(ctx, dbURL)
	require.NoError(t, err, "create cache")
	require.NotNil(t, cache)
	_, err = cache.(*PostgresCache).db.Exec("truncate postgres_cache_key_value_expired_v_1_4")
	require.NoError(t, err, "truncate postgres_cache_key_value_expired_v_1_4")

	defer func() {
		if err := cache.(*PostgresCache).db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	}()

	// Test Set and Get
	key := getRandomKey(t)
	value := "test_value"

	err = cache.Set(ctx, key, []byte(value), time.Second)
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	cachedValue, exists, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !exists || string(cachedValue) != value {
		t.Fatalf("Expected key '%s' to exist with value 'test_value', but it didn't.", key)
	}

	// Test SetNX
	newKey := "new_test_key"

	// Test Delete
	err = cache.Del(ctx, key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify key doesn't exist after deletion
	_, exists, err = cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key after deletion: %v", err)
	}

	if exists {
		t.Fatalf("Expected key '%s' to be deleted, but it still exists.", key)
	}

	// test expiration
	_, exists, err = cache.Get(ctx, newKey)
	require.NoError(t, err)
	require.True(t, exists, "new key still shall exists")
	time.Sleep(time.Second)
	_, exists, err = cache.Get(ctx, newKey)
	require.NoError(t, err)
	require.False(t, exists, "new key shall be expired")

}

func TestExternalCachePostgres(t *testing.T) {
	ctx := context.TODO()
	logger.Println("TestExternalCachePostgres...")
	testExternalCache(t, ctx, initPgCache(t))
	log.Println("TestExternalCachePostgres done")
}

func initPgCache(t *testing.T) func(context.Context) ExternalCache {
	dbURL := PostgreUrl()
	return func(ctx context.Context) ExternalCache {
		cache, err := NewPostgresCache(ctx, dbURL)
		if err != nil {
			t.Skipf("skip test due external cache not available: %s", err)
		}
		return cache
	}
}

func TestRemotePostgres(t *testing.T) {
	ctx := context.TODO()
	log.Printf("TestRemotePostgres...")
	testRemote(t, ctx, initPgCache(t))
}

func TestRemoteConcurrentPostgres(t *testing.T) {
	ctx := context.TODO()
	log.Printf("TestRemoteConcurrentPostgres...")
	testRemoteConcurrent(t, ctx, initPgCache(t))

}

func TestExpirationPostgres(t *testing.T) {
	logger.Println("TestExpirationRedis...")
	// init redis external cache
	testKeyExpiration(t, initPgCache(t))
}

func TestGetExternalLockPostgres(t *testing.T) {
	logger.Println("TestGetExternalLockPostgres...")
	testGetExternalLock(t, initPgCache, time.Second)
}
