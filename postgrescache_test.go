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
	newValue := []byte("new_test_value")

	created, err := cache.SetNX(ctx, newKey, newValue, time.Second)
	if err != nil {
		t.Fatalf("Failed to setNX key: %v", err)
	}

	if !created {
		t.Fatalf("Expected key '%s' to be created with value 'new_test_value', but it wasn't.", newKey)
	}

	// Attempt to create the same key again
	created, err = cache.SetNX(ctx, newKey, newValue, time.Second)
	if err != nil {
		t.Fatalf("Failed to setNX key: %v", err)
	}

	if created {
		t.Fatalf("Expected key '%s' to not be created again, but it was.", newKey)
	}

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

func TestPostgresDelValue(t *testing.T) {
	// Set up the context and PostgresCache
	ctx := context.Background()
	dbURL := PostgreUrl()
	cache, err := NewPostgresCache(ctx, dbURL)
	require.NoError(t, err, "Failed to create PostgresCache")
	require.NotNil(t, cache)

	defer func() {
		if err := cache.(*PostgresCache).db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	}()

	// Initialize test data
	key := "test_key"
	value := []byte("test_value")

	// Insert a value into the cache
	err = cache.Set(ctx, key, value, time.Minute)
	require.NoError(t, err, "Failed to set value")

	// Test deleting the value
	err = cache.DelValue(ctx, key, value)
	require.NoError(t, err, "Failed to delete value")

	// repeat the test to ensure the value is deleted and returns error
	err = cache.DelValue(ctx, key, value)
	require.ErrorIs(t, err, ErrNoLockFound, "Expected ErrNoLockFound when deleting value again")

	// Verify that the key no longer exists
	_, exists, err := cache.Get(ctx, key)
	require.NoError(t, err, "Failed to get key after deletion")
	require.False(t, exists, "Expected key to be deleted")

	// Test deleting a non-existing value (should return ErrNoLockFound)
	err = cache.DelValue(ctx, "non_existing_key", value)
	require.ErrorIs(t, err, ErrNoLockFound, "Expected ErrNoLockFound when deleting non-existing value")

	// Test deleting a key with an incorrect value (should return ErrNoLockFound)
	incorrectValue := []byte("incorrect_value")
	err = cache.DelValue(ctx, key, incorrectValue)
	require.ErrorIs(t, err, ErrNoLockFound, "Expected ErrNoLockFound when deleting with incorrect value")
}
