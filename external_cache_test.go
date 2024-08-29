package cachecalc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var ttl = 1 * time.Millisecond * 32 * 16

// testSet tests the Set method of the ExternalCache interface, including TTL expiration.
func testSet(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-key"
	value := []byte("test-value")

	err := cache.Set(context.Background(), key, value, ttl)
	require.NoError(t, err, "Set should not return an error")

	// Verify that the value can be retrieved before expiration
	retrievedValue, exists, err := cache.Get(context.Background(), key)
	require.NoError(t, err, "Get should not return an error")
	require.True(t, exists, "Get should return true for exists")
	require.Equal(t, value, retrievedValue, "Retrieved value should match set value")

	// Wait for the TTL to expire
	time.Sleep(ttl * 33 / 32)

	// Verify that the value has expired
	retrievedValue, exists, err = cache.Get(context.Background(), key)
	require.NoError(t, err, "Get should not return an error after TTL expiration")
	require.False(t, exists, "Get should return false for exists after TTL expiration")
	require.Nil(t, retrievedValue, "Retrieved value should be nil after TTL expiration")
}

// testGetLock tests the GetLock method of the ExternalCache interface, including context cancellation.
func testGetLock(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-lock"
	ctx, cancel := context.WithCancel(context.Background())

	// Attempt to acquire the lock
	release, err := cache.GetLock(ctx, key)
	require.NoError(t, err, "GetLock should not return an error")
	require.NotNil(t, release, "Release function should not be nil")

	// Cancel the context and ensure it affects the operation
	cancel()
	_, err = cache.GetLock(ctx, key)
	require.Error(t, err, "GetLock should return an error on context cancellation")

	// Verify that the lock is still held
	release2, err := cache.GetLock(context.Background(), key)
	require.NoError(t, err, "GetLock should not return an error")
	require.Nil(t, release2, "Release function should be nil as the lock is still held")

	// Release the lock and ensure context cancellation handling works
	err = release()
	require.NoError(t, err, "Release should not return an error")

	// Re-acquire the lock after releasing
	release3, err := cache.GetLock(context.Background(), key)
	require.NoError(t, err, "GetLock should not return an error")
	require.NotNil(t, release3, "Release function should not be nil after releasing the lock")
}

// testGet tests the Get method of the ExternalCache interface, including TTL expiration and context cancellation.
func testGet(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-key"
	value := []byte("test-value")

	err := cache.Set(context.Background(), key, value, ttl)
	require.NoError(t, err, "Set should not return an error")

	// Retrieve the value and check it before expiration
	retrievedValue, exists, err := cache.Get(context.Background(), key)
	require.NoError(t, err, "Get should not return an error")
	require.True(t, exists, "Get should return true for exists")
	require.Equal(t, value, retrievedValue, "Retrieved value should match set value")

	// Wait for TTL to expire
	time.Sleep(ttl * 33 / 32)

	// Ensure the entry has expired
	retrievedValue, exists, err = cache.Get(context.Background(), key)
	require.NoError(t, err, "Get should not return an error after TTL expiration")
	require.False(t, exists, "Get should return false for exists after TTL expiration")
	require.Nil(t, retrievedValue, "Retrieved value should be nil after TTL expiration")

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = cache.Get(ctx, key)
	require.Error(t, err, "Get should return an error on context cancellation")
}

// testDel tests the Del method of the ExternalCache interface, including context cancellation.
func testDel(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-key"
	value := []byte("test-value")
	ttl := 1 * time.Hour

	err := cache.Set(context.Background(), key, value, ttl)
	require.NoError(t, err, "Set should not return an error")

	// Test context cancellation during delete
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cache.Del(ctx, key)
	require.Error(t, err, "Del should return an error on context cancellation")

	// Delete the key without context cancellation and check that it no longer exists
	err = cache.Del(context.Background(), key)
	require.NoError(t, err, "Del should not return an error")

	// Ensure the entry has expired
	retrievedValue, exists, err := cache.Get(context.Background(), key)
	require.NoError(t, err, "Get should not return an error after deletion")
	require.False(t, exists, "Get should return false for exists after deletion")
	require.Nil(t, retrievedValue, "Retrieved value should be nil after deletion")
}

// testExpireEntries tests the ExpireEntries method of the ExternalCache interface, including TTL expiration.
func testExpireEntries(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-key"
	value := []byte("test-value")

	err := cache.Set(context.Background(), key, value, ttl)
	require.NoError(t, err, "Set should not return an error")

	expireCh := cache.ExpireEntries(context.Background())

	// Ensure key expiration after TTL
	select {
	case expiredKey := <-expireCh:
		require.Equal(t, key, expiredKey, "Expired key should match set key")
	case <-time.After(ttl * 33 / 32):
		t.Error("Expected key to expire, but it did not")
	}
}

// testRefreshEntry tests the RefreshEntry method of the ExternalCache interface, including context cancellation.
func testRefreshEntry(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-key"
	value := []byte("test-value")

	err := cache.Set(context.Background(), key, value, ttl)
	require.NoError(t, err, "Set should not return an error")

	refreshCh := cache.RefreshEntry(context.Background(), key)

	// Simulate a refresh by setting a new value
	newValue := []byte("new-value")
	go func() {
		time.Sleep(ttl * 33 / 32)
		cache.Set(context.Background(), key, newValue, ttl)
	}()

	select {
	case refreshedValue := <-refreshCh:
		require.Equal(t, newValue, refreshedValue, "Refreshed value should match new set value")
	case <-time.After(ttl * 33 / 32):
		t.Error("Expected entry to be refreshed, but it was not")
	}

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := cache.RefreshEntry(ctx, key)
	cancel()
	// Ensure the channel is closed on context cancellation
	select {
	case _, ok := <-ch:
		require.False(t, ok, "Expected channel to be closed on context cancellation")
	case <-time.After(time.Second):
		t.Error("Expected channel to be closed on context cancellation")
	}
}

// testClose tests the Close method of the ExternalCache interface.
func testClose(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	err := cache.Close()
	require.NoError(t, err, "Close should not return an error")
}
