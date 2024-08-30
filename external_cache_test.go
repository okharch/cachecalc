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
func testGetLock(t *testing.T, initCache initCacheFunc, minTTL time.Duration) {
	cache := initCache(t)(context.Background())

	key := "test-lock"
	ctx, cancel := context.WithCancel(context.Background())

	// Attempt to acquire the lock
	release, err := cache.GetLock(ctx, key, minTTL)
	require.NoError(t, err, "GetLock should not return an error")
	require.NotNil(t, release, "Release function should not be nil")

	// Cancel the context and ensure it affects the operation
	cancel()
	// give some time for the release function to be called
	time.Sleep(time.Millisecond * 20)
	_, err = cache.GetLock(ctx, key, minTTL)
	require.Error(t, err, "GetLock should return an error on context cancellation")

	// Verify that the lock is no more held
	release2, err := cache.GetLock(context.Background(), key, minTTL)
	require.NoError(t, err, "GetLock should not return an error")
	require.NotNil(t, release2, "Release function should not be nil")

	// Release the lock and ensure context cancellation handling works
	err = release2()
	require.NoError(t, err, "Release should not return an error")

	// Re-acquire the lock after releasing
	release3, err := cache.GetLock(context.Background(), key, minTTL)
	require.NoError(t, err, "GetLock should not return an error")
	require.NotNil(t, release3, "Release function should not be nil after releasing the lock")
	// now try to get another lock and make sure it is not acquired
	release5, err := cache.GetLock(context.Background(), key, minTTL)
	require.NoError(t, err, "GetLock should not return an error")
	require.Nil(t, release5, "Release function should be nil as the lock is still held")
	// now wait for the lock to expire
	time.Sleep(minTTL * 33 / 32)
	// now try to get another lock and make sure it is acquired
	release4, err := cache.GetLock(context.Background(), key, minTTL)
	require.NoError(t, err, "GetLock should not return an error")
	require.NotNil(t, release4, "Release function should not be nil as the lock is not held")
	// now release the lock
	err = release4()
	require.NoError(t, err, "Release should not return an error")
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

	// Delete the key without context cancellation and check that it no longer exists
	err = cache.Del(context.Background(), key)
	require.NoError(t, err, "Del should not return an error")

	// Ensure the entry has expired
	retrievedValue, exists, err := cache.Get(context.Background(), key)
	require.NoError(t, err, "Get should not return an error after deletion")
	require.False(t, exists, "Get should return false for exists after deletion")
	require.Nil(t, retrievedValue, "Retrieved value should be nil after deletion")
}

// testEntryUpdates tests the EntryUpdates method of the ExternalCache interface, including context cancellation.
func testEntryUpdates(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	key := "test-key"
	value := []byte("test-value")

	// create channel first, so it will be ready to receive the updated value
	ctx, cancel := context.WithCancel(context.Background())
	updatesCh, chKey, err := cache.EntryUpdates(ctx, key)
	require.NoError(t, err, "EntryUpdates should not return an error")

	err = cache.Set(context.Background(), key, value, ttl)
	require.NoError(t, err, "Set should not return an error")

	// make sure we receive the value
	select {
	case updatedValue := <-updatesCh:
		require.Equal(t, value, updatedValue, "updated value should match set value")
	case <-time.After(ttl * 33 / 32):
		t.Error("Expected entry to be refreshed, but it was not")
	}

	// Simulate a refresh by setting a new value
	newValue := []byte("new-value")
	cache.Set(context.Background(), key, newValue, ttl)

	select {
	case updatedValue := <-updatesCh:
		require.Equal(t, newValue, updatedValue, "updated value should match new set value")
	case <-time.After(ttl * 33 / 32):
		t.Error("Expected entry to be refreshed, but it was not")
	}

	// now test that when we delete the key, the channel will send the nil as a value
	err = cache.Del(context.Background(), key)
	require.NoError(t, err, "Del should not return an error")
	select {
	case updatedValue := <-updatesCh:
		require.Equal(t, nil, updatedValue, "updated value should be nil after deletion")
	case <-time.After(ttl * 33 / 32):
		t.Error("Expected to receive nil value, but it was not")
	}

	// now check that subscriptions still work after deletion
	newValue = []byte("new-value")
	cache.Set(context.Background(), key, newValue, ttl)
	select {
	case updatedValue := <-updatesCh:
		require.Equal(t, newValue, updatedValue, "updated value should match new set value")
	case <-time.After(ttl * 33 / 32):
		t.Error("Expected entry to be refreshed, but it was not")
	}

	// Test context cancellation
	cancel()
	// Ensure the channel is closed on context cancellation
	select {
	case _, ok := <-updatesCh:
		require.False(t, ok, "Expected channel to be closed on context cancellation")
	case <-time.After(time.Second):
		t.Error("Expected channel to be closed on context cancellation")
	}
	_ = chKey
}

// testClose tests the Close method of the ExternalCache interface.
func testClose(t *testing.T, initCache initCacheFunc) {
	cache := initCache(t)(context.Background())

	err := cache.Close()
	require.NoError(t, err, "Close should not return an error")
}
