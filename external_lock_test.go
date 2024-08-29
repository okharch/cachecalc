package cachecalc

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type initCacheFunc func(*testing.T) func(context.Context) ExternalCache

// Helper function to test external lock with a given cache initializer
func testGetExternalLock(t *testing.T, initCache initCacheFunc) {
	ctx1 := context.WithValue(context.TODO(), "thread", 1)
	ctx2 := context.WithValue(context.TODO(), "thread", 2)
	ecInit := initCache(t)
	ec := ecInit(ctx1)
	logger.Println("trying to obtain lock thread 1...")
	releaseLock, err := ec.GetLock(ctx1, "test")
	logger.Println("lock obtained")
	require.NoError(t, err)
	require.NotNil(t, releaseLock)
	started := time.Now()
	var concurrentLockDur time.Duration
	sleepDuration := time.Millisecond * 100
	// now try to get the lock again concurrently
	var wg sync.WaitGroup
	wg.Add(1)                   // main wait goroutine
	var waitMain sync.WaitGroup // goroutine to wait for the main to finish releasing lock second time
	waitMain.Add(1)
	go func() {
		defer wg.Done()
		releaseConcurrentLock, err := ec.GetLock(ctx2, "test")
		logger.Println("concurrent lock obtained")
		require.NoError(t, err)
		require.NotNil(t, releaseConcurrentLock)
		concurrentLockDur = time.Since(started)
		require.Greater(t, concurrentLockDur, sleepDuration)
		waitMain.Wait()
		// first try to release the lock from main goroutine
		logger.Println("releasing lock from main goroutine (third time, alternative lock from concurrent goroutine is set now)")
		err = releaseLock()
		logger.Println("lock released for the third time returning:", err)
		require.NoError(t, releaseConcurrentLock(), "release concurrent lock")
		logger.Println("concurrent lock released")
	}()
	time.Sleep(sleepDuration)
	err = releaseLock()
	logger.Println("lock released for the first time")
	require.NoError(t, err)
	err = releaseLock()
	logger.Println("lock released for the second time")
	waitMain.Done() // release the concurrent goroutine so it can take a lock
	wg.Wait()
	logger.Println("TestGetExternalLock done")
}

func testGetExternalLockExpiration(t *testing.T, initCache initCacheFunc) {
	// Initialize the base context and external cache
	ctx, cancel := context.WithCancel(context.TODO())
	ecInit := initCache(t)
	ec := ecInit(ctx)

	// Acquire the lock initially
	releaseLock, err := ec.GetLock(ctx, "test-expiration")
	logger.Println("Initial lock obtained")
	require.NoError(t, err)
	require.NotNil(t, releaseLock)

	// Channel to signal that the lock was successfully acquired by the new goroutine
	lockAcquiredCh := make(chan struct{})

	// Start a goroutine that tries to acquire the lock after cancellation

	go func() {
		ctxNew := context.TODO() // New context
		releaseLockNew, err := ec.GetLock(ctxNew, "test-expiration")
		if err == nil && releaseLockNew != nil {
			close(lockAcquiredCh) // Signal that the lock has been acquired
			// Clean up by releasing the new lock
			_ = releaseLockNew()
		}
	}()

	// Cancel the context to simulate lock expiration
	cancel()

	// Wait for the lock to be acquired or time out after 2 seconds
	select {
	case <-lockAcquiredCh:
		logger.Println("Lock was successfully re-acquired after expiration")
	case <-time.After(2 * time.Second):
		t.Fatal("Test failed: Lock was not re-acquired within the expected time after expiration")
	}

	logger.Println("TestGetExternalLockExpiration done")
}

func testGetLock1(t *testing.T, initCache initCacheFunc) {
	ctx1 := context.WithValue(context.TODO(), "thread", 1)
	ctx2 := context.WithValue(context.TODO(), "thread", 2)

	ec1 := initCache(t)(ctx1)
	ec2 := initCache(t)(ctx2)

	//require.NoError(t, ec1.InitLock(ctx1, "test-lock"))
	//require.NoError(t, ec2.InitLock(ctx2, "test-lock"))

	logger.Println("*** Test concurrent locks with the same cache")
	runTestLocks(t, ec1, ec1, ctx1, ctx2) // Test with the same cache
	logger.Println("*** Test concurrent locks with different caches")
	runTestLocks(t, ec1, ec2, ctx1, ctx2) // Test with different caches
}

func runTestLocks(t *testing.T, ec1, ec2 ExternalCache, ctx1, ctx2 context.Context) {
	const testKey = "test-lock"
	for _, key := range []string{testKey, fmt.Sprintf("lock_queue:%s", testKey)} {
		require.NoError(t, ec1.Del(ctx1, key)) // forcefully remove the lock before starting
	}
	releaseLock := acquireLock(t, ec1, ctx1, testKey, "main thread")

	testValue := 1
	var tvLock sync.Mutex

	incTestValue := func() {
		tvLock.Lock()
		defer tvLock.Unlock()
		testValue++
	}

	getTestValue := func() int {
		tvLock.Lock()
		defer tvLock.Unlock()
		return testValue
	}

	require.Equal(t, 1, getTestValue(), "test value shall be 1")

	var wg sync.WaitGroup
	wg.Add(1)
	var waitLock sync.WaitGroup
	waitLock.Add(1)

	go func() {
		defer wg.Done()
		waitLock.Done()
		releaseConcurrentLock := acquireLock(t, ec2, ctx2, testKey, "concurrent thread")
		incTestValue()
		logger.Println("test value incremented by concurrent thread")
		require.NoError(t, releaseConcurrentLock())
		logger.Println("concurrent lock released")
	}()

	waitLock.Wait()
	time.Sleep(remoteTick)
	require.Equal(t, 1, getTestValue(), "test value shall be 1")

	require.NoError(t, releaseLock())
	logger.Println("main lock released")

	time.Sleep(tick)
	require.Equal(t, 2, getTestValue(), "test value shall be 2")

	wg.Wait()
}

func acquireLock(t *testing.T, ec ExternalCache, ctx context.Context, lockName, threadName string) func() error {
	logger.Printf("Acquiring lock: %s from %s", lockName, threadName)
	releaseLock, err := ec.GetLock(ctx, lockName)
	require.NoError(t, err)
	require.NotNil(t, releaseLock)
	logger.Printf("Lock %s acquired from %s", lockName, threadName)
	return releaseLock
}

// Helper function to test GetLock expiration with a given cache initializer
func testGetLockExpiration(t *testing.T, initCache initCacheFunc) {
	// Initialize the base context and external cache
	ctx, cancel := context.WithCancel(context.TODO())
	ecInit := initCache(t)
	ec := ecInit(ctx)

	// Acquire the lock initially
	key := "test-lock-expiration"
	releaseLock, err := ec.GetLock(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, releaseLock)

	// Cancel the context to simulate lock expiration
	cancel()

	// Channel to signal that the lock was successfully acquired by the new goroutine
	lockAcquiredCh := make(chan struct{})

	// Start a goroutine that tries to acquire the lock after cancellation
	go func() {
		ctxNew := context.TODO() // New context
		releaseLockNew, err := ec.GetLock(ctxNew, key)
		if err == nil && releaseLockNew != nil {
			close(lockAcquiredCh) // Signal that the lock has been acquired
			// Clean up by releasing the new lock
			_ = releaseLockNew()
		}
	}()

	// Wait for the lock to be acquired or time out after 2 seconds
	select {
	case <-lockAcquiredCh:
		// Lock was successfully re-acquired after expiration
	case <-time.After(2 * time.Second):
		t.Fatal("Test failed: Lock was not re-acquired within the expected time after expiration")
	}
}

// Example of how to use the tests with a PostgresCache
func TestPostgresCache_GetLock(t *testing.T) {
	logger.Println("TestPostgresCache_GetLock...")
	testGetLock(t, initPgCache)
}

// Example of how to use the tests with a RedisExternalCache
func TestRedisExternalCache_GetLock(t *testing.T) {
	logger.Println("TestRedisExternalCache_GetLock...")
	testGetLock(t, initRedisCache)
}

func TestPostgresCache_GetLockExpiration(t *testing.T) {
	logger.Println("TestPostgresCache_GetLockExpiration...")
	testGetLockExpiration(t, initPgCache)
}

func TestRedisExternalCache_GetLockExpiration(t *testing.T) {
	logger.Println("TestRedisExternalCache_GetLockExpiration...")
	testGetLockExpiration(t, initRedisCache)
}
