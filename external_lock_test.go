package cachecalc

import (
	"context"
	"errors"
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
	releaseLock, err := GetExternalLock(ctx1, ec, "test")
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
		releaseConcurrentLock, err := GetExternalLock(ctx2, ec, "test")
		logger.Println("concurrent lock obtained")
		require.NoError(t, err)
		require.NotNil(t, releaseConcurrentLock)
		concurrentLockDur = time.Since(started)
		require.Greater(t, concurrentLockDur, sleepDuration)
		waitMain.Wait()
		// first try to release the lock from main goroutine
		logger.Println("releasing lock from main goroutine (third time, alternative lock from concurrent goroutine is set now)")
		err = releaseLock()
		require.True(t, errors.Is(err, ErrNoLockFound), "expected error: %v", ErrNoLockFound)
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
	require.True(t, errors.Is(err, ErrNoLockFound), "expected error: %v", ErrNoLockFound)
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
	releaseLock, err := GetExternalLock(ctx, ec, "test-expiration")
	logger.Println("Initial lock obtained")
	require.NoError(t, err)
	require.NotNil(t, releaseLock)

	// Cancel the context to simulate lock expiration
	cancel()

	// Channel to signal that the lock was successfully acquired by the new goroutine
	lockAcquiredCh := make(chan struct{})

	// Start a goroutine that tries to acquire the lock after cancellation
	go func() {
		ctxNew := context.TODO() // New context
		releaseLockNew, err := GetExternalLock(ctxNew, ec, "test-expiration")
		if err == nil && releaseLockNew != nil {
			close(lockAcquiredCh) // Signal that the lock has been acquired
			// Clean up by releasing the new lock
			_ = releaseLockNew()
		}
	}()

	// Wait for the lock to be acquired or time out after 2 seconds
	select {
	case <-lockAcquiredCh:
		logger.Println("Lock was successfully re-acquired after expiration")
	case <-time.After(2 * time.Second):
		t.Fatal("Test failed: Lock was not re-acquired within the expected time after expiration")
	}

	logger.Println("TestGetExternalLockExpiration done")
}
