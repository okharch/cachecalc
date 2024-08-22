package cachecalc

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestGetExternalLock(t *testing.T) {
	ctx := context.TODO()
	logger.Println("TestGetExternalLock...")
	ecInit := initRedisCache(t)
	ec := ecInit(ctx)
	releaseLock, err := GetExternalLock(ctx, ec, "test")
	logger.Println("lock obtained")
	require.NoError(t, err)
	require.NotNil(t, releaseLock)
	started := time.Now()
	var concurrentLockDur time.Duration
	sleepDuration := time.Millisecond * 100
	// now try to get the releaseLock again in concurrent way
	var wg sync.WaitGroup
	wg.Add(1)                   // main wait goroutine
	var waitMain sync.WaitGroup // goroutine to wait for the main to finish releasing lock second time
	waitMain.Add(1)
	go func() {
		defer wg.Done()
		releaseConcurrentLock, err := GetExternalLock(ctx, ec, "test")
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
