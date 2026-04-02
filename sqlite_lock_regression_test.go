package cachecalc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQLiteSetNXAllowsExpiredLockToBeReacquired(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "sqlitecache-regression")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(tmpfile.Name()))
	}()

	cache, err := NewSQLiteCache(tmpfile.Name())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, cache.Close())
	}()

	ctx := context.Background()
	created, err := cache.SetNX(ctx, "stale.lock", []byte("owner-1"), 10*time.Millisecond)
	require.NoError(t, err)
	require.True(t, created)

	time.Sleep(30 * time.Millisecond)

	created, err = cache.SetNX(ctx, "stale.lock", []byte("owner-2"), 10*time.Millisecond)
	require.NoError(t, err)
	require.True(t, created, "expired lock row should not block a new owner")
}
