package cachecalc

import (
	"context"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSQLiteCache(t *testing.T) {
	// Create a temporary file for the SQLite database
	tmpfile, err := ioutil.TempFile("", "sqlitecache")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.Remove(tmpfile.Name())
		require.NoError(t, err)
	}()

	// Create a new instance of the SQLiteCache type
	cache, err := NewSQLiteCache(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Test the Set method
	err = cache.Set(context.Background(), "foo", []byte("bar"), time.Second)
	assert.NoError(t, err)

	// Test the Get method for an existing key
	value, exists, err := cache.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("bar"), value)

	// Test the Get method for a non-existent key
	value, exists, err = cache.Get(context.Background(), "baz")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, value)

	// Test the SetNX method for a new key
	keyCreated, err := cache.SetNX(context.Background(), "baz", []byte("qux"), time.Second)
	assert.NoError(t, err)
	assert.True(t, keyCreated)

	// Test the SetNX method for an existing key
	keyCreated, err = cache.SetNX(context.Background(), "baz", []byte("quux"), time.Second)
	assert.NoError(t, err)
	assert.False(t, keyCreated)

	// Test the Del method for an existing key
	err = cache.Del(context.Background(), "foo")
	assert.NoError(t, err)

	// Test the Del method for a non-existent key
	err = cache.Del(context.Background(), "foo")
	assert.NoError(t, err)

	// Test the Get method for a deleted key
	value, exists, err = cache.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, value)
}

var sqliteFile *os.File

func initSqlite(t *testing.T) {
	var err error
	if sqliteFile != nil {
		return
	}
	sqliteFile, err = ioutil.TempFile("", "sqlitecache")
	if err != nil {
		t.Fatal(err)
	}
}

func initSqliteCache(t *testing.T) func(context.Context) ExternalCache {
	initSqlite(t)
	return func(ctx context.Context) ExternalCache {
		// Create a new instance of the SQLiteCache type
		cache, err := NewSQLiteCache(sqliteFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		return cache
	}
}

func TestRemoteSqlite(t *testing.T) {
	ctx := context.TODO()
	log.Printf("TestRemoteSqlite...")
	testRemote(t, ctx, initSqliteCache(t))
}

func TestRemoteConcurrentSqlite(t *testing.T) {
	ctx := context.TODO()
	log.Printf("TestRemoteConcurrentSqlite...")
	testRemoteConcurrent(t, ctx, initSqliteCache(t))

}
