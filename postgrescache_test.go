package cachecalc

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func PostgreUrl() string {
	//return "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	return os.Getenv("POSTGRE_URL")
}

// TestPostgresCache tests the PostgresCache implementation
func TestPostgresCache(t *testing.T) {
	// Adjust these variables to match your PostgreSQL configuration
	dbURL := PostgreUrl()
	ctx := context.Background()

	// Initialize the cache
	cache, err := NewPostgresCache(ctx, dbURL)
	require.NoError(t, err, "create cache")
	require.NotNil(t, cache)
	_, err = cache.(*PostgresCache).db.Exec("truncate cachecalc")
	require.NoError(t, err, "truncate cache")

	defer func() {
		if err := cache.(*PostgresCache).db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	}()

	// Test Set and Get
	key := "test_key"
	value := []byte("test_value")

	err = cache.Set(ctx, key, value, time.Second)
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	cachedValue, exists, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !exists || string(cachedValue) != "test_value" {
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

func TestMain(m *testing.M) {
	// You can set up any database initialization or cleanup code here.
	// Make sure to create a test database for unit tests and specify the connection URL.
	dbURL := PostgreUrl()

	// Initialize the database connection for tests
	testDB, err := sql.Open("postgres", dbURL)
	if err != nil {
		fmt.Printf("Failed to open test database connection: %v", err)
	}

	// Create the cachecalc table for tests if it doesn't exist
	_, err = testDB.Exec(`
		CREATE TABLE IF NOT EXISTS cachecalc (
			key text PRIMARY KEY,
			value bytea NOT NULL
		)`)
	if err != nil {
		fmt.Printf("Failed to create test table: %v", err)
	}

	// Run the tests
	exitCode := m.Run()

	// Cleanup and close the test database connection
	if err := testDB.Close(); err != nil {
		fmt.Printf("Failed to close test database connection: %v", err)
	}

	// Exit with the test result
	os.Exit(exitCode)
}
