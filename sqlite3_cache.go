package cachecalc

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

// SQLiteCache implements the ExternalCache interface using a SQLite database.
type SQLiteCache struct {
	db *sql.DB
}

// NewSQLiteCache creates a new instance of SQLiteCache.
func NewSQLiteCache(dbPath string) (*SQLiteCache, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// create the cache table if it does not exist
	createTable := `
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value BLOB NOT NULL,
            expiry INTEGER NOT NULL
        );
    `

	_, err = db.Exec(createTable)
	if err != nil {
		return nil, err
	}

	return &SQLiteCache{db: db}, nil
}

// Set sets key to hold the string value.
// If key already holds a value, it is overwritten, regardless of its type.
// Any previous time to live associated with the key is discarded on successful SET operation.
// Should return nil if successful
func (c *SQLiteCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	query := "INSERT OR REPLACE INTO cache (key, value, expiry) VALUES (?, ?, ?)"
	expiry := time.Now().Add(ttl).UnixNano()

	_, err := c.db.ExecContext(ctx, query, key, value, expiry)
	if err != nil {
		return err
	}

	return nil
}

// SetNX sets key to hold string value if key does not exist.
// In that case, it is equal to SET.
// When key already holds a value, no operation is performed.
// SETNX is short for "SET if Not eXists".
// keyCreated must return true if value is set
func (c *SQLiteCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (keyCreated bool, err error) {
	expiry := time.Now().Add(ttl).UnixNano()
	deleteQuery := "DELETE FROM cache WHERE key = ? AND expiry < ?"
	_, err = c.db.ExecContext(ctx, deleteQuery, key, time.Now().UnixNano())
	if err != nil {
		return false, err
	}

	query := "INSERT INTO cache (key, value, expiry) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM cache WHERE key = ?) RETURNING key"
	var insertedKey string

	err = c.db.QueryRowContext(ctx, query, key, value, expiry, key).Scan(&insertedKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// GetCachedCalc gets the value of key.
// exists will be false If the key does not exist.
// An error is returned if the implementor want to signal any errors.
func (c *SQLiteCache) Get(ctx context.Context, key string) (value []byte, exists bool, err error) {
	// Delete expired records
	deleteQuery := "DELETE FROM cache WHERE expiry < ?"
	expiry := time.Now().UnixNano()

	_, err = c.db.ExecContext(ctx, deleteQuery, expiry)
	if err != nil {
		return nil, false, err
	}

	// Fetch the value of the key
	selectQuery := "SELECT value FROM cache WHERE key = ?"

	err = c.db.QueryRowContext(ctx, selectQuery, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return value, true, nil
}

func (c *SQLiteCache) ExtendIfValue(ctx context.Context, key string, expectedValue []byte, ttl time.Duration) (bool, error) {
	query := "UPDATE cache SET expiry = ? WHERE key = ? AND value = ? AND expiry >= ?"
	result, err := c.db.ExecContext(ctx, query, time.Now().Add(ttl).UnixNano(), key, expectedValue, time.Now().UnixNano())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (c *SQLiteCache) DelIfValue(ctx context.Context, key string, expectedValue []byte) (bool, error) {
	query := "DELETE FROM cache WHERE key = ? AND value = ?"
	result, err := c.db.ExecContext(ctx, query, key, expectedValue)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (c *SQLiteCache) SetIfLockOwned(ctx context.Context, lockKey string, expectedLockValue []byte, key string, value []byte, ttl time.Duration) (bool, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var owned int
	err = tx.QueryRowContext(
		ctx,
		"SELECT 1 FROM cache WHERE key = ? AND value = ? AND expiry >= ?",
		lockKey,
		expectedLockValue,
		time.Now().UnixNano(),
	).Scan(&owned)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if _, err = tx.ExecContext(
		ctx,
		"INSERT OR REPLACE INTO cache (key, value, expiry) VALUES (?, ?, ?)",
		key,
		value,
		time.Now().Add(nzDuration(ttl)).UnixNano(),
	); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Del Removes the specified key. A key is ignored if it does not exist.
func (c *SQLiteCache) Del(ctx context.Context, key string) error {
	query := "DELETE FROM cache WHERE key = ?"

	_, err := c.db.ExecContext(ctx, query, key)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the SQLite database connection.
func (c *SQLiteCache) Close() error {
	return c.db.Close()
}
