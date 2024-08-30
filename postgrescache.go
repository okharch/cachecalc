package cachecalc

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq" // Import the pq driver
	"sync"
	"time"
)

type PostgresCache struct {
	db  *sql.DB
	dsn string
	sync.Mutex
	locks map[string]*sync.Mutex
}

const (
	createTableQuery = `CREATE TABLE IF NOT EXISTS postgres_cache_key_value_expired_v_1_4(
    key text PRIMARY KEY,
    value bytea NOT NULL,
    expires_at TIMESTAMP)
		`
	deleteExpiredQuery = `DELETE FROM postgres_cache_key_value_expired_v_1_4 WHERE expires_at <= now() RETURNING key`

	upsertValueQuery = `INSERT INTO postgres_cache_key_value_expired_v_1_4(key, value, expires_at) VALUES($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3`
	insertIfNotExistQuery = `INSERT INTO postgres_cache_key_value_expired_v_1_4(key, value, expires_at) VALUES($1, $2, $3)`
	getValueQuery         = `SELECT value FROM postgres_cache_key_value_expired_v_1_4 WHERE key = $1 and expires_at > now()`
	deleteKeyQuery        = `DELETE FROM postgres_cache_key_value_expired_v_1_4 WHERE key = $1`
	deleteKeyValueQuery   = `DELETE FROM postgres_cache_key_value_expired_v_1_4 WHERE key = $1 and value = $2`

	dropTriggerQuery  = `DROP TRIGGER IF EXISTS cache_entry_deleted_trigger ON postgres_cache_key_value_expired_v_1_4`
	dropFunctionQuery = `DROP FUNCTION IF EXISTS notify_cache_entry_deleted`

	createAquireLockFunctionQuery = `CREATE OR REPLACE FUNCTION acquire_lock(key text, lock_value bytea, lock_ttl interval)
RETURNS boolean AS $$
DECLARE
    lock_key text := 'lock-key-' || key;
BEGIN
    -- Delete the lock if it has expired
    DELETE FROM postgres_cache_key_value_expired_v_1_4
    WHERE key = lock_key AND expires_at <= NOW();

    -- Try to insert the new lock
    INSERT INTO postgres_cache_key_value_expired_v_1_4 (key, value, expires_at)
    VALUES (lock_key, lock_value, NOW() + lock_ttl)
    ON CONFLICT DO NOTHING;

    -- Check if the lock was successfully acquired
    IF FOUND THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;
`

	createTriggerFunctionQuery = `
	CREATE OR REPLACE FUNCTION notify_cache_entry_deleted() RETURNS trigger AS $$
	BEGIN
		PERFORM pg_notify('cache_entry_deleted', OLD.key);
		RETURN OLD;
	END;
	$$ LANGUAGE plpgsql;
	`

	createDeleteTriggerQuery = `
	CREATE TRIGGER cache_entry_deleted_trigger
	AFTER DELETE ON postgres_cache_key_value_expired_v_1_4
	FOR EACH ROW EXECUTE FUNCTION notify_cache_entry_deleted();
	`
)

func NewPostgresCache(ctx context.Context, dbUrl string) (ExternalCache, error) {
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	// create cachecalc table if not exists
	_, err = db.ExecContext(ctx, createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create cachecalc table: %w", err)
	}

	// drop existing trigger and function if they exist
	_, err = db.ExecContext(ctx, dropTriggerQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to drop existing trigger: %w", err)
	}

	_, err = db.ExecContext(ctx, dropFunctionQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to drop existing function: %w", err)
	}

	// create trigger function
	_, err = db.ExecContext(ctx, createTriggerFunctionQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create trigger function: %w", err)
	}

	// create delete trigger
	_, err = db.ExecContext(ctx, createDeleteTriggerQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete trigger: %w", err)
	}

	p := &PostgresCache{
		db:    db,
		dsn:   dbUrl,
		locks: make(map[string]*sync.Mutex),
	}

	if err = p.purgeExpired(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *PostgresCache) EntryUpdates(ctx context.Context, key string) (chan []byte, string, error) {
	return nil, "", nil
}

func (p *PostgresCache) UnsubscribeUpdates(key, chanKey string) error {
	return nil
}

func (p *PostgresCache) purgeExpired(ctx context.Context) error {
	thread := getThread(ctx)
	r, err := p.db.ExecContext(ctx, deleteExpiredQuery)
	if err != nil {
		err = fmt.Errorf("failed to purge expired items: %w", err)
		return err
	}
	rowsAffected, _ := r.RowsAffected()
	if rowsAffected > 0 {
		logger.Printf("thread %v: purged %d records", thread, rowsAffected)
	}
	return err
}

func (p *PostgresCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	fixTTL := nzDuration(ttl)
	expiresAt := time.Now().Add(fixTTL).UTC()
	_, err := p.db.ExecContext(ctx, upsertValueQuery, key, value, expiresAt)

	return err
}

func (p *PostgresCache) Get(ctx context.Context, key string) (value []byte, exists bool, err error) {
	err = p.db.QueryRowContext(ctx, getValueQuery, key).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

func (p *PostgresCache) Del(ctx context.Context, key string) error {
	thread := getThread(ctx)
	_, err := p.db.ExecContext(ctx, deleteKeyQuery, key)
	logger.Printf("thread %v: deleted %s", thread, key)
	return err
}

func (p *PostgresCache) Close() error {
	if err := p.purgeExpired(context.TODO()); err != nil {
		return err
	}
	return p.db.Close()
}

//func (p *PostgresCache) ExpireEntries(ctx context.Context) chan string {
//	ch := make(chan string)
//
//	// Listen for notifications
//	listener := pq.NewListener(p.dsn, 10*time.Second, time.Minute, nil)
//	logger.Printf("Listening for notifications on %s", p.dsn)
//	err := listener.Listen("cache_entry_deleted")
//	if err != nil {
//		logger.Printf("Error setting up listener: %v", err)
//		return nil
//	}
//	defer func() {
//		// ignore error on close
//		_ = listener.Close()
//	}()
//
//	go func() {
//		defer close(ch)
//
//		logger.Printf("Listening for db notifications on cache_entry_deleted")
//		for {
//			select {
//			case <-ctx.Done():
//				return
//			case notification := <-listener.Notify:
//				if notification != nil {
//					ch <- notification.Extra
//				}
//			case <-time.After(90 * time.Second):
//				go func() {
//					// ignore error
//					_ = listener.Ping()
//				}()
//			}
//		}
//	}()
//
//	return ch
//}

// GetLock attempts to acquire a distributed lock using pg_advisory_lock.
func (p *PostgresCache) GetLock(ctx context.Context, key string, minTTL time.Duration) (releaseLock func() error, err error) {
	// Generate a unique value for this lock attempt
	uniqueValue := []byte("unique-lock-value")

	// Define the TTL for the lock
	lockTTL := time.Minute // or any appropriate duration

	// Attempt to acquire the lock by calling the stored procedure
	var lockAcquired bool
	err = p.db.QueryRowContext(ctx, "SELECT acquire_lock($1, $2, $3)", key, uniqueValue, lockTTL).Scan(&lockAcquired)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock for key %s: %w", key, err)
	}

	if !lockAcquired {
		// Lock was not acquired, return nil for release function
		return nil, nil
	}

	// Lock was successfully acquired, return the release function
	releaseLock = func() error {
		// Remove the key from the table to release the lock
		lockKey := "lock-key-" + key
		query := `DELETE FROM postgres_cache_key_value_expired_v_1_4 WHERE key = $1`
		_, err := p.db.ExecContext(ctx, query, lockKey)
		if err != nil {
			return fmt.Errorf("failed to release lock for key %s: %w", key, err)
		}
		return nil
	}

	return releaseLock, nil
}
