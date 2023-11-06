package cachecalc

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq" // Import the pq driver
	"strings"
	"time"
)

const (
	createTableQuery = `CREATE TABLE IF NOT EXISTS postgres_cache_key_value_expired_v_1_4(
    key text PRIMARY KEY,
    value bytea NOT NULL,
    expires_at TIMESTAMP)
		`
	deleteExpiredQuery = `delete from postgres_cache_key_value_expired_v_1_4 where expires_at <= now()`

	upsertValueQuery = `INSERT INTO postgres_cache_key_value_expired_v_1_4(key, value, expires_at) VALUES($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3`
	insertIfNotExistQuery = `INSERT INTO postgres_cache_key_value_expired_v_1_4(key, value, expires_at) VALUES($1, $2, $3)`
	getValueQuery         = `SELECT value FROM postgres_cache_key_value_expired_v_1_4 WHERE key = $1 and expires_at>now()`
	deleteKeyQuery        = `DELETE FROM postgres_cache_key_value_expired_v_1_4 WHERE key = $1`
)

type PostgresCache struct {
	db *sql.DB
}

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
	p := &PostgresCache{
		db: db,
	}
	if err = p.purgeExpired(ctx); err != nil {
		return nil, err
	}
	return p, nil
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

func (p *PostgresCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (keyCreated bool, err error) {
	if err = p.purgeExpired(ctx); err != nil {
		return
	}
	fixTTL := nzDuration(ttl)
	expiresAt := time.Now().Add(fixTTL).UTC()
	_, err = p.db.ExecContext(ctx, insertIfNotExistQuery, key, value, expiresAt)

	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "unique constraint") || strings.Contains(msg, "duplicate key") {
			return false, nil
		}
		return false, err
	}
	thread := getThread(ctx)
	logger.Printf("thread %v: %s=%s expires in %dms", thread, key, string(value), fixTTL.Milliseconds())
	return true, nil
}

func (p *PostgresCache) Get(ctx context.Context, key string) (value []byte, exists bool, err error) {
	err = p.db.QueryRowContext(ctx, getValueQuery, key).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	thread := getThread(ctx)
	logger.Printf("thread %v: obtained value", thread)

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
