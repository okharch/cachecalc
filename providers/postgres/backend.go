package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/valuestore"
)

const (
	createValueTable = `CREATE TABLE IF NOT EXISTS smartcache_values (
key text PRIMARY KEY,
snapshot bytea NOT NULL,
expires_at timestamptz NOT NULL
)`
	createLockTable = `CREATE TABLE IF NOT EXISTS smartcache_locks (
key text PRIMARY KEY,
token bytea NOT NULL,
expires_at timestamptz NOT NULL
)`
)

// Backend provides shared value storage and distributed locks on PostgreSQL.
type Backend struct {
	db *sql.DB
}

func New(ctx context.Context, dsn string) (*Backend, error) {
	if dsn == "" {
		dsn = os.Getenv("POSTGRE_URL")
	}
	if dsn == "" {
		dsn = "postgres://dev:dev@localhost:29510/dev?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	for _, query := range []string{createValueTable, createLockTable} {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return nil, err
		}
	}
	return &Backend{db: db}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	_, _ = b.db.ExecContext(ctx, `DELETE FROM smartcache_values WHERE expires_at <= now()`)
	var buf []byte
	err := b.db.QueryRowContext(ctx, `SELECT snapshot FROM smartcache_values WHERE key = $1 AND expires_at > now()`, key).Scan(&buf)
	if errors.Is(err, sql.ErrNoRows) {
		return valuestore.EntrySnapshot{}, false, nil
	}
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	entry, err := valuestore.Unmarshal(buf)
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	return entry, true, nil
}

func (b *Backend) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	buf, err := valuestore.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = b.db.ExecContext(ctx, `
INSERT INTO smartcache_values(key, snapshot, expires_at) VALUES($1, $2, $3)
ON CONFLICT (key) DO UPDATE SET snapshot = EXCLUDED.snapshot, expires_at = EXCLUDED.expires_at
`, key, buf, entry.ExpireAt.UTC())
	return err
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.db.ExecContext(ctx, `DELETE FROM smartcache_values WHERE key = $1`, key)
	return err
}

func (b *Backend) TryAcquire(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	_, _ = b.db.ExecContext(ctx, `DELETE FROM smartcache_locks WHERE expires_at <= now()`)
	res, err := b.db.ExecContext(ctx, `INSERT INTO smartcache_locks(key, token, expires_at) VALUES($1, $2, $3) ON CONFLICT DO NOTHING`, key, token, time.Now().Add(lockTTL).UTC())
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (b *Backend) Renew(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	res, err := b.db.ExecContext(ctx, `UPDATE smartcache_locks SET expires_at = $3 WHERE key = $1 AND token = $2 AND expires_at > now()`, key, token, time.Now().Add(lockTTL).UTC())
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (b *Backend) Release(ctx context.Context, key string, token []byte) (bool, error) {
	res, err := b.db.ExecContext(ctx, `DELETE FROM smartcache_locks WHERE key = $1 AND token = $2`, key, token)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (b *Backend) LockProvider() distlock.Provider {
	return distlock.NewProvider(b)
}

func (b *Backend) Close() error {
	return b.db.Close()
}

func (b *Backend) String() string {
	return fmt.Sprintf("postgres-backend(%p)", b.db)
}
