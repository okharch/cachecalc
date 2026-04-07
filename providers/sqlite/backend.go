package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/okharch/cachecalc/distlock"
	"github.com/okharch/cachecalc/valuestore"
)

// Backend provides shared value storage and distributed locks on SQLite.
type Backend struct {
	db *sql.DB
}

func New(path string) (*Backend, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			return nil, err
		}
	}
	for _, query := range []string{
		`CREATE TABLE IF NOT EXISTS smartcache_values(key TEXT PRIMARY KEY, snapshot BLOB NOT NULL, expiry INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS smartcache_locks(key TEXT PRIMARY KEY, token BLOB NOT NULL, expiry INTEGER NOT NULL)`,
	} {
		if _, err := db.Exec(query); err != nil {
			return nil, err
		}
	}
	return &Backend{db: db}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	_, _ = b.db.ExecContext(ctx, `DELETE FROM smartcache_values WHERE expiry <= ?`, time.Now().UnixNano())
	var buf []byte
	err := b.db.QueryRowContext(ctx, `SELECT snapshot FROM smartcache_values WHERE key = ? AND expiry > ?`, key, time.Now().UnixNano()).Scan(&buf)
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
	_, err = b.db.ExecContext(ctx, `INSERT OR REPLACE INTO smartcache_values(key, snapshot, expiry) VALUES(?, ?, ?)`, key, buf, entry.ExpireAt.UnixNano())
	return err
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.db.ExecContext(ctx, `DELETE FROM smartcache_values WHERE key = ?`, key)
	return err
}

func (b *Backend) TryAcquire(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	_, _ = b.db.ExecContext(ctx, `DELETE FROM smartcache_locks WHERE expiry <= ?`, time.Now().UnixNano())
	res, err := b.db.ExecContext(ctx, `INSERT OR IGNORE INTO smartcache_locks(key, token, expiry) VALUES(?, ?, ?)`, key, token, time.Now().Add(lockTTL).UnixNano())
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
	res, err := b.db.ExecContext(ctx, `UPDATE smartcache_locks SET expiry = ? WHERE key = ? AND token = ? AND expiry > ?`, time.Now().Add(lockTTL).UnixNano(), key, token, time.Now().UnixNano())
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
	res, err := b.db.ExecContext(ctx, `DELETE FROM smartcache_locks WHERE key = ? AND token = ?`, key, token)
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
