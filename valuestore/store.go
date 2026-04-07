package valuestore

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"
)

// EntrySnapshot is the shared representation of a cached value.
// Local synchronization state lives in smartcache; only immutable cache data is
// published through ValueStore implementations.
type EntrySnapshot struct {
	Value        []byte
	RefreshAt    time.Time
	ExpireAt     time.Time
	CalcDuration time.Duration
	Error        string
}

func (s EntrySnapshot) Fresh(now time.Time) bool {
	return !s.RefreshAt.IsZero() && now.Before(s.RefreshAt)
}

func (s EntrySnapshot) Usable(now time.Time) bool {
	return !s.ExpireAt.IsZero() && now.Before(s.ExpireAt)
}

func (s EntrySnapshot) TTL(now time.Time) time.Duration {
	if s.ExpireAt.IsZero() {
		return 0
	}
	return time.Until(s.ExpireAt)
}

// Store provides shared cache snapshots across instances.
type Store interface {
	Get(ctx context.Context, key string) (EntrySnapshot, bool, error)
	Put(ctx context.Context, key string, entry EntrySnapshot) error
	Delete(ctx context.Context, key string) error
}

// Marshal encodes a snapshot for backends that store opaque blobs.
func Marshal(entry EntrySnapshot) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal decodes a snapshot previously created by Marshal.
func Unmarshal(buf []byte) (EntrySnapshot, error) {
	var entry EntrySnapshot
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&entry); err != nil {
		return EntrySnapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return entry, nil
}
