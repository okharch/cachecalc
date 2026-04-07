package distlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Backend is the primitive distributed-lock storage contract. Providers wrap a
// backend and manage renewal/loss tracking for callers.
type Backend interface {
	TryAcquire(ctx context.Context, key string, token []byte, ttl time.Duration) (bool, error)
	Renew(ctx context.Context, key string, token []byte, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key string, token []byte) (bool, error)
}

// Lease is an owned distributed lock lease.
type Lease interface {
	Lost() <-chan struct{}
	Release(ctx context.Context) error
}

// Provider acquires auto-renewing leases from a backend.
type Provider interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, bool, error)
}

type provider struct {
	backend Backend
}

// NewProvider wraps a backend with token generation and renewal logic.
func NewProvider(backend Backend) Provider {
	return &provider{backend: backend}
}

func (p *provider) Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, bool, error) {
	token, err := newToken()
	if err != nil {
		return nil, false, err
	}
	acquired, err := p.backend.TryAcquire(ctx, key, token, ttl)
	if err != nil || !acquired {
		return nil, acquired, err
	}
	return newManagedLease(ctx, p.backend, key, token, ttl), true, nil
}

func newToken() ([]byte, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	dst := make([]byte, hex.EncodedLen(len(buf)))
	hex.Encode(dst, buf)
	return dst, nil
}
