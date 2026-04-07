package memory

import (
	"context"
	"sync"
	"time"

	"github.com/okharch/cachecalc/distlock"
)

type record struct {
	token  []byte
	expiry time.Time
}

// Backend is an in-memory distlock backend.
type Backend struct {
	mu    sync.Mutex
	locks map[string]record
}

func NewBackend() *Backend {
	return &Backend{locks: make(map[string]record)}
}

func NewProvider() distlock.Provider {
	return distlock.NewProvider(NewBackend())
}

func (b *Backend) TryAcquire(_ context.Context, key string, token []byte, ttl time.Duration) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if current, ok := b.locks[key]; ok && now.Before(current.expiry) {
		return false, nil
	}
	b.locks[key] = record{token: append([]byte(nil), token...), expiry: now.Add(ttl)}
	return true, nil
}

func (b *Backend) Renew(_ context.Context, key string, token []byte, ttl time.Duration) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	current, ok := b.locks[key]
	if !ok || now.After(current.expiry) || string(current.token) != string(token) {
		return false, nil
	}
	current.expiry = now.Add(ttl)
	b.locks[key] = current
	return true, nil
}

func (b *Backend) Release(_ context.Context, key string, token []byte) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.locks[key]
	if !ok || string(current.token) != string(token) {
		return false, nil
	}
	delete(b.locks, key)
	return true, nil
}
