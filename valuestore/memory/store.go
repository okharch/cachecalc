package memory

import (
	"context"
	"sync"
	"time"

	"github.com/okharch/cachecalc/valuestore"
)

// Store is an in-memory ValueStore implementation useful for tests and
// single-process deployments.
type Store struct {
	mu      sync.RWMutex
	entries map[string]valuestore.EntrySnapshot
}

func New() *Store {
	return &Store{entries: make(map[string]valuestore.EntrySnapshot)}
}

func (s *Store) Get(_ context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	now := time.Now()
	s.mu.RLock()
	entry, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return valuestore.EntrySnapshot{}, false, nil
	}
	if !entry.Usable(now) {
		s.mu.Lock()
		if current, ok := s.entries[key]; ok && !current.Usable(now) {
			delete(s.entries, key)
		}
		s.mu.Unlock()
		return valuestore.EntrySnapshot{}, false, nil
	}
	return cloneSnapshot(entry), true, nil
}

func (s *Store) Put(_ context.Context, key string, entry valuestore.EntrySnapshot) error {
	s.mu.Lock()
	s.entries[key] = cloneSnapshot(entry)
	s.mu.Unlock()
	return nil
}

func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	delete(s.entries, key)
	s.mu.Unlock()
	return nil
}

func cloneSnapshot(entry valuestore.EntrySnapshot) valuestore.EntrySnapshot {
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}
