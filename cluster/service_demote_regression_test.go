package cluster

import (
	"context"
	"testing"
	"time"

	lockmem "github.com/okharch/cachecalc/distlock/memory"
	"github.com/okharch/cachecalc/valuestore"
	vmemory "github.com/okharch/cachecalc/valuestore/memory"
)

// TestServiceDemoteWaitsForInFlightLeaderPut documents the fencing behavior for
// leader-local mutations during role changes.
//
// Scenario:
//  1. A service starts a Put while it is still leader, so the operation uses
//     the leader-local store path.
//  2. Demotion starts before that local Put completes.
//  3. The in-flight Put then resumes and finishes.
//
// Required behavior:
// demotion must not complete until the in-flight leader-local operation has
// finished. That prevents the old leader from continuing to mutate its private
// local store after demotion has already taken effect.
func TestServiceDemoteWaitsForInFlightLeaderPut(t *testing.T) {
	backing := vmemory.New()
	blocking := &blockingStore{
		Store:   backing,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := &Service{
		localValues: blocking,
		localLocks:  lockmem.NewBackend(),
		server:      newGRPCServer("127.0.0.1:0"),
		values:      newRemoteValueStore(nil, 50*time.Millisecond, 0),
		locks:       newRemoteLockBackend(nil, 50*time.Millisecond),
	}
	service.isLeader.Store(true)

	putDone := make(chan error, 1)
	go func() {
		putDone <- service.Put(context.Background(), "demotion-put", valuestore.EntrySnapshot{
			Value:    []byte("alpha"),
			ExpireAt: time.Now().Add(time.Second),
		})
	}()

	<-blocking.started

	demoteDone := make(chan struct{})
	go func() {
		service.demote()
		close(demoteDone)
	}()

	select {
	case <-demoteDone:
		t.Fatal("demotion completed before in-flight leader-local Put finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(blocking.release)

	if err := <-putDone; err != nil {
		t.Fatalf("put returned unexpected error: %v", err)
	}

	select {
	case <-demoteDone:
	case <-time.After(time.Second):
		t.Fatal("demotion did not complete after in-flight leader-local Put finished")
	}

	if service.IsLeader() {
		t.Fatal("service is still leader after demotion")
	}
	if _, ok, err := backing.Get(context.Background(), "demotion-put"); err != nil {
		t.Fatalf("backing get: %v", err)
	} else if !ok {
		t.Fatal("leader-local Put did not complete before demotion finished")
	}
}

// TestServiceCloseDoesNotBlockIndefinitelyOnSlowLocalBackend documents the
// shutdown expectation for services that use custom leader-local stores.
//
// Scenario:
//  1. A leader-local operation enters a custom local backend and blocks there.
//  2. Service shutdown begins while that backend call is still blocked.
//  3. Close internally calls demote, which currently waits for the in-flight
//     leader-local operation because it holds the service read lock.
//
// Required behavior:
// service shutdown should not block indefinitely on a slow or wedged custom
// leader-local backend. Otherwise one stuck backend call can freeze demotion
// and process shutdown.
func TestServiceCloseDoesNotBlockIndefinitelyOnSlowLocalBackend(t *testing.T) {
	backing := vmemory.New()
	blocking := &blockingStore{
		Store:   backing,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := &Service{
		localValues: blocking,
		localLocks:  lockmem.NewBackend(),
		server:      newGRPCServer("127.0.0.1:0"),
		values:      newRemoteValueStore(nil, 50*time.Millisecond, 0),
		locks:       newRemoteLockBackend(nil, 50*time.Millisecond),
	}
	service.isLeader.Store(true)

	readDone := make(chan struct{})
	go func() {
		_, _, _ = service.Get(context.Background(), "slow-close")
		close(readDone)
	}()

	<-blocking.started

	closeDone := make(chan struct{})
	go func() {
		_ = service.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("service.Close blocked on a slow local backend")
	}

	close(blocking.release)
	<-readDone
}

type blockingStore struct {
	valuestore.Store
	started chan struct{}
	release chan struct{}
}

func (s *blockingStore) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	close(s.started)
	<-s.release
	return s.Store.Get(ctx, key)
}

func (s *blockingStore) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	close(s.started)
	<-s.release
	return s.Store.Put(ctx, key, entry)
}
