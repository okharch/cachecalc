package cluster

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/okharch/cachecalc/distlock"
	"github.com/okharch/cachecalc/valuestore"
	vmemory "github.com/okharch/cachecalc/valuestore/memory"
)

// Service exposes a leader-backed ValueStore and distlock.Backend over gRPC.
// Leaders serve local implementations; followers proxy to the current leader.
type Service struct {
	localValues valuestore.Store
	localLocks  distlock.Backend
	elector     LeaderElector
	server      *grpcServer
	values      *remoteValueStore
	locks       *remoteLockBackend
	logger      *log.Logger
	isLeader    atomic.Bool
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

func New(ctx context.Context, cfg Config, localValues valuestore.Store, localLocks distlock.Backend) (*Service, error) {
	if localValues == nil {
		localValues = vmemory.New()
	}
	if localLocks == nil {
		return nil, fmt.Errorf("local lock backend is required")
	}
	elector, err := newElector(cfg)
	if err != nil {
		return nil, err
	}
	service := &Service{
		localValues: localValues,
		localLocks:  localLocks,
		elector:     elector,
		server:      newGRPCServer(cfg.GRPCListenAddress),
		values:      newRemoteValueStore(elector, cfg.DialTimeout, cfg.ReadThroughTTL),
		locks:       newRemoteLockBackend(elector, cfg.DialTimeout),
		logger:      log.New(os.Stderr, "cluster: ", log.LstdFlags),
	}
	service.ctx, service.cancel = context.WithCancel(ctx)
	if err := elector.Start(service.ctx, service.promote, service.demote); err != nil {
		return nil, err
	}
	return service, nil
}

func newElector(cfg Config) (LeaderElector, error) {
	switch cfg.Mode {
	case ModeLocal:
		return NewLocalLeaderElector(cfg), nil
	case ModeK8s:
		return newKubernetesLeaderElector(cfg)
	default:
		return nil, fmt.Errorf("unsupported cluster mode %q", cfg.Mode)
	}
}

func (s *Service) IsLeader() bool                  { return s.isLeader.Load() }
func (s *Service) LeaderAddress() string           { return s.elector.LeaderAddress() }
func (s *Service) LockProvider() distlock.Provider { return distlock.NewProvider(s) }

func (s *Service) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	if s.isLeader.Load() {
		return s.localValues.Get(ctx, key)
	}
	return s.values.Get(ctx, key)
}

func (s *Service) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	if s.withLeaderLocalOp() {
		defer s.mu.RUnlock()
		return s.localValues.Put(ctx, key, entry)
	}
	return s.values.Put(ctx, key, entry)
}

func (s *Service) Delete(ctx context.Context, key string) error {
	if s.withLeaderLocalOp() {
		defer s.mu.RUnlock()
		return s.localValues.Delete(ctx, key)
	}
	return s.values.Delete(ctx, key)
}

func (s *Service) TryAcquire(ctx context.Context, key string, token []byte, ttl time.Duration) (bool, error) {
	if s.withLeaderLocalOp() {
		defer s.mu.RUnlock()
		return s.localLocks.TryAcquire(ctx, key, token, ttl)
	}
	return s.locks.TryAcquire(ctx, key, token, ttl)
}

func (s *Service) Renew(ctx context.Context, key string, token []byte, ttl time.Duration) (bool, error) {
	if s.withLeaderLocalOp() {
		defer s.mu.RUnlock()
		return s.localLocks.Renew(ctx, key, token, ttl)
	}
	return s.locks.Renew(ctx, key, token, ttl)
}

func (s *Service) Release(ctx context.Context, key string, token []byte) (bool, error) {
	if s.withLeaderLocalOp() {
		defer s.mu.RUnlock()
		return s.localLocks.Release(ctx, key, token)
	}
	return s.locks.Release(ctx, key, token)
}

func (s *Service) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.demote()
	if err := s.values.Close(); err != nil {
		return err
	}
	return s.locks.Close()
}

// WaitReady blocks until the service is usable either as leader or as a
// follower that can reach the current leader.
func (s *Service) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.IsLeader() {
			return nil
		}
		if err := s.values.Healthy(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func readinessTimeout(cfg Config) time.Duration {
	timeout := 2 * time.Second
	if cfg.DialTimeout > 0 && 3*cfg.DialTimeout > timeout {
		timeout = 3 * cfg.DialTimeout
	}
	if cfg.ElectionRetryInterval > 0 {
		candidate := 2*cfg.ElectionRetryInterval + cfg.DialTimeout
		if candidate > timeout {
			timeout = candidate
		}
	}
	return timeout
}

func (s *Service) withLeaderLocalOp() bool {
	s.mu.RLock()
	if s.isLeader.Load() {
		return true
	}
	s.mu.RUnlock()
	return false
}

func (s *Service) promote() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isLeader.Load() {
		return
	}
	if err := s.server.Start(s.localValues, s.localLocks); err != nil {
		s.logger.Printf("failed to start grpc server: %v", err)
		return
	}
	s.isLeader.Store(true)
}

func (s *Service) demote() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isLeader.Load() {
		return
	}
	s.isLeader.Store(false)
	s.server.Stop()
	_ = s.values.Close()
	_ = s.locks.Close()
}
