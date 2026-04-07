package cluster

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	cachecalc "github.com/okharch/cachecalc"
)

// DistributedExternalCache is the main cluster entry point. It preserves the
// ExternalCache contract while routing calls either to a leader-local
// authoritative cache or to a follower-side gRPC proxy, depending on the
// current leadership state.
type DistributedExternalCache struct {
	local    cachecalc.ExternalCache
	client   cachecalc.ExternalCache
	elector  LeaderElector
	server   *grpcCacheServer
	logger   *log.Logger
	observer func(CacheEvent)
	isLeader atomic.Bool
	mu       sync.RWMutex
}

// NewDistributedExternalCache constructs the leader/follower cache wrapper,
// choosing an elector from Config and defaulting the leader-local store to the
// in-memory implementation when no custom local cache is supplied.
func NewDistributedExternalCache(ctx context.Context, cfg Config, local cachecalc.ExternalCache) (*DistributedExternalCache, error) {
	if local == nil {
		local = NewMemoryExternalCache()
	}
	elector, err := NewLeaderElector(cfg)
	if err != nil {
		return nil, err
	}
	d := &DistributedExternalCache{
		local:   local,
		client:  newGRPCExternalCacheClient(elector, cfg.DialTimeout, cfg.EnableL2, cfg.L2TTL, cfg.Observer),
		elector: elector,
		server:  newGRPCCacheServer(cfg.GRPCListenAddress),
		logger:  log.New(os.Stderr, "cluster: ", log.LstdFlags),
		observer: cfg.Observer,
	}
	if err := elector.Start(ctx, d.promoteToLeader, d.demoteToFollower); err != nil {
		return nil, err
	}
	return d, nil
}

// NewLeaderElector selects the election mechanism for the configured mode.
func NewLeaderElector(cfg Config) (LeaderElector, error) {
	switch cfg.Mode {
	case ModeLocal:
		return NewLocalLeaderElector(cfg), nil
	case ModeK8s:
		return newKubernetesLeaderElector(cfg)
	default:
		return nil, fmt.Errorf("unsupported cluster mode %q", cfg.Mode)
	}
}

func (d *DistributedExternalCache) IsLeader() bool {
	return d.isLeader.Load()
}

func (d *DistributedExternalCache) LeaderAddress() string {
	return d.elector.LeaderAddress()
}

func (d *DistributedExternalCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return d.activeCache().Set(ctx, key, value, ttl)
}

func (d *DistributedExternalCache) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return d.activeCache().SetNX(ctx, key, value, ttl)
}

func (d *DistributedExternalCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	cache := d.activeCache()
	value, exists, err := cache.Get(ctx, key)
	if err == nil && d.isLeader.Load() && d.observer != nil {
		d.observer(CacheEvent{Operation: "get", Role: "leader", Source: "leader-authoritative", Key: key, Hit: exists})
	}
	return value, exists, err
}

func (d *DistributedExternalCache) ExtendIfValue(ctx context.Context, key string, expectedValue []byte, ttl time.Duration) (bool, error) {
	return d.activeCache().ExtendIfValue(ctx, key, expectedValue, ttl)
}

func (d *DistributedExternalCache) DelIfValue(ctx context.Context, key string, expectedValue []byte) (bool, error) {
	return d.activeCache().DelIfValue(ctx, key, expectedValue)
}

func (d *DistributedExternalCache) SetIfLockOwned(ctx context.Context, lockKey string, expectedLockValue []byte, key string, value []byte, ttl time.Duration) (bool, error) {
	return d.activeCache().SetIfLockOwned(ctx, lockKey, expectedLockValue, key, value, ttl)
}

func (d *DistributedExternalCache) Del(ctx context.Context, key string) error {
	return d.activeCache().Del(ctx, key)
}

// Close stops leader serving if active, closes the follower client, and closes
// the local authoritative cache implementation.
func (d *DistributedExternalCache) Close() error {
	d.demoteToFollower()
	if err := d.client.Close(); err != nil {
		return err
	}
	return d.local.Close()
}

func (d *DistributedExternalCache) activeCache() cachecalc.ExternalCache {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.isLeader.Load() {
		return d.local
	}
	return d.client
}

// promoteToLeader starts the leader gRPC server and flips request routing to
// the local authoritative cache.
func (d *DistributedExternalCache) promoteToLeader() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isLeader.Load() {
		return
	}
	if err := d.server.Start(d.local); err != nil {
		d.logger.Printf("failed to start grpc leader server: %v", err)
		return
	}
	d.isLeader.Store(true)
}

// demoteToFollower stops leader serving and forces future requests through the
// follower gRPC client.
func (d *DistributedExternalCache) demoteToFollower() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.isLeader.Load() {
		return
	}
	d.isLeader.Store(false)
	d.server.Stop()
	_ = d.client.Close()
}
