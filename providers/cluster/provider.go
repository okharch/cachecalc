package cluster

import (
	"context"
	"time"

	"github.com/okharch/cachecalc/v4/cluster"
	"github.com/okharch/cachecalc/v4/distlock/memory"
	"github.com/okharch/cachecalc/v4/smartcache"
	"github.com/okharch/cachecalc/v4/valuestore"
	vmemory "github.com/okharch/cachecalc/v4/valuestore/memory"
)

// Bind wires a smartcache instance to a cluster service. The cache's local
// snapshots become the leader-authoritative shared value store after election.
func Bind(ctx context.Context, cache *smartcache.Cache, cfg cluster.Config) (*cluster.Service, error) {
	localValues := cache.LocalValues()
	if localValues == nil {
		localValues = vmemory.New()
	}
	prevLocks, prevValues := cache.Shared()
	service, err := cluster.New(ctx, cfg, localValues, memory.NewBackend())
	if err != nil {
		return nil, err
	}
	service.SetWarmUpSource(cache.RangeLocal)
	cache.SetShared(service.LockProvider(), service)
	if err := waitReady(ctx, service, cfg); err != nil {
		cache.SetShared(prevLocks, prevValues)
		_ = service.Close()
		return nil, err
	}
	return service, nil
}

// NewWithLocal lets callers provide a custom leader-local value store.
func NewWithLocal(ctx context.Context, cfg cluster.Config, localValues valuestore.Store) (*cluster.Service, error) {
	if localValues == nil {
		localValues = vmemory.New()
	}
	service, err := cluster.New(ctx, cfg, localValues, memory.NewBackend())
	if err != nil {
		return nil, err
	}
	if err := waitReady(ctx, service, cfg); err != nil {
		_ = service.Close()
		return nil, err
	}
	return service, nil
}

func waitReady(ctx context.Context, service *cluster.Service, cfg cluster.Config) error {
	waitCtx, cancel := context.WithTimeout(ctx, maxDuration(3*cfg.DialTimeout, 2*cfg.ElectionRetryInterval+cfg.DialTimeout, 2*time.Second))
	defer cancel()
	return service.WaitReady(waitCtx)
}

func maxDuration(values ...time.Duration) time.Duration {
	var max time.Duration
	for _, value := range values {
		if value > max {
			max = value
		}
	}
	if max <= 0 {
		return 2 * time.Second
	}
	return max
}
