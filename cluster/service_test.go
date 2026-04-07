package cluster_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	clustercfg "github.com/okharch/cachecalc/cluster"
	pcluster "github.com/okharch/cachecalc/providers/cluster"
	"github.com/okharch/cachecalc/smartcache"
)

func TestPromotedLeaderServesWarmLocalValue(t *testing.T) {
	cfg := clustercfg.DefaultConfig()
	cfg.GRPCListenAddress = reserveTCPAddress(t)
	cfg.LeaderLockAddress = reserveTCPAddress(t)
	cfg.ElectionRetryInterval = 100 * time.Millisecond
	cfg.DialTimeout = 200 * time.Millisecond
	cfg.ReadThroughTTL = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheA := smartcache.New(smartcache.Config{MaxWorkers: 2})
	serviceA, err := pcluster.Bind(ctx, cacheA, cfg)
	if err != nil {
		t.Fatalf("bind first cache: %v", err)
	}
	defer serviceA.Close()
	defer cacheA.Close()

	cacheB := smartcache.New(smartcache.Config{MaxWorkers: 2})
	serviceB, err := pcluster.Bind(ctx, cacheB, cfg)
	if err != nil {
		t.Fatalf("bind second cache: %v", err)
	}
	defer serviceB.Close()
	defer cacheB.Close()

	waitForLeader(t, serviceA, serviceB)

	var aCalls atomic.Int32
	valueA, err := smartcache.Get(context.Background(), cacheA, "item", true, func(ctx context.Context) (string, smartcache.Policy, error) {
		aCalls.Add(1)
		return "alpha", smartcache.Policy{MinTTL: time.Second, MaxTTL: 3 * time.Second}, nil
	})
	if err != nil || valueA != "alpha" {
		t.Fatalf("cacheA initial get = %q, err=%v", valueA, err)
	}

	valueB, err := smartcache.Get(context.Background(), cacheB, "item", true, func(ctx context.Context) (string, smartcache.Policy, error) {
		return "beta", smartcache.Policy{MinTTL: time.Second, MaxTTL: 3 * time.Second}, nil
	})
	if err != nil || valueB != "alpha" {
		t.Fatalf("cacheB warm get = %q, err=%v", valueB, err)
	}

	if serviceA.IsLeader() {
		_ = serviceA.Close()
	} else {
		_ = serviceB.Close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if serviceB.IsLeader() || serviceA.IsLeader() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cacheC := smartcache.New(smartcache.Config{MaxWorkers: 2})
	serviceC, err := pcluster.Bind(ctx, cacheC, cfg)
	if err != nil {
		t.Fatalf("bind third cache: %v", err)
	}
	defer serviceC.Close()
	defer cacheC.Close()

	var cCalls atomic.Int32
	got, err := smartcache.Get(context.Background(), cacheC, "item", true, func(ctx context.Context) (string, smartcache.Policy, error) {
		cCalls.Add(1)
		return "gamma", smartcache.Policy{MinTTL: time.Second, MaxTTL: 3 * time.Second}, nil
	})
	if err != nil {
		t.Fatalf("cacheC get: %v", err)
	}
	if got != "alpha" {
		t.Fatalf("expected promoted leader to serve warm value alpha, got %q", got)
	}
	if cCalls.Load() != 0 {
		t.Fatalf("expected no recomputation on third cache, got %d", cCalls.Load())
	}
}

func waitForLeader(t *testing.T, services ...*clustercfg.Service) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, service := range services {
			if service.IsLeader() {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("leader election did not complete")
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}
