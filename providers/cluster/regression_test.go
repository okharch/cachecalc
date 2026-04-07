package cluster_test

import (
	"context"
	"net"
	"testing"
	"time"

	clustercfg "github.com/okharch/cachecalc/cluster"
	providerscluster "github.com/okharch/cachecalc/providers/cluster"
	"github.com/okharch/cachecalc/smartcache"
)

// TestBindDoesNotPoisonCacheWhenReadinessFails documents the initialization
// contract for providers/cluster.Bind.
//
// Scenario:
//  1. Bind creates a cluster service and temporarily wires the passed cache to
//     that service.
//  2. Readiness then fails because this instance is forced into follower mode
//     and no leader is reachable.
//  3. Bind returns an initialization error to the caller.
//
// Required behavior:
// a failed Bind must leave the passed cache usable in its previous local-only
// configuration. It must not leave dead cluster lock/value providers attached
// after returning an error.
func TestBindDoesNotPoisonCacheWhenReadinessFails(t *testing.T) {
	cache := smartcache.New(smartcache.Config{MaxWorkers: 1})
	defer cache.Close()

	cfg := clustercfg.DefaultConfig()
	cfg.GRPCListenAddress = reserveTCPAddress(t)
	cfg.LeaderLockAddress = reserveTCPAddress(t)
	cfg.ElectionRetryInterval = 100 * time.Millisecond
	cfg.DialTimeout = 200 * time.Millisecond
	cfg.ReadThroughTTL = 0

	lockLis, err := net.Listen("tcp", cfg.LeaderLockAddress)
	if err != nil {
		t.Fatalf("occupy leader lock address: %v", err)
	}
	defer lockLis.Close()

	service, err := providerscluster.Bind(context.Background(), cache, cfg)
	if err == nil {
		defer service.Close()
		t.Fatal("expected Bind to fail when no leader is reachable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	value, calcErr := smartcache.GetWithTTL(ctx, cache, "bind-failure-local-fallback", 20*time.Millisecond, 50*time.Millisecond, false, func(ctx context.Context) (string, error) {
		return "local", nil
	})
	if calcErr != nil {
		t.Fatalf("cache was left unusable after Bind failure: %v", calcErr)
	}
	if value != "local" {
		t.Fatalf("local fallback value = %q, want local", value)
	}
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
