package cluster_test

import (
	"context"
	"net"
	"testing"
	"time"

	clustercfg "github.com/okharch/cachecalc/v4/cluster"
	lockmem "github.com/okharch/cachecalc/v4/distlock/memory"
	providerscluster "github.com/okharch/cachecalc/v4/providers/cluster"
	vmemory "github.com/okharch/cachecalc/v4/valuestore/memory"
)

// TestClusterNewDoesNotFailFollowerStartupWithoutReachableLeader documents the
// intended startup behavior for follower instances.
//
// Scenario:
//  1. Another process already owns the election socket, so this instance cannot
//     become leader during construction.
//  2. No leader gRPC server is reachable yet at the advertised leader address.
//  3. The constructor should still succeed and let the instance keep retrying
//     in follower mode, because the cluster design expects followers to wait
//     for leader availability rather than fail process startup.
//
// Required behavior:
// cluster.New should not return an initialization error merely because the
// current leader endpoint is not reachable yet while another process owns the
// election socket.
func TestClusterNewDoesNotFailFollowerStartupWithoutReachableLeader(t *testing.T) {
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

	service, err := clustercfg.New(context.Background(), cfg, vmemory.New(), lockmem.NewBackend())
	if err != nil {
		t.Fatalf("cluster.New should allow follower startup without a reachable leader, got error: %v", err)
	}
	defer service.Close()
}

// TestClusterNewWithLocalIsReadyBeforeFirstUse documents the constructor
// readiness contract for the public cluster entry point.
//
// Scenario:
//  1. Another process already owns the election socket, so this instance starts
//     in follower mode.
//  2. No leader gRPC server is actually reachable at the advertised leader
//     address.
//  3. providers/cluster.NewWithLocal must not return a Service that is not yet
//     usable.
//  4. In this situation it should fail initialization instead of returning an
//     object whose first operation times out.
//
// Required behavior:
// a public constructor that returns a cluster-backed Service should not hand
// back an object that immediately fails on first use due to unresolved
// leadership/readiness. It should either:
//   - wait until the service is usable as leader or follower, or
//   - return an initialization error instead of an unusable Service.
func TestClusterNewWithLocalIsReadyBeforeFirstUse(t *testing.T) {
	cfg := clustercfg.DefaultConfig()
	cfg.GRPCListenAddress = reserveTCPAddress(t)
	cfg.LeaderLockAddress = reserveTCPAddress(t)
	cfg.ElectionRetryInterval = 100 * time.Millisecond
	cfg.DialTimeout = 200 * time.Millisecond
	cfg.ReadThroughTTL = 0

	// Force follower mode by pre-owning the election socket, but do not start a
	// leader gRPC server on the advertised leader address. A readiness-aware
	// constructor should not return a service that immediately fails on first
	// use in this state.
	lockLis, err := net.Listen("tcp", cfg.LeaderLockAddress)
	if err != nil {
		t.Fatalf("occupy leader lock address: %v", err)
	}
	defer lockLis.Close()

	service, err := providerscluster.NewWithLocal(context.Background(), cfg, vmemory.New())
	if err == nil {
		defer service.Close()
		t.Fatal("expected constructor to fail when no leader is reachable")
	}
}
