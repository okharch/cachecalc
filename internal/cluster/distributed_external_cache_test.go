package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	cachecalc "github.com/okharch/cachecalc"
)

// TestDistributedExternalCacheSingleLeaderAndProxyIO verifies the steady-state
// local-mode behavior: one leader is elected and followers can read and write
// through the leader via the ExternalCache interface.
func TestDistributedExternalCacheSingleLeaderAndProxyIO(t *testing.T) {
	lockAddr := freeAddr(t)
	grpcAddr := freeAddr(t)
	cfg := DefaultConfig()
	cfg.LeaderLockAddress = lockAddr
	cfg.GRPCListenAddress = grpcAddr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c1, err := NewDistributedExternalCache(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("create cache 1: %v", err)
	}
	defer c1.Close()
	c2, err := NewDistributedExternalCache(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("create cache 2: %v", err)
	}
	defer c2.Close()
	c3, err := NewDistributedExternalCache(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("create cache 3: %v", err)
	}
	defer c3.Close()

	waitForLeaderCount(t, []*DistributedExternalCache{c1, c2, c3}, 1)

	if err := c2.Set(context.Background(), "alpha", []byte("value"), time.Second); err != nil {
		t.Fatalf("set through follower: %v", err)
	}
	got, ok, err := c3.Get(context.Background(), "alpha")
	if err != nil {
		t.Fatalf("get through follower: %v", err)
	}
	if !ok || string(got) != "value" {
		t.Fatalf("unexpected get result ok=%v value=%q", ok, string(got))
	}
}

// TestDistributedExternalCacheFailover verifies that when the current leader's
// context is canceled, a follower can acquire leadership and continue serving
// cache requests.
func TestDistributedExternalCacheFailover(t *testing.T) {
	lockAddr := freeAddr(t)
	grpcAddr := freeAddr(t)
	cfg := DefaultConfig()
	cfg.LeaderLockAddress = lockAddr
	cfg.GRPCListenAddress = grpcAddr
	cfg.ElectionRetryInterval = 200 * time.Millisecond

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()

	c1, err := NewDistributedExternalCache(ctx1, cfg, nil)
	if err != nil {
		t.Fatalf("create cache 1: %v", err)
	}
	defer c1.Close()
	c2, err := NewDistributedExternalCache(ctx2, cfg, nil)
	if err != nil {
		t.Fatalf("create cache 2: %v", err)
	}
	defer c2.Close()
	c3, err := NewDistributedExternalCache(ctx3, cfg, nil)
	if err != nil {
		t.Fatalf("create cache 3: %v", err)
	}
	defer c3.Close()

	caches := []*DistributedExternalCache{c1, c2, c3}
	waitForLeaderCount(t, caches, 1)
	leader := currentLeader(caches)
	if leader == nil {
		t.Fatal("expected a leader")
	}

	switch leader {
	case c1:
		cancel1()
	case c2:
		cancel2()
	case c3:
		cancel3()
	}

	newLeader := waitForDifferentLeader(t, caches, leader)

	if err := newLeader.Set(context.Background(), "beta", []byte("fresh"), time.Second); err != nil {
		t.Fatalf("set on new leader: %v", err)
	}
	got, ok, err := c1.Get(context.Background(), "beta")
	if leader == c1 {
		got, ok, err = c2.Get(context.Background(), "beta")
	}
	if err != nil {
		t.Fatalf("get after failover: %v", err)
	}
	if !ok || string(got) != "fresh" {
		t.Fatalf("unexpected failover get result ok=%v value=%q", ok, string(got))
	}
}

// TestClusteredCachedCalculationsWarmPromotion verifies that a follower which
// already has a warm local CachedCalculations entry can become leader and
// immediately serve that entry through L2 to a newly started follower.
func TestClusteredCachedCalculationsWarmPromotion(t *testing.T) {
	lockAddr := freeAddr(t)
	grpcAddr := freeAddr(t)
	cfg := DefaultConfig()
	cfg.LeaderLockAddress = lockAddr
	cfg.GRPCListenAddress = grpcAddr
	cfg.ElectionRetryInterval = 200 * time.Millisecond

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	cc1, d1, err := NewClusteredCachedCalculations(ctx1, 2, cfg)
	if err != nil {
		t.Fatalf("create clustered cache 1: %v", err)
	}
	defer cc1.Close()
	defer d1.Close()

	cc2, d2, err := NewClusteredCachedCalculations(ctx2, 2, cfg)
	if err != nil {
		t.Fatalf("create clustered cache 2: %v", err)
	}
	defer cc2.Close()
	defer d2.Close()

	waitForLeaderCount(t, []*DistributedExternalCache{d1, d2}, 1)

	produced := "follower-value"
	calcsOnSecond := 0
	value, err := cachecalc.GetCachedCalcX(cc2, context.Background(), "warm-key", 100*time.Millisecond, time.Second, true, func(context.Context) (string, error) {
		calcsOnSecond++
		return produced, nil
	})
	if err != nil {
		t.Fatalf("initial clustered calc: %v", err)
	}
	if value != produced {
		t.Fatalf("unexpected initial value %q", value)
	}
	if calcsOnSecond != 1 {
		t.Fatalf("expected one local calculation on second node, got %d", calcsOnSecond)
	}

	if d1.IsLeader() {
		cancel1()
	} else {
		cancel2()
		t.Fatal("expected the first node to be leader in this test setup")
	}

	newLeader := waitForDifferentLeader(t, []*DistributedExternalCache{d1, d2}, d1)
	if newLeader != d2 {
		t.Fatalf("expected second node to become leader, got %#v", newLeader)
	}

	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	cc3, d3, err := NewClusteredCachedCalculations(ctx3, 2, cfg)
	if err != nil {
		t.Fatalf("create clustered cache 3: %v", err)
	}
	defer cc3.Close()
	defer d3.Close()

	time.Sleep(200 * time.Millisecond)

	calcsOnThird := 0
	value, err = cachecalc.GetCachedCalcX(cc3, context.Background(), "warm-key", 100*time.Millisecond, time.Second, true, func(context.Context) (string, error) {
		calcsOnThird++
		return "third-value", nil
	})
	if err != nil {
		t.Fatalf("fetch through promoted leader: %v", err)
	}
	if value != produced {
		t.Fatalf("expected promoted leader to expose warm value %q, got %q", produced, value)
	}
	if calcsOnThird != 0 {
		t.Fatalf("expected no recalculation on third node, got %d", calcsOnThird)
	}
}

// freeAddr reserves an ephemeral TCP port long enough to learn a usable local
// address for test configuration.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer lis.Close()
	return lis.Addr().String()
}

// waitForLeaderCount polls until the expected number of leaders is observed
// across the test caches.
func waitForLeaderCount(t *testing.T, caches []*DistributedExternalCache, expected int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, cache := range caches {
			if cache != nil && cache.IsLeader() {
				count++
			}
		}
		if count == expected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	count := 0
	for _, cache := range caches {
		if cache != nil && cache.IsLeader() {
			count++
		}
	}
	t.Fatalf("leader count mismatch: got %d want %d", count, expected)
}

// currentLeader returns the first cache currently reporting itself as leader.
func currentLeader(caches []*DistributedExternalCache) *DistributedExternalCache {
	for _, cache := range caches {
		if cache != nil && cache.IsLeader() {
			return cache
		}
	}
	return nil
}

// waitForDifferentLeader waits until leadership has moved away from the
// previous leader instance.
func waitForDifferentLeader(t *testing.T, caches []*DistributedExternalCache, previous *DistributedExternalCache) *DistributedExternalCache {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader := currentLeader(caches)
		if leader != nil && leader != previous {
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected a new leader after cancellation")
	return nil
}
