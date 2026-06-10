package cluster_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	clustercfg "github.com/okharch/cachecalc/v4/cluster"
	pcluster "github.com/okharch/cachecalc/v4/providers/cluster"
	"github.com/okharch/cachecalc/v4/smartcache"
)

// TestWarmUpAllFollowers verifies that when leader A dies and a new leader is
// elected, all surviving followers stream their local entries to the new leader.
// Each of the 4 nodes (A, B, C, D) computes its own unique value. After killing
// A, the new leader must end up with B's, C's, and D's values in its shared
// store — proving every follower pushed its warm-up data.
func TestWarmUpAllFollowers(t *testing.T) {
	cfg := clustercfg.DefaultConfig()
	cfg.GRPCListenAddress = reserveTCPAddress(t)
	cfg.LeaderLockAddress = reserveTCPAddress(t)
	cfg.ElectionRetryInterval = 100 * time.Millisecond
	cfg.DialTimeout = 200 * time.Millisecond
	cfg.ReconnectBaseDelay = 100 * time.Millisecond
	cfg.ReadThroughTTL = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	longTTL := smartcache.Policy{MinTTL: 30 * time.Second, MaxTTL: 60 * time.Second}
	names := []string{"A", "B", "C", "D"}

	caches := make([]*smartcache.Cache, 4)
	services := make([]*clustercfg.Service, 4)
	for i := 0; i < 4; i++ {
		caches[i] = smartcache.New(smartcache.Config{MaxWorkers: 2})
		nodeCfg := cfg
		nodeCfg.Name = names[i]
		var err error
		services[i], err = pcluster.Bind(ctx, caches[i], nodeCfg)
		if err != nil {
			t.Fatalf("bind %s: %v", names[i], err)
		}
		defer services[i].Close()
		defer caches[i].Close()
	}

	waitForLeader(t, services...)

	var leaderIdx int
	for i, s := range services {
		if s.IsLeader() {
			leaderIdx = i
			break
		}
	}
	t.Logf("initial leader: %s", names[leaderIdx])

	// Each node computes its own unique key via the leader's shared store.
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("key-%s", names[i])
		val := fmt.Sprintf("val-%s", names[i])
		got, err := smartcache.Get(ctx, caches[i], key, func(ctx context.Context) (string, smartcache.Policy, error) {
			return val, longTTL, nil
		})
		if err != nil {
			t.Fatalf("%s compute %s: %v", names[i], key, err)
		}
		if got != val {
			t.Fatalf("%s compute %s = %q, want %q", names[i], key, got, val)
		}
	}

	// Kill the leader.
	_ = services[leaderIdx].Close()
	caches[leaderIdx].Close()
	t.Logf("killed leader %s", names[leaderIdx])

	// Collect surviving indices.
	survivors := make([]int, 0, 3)
	for i := 0; i < 4; i++ {
		if i != leaderIdx {
			survivors = append(survivors, i)
		}
	}

	// Wait for a new leader among survivors.
	deadline := time.Now().Add(3 * time.Second)
	var newLeaderIdx int
	for time.Now().Before(deadline) {
		for _, si := range survivors {
			if services[si].IsLeader() {
				newLeaderIdx = si
				goto elected
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no new leader elected")
elected:
	t.Logf("new leader: %s", names[newLeaderIdx])

	// Wait for followers to detect reconnection and stream warm-up entries.
	time.Sleep(20 * time.Millisecond)

	// Verify the new leader's shared store has all 3 follower values (B, C, D
	// minus the dead leader A). The new leader's own entry is in its local
	// store already; the other two must have arrived via warm-up streams.
	for _, si := range survivors {
		if si == newLeaderIdx {
			continue
		}
		key := fmt.Sprintf("key-%s", names[si])
		wantVal := fmt.Sprintf("val-%s", names[si])
		snap, ok, err := services[newLeaderIdx].Get(ctx, key)
		if err != nil {
			t.Errorf("leader shared store Get(%s): %v", key, err)
			continue
		}
		if !ok {
			t.Errorf("leader shared store missing %s — follower %s did not warm up", key, names[si])
			continue
		}
		var got string
		if err := gob.NewDecoder(bytes.NewReader(snap.Value)).Decode(&got); err != nil {
			t.Errorf("decode %s: %v", key, err)
			continue
		}
		if got != wantVal {
			t.Errorf("leader store %s = %q, want %q", key, got, wantVal)
		}
	}
}
