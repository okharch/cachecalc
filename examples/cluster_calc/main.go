package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/okharch/cachecalc"
	"github.com/okharch/cachecalc/internal/cluster"
)

const (
	totalRequests = 8
	waitInterval  = 3 * time.Second
	minTTL        = 6 * time.Second
	maxTTL        = 15 * time.Second
)

func main() {
	cfg, err := cluster.ConfigFromEnv()
	if err != nil {
		log.Fatalf("load cluster config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instanceID := randomID(4)
	if fromEnv := strings.TrimSpace(os.Getenv("NODE_NAME")); fromEnv != "" {
		instanceID = fromEnv
	}

	events := make(chan cluster.CacheEvent, 32)
	cfg.Observer = func(event cluster.CacheEvent) {
		select {
		case events <- event:
		default:
		}
	}

	cc, externalCache, err := cluster.NewClusteredCachedCalculations(ctx, 4, cfg)
	if err != nil {
		log.Fatalf("create clustered cache: %v", err)
	}
	defer cc.Close()
	defer externalCache.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	log.Printf("[%s] demo starting; it will issue %d requests about every %s and then exit", instanceID, totalRequests, waitInterval)
	log.Printf("[%s] note: in this demo L1 means per-instance local cache, and L2 means the remote leader-authoritative cache", instanceID)
	log.Printf("[%s] with GetCachedCalc, the leader owns L2, but the node that wins the distributed lock still performs the fresh calculation locally and then publishes it to L2", instanceID)
	log.Printf("[%s] timings: minTTL=%s maxTTL=%s calc_delay=random(1s..2s)", instanceID, minTTL, maxTTL)
	for i := 0; i < totalRequests; i++ {
		key := "current-time-demo"
		calculatedLocally := false
		drainEvents(events)
		reqID := fmt.Sprintf("%s-%03d", instanceID, i+1)
		log.Printf("cluster_calc event=request_start instance=%s req=%s role=%s leader=%s key=%s", instanceID, reqID, currentRole(externalCache), externalCache.LeaderAddress(), key)

		value, err := cachecalc.GetCachedCalcX(cc, ctx, key, minTTL, maxTTL, true, func(ctx context.Context) (string, error) {
			calculatedLocally = true
			delay := time.Duration(rng.Intn(2)+1) * time.Second
			log.Printf("cluster_calc event=calculation_start instance=%s req=%s role=%s leader=%s key=%s delay=%s", instanceID, reqID, currentRole(externalCache), externalCache.LeaderAddress(), key, delay)
			time.Sleep(delay)
			value := fmt.Sprintf("%s calculated_at=%s", instanceID, time.Now().Format(time.RFC3339Nano))
			log.Printf("cluster_calc event=calculation_done instance=%s req=%s role=%s leader=%s key=%s produced_by=%s", instanceID, reqID, currentRole(externalCache), externalCache.LeaderAddress(), key, instanceID)
			return value, nil
		})
		if err != nil {
			log.Fatalf("[%s] get cached calc: %v", instanceID, err)
		}

		role := currentRole(externalCache)
		path := classifyResult(instanceID, role, calculatedLocally, latestEvent(events), value)
		fmt.Printf("cluster_calc event=request_done instance=%s req=%s role=%s leader=%s key=%s path=%s value_origin=%s value=%q\n",
			instanceID, reqID, role, externalCache.LeaderAddress(), key, path, valueOrigin(value), value)

		if i+1 < totalRequests {
			select {
			case <-ctx.Done():
				return
			case <-time.After(waitInterval):
			}
		}
	}
	log.Printf("[%s] demo finished after %d requests; exit this instance or restart it later to observe re-election or warm/cold cache behavior", instanceID, totalRequests)
}

func currentRole(externalCache *cluster.DistributedExternalCache) string {
	if externalCache.IsLeader() {
		return "LEADER"
	}
	return "FOLLOWER"
}

func classifyResult(instanceID, role string, calculatedLocally bool, event cluster.CacheEvent, value string) string {
	origin := valueOrigin(value)
	if calculatedLocally {
		if role == "FOLLOWER" {
			return "follower-computed-and-published-to-L2"
		}
		return "leader-computed-and-stored-in-L2"
	}
	if event.Source == "" {
		if origin != "" && origin != instanceID {
			return "L1-hit(originated-on-" + origin + ")"
		}
		return "L1-hit"
	}
	switch event.Source {
	case "leader-authoritative":
		if event.Hit {
			if origin != "" && origin != instanceID {
				return "leader-L2-hit(value-originated-on-" + origin + ")"
			}
			return "leader-L2-hit"
		}
		return "leader-L2-miss"
	case "follower-l2":
		if origin != "" && origin != instanceID {
			return "follower-L1-readthrough-hit(value-originated-on-" + origin + ")"
		}
		return "follower-L1-readthrough-hit"
	case "follower-remote":
		if event.Hit {
			if origin != "" && origin != instanceID {
				return "follower-fetched-from-L2(value-originated-on-" + origin + ")"
			}
			return "follower-fetched-from-L2"
		}
		return "follower-L2-miss"
	default:
		return "cached-unknown-path"
	}
}

func valueOrigin(value string) string {
	parts := strings.SplitN(value, " ", 2)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func latestEvent(events <-chan cluster.CacheEvent) cluster.CacheEvent {
	var event cluster.CacheEvent
	var lastHit cluster.CacheEvent
	haveHit := false
	for {
		select {
		case event = <-events:
			if event.Hit {
				lastHit = event
				haveHit = true
			}
		default:
			if haveHit {
				return lastHit
			}
			return event
		}
	}
}

func drainEvents(events <-chan cluster.CacheEvent) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}

func randomID(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(buf)
}
