package main

import (
	"context"
	"fmt"
	"time"

	clustercfg "github.com/okharch/cachecalc/cluster"
	providerscluster "github.com/okharch/cachecalc/providers/cluster"
	"github.com/okharch/cachecalc/smartcache"
)

func main() {
	ctx := context.Background()

	cfg, err := clustercfg.ConfigFromEnv()
	if err != nil {
		panic(err)
	}

	cache := smartcache.New(smartcache.Config{MaxWorkers: 4})
	service, err := providerscluster.Bind(ctx, cache, cfg)
	if err != nil {
		panic(err)
	}
	defer service.Close()
	defer cache.Close()

	value, err := smartcache.GetWithTTL(ctx, cache, "current-time", 2*time.Second, 10*time.Second, true, func(ctx context.Context) (string, error) {
		time.Sleep(500 * time.Millisecond)
		return fmt.Sprintf("leader=%v calculated_at=%s", service.IsLeader(), time.Now().Format(time.RFC3339Nano)), nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("value=%s leader=%v leader_addr=%s\n", value, service.IsLeader(), service.LeaderAddress())
}
