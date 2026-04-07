// Package cluster provides a leader/follower implementation of
// cachecalc.ExternalCache backed by a single authoritative in-memory cache on
// the elected leader and a gRPC proxy on followers.
package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ModeLocal = "local"
	ModeK8s   = "k8s"
)

// Config describes how distributed cache instances find a leader, expose the
// leader gRPC cache endpoint, and optionally keep a small follower-side L2
// cache.
type Config struct {
	Mode                 string
	GRPCListenAddress    string
	LeaderLockAddress    string
	LeaderAddress        string
	ElectionRetryInterval time.Duration
	DialTimeout          time.Duration
	EnableL2             bool
	L2TTL                time.Duration
	Observer             func(CacheEvent)
}

// DefaultConfig returns a local-development oriented configuration that uses a
// TCP listener for election and a gRPC endpoint for follower proxy traffic.
func DefaultConfig() Config {
	return Config{
		Mode:                  ModeLocal,
		GRPCListenAddress:     ":50051",
		LeaderLockAddress:     ":9000",
		ElectionRetryInterval: 500 * time.Millisecond,
		DialTimeout:           time.Second,
		EnableL2:              true,
		L2TTL:                 2 * time.Second,
	}
}

// ConfigFromEnv builds Config from the supported environment variables and
// keeps local mode as the default when no explicit mode is provided.
func ConfigFromEnv() (Config, error) {
	cfg := DefaultConfig()
	if mode := strings.TrimSpace(os.Getenv("CLUSTER_MODE")); mode != "" {
		cfg.Mode = mode
	}
	if port := strings.TrimSpace(os.Getenv("GRPC_PORT")); port != "" {
		cfg.GRPCListenAddress = normalizePort(port)
	}
	if port := strings.TrimSpace(os.Getenv("LEADER_LOCK_PORT")); port != "" {
		cfg.LeaderLockAddress = normalizePort(port)
	}
	if leaderAddr := strings.TrimSpace(os.Getenv("LEADER_ADDR")); leaderAddr != "" {
		cfg.LeaderAddress = leaderAddr
	}
	if cfg.Mode != ModeLocal && cfg.Mode != ModeK8s {
		return Config{}, fmt.Errorf("unsupported cluster mode %q", cfg.Mode)
	}
	return cfg, nil
}

// normalizePort accepts either a bare port or a full host:port/listen address
// and returns the value in a form suitable for net.Listen.
func normalizePort(port string) string {
	if _, err := strconv.Atoi(port); err == nil {
		return ":" + port
	}
	return port
}
