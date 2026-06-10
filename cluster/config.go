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

// Config describes cluster election and gRPC transport settings.
type Config struct {
	Name                  string
	Mode                  string
	GRPCListenAddress     string
	LeaderLockAddress     string
	LeaderAddress         string
	ElectionRetryInterval time.Duration
	DialTimeout           time.Duration
	ReadThroughTTL        time.Duration
	ReconnectBaseDelay   time.Duration
}

func DefaultConfig() Config {
	return Config{
		Mode:                  ModeLocal,
		GRPCListenAddress:     ":50051",
		LeaderLockAddress:     ":9000",
		ElectionRetryInterval: 500 * time.Millisecond,
		DialTimeout:           time.Second,
		ReadThroughTTL:        2 * time.Second,
		ReconnectBaseDelay:   time.Second,
	}
}

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
	if addr := strings.TrimSpace(os.Getenv("LEADER_ADDR")); addr != "" {
		cfg.LeaderAddress = addr
	}
	if cfg.Mode != ModeLocal && cfg.Mode != ModeK8s {
		return Config{}, fmt.Errorf("unsupported cluster mode %q", cfg.Mode)
	}
	return cfg, nil
}

func normalizePort(value string) string {
	if _, err := strconv.Atoi(value); err == nil {
		return ":" + value
	}
	return value
}
