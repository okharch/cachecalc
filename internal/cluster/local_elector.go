package cluster

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LocalLeaderElector implements best-effort single-leader election using TCP
// listener ownership. The node that successfully binds the lock port becomes
// leader; other nodes stay followers and retry until the lock listener is
// released.
type LocalLeaderElector struct {
	lockAddr      string
	leaderAddr    string
	retryInterval time.Duration
	dialTimeout   time.Duration
	isLeader      atomic.Bool
	mu            sync.Mutex
	lockListener  net.Listener
}

// NewLocalLeaderElector creates the default local-mode elector that relies on
// OS-level TCP bind exclusivity instead of a separate coordination service.
func NewLocalLeaderElector(cfg Config) *LocalLeaderElector {
	return &LocalLeaderElector{
		lockAddr:      cfg.LeaderLockAddress,
		leaderAddr:    deriveLeaderAddress(cfg),
		retryInterval: cfg.ElectionRetryInterval,
		dialTimeout:   cfg.DialTimeout,
	}
}

// Start begins the promotion/demotion loop in the background.
func (e *LocalLeaderElector) Start(ctx context.Context, onStartLeading func(), onStopLeading func()) error {
	go e.run(ctx, onStartLeading, onStopLeading)
	return nil
}

func (e *LocalLeaderElector) IsLeader() bool {
	return e.isLeader.Load()
}

func (e *LocalLeaderElector) LeaderAddress() string {
	return e.leaderAddr
}

// run continuously tries to acquire the lock port, notifies on leadership
// changes, and falls back to follower retry behavior when another instance
// already owns the lock.
func (e *LocalLeaderElector) run(ctx context.Context, onStartLeading func(), onStopLeading func()) {
	for ctx.Err() == nil {
		lis, err := net.Listen("tcp", e.lockAddr)
		if err == nil {
			e.mu.Lock()
			e.lockListener = lis
			e.mu.Unlock()
			e.isLeader.Store(true)
			onStartLeading()
			e.holdLeadership(ctx, lis)
			e.isLeader.Store(false)
			onStopLeading()
			continue
		}
		e.waitForLeader(ctx)
	}
}

// holdLeadership accepts and immediately closes lock-port connections until
// leadership ends, keeping exclusive ownership of the election port.
func (e *LocalLeaderElector) holdLeadership(ctx context.Context, lis net.Listener) {
	defer func() {
		_ = lis.Close()
		e.mu.Lock()
		if e.lockListener == lis {
			e.lockListener = nil
		}
		e.mu.Unlock()
	}()

	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	for ctx.Err() == nil {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}
}

// waitForLeader sleeps between election attempts and optionally probes the
// leader endpoint to avoid needlessly tight retry loops.
func (e *LocalLeaderElector) waitForLeader(ctx context.Context) {
	timer := time.NewTimer(e.retryInterval)
	defer timer.Stop()

	if e.checkLeaderReachable() {
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		return
	}

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// checkLeaderReachable probes the configured leader gRPC address.
func (e *LocalLeaderElector) checkLeaderReachable() bool {
	dialer := net.Dialer{Timeout: e.dialTimeout}
	conn, err := dialer.Dial("tcp", e.leaderAddr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// deriveLeaderAddress computes the address followers should dial for gRPC when
// no explicit LEADER_ADDR override is provided.
func deriveLeaderAddress(cfg Config) string {
	if cfg.LeaderAddress != "" {
		return cfg.LeaderAddress
	}
	if strings.HasPrefix(cfg.GRPCListenAddress, ":") {
		return "127.0.0.1" + cfg.GRPCListenAddress
	}
	host, port, err := net.SplitHostPort(cfg.GRPCListenAddress)
	if err != nil {
		return cfg.GRPCListenAddress
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
