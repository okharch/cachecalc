package cluster

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LocalLeaderElector struct {
	lockAddr      string
	leaderAddr    string
	retryInterval time.Duration
	dialTimeout   time.Duration
	isLeader      atomic.Bool
	mu            sync.Mutex
	lockListener  net.Listener
}

func NewLocalLeaderElector(cfg Config) *LocalLeaderElector {
	return &LocalLeaderElector{
		lockAddr:      cfg.LeaderLockAddress,
		leaderAddr:    deriveLeaderAddress(cfg),
		retryInterval: cfg.ElectionRetryInterval,
		dialTimeout:   cfg.DialTimeout,
	}
}

func (e *LocalLeaderElector) Start(ctx context.Context, onStartLeading func(), onStopLeading func()) error {
	go e.run(ctx, onStartLeading, onStopLeading)
	return nil
}

func (e *LocalLeaderElector) IsLeader() bool        { return e.isLeader.Load() }
func (e *LocalLeaderElector) LeaderAddress() string { return e.leaderAddr }

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

func (e *LocalLeaderElector) checkLeaderReachable() bool {
	dialer := net.Dialer{Timeout: e.dialTimeout}
	conn, err := dialer.Dial("tcp", e.leaderAddr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

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
