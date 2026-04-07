package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	cachecalc "github.com/okharch/cachecalc"
	"github.com/okharch/cachecalc/internal/cluster/cachepb"
	"google.golang.org/grpc"
)

type ttlReader interface {
	RemainingTTL(key string) (time.Duration, bool)
}

// cacheServiceServer adapts a local ExternalCache implementation to the gRPC
// CacheService API served by the elected leader.
type cacheServiceServer struct {
	cachepb.UnimplementedCacheServiceServer
	cache cachecalc.ExternalCache
}

// newCacheServiceServer binds the proto service handlers to the provided local
// ExternalCache implementation.
func newCacheServiceServer(cache cachecalc.ExternalCache) *cacheServiceServer {
	return &cacheServiceServer{cache: cache}
}

func (s *cacheServiceServer) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.Empty, error) {
	return &cachepb.Empty{}, s.cache.Set(ctx, req.GetKey(), req.GetValue(), time.Duration(req.GetTtlNanos()))
}

func (s *cacheServiceServer) SetNX(ctx context.Context, req *cachepb.SetNXRequest) (*cachepb.SetNXResponse, error) {
	created, err := s.cache.SetNX(ctx, req.GetKey(), req.GetValue(), time.Duration(req.GetTtlNanos()))
	if err != nil {
		return nil, err
	}
	return &cachepb.SetNXResponse{Created: created}, nil
}

func (s *cacheServiceServer) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	value, exists, err := s.cache.Get(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	resp := &cachepb.GetResponse{Value: value, Exists: exists}
	if !exists {
		return resp, nil
	}
	if reader, ok := s.cache.(ttlReader); ok {
		if ttl, ok := reader.RemainingTTL(req.GetKey()); ok {
			resp.TtlRemainingNanos = ttl.Nanoseconds()
		}
	}
	return resp, nil
}

func (s *cacheServiceServer) ExtendIfValue(ctx context.Context, req *cachepb.ExtendRequest) (*cachepb.BoolResponse, error) {
	ok, err := s.cache.ExtendIfValue(ctx, req.GetKey(), req.GetExpectedValue(), time.Duration(req.GetTtlNanos()))
	if err != nil {
		return nil, err
	}
	return &cachepb.BoolResponse{Ok: ok}, nil
}

func (s *cacheServiceServer) DelIfValue(ctx context.Context, req *cachepb.DelIfValueRequest) (*cachepb.BoolResponse, error) {
	ok, err := s.cache.DelIfValue(ctx, req.GetKey(), req.GetExpectedValue())
	if err != nil {
		return nil, err
	}
	return &cachepb.BoolResponse{Ok: ok}, nil
}

func (s *cacheServiceServer) SetIfLockOwned(ctx context.Context, req *cachepb.SetIfLockOwnedRequest) (*cachepb.BoolResponse, error) {
	ok, err := s.cache.SetIfLockOwned(ctx, req.GetLockKey(), req.GetExpectedLockValue(), req.GetKey(), req.GetValue(), time.Duration(req.GetTtlNanos()))
	if err != nil {
		return nil, err
	}
	return &cachepb.BoolResponse{Ok: ok}, nil
}

func (s *cacheServiceServer) Del(ctx context.Context, req *cachepb.DelRequest) (*cachepb.Empty, error) {
	return &cachepb.Empty{}, s.cache.Del(ctx, req.GetKey())
}

func (s *cacheServiceServer) Health(ctx context.Context, req *cachepb.Empty) (*cachepb.HealthResponse, error) {
	return &cachepb.HealthResponse{Leader: true}, nil
}

// grpcCacheServer owns the leader-only gRPC listener that exposes cache
// operations to followers. It is started on promotion and stopped on demotion.
type grpcCacheServer struct {
	mu      sync.Mutex
	addr    string
	server  *grpc.Server
	lis     net.Listener
	started bool
}

// newGRPCCacheServer constructs the leader transport bound to the configured
// listen address.
func newGRPCCacheServer(addr string) *grpcCacheServer {
	return &grpcCacheServer{addr: addr}
}

// Start begins serving the cache gRPC API for the leader if it is not already
// running.
func (s *grpcCacheServer) Start(cache cachecalc.ExternalCache) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", s.addr, err)
	}
	server := grpc.NewServer()
	cachepb.RegisterCacheServiceServer(server, newCacheServiceServer(cache))
	s.lis = lis
	s.server = server
	s.started = true
	go func() {
		_ = server.Serve(lis)
	}()
	return nil
}

// Stop shuts down the leader's gRPC server and releases its listener.
func (s *grpcCacheServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	s.server.Stop()
	_ = s.lis.Close()
	s.server = nil
	s.lis = nil
	s.started = false
}
