package cluster

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/okharch/cachecalc/v4/cluster/cachepb"
	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/valuestore"
	"google.golang.org/grpc"
)

type grpcServer struct {
	addr   string
	mu     sync.Mutex
	server *grpc.Server
	lis    net.Listener
}

func newGRPCServer(addr string) *grpcServer {
	return &grpcServer{addr: addr}
}

func (s *grpcServer) Start(values valuestore.Store, locks distlock.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return nil
	}
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	cachepb.RegisterClusterServiceServer(server, &rpcServer{values: values, locks: locks})
	s.server = server
	s.lis = lis
	go server.Serve(lis)
	return nil
}

func (s *grpcServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server == nil {
		return
	}
	s.server.Stop()
	_ = s.lis.Close()
	s.server = nil
	s.lis = nil
}

type rpcServer struct {
	cachepb.UnimplementedClusterServiceServer
	values valuestore.Store
	locks  distlock.Backend
}

func (s *rpcServer) GetValue(ctx context.Context, req *cachepb.GetValueRequest) (*cachepb.GetValueResponse, error) {
	entry, ok, err := s.values.Get(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	if !ok {
		return &cachepb.GetValueResponse{}, nil
	}
	buf, err := valuestore.Marshal(entry)
	if err != nil {
		return nil, err
	}
	return &cachepb.GetValueResponse{Exists: true, Snapshot: buf}, nil
}

func (s *rpcServer) PutValue(ctx context.Context, req *cachepb.PutValueRequest) (*cachepb.Empty, error) {
	entry, err := valuestore.Unmarshal(req.GetSnapshot())
	if err != nil {
		return nil, err
	}
	return &cachepb.Empty{}, s.values.Put(ctx, req.GetKey(), entry)
}

func (s *rpcServer) DeleteValue(ctx context.Context, req *cachepb.DeleteValueRequest) (*cachepb.Empty, error) {
	return &cachepb.Empty{}, s.values.Delete(ctx, req.GetKey())
}

func (s *rpcServer) TryAcquire(ctx context.Context, req *cachepb.TryAcquireRequest) (*cachepb.BoolResponse, error) {
	ok, err := s.locks.TryAcquire(ctx, req.GetKey(), req.GetToken(), time.Duration(req.GetTtlNanos()))
	if err != nil {
		return nil, err
	}
	return &cachepb.BoolResponse{Ok: ok}, nil
}

func (s *rpcServer) Renew(ctx context.Context, req *cachepb.RenewRequest) (*cachepb.BoolResponse, error) {
	ok, err := s.locks.Renew(ctx, req.GetKey(), req.GetToken(), time.Duration(req.GetTtlNanos()))
	if err != nil {
		return nil, err
	}
	return &cachepb.BoolResponse{Ok: ok}, nil
}

func (s *rpcServer) Release(ctx context.Context, req *cachepb.ReleaseRequest) (*cachepb.BoolResponse, error) {
	ok, err := s.locks.Release(ctx, req.GetKey(), req.GetToken())
	if err != nil {
		return nil, err
	}
	return &cachepb.BoolResponse{Ok: ok}, nil
}

func (s *rpcServer) Health(context.Context, *cachepb.Empty) (*cachepb.HealthResponse, error) {
	return &cachepb.HealthResponse{Leader: true}, nil
}

func (s *rpcServer) WarmUp(stream grpc.ClientStreamingServer[cachepb.WarmUpEntry, cachepb.WarmUpSummary]) error {
	var accepted, rejected, total int32
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&cachepb.WarmUpSummary{
				Accepted: accepted, Rejected: rejected, Total: total,
			})
		}
		if err != nil {
			return err
		}
		total++

		snap, err := valuestore.Unmarshal(entry.GetSnapshot())
		if err != nil {
			rejected++
			continue
		}

		existing, ok, _ := s.values.Get(stream.Context(), entry.GetKey())
		if ok && !existing.CreatedAt.Before(snap.CreatedAt) {
			rejected++
			continue
		}

		if err := s.values.Put(stream.Context(), entry.GetKey(), snap); err != nil {
			rejected++
			continue
		}
		accepted++
	}
}
