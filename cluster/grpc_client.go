package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/okharch/cachecalc/v4/cluster/cachepb"
	"github.com/okharch/cachecalc/v4/valuestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type readThroughEntry struct {
	snapshot valuestore.EntrySnapshot
	deadline time.Time
}

type remoteValueStore struct {
	elector            LeaderElector
	dialTimeout        time.Duration
	reconnectBaseDelay time.Duration
	cacheTTL           time.Duration
	mu                 sync.Mutex
	conn               *grpc.ClientConn
	connAddr           string
	client             cachepb.ClusterServiceClient
	cache              map[string]readThroughEntry
	warmUpSource       func(func(string, valuestore.EntrySnapshot) bool)
	isLeader           func() bool
	logger             *log.Logger
	ctx                context.Context
}

func newRemoteValueStore(elector LeaderElector, dialTimeout, reconnectBaseDelay, cacheTTL time.Duration) *remoteValueStore {
	return &remoteValueStore{
		elector:            elector,
		dialTimeout:        dialTimeout,
		reconnectBaseDelay: reconnectBaseDelay,
		cacheTTL:           cacheTTL,
		cache:              make(map[string]readThroughEntry),
	}
}

func (s *remoteValueStore) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	if entry, ok := s.getCached(key); ok {
		return entry, true, nil
	}
	client, err := s.ensureClient(ctx)
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	resp, err := client.GetValue(ctx, &cachepb.GetValueRequest{Key: key})
	if err != nil {
		return valuestore.EntrySnapshot{}, false, s.handleError(err)
	}
	if !resp.GetExists() {
		return valuestore.EntrySnapshot{}, false, nil
	}
	entry, err := valuestore.Unmarshal(resp.GetSnapshot())
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	s.setCached(key, entry)
	return cloneEntrySnapshot(entry), true, nil
}

func (s *remoteValueStore) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	client, err := s.ensureClient(ctx)
	if err != nil {
		return err
	}
	buf, err := valuestore.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = client.PutValue(ctx, &cachepb.PutValueRequest{Key: key, Snapshot: buf})
	s.invalidate(key)
	return s.handleError(err)
}

func (s *remoteValueStore) Delete(ctx context.Context, key string) error {
	client, err := s.ensureClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.DeleteValue(ctx, &cachepb.DeleteValueRequest{Key: key})
	s.invalidate(key)
	return s.handleError(err)
}

func (s *remoteValueStore) Healthy(ctx context.Context) error {
	client, err := s.ensureClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.Health(ctx, &cachepb.Empty{})
	return s.handleError(err)
}

func (s *remoteValueStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[string]readThroughEntry)
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	s.client = nil
	return err
}

func (s *remoteValueStore) ensureClient(ctx context.Context) (cachepb.ClusterServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	addr := s.elector.LeaderAddress()
	if addr == "" {
		return nil, fmt.Errorf("leader address is unknown")
	}
	if s.conn != nil && s.connAddr == addr {
		return s.client, nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
		s.client = nil
		s.connAddr = ""
		s.cache = make(map[string]readThroughEntry)
	}
	dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)
	defer cancel()
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	if s.reconnectBaseDelay > 0 {
		bc := backoff.DefaultConfig
		bc.BaseDelay = s.reconnectBaseDelay
		dialOpts = append(dialOpts, grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: bc,
		}))
	}
	conn, err := grpc.DialContext(dialCtx, addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	s.conn = conn
	s.connAddr = addr
	s.client = cachepb.NewClusterServiceClient(conn)
	if s.warmUpSource != nil {
		go s.doWarmUp(s.client)
		go s.watchReconnect(s.ctx, conn, s.client)
	}
	return s.client, nil
}

func (s *remoteValueStore) watchReconnect(ctx context.Context, conn *grpc.ClientConn, client cachepb.ClusterServiceClient) {
	for ctx.Err() == nil {
		state := conn.GetState()
		if state == connectivity.Idle {
			conn.Connect()
		}
		if !conn.WaitForStateChange(ctx, state) {
			return
		}
		s.mu.Lock()
		sameConn := s.conn == conn
		s.mu.Unlock()
		if !sameConn {
			return
		}
		newState := conn.GetState()
		if newState == connectivity.Ready && state != connectivity.Ready {
			go s.doWarmUp(client)
		}
	}
}

func (s *remoteValueStore) doWarmUp(client cachepb.ClusterServiceClient) {
	if s.isLeader != nil && s.isLeader() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.WarmUp(ctx)
	if err != nil {
		if s.logger != nil {
			s.logger.Printf("warm-up: failed to open stream: %v", err)
		}
		return
	}

	var sent int
	s.warmUpSource(func(key string, snap valuestore.EntrySnapshot) bool {
		buf, err := valuestore.Marshal(snap)
		if err != nil {
			return true
		}
		err = stream.Send(&cachepb.WarmUpEntry{
			Key:            key,
			Snapshot:       buf,
			CreatedAtNanos: snap.CreatedAt.UnixNano(),
		})
		if err == nil {
			sent++
		}
		return err == nil
	})

	summary, err := stream.CloseAndRecv()
	if err != nil {
		if s.logger != nil {
			s.logger.Printf("warm-up: sent %d entries, stream error: %v", sent, err)
		}
		return
	}
	if s.logger != nil {
		s.logger.Printf("warm-up: sent %d, accepted %d, rejected %d",
			sent, summary.GetAccepted(), summary.GetRejected())
	}
}

func (s *remoteValueStore) handleError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if ok && (st.Code() == codes.Unavailable || st.Code() == codes.Canceled) {
		_ = s.Close()
	}
	return err
}

func (s *remoteValueStore) getCached(key string) (valuestore.EntrySnapshot, bool) {
	if s.cacheTTL <= 0 {
		return valuestore.EntrySnapshot{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[key]
	if !ok || time.Now().After(entry.deadline) {
		delete(s.cache, key)
		return valuestore.EntrySnapshot{}, false
	}
	return cloneEntrySnapshot(entry.snapshot), true
}

func (s *remoteValueStore) setCached(key string, entry valuestore.EntrySnapshot) {
	if s.cacheTTL <= 0 {
		return
	}
	ttl := time.Until(entry.ExpireAt)
	if ttl <= 0 {
		return
	}
	if ttl > s.cacheTTL {
		ttl = s.cacheTTL
	}
	s.mu.Lock()
	s.cache[key] = readThroughEntry{snapshot: cloneEntrySnapshot(entry), deadline: time.Now().Add(ttl)}
	s.mu.Unlock()
}

func (s *remoteValueStore) invalidate(key string) {
	s.mu.Lock()
	delete(s.cache, key)
	s.mu.Unlock()
}

func cloneEntrySnapshot(entry valuestore.EntrySnapshot) valuestore.EntrySnapshot {
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}

type remoteLockBackend struct {
	elector            LeaderElector
	dialTimeout        time.Duration
	reconnectBaseDelay time.Duration
	mu                 sync.Mutex
	conn               *grpc.ClientConn
	client             cachepb.ClusterServiceClient
}

func newRemoteLockBackend(elector LeaderElector, dialTimeout, reconnectBaseDelay time.Duration) *remoteLockBackend {
	return &remoteLockBackend{elector: elector, dialTimeout: dialTimeout, reconnectBaseDelay: reconnectBaseDelay}
}

func (b *remoteLockBackend) TryAcquire(ctx context.Context, key string, token []byte, ttl time.Duration) (bool, error) {
	client, err := b.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.TryAcquire(ctx, &cachepb.TryAcquireRequest{Key: key, Token: append([]byte(nil), token...), TtlNanos: ttl.Nanoseconds()})
	if err != nil {
		return false, b.handleError(err)
	}
	return resp.GetOk(), nil
}

func (b *remoteLockBackend) Renew(ctx context.Context, key string, token []byte, ttl time.Duration) (bool, error) {
	client, err := b.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.Renew(ctx, &cachepb.RenewRequest{Key: key, Token: append([]byte(nil), token...), TtlNanos: ttl.Nanoseconds()})
	if err != nil {
		return false, b.handleError(err)
	}
	return resp.GetOk(), nil
}

func (b *remoteLockBackend) Release(ctx context.Context, key string, token []byte) (bool, error) {
	client, err := b.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.Release(ctx, &cachepb.ReleaseRequest{Key: key, Token: append([]byte(nil), token...)})
	if err != nil {
		return false, b.handleError(err)
	}
	return resp.GetOk(), nil
}

func (b *remoteLockBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil {
		return nil
	}
	err := b.conn.Close()
	b.conn = nil
	b.client = nil
	return err
}

func (b *remoteLockBackend) ensureClient(ctx context.Context) (cachepb.ClusterServiceClient, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		return b.client, nil
	}
	addr := b.elector.LeaderAddress()
	if addr == "" {
		return nil, fmt.Errorf("leader address is unknown")
	}
	dialCtx, cancel := context.WithTimeout(ctx, b.dialTimeout)
	defer cancel()
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	if b.reconnectBaseDelay > 0 {
		bc := backoff.DefaultConfig
		bc.BaseDelay = b.reconnectBaseDelay
		dialOpts = append(dialOpts, grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: bc,
		}))
	}
	conn, err := grpc.DialContext(dialCtx, addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	b.conn = conn
	b.client = cachepb.NewClusterServiceClient(conn)
	return b.client, nil
}

func (b *remoteLockBackend) handleError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if ok && (st.Code() == codes.Unavailable || st.Code() == codes.Canceled) {
		_ = b.Close()
	}
	return err
}
