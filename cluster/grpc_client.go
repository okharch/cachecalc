package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/okharch/cachecalc/cluster/cachepb"
	"github.com/okharch/cachecalc/valuestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type readThroughEntry struct {
	snapshot valuestore.EntrySnapshot
	deadline time.Time
}

type remoteValueStore struct {
	elector     LeaderElector
	dialTimeout time.Duration
	cacheTTL    time.Duration
	mu          sync.Mutex
	conn        *grpc.ClientConn
	client      cachepb.ClusterServiceClient
	cache       map[string]readThroughEntry
}

func newRemoteValueStore(elector LeaderElector, dialTimeout, cacheTTL time.Duration) *remoteValueStore {
	return &remoteValueStore{
		elector:     elector,
		dialTimeout: dialTimeout,
		cacheTTL:    cacheTTL,
		cache:       make(map[string]readThroughEntry),
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
	if s.conn != nil {
		return s.client, nil
	}
	addr := s.elector.LeaderAddress()
	if addr == "" {
		return nil, fmt.Errorf("leader address is unknown")
	}
	dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	s.conn = conn
	s.client = cachepb.NewClusterServiceClient(conn)
	return s.client, nil
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
	elector     LeaderElector
	dialTimeout time.Duration
	mu          sync.Mutex
	conn        *grpc.ClientConn
	client      cachepb.ClusterServiceClient
}

func newRemoteLockBackend(elector LeaderElector, dialTimeout time.Duration) *remoteLockBackend {
	return &remoteLockBackend{elector: elector, dialTimeout: dialTimeout}
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
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
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
