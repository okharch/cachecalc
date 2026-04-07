package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	cachecalc "github.com/okharch/cachecalc"
	"github.com/okharch/cachecalc/internal/cluster/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type l2Entry struct {
	value    []byte
	deadline time.Time
}

// grpcExternalCacheClient implements cachecalc.ExternalCache for followers by
// forwarding all operations to the leader's gRPC cache service. Reads may be
// satisfied from an optional follower-side L2 cache when the entry is still
// fresh.
type grpcExternalCacheClient struct {
	mu          sync.Mutex
	elector     LeaderElector
	dialTimeout time.Duration
	enableL2    bool
	l2TTL       time.Duration
	observer    func(CacheEvent)
	conn        *grpc.ClientConn
	client      cachepb.CacheServiceClient
	l2          map[string]l2Entry
}

// newGRPCExternalCacheClient creates the follower-side ExternalCache proxy.
func newGRPCExternalCacheClient(elector LeaderElector, dialTimeout time.Duration, enableL2 bool, l2TTL time.Duration, observer func(CacheEvent)) *grpcExternalCacheClient {
	return &grpcExternalCacheClient{
		elector:     elector,
		dialTimeout: dialTimeout,
		enableL2:    enableL2,
		l2TTL:       l2TTL,
		observer:    observer,
		l2:          make(map[string]l2Entry),
	}
}

func (c *grpcExternalCacheClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	client, err := c.ensureClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.Set(ctx, &cachepb.SetRequest{Key: key, Value: cloneBytes(value), TtlNanos: ttl.Nanoseconds()})
	c.invalidateL2(key)
	return c.handleRPCError(err)
}

func (c *grpcExternalCacheClient) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	client, err := c.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.SetNX(ctx, &cachepb.SetNXRequest{Key: key, Value: cloneBytes(value), TtlNanos: ttl.Nanoseconds()})
	if err == nil && resp.GetCreated() {
		c.invalidateL2(key)
	}
	return resp.GetCreated(), c.handleRPCError(err)
}

func (c *grpcExternalCacheClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if value, ok := c.getL2(key); ok {
		c.emit(CacheEvent{Operation: "get", Role: "follower", Source: "follower-l2", Key: key, Hit: true})
		return value, true, nil
	}
	client, err := c.ensureClient(ctx)
	if err != nil {
		return nil, false, err
	}
	resp, err := client.Get(ctx, &cachepb.GetRequest{Key: key})
	if err != nil {
		return nil, false, c.handleRPCError(err)
	}
	if resp.GetExists() {
		c.setL2(key, resp.GetValue(), time.Duration(resp.GetTtlRemainingNanos()))
	}
	c.emit(CacheEvent{Operation: "get", Role: "follower", Source: "follower-remote", Key: key, Hit: resp.GetExists()})
	return cloneBytes(resp.GetValue()), resp.GetExists(), nil
}

func (c *grpcExternalCacheClient) ExtendIfValue(ctx context.Context, key string, expectedValue []byte, ttl time.Duration) (bool, error) {
	client, err := c.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.ExtendIfValue(ctx, &cachepb.ExtendRequest{
		Key:           key,
		ExpectedValue: cloneBytes(expectedValue),
		TtlNanos:      ttl.Nanoseconds(),
	})
	c.invalidateL2(key)
	if err != nil {
		return false, c.handleRPCError(err)
	}
	return resp.GetOk(), nil
}

func (c *grpcExternalCacheClient) DelIfValue(ctx context.Context, key string, expectedValue []byte) (bool, error) {
	client, err := c.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.DelIfValue(ctx, &cachepb.DelIfValueRequest{
		Key:           key,
		ExpectedValue: cloneBytes(expectedValue),
	})
	c.invalidateL2(key)
	if err != nil {
		return false, c.handleRPCError(err)
	}
	return resp.GetOk(), nil
}

func (c *grpcExternalCacheClient) SetIfLockOwned(ctx context.Context, lockKey string, expectedLockValue []byte, key string, value []byte, ttl time.Duration) (bool, error) {
	client, err := c.ensureClient(ctx)
	if err != nil {
		return false, err
	}
	resp, err := client.SetIfLockOwned(ctx, &cachepb.SetIfLockOwnedRequest{
		LockKey:           lockKey,
		ExpectedLockValue: cloneBytes(expectedLockValue),
		Key:               key,
		Value:             cloneBytes(value),
		TtlNanos:          ttl.Nanoseconds(),
	})
	c.invalidateL2(key)
	if err != nil {
		return false, c.handleRPCError(err)
	}
	return resp.GetOk(), nil
}

func (c *grpcExternalCacheClient) Del(ctx context.Context, key string) error {
	client, err := c.ensureClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.Del(ctx, &cachepb.DelRequest{Key: key})
	c.invalidateL2(key)
	return c.handleRPCError(err)
}

func (c *grpcExternalCacheClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.l2 = make(map[string]l2Entry)
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.client = nil
	return err
}

// ensureClient resolves and dials the current leader address lazily, allowing
// followers to reconnect after leadership changes.
func (c *grpcExternalCacheClient) ensureClient(ctx context.Context) (cachepb.CacheServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	leaderAddr := c.elector.LeaderAddress()
	if leaderAddr == "" {
		return nil, fmt.Errorf("leader address is unknown")
	}
	if c.conn != nil {
		return c.client, nil
	}
	dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	c.conn = conn
	c.client = cachepb.NewCacheServiceClient(conn)
	return c.client, nil
}

// handleRPCError resets the active connection on transport-level failures so
// the next request will redial the current leader.
func (c *grpcExternalCacheClient) handleRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if ok && (st.Code() == codes.Unavailable || st.Code() == codes.Canceled) {
		_ = c.Close()
	}
	return err
}

func (c *grpcExternalCacheClient) getL2(key string) ([]byte, bool) {
	if !c.enableL2 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.l2[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.deadline) {
		delete(c.l2, key)
		return nil, false
	}
	return cloneBytes(entry.value), true
}

func (c *grpcExternalCacheClient) setL2(key string, value []byte, ttl time.Duration) {
	if !c.enableL2 || ttl <= 0 {
		return
	}
	if c.l2TTL > 0 && ttl > c.l2TTL {
		ttl = c.l2TTL
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.l2[key] = l2Entry{value: cloneBytes(value), deadline: time.Now().Add(ttl)}
}

func (c *grpcExternalCacheClient) invalidateL2(key string) {
	if !c.enableL2 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.l2, key)
}

func (c *grpcExternalCacheClient) emit(event CacheEvent) {
	if c.observer != nil {
		c.observer(event)
	}
}

var _ cachecalc.ExternalCache = (*grpcExternalCacheClient)(nil)
