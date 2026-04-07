package cluster

import "context"

// LeaderElector abstracts how a single instance is chosen to own the
// authoritative in-memory cache. Implementations notify the distributed cache
// when leadership starts and stops, and expose the address followers should use
// to reach the current leader's gRPC cache service.
type LeaderElector interface {
	Start(ctx context.Context, onStartLeading func(), onStopLeading func()) error
	IsLeader() bool
	LeaderAddress() string
}
