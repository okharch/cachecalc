package cluster

import "context"

// LeaderElector chooses the active leader and advertises the leader gRPC
// address to followers.
type LeaderElector interface {
	Start(ctx context.Context, onStartLeading func(), onStopLeading func()) error
	IsLeader() bool
	LeaderAddress() string
}
