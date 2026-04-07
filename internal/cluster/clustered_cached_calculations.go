package cluster

import (
	"context"

	cachecalc "github.com/okharch/cachecalc"
)

// NewClusteredCachedCalculations wires a CachedCalculations instance to a
// DistributedExternalCache whose leader-side L2 is backed directly by that
// CachedCalculations instance. This avoids duplicating leader value storage and
// lets a promoted instance expose its warm local entries to followers.
func NewClusteredCachedCalculations(ctx context.Context, maxWorkers int, cfg Config) (*cachecalc.CachedCalculations, *DistributedExternalCache, error) {
	cc := cachecalc.NewCachedCalculations(maxWorkers, nil)
	local := cachecalc.NewCachedCalculationsExternalAdapter(cc)
	dist, err := NewDistributedExternalCache(ctx, cfg, local)
	if err != nil {
		cc.Close()
		return nil, nil, err
	}
	cc.SetExternalCache(dist)
	return cc, dist, nil
}
