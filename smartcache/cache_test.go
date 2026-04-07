package smartcache_test

import (
	"testing"

	lockmem "github.com/okharch/cachecalc/distlock/memory"
	"github.com/okharch/cachecalc/internal/contracttest"
	"github.com/okharch/cachecalc/smartcache"
	vmemory "github.com/okharch/cachecalc/valuestore/memory"
)

func TestMemorySmartcacheContract(t *testing.T) {
	contracttest.RunSmartcacheContract(t, func(t *testing.T) (a, b *smartcache.Cache, cleanup func()) {
		values := vmemory.New()
		locks := lockmem.NewProvider()
		cacheA := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: locks, Values: values})
		cacheB := smartcache.New(smartcache.Config{MaxWorkers: 2, Locks: locks, Values: values})
		return cacheA, cacheB, func() {
			cacheA.Close()
			cacheB.Close()
		}
	})
}
