package memory_test

import (
	"testing"

	"github.com/okharch/cachecalc/distlock"
	"github.com/okharch/cachecalc/distlock/memory"
	"github.com/okharch/cachecalc/internal/contracttest"
)

func TestMemoryLockBackendContract(t *testing.T) {
	contracttest.RunLockBackendContract(t, func(t *testing.T) (distlock.Backend, func()) {
		return memory.NewBackend(), func() {}
	})
}
