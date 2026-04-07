package memory_test

import (
	"testing"

	"github.com/okharch/cachecalc/internal/contracttest"
	"github.com/okharch/cachecalc/valuestore"
	"github.com/okharch/cachecalc/valuestore/memory"
)

func TestMemoryValueStoreContract(t *testing.T) {
	contracttest.RunValueStoreContract(t, func(t *testing.T) (valuestore.Store, func()) {
		return memory.New(), func() {}
	})
}
