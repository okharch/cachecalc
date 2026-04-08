package memory_test

import (
	"testing"

	"github.com/okharch/cachecalc/v4/internal/contracttest"
	"github.com/okharch/cachecalc/v4/valuestore"
	"github.com/okharch/cachecalc/v4/valuestore/memory"
)

func TestMemoryValueStoreContract(t *testing.T) {
	contracttest.RunValueStoreContract(t, func(t *testing.T) (valuestore.Store, func()) {
		return memory.New(), func() {}
	})
}
