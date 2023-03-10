package cachecalc

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const tick = time.Millisecond * 300
const refresh = tick * 2
const expire = tick * 5
const key = "key"

var counter int
var mu sync.Mutex

func initCalcTest(t *testing.T, wg *sync.WaitGroup, cc *CachedCalculations) func(result *int, thread int) {
	logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	ctx := context.TODO()
	counter = 0
	logger.Printf("init calculations")
	DefaultCCs = NewCachedCalculations(4, nil)
	getI := func(ctx context.Context) (int, error) {
		time.Sleep(tick)
		mu.Lock()
		counter++
		mu.Unlock()
		return counter, nil
	}
	GetI := func(result *int, thread int) {
		defer wg.Done()
		ctx := context.WithValue(ctx, "thread", thread)
		r, err := GetCachedCalcX(cc, ctx, "key", refresh, expire, true, getI)
		require.NoError(t, err)
		*result = r
	}
	return GetI
}

func TestLocalSimple(t *testing.T) {
	const nThreads = 5
	var wg sync.WaitGroup
	GetI := initCalcTest(t, &wg, DefaultCCs)
	wg.Add(nThreads)
	dest := make([]int, nThreads)
	for i := 0; i < nThreads; i++ {
		go func(i int) {
			GetI(&dest[i], i+1)
		}(i)
	}
	wg.Wait()
	DefaultCCs.Wait()
	for i := 0; i < nThreads; i++ {
		require.Equal(t, 1, dest[i])
	}
}

func TestLocal(t *testing.T) {
	var d1, d2, d3 int
	var wg sync.WaitGroup
	GetI := initCalcTest(t, &wg, DefaultCCs)
	wg.Add(4)
	GetI(&d1, 1)
	require.Equal(t, 1, d1)
	time.Sleep(refresh)
	GetI(&d2, 2) // will return the same but will trigger calculation
	require.Equal(t, 1, d2)
	GetI(&d3, 3)
	require.Equal(t, 1, d3)
	time.Sleep(tick) // its enough time to refresh value
	GetI(&d3, 3)
	require.Equal(t, 2, d3)
	wg.Wait()
	DefaultCCs.Wait()
}
