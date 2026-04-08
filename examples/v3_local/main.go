package main

import (
	"context"
	"fmt"
	"time"

	lockmem "github.com/okharch/cachecalc/distlock/memory"
	"github.com/okharch/cachecalc/smartcache"
	vmemory "github.com/okharch/cachecalc/valuestore/memory"
)

func main() {
	values := vmemory.New()
	locks := lockmem.NewProvider()

	cache := smartcache.New(smartcache.Config{
		MaxWorkers: 4,
		Locks:      locks,
		Values:     values,
	})
	defer cache.Close()

	value, err := smartcache.GetWithTTL(context.Background(), cache, "commodities:v1", 2*time.Second, 10*time.Second, func(ctx context.Context) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "calculated at " + time.Now().Format(time.RFC3339Nano), nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(value)
}
