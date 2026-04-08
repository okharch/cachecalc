package smartcache

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestForegroundCalculationDoesNotLeakGoroutines(t *testing.T) {
	cache := New(Config{MaxWorkers: 1})
	defer cache.Close()

	baseline := runtime.NumGoroutine()

	for i := 0; i < 200; i++ {
		_, err := GetWithTTL(context.Background(), cache, uniqueLeakKey(i), 20*time.Millisecond, 40*time.Millisecond, func(ctx context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > baseline+20 {
		t.Fatalf("goroutine count grew from %d to %d; foreground calc contexts appear to leak", baseline, after)
	}
}

func uniqueLeakKey(i int) string {
	return "foreground-goroutine-leak-" + time.Now().Add(time.Duration(i)*time.Nanosecond).Format(time.RFC3339Nano)
}
