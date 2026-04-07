package distlock

import (
	"context"
	"sync"
	"time"
)

type managedLease struct {
	backend Backend
	key     string
	token   []byte
	ttl     time.Duration

	lost chan struct{}
	stop chan struct{}
	once sync.Once
}

func newManagedLease(ctx context.Context, backend Backend, key string, token []byte, ttl time.Duration) *managedLease {
	lease := &managedLease{
		backend: backend,
		key:     key,
		token:   append([]byte(nil), token...),
		ttl:     ttl,
		lost:    make(chan struct{}),
		stop:    make(chan struct{}),
	}
	go lease.renewLoop(ctx)
	return lease
}

func (l *managedLease) Lost() <-chan struct{} {
	return l.lost
}

func (l *managedLease) Release(ctx context.Context) error {
	l.once.Do(func() {
		close(l.stop)
	})
	_, err := l.backend.Release(ctx, l.key, l.token)
	l.markLost()
	return err
}

func (l *managedLease) renewLoop(ctx context.Context) {
	ticker := time.NewTicker(renewEvery(l.ttl))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_, _ = l.backend.Release(context.Background(), l.key, l.token)
			l.markLost()
			return
		case <-l.stop:
			return
		case <-ticker.C:
			ok, err := l.backend.Renew(ctx, l.key, l.token, l.ttl)
			if err != nil || !ok {
				l.markLost()
				return
			}
		}
	}
}

func (l *managedLease) markLost() {
	l.once.Do(func() {
		close(l.stop)
	})
	select {
	case <-l.lost:
	default:
		close(l.lost)
	}
}

func renewEvery(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 10 * time.Millisecond
	}
	interval := ttl / 2
	if interval <= 0 {
		return 10 * time.Millisecond
	}
	return interval
}
