package lock

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/spindle"
)

type SpindleLockOptions struct {
	Client   *spanner.Client // Spanner client
	Table    string          // Spanner table name
	Name     string          // lock name
	Id       string          // optional, generated if empty
	Duration int64           // optional, will use spindle's default
	Logger   *log.Logger     // pass to spindle
}

func NewSpindleLock(opts *SpindleLockOptions) *SpindleLock {
	if opts == nil {
		return nil
	}

	s := &SpindleLock{opts: opts}
	sopts := []spindle.Option{}
	if opts.Id != "" {
		sopts = append(sopts, spindle.WithId(opts.Id))
	}

	if opts.Duration != 0 {
		sopts = append(sopts, spindle.WithDuration(opts.Duration))
	}

	if opts.Logger != nil {
		sopts = append(sopts, spindle.WithLogger(opts.Logger))
	}

	s.lock = spindle.New(opts.Client, opts.Table, opts.Name, sopts...)
	return s
}

type SpindleLock struct {
	opts   *SpindleLockOptions
	lock   *spindle.Lock
	quit   context.Context
	cancel context.CancelFunc
	locked int32
	done   chan error
}

func (l *SpindleLock) Lock(ctx context.Context) error {
	if atomic.LoadInt32(&l.locked) == 1 {
		// Lock only once for this instance.
		return nil
	}

	l.quit, l.cancel = context.WithCancel(ctx)
	l.done = make(chan error, 1)
	l.lock.Run(l.quit, l.done)

	// Block until we get the lock.
	for {
		hl, _ := l.lock.HasLock()
		if l.lock.Iterations() > 1 && hl {
			break
		}

		time.Sleep(time.Millisecond * 500)
	}

	atomic.StoreInt32(&l.locked, 1)
	return nil
}

func (l *SpindleLock) Unlock() error {
	if atomic.LoadInt32(&l.locked) != 1 {
		return nil
	}

	l.cancel() // terminate lock loop
	<-l.done   // and wait
	atomic.StoreInt32(&l.locked, 0)
	return nil
}
