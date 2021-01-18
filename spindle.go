package spindle

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

type Option interface {
	Apply(*Lock)
}

type withId string

func (w withId) Apply(o *Lock) { o.id = string(w) }

// WithId sets this instance's unique id.
func WithId(v string) Option { return withId(v) }

type withDuration int64

func (w withDuration) Apply(o *Lock) { o.duration = int64(w) }

// WithDuration sets the locker's lease duration.
func WithDuration(v int64) Option { return withDuration(v) }

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *Lock) { o.logger = w.l }

// WithLogger sets the locker's logger object.
func WithLogger(v *log.Logger) Option { return withLogger{v} }

type Lock struct {
	db       *spanner.Client
	table    string // table name
	name     string // lock name
	id       string // unique id for this instance
	duration int64  // lock duration in ms
	leader   int32  // 1 = leader, 0 = !leader
	iter     int64
	token    *time.Time
	mtx      *sync.Mutex
	logger   *log.Logger
}

// Run starts the main lock loop which can be canceled using the input context. You can provide
// an optional 'done' channel if you want to be notified when the loop is done.
func (l *Lock) Run(ctx context.Context, done ...chan error) error {
	// Two heartbeats per duration. This will prevent unnecessary lock changes just because of
	// delayed heartbeat updates, which would cause some diffs to be slightly beyond 'duration'.
	ticker1 := time.NewTicker(time.Millisecond * time.Duration(l.duration/2))
	ticker2 := time.NewTicker(time.Millisecond * time.Duration(l.duration))
	quit := context.WithValue(ctx, struct{}{}, nil)

	go func() {
		initial := true
		var active1 int32
		var active2 int32

		locked := func() bool {
			// See if there is an active leased lock (could be us, could be somebody else).
			tokenlocked, diff, err := l.checkLock()
			if err != nil {
				l.logger.Println(err)
				return true // err on safer side
			}

			if l.tokenString() != "" && l.tokenString() == tokenlocked {
				l.logger.Println("leader active (me)")
				atomic.StoreInt32(&l.leader, 1)
				l.heartbeat()
				return true
			}

			if diff > 0 && diff < l.duration {
				l.logger.Println("leader active (not me)")
				atomic.StoreInt32(&l.leader, 0)
				return true
			}

			// Lock available.
			return false
		}

		for {
			select {
			case <-ticker1.C: // middle heartbeat
				if atomic.LoadInt32(&active1) == 1 {
					continue
				}

				go func() {
					defer func(begin time.Time) {
						l.logger.Printf("[1/2] duration=%v", time.Since(begin))
						atomic.StoreInt32(&active1, 0)
					}(time.Now())

					atomic.StoreInt32(&active1, 1)
					locked()
				}()
			case <-ticker2.C: // duration heartbeat
				if atomic.LoadInt32(&active2) == 1 {
					continue
				}

				go func() {
					defer func(begin time.Time) {
						l.logger.Printf("[2/2] duration=%v, iter=%v", time.Since(begin), l.Iterations())
						atomic.StoreInt32(&active2, 0)
						atomic.AddInt64(&l.iter, 1)
					}(time.Now())

					atomic.StoreInt32(&active2, 1)
					if yes := locked(); yes {
						return
					}

					if initial {
						// Attempt first ever lock; use insert instead of update. Only one client should be able to do this.
						// The return commit timestamp will be our fencing token.
						cts, err := l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
							ts := "PENDING_COMMIT_TIMESTAMP()"
							stmt := spanner.Statement{
								SQL: fmt.Sprintf("insert %v (name, heartbeat, token) values ('%s', %v, %v)", l.table, l.name, ts, ts),
							}

							n, err := txn.Update(ctx, stmt)
							l.logger.Println("insert:", err, n)
							return err
						})

						if err == nil {
							l.setToken(&cts)
							l.logger.Printf("leadertoken: %v", l.tokenString())
							atomic.StoreInt32(&l.leader, 1)
							return
						}

						l.logger.Printf("leader attempt failed: %v", err)
						initial = false
					}

					if !initial {
						// Read the current token and use it as suffix for the next lock name.
						token, err := l.getTokenFromDb()
						if err != nil {
							l.logger.Println("getTokenFromDb failed:", err)
							return
						}

						if token == "" {
							l.logger.Printf("read token failed: %v", err)
							return
						}

						// Attempt to grab the next lock.
						nxtcts, err := l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
							nxtname := fmt.Sprintf("%v_%v", l.name, token)
							stmt := spanner.Statement{
								SQL: fmt.Sprintf("insert %v (name) values ('%s')", l.table, nxtname),
							}

							_, err := txn.Update(ctx, stmt)
							l.logger.Println("insert:", err, nxtname)
							return err
						})

						if err == nil {
							// We got the lock. Attempt to update the current token to this commit timestamp.
							_, err := l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
								ts := "PENDING_COMMIT_TIMESTAMP()"
								stmt := spanner.Statement{
									SQL:    fmt.Sprintf("update %v set heartbeat = %v, token = @token where name = @name", l.table, ts),
									Params: map[string]interface{}{"name": l.name, "token": nxtcts},
								}

								_, err := txn.Update(ctx, stmt)
								return err
							})

							if err != nil {
								l.logger.Println("update token failed:", err)
								return
							}

							atomic.StoreInt32(&l.leader, 1)
							l.setToken(&nxtcts)
						}
					}
				}()
			case <-quit.Done():
				if len(done) > 0 {
					done[0] <- nil
				}

				atomic.StoreInt32(&l.leader, 0)
				return
			}
		}
	}()

	return nil
}

// HasLock returns true if this instance got the lock, together with the lock token.
func (l *Lock) HasLock() (bool, string) {
	token, err := l.getTokenFromDb()
	if err != nil {
		return false, token
	}

	if token == l.tokenString() {
		return true, token
	}

	return false, token
}

// Duration returns the duration in main loop in milliseconds.
func (l *Lock) Duration() int64 { return l.duration }

// Iterations returns the number of iterations done by the main loop.
func (l *Lock) Iterations() int64 { return atomic.LoadInt64(&l.iter) }

func (l *Lock) tokenString() string {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if l.token == nil {
		return ""
	}

	return (*l.token).UTC().Format(time.RFC3339Nano)
}

func (l *Lock) setToken(v *time.Time) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.token = v
}

func (l *Lock) checkLock() (string, int64, error) {
	var tokenlocked string
	var diff int64

	err := func() error {
		stmt := spanner.Statement{
			SQL:    fmt.Sprintf("select timestamp_diff(current_timestamp(), heartbeat, millisecond), token from %v where name = @name", l.table),
			Params: map[string]interface{}{"name": l.name},
		}

		var reterr error
		iter := l.db.Single().Query(context.Background(), stmt)
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}

			if err != nil {
				reterr = err
				break
			}

			var v spanner.NullInt64
			var t spanner.NullTime
			err = row.Columns(&v, &t)
			if err != nil {
				return err
			}

			diff = v.Int64
			tokenlocked = t.Time.UTC().Format(time.RFC3339Nano)
			l.logger.Printf("diff=%v, token=%v", v.Int64, tokenlocked)
		}

		return reterr
	}()

	return tokenlocked, diff, err
}

func (l *Lock) getTokenFromDb() (string, error) {
	var token string
	stmt := spanner.Statement{
		SQL:    fmt.Sprintf("select token from %v where name = @name", l.table),
		Params: map[string]interface{}{"name": l.name},
	}

	iter := l.db.Single().Query(context.Background(), stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return "", err
		}

		var t spanner.NullTime
		err = row.Columns(&t)
		if err != nil {
			return "", err
		}

		token = t.Time.UTC().Format(time.RFC3339Nano)
	}

	return token, nil
}

func (l *Lock) heartbeat(clean ...bool) {
	ctx := context.Background()
	l.db.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL:    fmt.Sprintf("update %v set heartbeat = PENDING_COMMIT_TIMESTAMP() where name = @name", l.table),
			Params: map[string]interface{}{"name": l.name},
		}

		_, err := txn.Update(ctx, stmt)
		l.logger.Println("heartbeat:", err, l.id)

		if len(clean) > 0 {
			if clean[0] {
				delname := fmt.Sprintf("%v_", l.name)
				stmt = spanner.Statement{
					SQL:    fmt.Sprintf("delete from %v where starts_with(name, '%v')", l.table, delname),
					Params: map[string]interface{}{"name": l.name},
				}

				txn.Update(ctx, stmt)
			}
		}

		return err
	})
}

// New returns a lock object with a default of 10s lease duration.
func New(db *spanner.Client, table, name string, o ...Option) *Lock {
	l := &Lock{
		db:       db,
		table:    table,
		name:     name,
		id:       uuid.New().String(),
		mtx:      &sync.Mutex{},
		duration: 10000,
	}

	for _, opt := range o {
		opt.Apply(l)
	}

	if l.logger == nil {
		prefix := fmt.Sprintf("[spindle][%v] ", l.id)
		l.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return l
}
