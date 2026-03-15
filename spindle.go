package spindle

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FnLeaderCallback func(data any, leader bool, token int64, ctx context.Context)

type Option interface {
	Apply(*Lock)
}

type withId string

func (w withId) Apply(o *Lock) { o.id = string(w) }

// WithId sets this instance's unique id.
func WithId(v string) Option { return withId(v) }

type withDuration int64

func (w withDuration) Apply(o *Lock) { o.duration = int64(w) }

// WithDuration sets the locker's lease duration in ms. Minimum is 3000ms.
func WithDuration(v int64) Option { return withDuration(v) }

type withLeaderCallback struct {
	d any
	h FnLeaderCallback
}

func (w withLeaderCallback) Apply(o *Lock) {
	o.cbLeaderData = w.d
	o.cbLeader = w.h
}

// WithLeaderCallback sets the node's callback function when a leader is
// selected (or deselected). When leader is true, the provided context is
// cancelled when leadership is lost. Use the token as a fencing token for
// downstream writes.
func WithLeaderCallback(d any, h FnLeaderCallback) Option {
	return withLeaderCallback{d, h}
}

type withDbAdminClient struct {
	c *admin.DatabaseAdminClient
}

func (w withDbAdminClient) Apply(o *Lock) {
	o.dbAdmin = w.c
}

// WithDatabaseAdminClient sets Lock's database admin client, which is used for
// creating the lock table if it doesn't exist. Create table permissions required.
func WithDatabaseAdminClient(c *admin.DatabaseAdminClient) Option {
	return withDbAdminClient{c}
}

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *Lock) { o.logger = w.l }

// WithLogger sets the locker's logger object.
func WithLogger(v *log.Logger) Option { return withLogger{v} }

type Lock struct {
	db       *spanner.Client
	dbAdmin  *admin.DatabaseAdminClient
	dbPath   string // needed for dbAdmin
	table    string // table name
	name     string // lock name
	id       string // unique id for this instance
	duration int64  // lock duration in ms
	iter     atomic.Int64
	ttoken   *time.Time
	mtx      *sync.Mutex
	logger   *log.Logger
	active   atomic.Int32

	cbLeader     FnLeaderCallback // leader callback
	cbLeaderData any              // arbitrary data passed to fnLeader
}

// Run starts the main lock loop which can be canceled using the input context. You can
// provide an optional done channel if you want to be notified when the loop is done.
func (l *Lock) Run(ctx context.Context, done ...chan error) {
	err := l.ensureLockTable(ctx)
	if err != nil {
		if len(done) > 0 {
			select {
			case done[0] <- err:
			default:
			}
		}

		return
	}

	l.active.Store(1)
	leaseDuration := time.Millisecond * time.Duration(l.duration)

	type cbEvent struct {
		leader bool
		token  int64
		ctx    context.Context
	}

	cbChIn := make(chan cbEvent)
	cbChOut := make(chan cbEvent)

	var wgSend sync.WaitGroup
	wgSend.Go(func() {
		defer close(cbChOut)
		var q []cbEvent
		inCh := cbChIn
		for {
			if len(q) == 0 {
				if inCh == nil {
					return
				}
				ev, ok := <-inCh
				if !ok {
					return
				}
				q = append(q, ev)
			} else {
				select {
				case ev, ok := <-inCh:
					if !ok {
						inCh = nil
					} else {
						q = append(q, ev)
					}
				case cbChOut <- q[0]:
					q = q[1:]
				}
			}
		}
	})

	var wgCb sync.WaitGroup
	wgCb.Go(func() {
		for ev := range cbChOut {
			if l.cbLeader != nil {
				l.cbLeader(l.cbLeaderData, ev.leader, ev.token, ev.ctx)
			}
		}
	})

	var leaderCancel context.CancelFunc

	leaderCallback := func(state int) {
		if l.cbLeader == nil {
			return
		}

		var evCtx context.Context
		if state == 1 {
			evCtx, leaderCancel = context.WithCancel(ctx)
		} else {
			if leaderCancel != nil {
				leaderCancel()
				leaderCancel = nil
			}
			evCtx = context.Background()
		}

		cbChIn <- cbEvent{state == 1, l.token(), evCtx}
	}

	// Returns (isLeader, token, elapsedSinceLastHeartbeat, error).
	attemptLeader := func() (bool, int64, time.Duration, error) {
		var token atomic.Int64
		var spannerElapsed atomic.Int64

		l.logger.Printf("get lock for %v/%v", l.table, l.name)
		cts, err := func() (time.Time, error) {
			ts, err := l.db.ReadWriteTransaction(ctx,
				func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
					var q strings.Builder
					fmt.Fprintf(&q, "SELECT owner, token, CURRENT_TIMESTAMP() ")
					fmt.Fprintf(&q, "FROM %s WHERE name = @name", l.table)
					stmt := spanner.Statement{
						SQL:    q.String(),
						Params: map[string]any{"name": l.name},
					}
					iter := txn.Query(ctx, stmt)
					defer iter.Stop()

					row, err := iter.Next()
					if err == nil {
						var currentOwner string
						var lastToken time.Time
						var spannerNow time.Time
						if err := row.Columns(&currentOwner, &lastToken, &spannerNow); err != nil {
							return err
						}

						if currentOwner != l.id && spannerNow.Sub(lastToken) < leaseDuration {
							token.Store(lastToken.UnixNano())
							spannerElapsed.Store(int64(spannerNow.Sub(lastToken)))
							return fmt.Errorf("lock held by %s", currentOwner)
						}
					} else if err != iterator.Done {
						return err
					}

					return txn.BufferWrite([]*spanner.Mutation{
						spanner.InsertOrUpdate(
							l.table,
							[]string{"name", "owner", "token"},
							[]any{l.name, l.id, spanner.CommitTimestamp},
						)})
				},
			)

			if err != nil {
				l.logger.Printf("tx failed: %v", err)
				return time.Time{}, err
			}

			return ts, nil
		}()

		if err != nil {
			return false, token.Load(), time.Duration(spannerElapsed.Load()), err
		}

		l.setToken(&cts)
		l.logger.Printf("got the lock with token %v", l.token())
		return true, token.Load(), 0, nil
	}

	go func() {
		defer func() {
			close(cbChIn)
			wgSend.Wait()
			wgCb.Wait()
			if len(done) > 0 {
				select {
				case done[0] <- nil:
				default:
				}
			}
		}()

		timer := time.NewTimer(0)
		defer timer.Stop()

		// Scale the latency buffer dynamically based on lease duration.
		// For a 10s lease, floor is 500ms. For a 3s lease, floor is 150ms.
		bufferFloor := max(50*time.Millisecond, leaseDuration/20)
		bufferCeil := leaseDuration / 3
		var avgLatency time.Duration
		buffer := bufferFloor // initial
		var expire time.Duration
		var leader bool
		var wasLeader bool
		firstRun := true
		var elapsed time.Duration
		var lastHeartbeatSuccess time.Time

		for {
			select {
			case <-ctx.Done():
				l.active.Store(0)
				if leader {
					if leaderCancel != nil {
						leaderCancel()
					}
					l.release()
					leaderCallback(0)
				}
				return
			case <-timer.C:
			}

			l.iter.Add(1)
			start := time.Now()

			var err error
			if leader {
				hbCtx, hbCancel := context.WithTimeout(ctx, buffer)
				if err = l.heartbeat(hbCtx); err != nil {
					// We failed to heartbeat. Drop leadership if the next attempt
					// might exceed the lease duration safely window.
					if time.Since(lastHeartbeatSuccess)+buffer >= leaseDuration {
						leader = false
						elapsed = 0 // Reset to immediately retry attemptLeader
					}
				} else {
					lastHeartbeatSuccess = time.Now()
				}
				hbCancel()
			} else {
				leader, _, elapsed, err = attemptLeader()
				if leader {
					lastHeartbeatSuccess = time.Now()
				}
			}

			// Update buffer based on measured Spanner latency.
			latency := time.Since(start)
			if avgLatency == 0 {
				avgLatency = latency
			} else {
				avgLatency = time.Duration(float64(avgLatency)*0.7 + float64(latency)*0.3)
			}

			buffer = max(avgLatency*3, bufferFloor)
			buffer = min(buffer, bufferCeil)

			if leader != wasLeader || firstRun {
				if leader {
					leaderCallback(1)
				} else {
					leaderCallback(0)
				}

				wasLeader = leader
				firstRun = false
			}

			if leader {
				if err != nil {
					// Retry quickly on failure, but don't hammer Spanner.
					// Use the larger of the calculated latency buffer or
					// 1/4th of the lease.
					expire = max(buffer, leaseDuration/4)
				} else {
					expire = (leaseDuration / 2) - time.Since(start)
				}
			} else {
				if err != nil && elapsed == 0 {
					expire = buffer
				} else {
					expire = leaseDuration - elapsed
				}
			}

			expire -= buffer
			if expire <= 0 {
				expire = buffer
			}

			me := "not me"
			if leader {
				me = "me"
			}

			l.logger.Printf("expire=%v, buffer=%v, leader active (%v) (%v)", expire, buffer, me, l.Iterations())
			timer.Reset(expire)

			l.logger.Printf("round %v took %v", l.Iterations(), time.Since(start))
		}
	}()
}

// Duration returns the duration in main loop in milliseconds.
func (l *Lock) Duration() int64 { return l.duration }

// Iterations returns the number of iterations done by the main loop.
func (l *Lock) Iterations() int64 { return l.iter.Load() }

// Client returns the Spanner client.
func (l *Lock) Client() *spanner.Client { return l.db }

func (l *Lock) token() int64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if l.ttoken == nil {
		return 0
	}

	return (*l.ttoken).UnixNano()
}

func (l *Lock) setToken(v *time.Time) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.ttoken = v
}

func (l *Lock) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := l.db.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var q strings.Builder
			fmt.Fprintf(&q, "DELETE FROM %s ", l.table)
			fmt.Fprintf(&q, "WHERE name = @name ")
			fmt.Fprintf(&q, "AND token = @oldToken ")
			fmt.Fprintf(&q, "AND owner = @owner")
			stmt := spanner.Statement{
				SQL: q.String(),
				Params: map[string]any{
					"name":     l.name,
					"oldToken": time.Unix(0, l.token()),
					"owner":    l.id,
				},
			}
			_, err := txn.Update(ctx, stmt)
			return err
		},
	)
	if err != nil {
		l.logger.Printf("release failed: %v", err)
	}
}

func (l *Lock) heartbeat(ctx context.Context) error {
	cts, err := l.db.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var q strings.Builder
			fmt.Fprintf(&q, "update %s ", l.table)
			fmt.Fprintf(&q, "set token = PENDING_COMMIT_TIMESTAMP() ")
			fmt.Fprintf(&q, "where name = @name ")
			fmt.Fprintf(&q, "and token = @oldToken ")
			fmt.Fprintf(&q, "and owner = @owner")
			stmt := spanner.Statement{
				SQL: q.String(),
				Params: map[string]any{
					"name":     l.name,
					"oldToken": time.Unix(0, l.token()),
					"owner":    l.id,
				},
			}

			count, err := txn.Update(ctx, stmt)
			if err != nil {
				return err
			}

			if count == 0 {
				return errors.New("heartbeat failed: lock row missing or token superseded")
			}

			return nil
		},
	)

	if err != nil {
		l.logger.Printf("heartbeat failed: id=%v, err=%v", l.id, err)
		return err
	}

	l.setToken(&cts)
	return nil
}

func (l *Lock) ensureLockTable(ctx context.Context) error {
	if l.dbAdmin == nil {
		return nil // assume table exists if no admin client provided
	}

	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE %s (", l.table)
	fmt.Fprintf(&ddl, "name STRING(MAX) NOT NULL,")
	fmt.Fprintf(&ddl, "token TIMESTAMP OPTIONS (allow_commit_timestamp=true),")
	fmt.Fprintf(&ddl, "owner STRING(MAX)")
	fmt.Fprintf(&ddl, ") PRIMARY KEY (name)")

	op, err := l.dbAdmin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   l.dbPath,
		Statements: []string{ddl.String()},
	})

	if err != nil {
		if errAlreadyExists(err) {
			err = nil // someone else is already creating it or it exists
			return err
		}

		return fmt.Errorf("spindle: failed to start DDL for %s: %w", l.table, err)
	}

	err = op.Wait(ctx)
	if err != nil {
		if errAlreadyExists(err) {
			err = nil // someone else is already creating it or it exists
			return err
		}

		return fmt.Errorf("spindle: DDL failed for %s: %w", l.table, err)
	}

	return nil
}

func errAlreadyExists(err error) bool {
	if err == nil {
		return false
	}

	return status.Code(err) == codes.AlreadyExists ||
		strings.Contains(strings.ToLower(err.Error()), "already exists") ||
		strings.Contains(strings.ToLower(err.Error()), "duplicate name in schema")
}

// New returns a lock object with a default of 10s lease duration.
func New(db *spanner.Client, table, name string, o ...Option) *Lock {
	var dbPath string
	if db != nil {
		dbPath = db.DatabaseName()
	}

	lock := &Lock{
		db:       db,
		dbPath:   dbPath,
		table:    table,
		name:     name,
		id:       uuid.New().String(),
		mtx:      &sync.Mutex{},
		duration: 10000,
	}

	for _, opt := range o {
		opt.Apply(lock)
	}

	if lock.logger == nil {
		prefix := fmt.Sprintf("[spindle/%v] ", lock.id)
		lock.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	if lock.duration < 3000 {
		lock.logger.Println("setting duration to 3s (minimum)")
		lock.duration = 3000 // minimum
	}

	return lock
}
