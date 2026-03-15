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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FnLeaderCallback func(data any, leader bool, token int64)

type Option interface {
	Apply(*Lock)
}

type withId string

func (w withId) Apply(o *Lock) { o.id = string(w) }

// WithId sets this instance's unique id.
func WithId(v string) Option { return withId(v) }

type withDuration int64

func (w withDuration) Apply(o *Lock) { o.duration = int64(w) }

// WithDuration sets the locker's lease duration in ms. Minimum is 1000ms.
func WithDuration(v int64) Option { return withDuration(v) }

type withLeaderCallback struct {
	d any
	h FnLeaderCallback
}

func (w withLeaderCallback) Apply(o *Lock) {
	o.cbLeaderData = w.d
	o.cbLeader = w.h
}

// WithLeaderCallback sets the node's callback function when it a
// leader is selected (or deselected). The msg arg for h will be
// set to either 0 or 1.
func WithLeaderCallback(d any, h FnLeaderCallback) Option {
	return withLeaderCallback{d, h}
}

type withDbAdminClient struct {
	c  *admin.DatabaseAdminClient
	db string
}

func (w withDbAdminClient) Apply(o *Lock) {
	o.dbAdmin = w.c
	o.dbPath = w.db
}

// WithDatabaseAdminClient sets Lock's database admin client, which is used for creating
// the lock table if it doesn't exist. Create table permissions required. 'db' is the
// database path (e.g. projects/test-project/instances/test-instance/databases/testdb).
func WithDatabaseAdminClient(c *admin.DatabaseAdminClient, db string) Option {
	return withDbAdminClient{c, db}
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
	err := l.ensureLockTable()
	if err != nil {
		if len(done) > 0 {
			done[0] <- err
		}

		return
	}

	l.active.Store(1)
	leaseDuration := time.Millisecond * time.Duration(l.duration)

	type cbEvent struct {
		leader bool
		token  int64
	}

	cbCh := make(chan cbEvent, 2)
	go func() {
		for ev := range cbCh {
			if l.cbLeader != nil {
				l.cbLeader(l.cbLeaderData, ev.leader, ev.token)
			}
		}
	}()

	leaderCallback := func(state int) {
		if l.cbLeader == nil {
			return
		}
		select {
		case cbCh <- cbEvent{state == 1, l.token()}:
		default:
			select {
			case <-cbCh:
			default:
			}
			cbCh <- cbEvent{state == 1, l.token()}
		}
	}

	attemptLeader := func() (bool, int64) {
		var token atomic.Int64

		prefix := "attempt:"
		l.logger.Printf("%v get lock for %v/%v", prefix, l.table, l.name)
		cts, err := func() (time.Time, error) {
			ts, err := l.db.ReadWriteTransaction(ctx,
				func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
					row, err := txn.ReadRow(
						ctx,
						l.table,
						spanner.Key{l.name}, []string{"writer", "token"},
					)

					if err == nil {
						var currentOwner string
						var lastToken time.Time
						if err := row.Columns(&currentOwner, &lastToken); err != nil {
							return err
						}

						if currentOwner != l.id && time.Since(lastToken) < leaseDuration {
							token.Store(lastToken.UnixNano())
							return fmt.Errorf("lock held by %s", currentOwner)
						}
					} else if spanner.ErrCode(err) != codes.NotFound {
						return err
					}

					return txn.BufferWrite([]*spanner.Mutation{
						spanner.InsertOrUpdate(
							l.table,
							[]string{"name", "writer", "token"},
							[]any{l.name, l.id, spanner.CommitTimestamp},
						)})
				},
			)

			if err != nil {
				l.logger.Printf("%v tx failed: %v", prefix, err)
				return time.Time{}, err
			}

			return ts, nil
		}()

		if err != nil {
			return false, token.Load()
		}

		l.setToken(&cts)
		l.logger.Printf("%v got the lock with token %v", prefix, l.token())
		return true, token.Load()
	}

	go func() {
		<-ctx.Done()
		l.active.Store(0)
		close(cbCh)
		if len(done) > 0 {
			done[0] <- nil
		}
	}()

	go func() {
		timer := time.NewTimer(0)
		defer timer.Stop()

		buffer := 800 * time.Millisecond
		var expire time.Duration
		var leader bool
		var wasLeader bool
		firstRun := true
		var token int64

		for {
			select {
			case <-ctx.Done():
				if leader {
					l.release()
				}
				return
			case <-timer.C:
			}

			l.iter.Add(1)
			start := time.Now()

			if leader {
				if err := l.heartbeat(ctx); err != nil {
					leader = false
				}
			} else {
				leader, token = attemptLeader()
			}

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
				expire = leaseDuration - time.Since(time.Unix(0, l.token()))
			} else {
				expire = leaseDuration - time.Since(time.Unix(0, token))
			}

			expire -= buffer
			if expire <= 0 {
				expire = buffer
			}

			l.logger.Printf("expire=%v, leader=%v (%v)", expire, leader, l.Iterations())
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
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.Delete(l.table, spanner.Key{l.name}),
			})
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
			fmt.Fprintf(&q, "and writer = @owner")
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

func (l *Lock) ensureLockTable() error {
	if l.dbAdmin == nil {
		return nil // assume table exists if no admin client provided
	}

	var err error
	ctx := context.Background()
	duration := time.Millisecond * time.Duration(l.duration)
	l.table = fmt.Sprintf("%s_%s", l.table, formatDuration(duration))
	defer func(e *error) {
		if *e == nil {
			l.logger.Println("lock table:", l.table)
		}
	}(&err)

	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE %s (", l.table)
	fmt.Fprintf(&ddl, "name STRING(MAX) NOT NULL,")
	fmt.Fprintf(&ddl, "heartbeat TIMESTAMP OPTIONS (allow_commit_timestamp=true),")
	fmt.Fprintf(&ddl, "token TIMESTAMP OPTIONS (allow_commit_timestamp=true),")
	fmt.Fprintf(&ddl, "writer STRING(MAX)")
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

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d <= 0 {
		return "0s"
	}

	var b strings.Builder
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		fmt.Fprintf(&b, "%dh", h)
	}
	if m > 0 {
		fmt.Fprintf(&b, "%dm", m)
	}
	if s > 0 || b.Len() == 0 {
		fmt.Fprintf(&b, "%ds", s)
	}

	return b.String()
}

// New returns a lock object with a default of 10s lease duration.
func New(db *spanner.Client, table, name string, o ...Option) *Lock {
	lock := &Lock{
		db:       db,
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

	if lock.duration < 1000 {
		lock.logger.Println("setting duration to 1s (minimum)")
		lock.duration = 1000 // minimum
	}

	return lock
}
