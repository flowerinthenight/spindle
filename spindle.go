package spindle

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	gaxv2 "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
)

var (
	ErrNotRunning = fmt.Errorf("spindle: not running")
)

type FnLeaderCallback func(data any, msg []byte)

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
func (l *Lock) Run(ctx context.Context, done ...chan error) error {
	var leader atomic.Int32 // for heartbeat
	go func() {
		min := (time.Millisecond * time.Duration(l.duration)) / 2
		bo := gaxv2.Backoff{
			Max: time.Millisecond * time.Duration(l.duration),
		}

		for {
			if ctx.Err() == context.Canceled {
				return // not foolproof due to delay
			}

			switch {
			case leader.Load() > 0:
				var tm time.Duration
				for {
					tm = bo.Pause()
					if tm >= min {
						break
					}
				}

				l.heartbeat()
				time.Sleep(tm)
			default:
				time.Sleep(time.Second)
			}
		}
	}()

	leaderCallback := func(state int) {
		if l.cbLeader != nil {
			l.cbLeader(l.cbLeaderData, []byte(fmt.Sprintf("%d", state)))
		}
	}

	locked := func() bool {
		// See if there is an active leased lock
		// (could be us, could be someone else).
		token, diff, err := l.checkLock()
		if err != nil {
			l.logger.Println(err)
			return true // err on safer side
		}

		// We are leader now.
		if l.token() == token {
			leader.Add(1)
			if leader.Load() == 1 {
				l.heartbeat() // only on 1
			}

			leaderCallback(1)
			l.logger.Println("leader active (me)")
			return true
		}

		// We're not leader now.
		if diff > 0 {
			var alive bool
			switch {
			case diff <= l.duration: // ideally
				alive = true
			case diff > l.duration:
				// Sometimes, its going to go beyond duration+drift, even in normal
				// situations. In that case, we will allow a new leader for now.
				ovr := float64((diff - l.duration))
				alive = ovr <= 1000 // allow 1s drift
			}

			if alive {
				leaderCallback(0)
				l.logger.Println("leader active (not me)")
				leader.Store(0) // reset heartbeat
				return true
			}
		}

		return false // lock available
	}

	var initial atomic.Int32
	var active atomic.Int32
	initial.Store(1)

	attemptLeader := func() {
		defer func(begin time.Time) {
			l.logger.Printf("round %v took %v", l.Iterations(), time.Since(begin))
			l.active.Store(1) // global
			active.Store(0)   // local
			l.iter.Add(1)
		}(time.Now())

		active.Store(1) // local
		if locked() {
			return
		}

		// Attempt first ever lock. The return commit timestamp will be our fencing
		// token. Only one node should be able to do this successfully.
		if initial.Load() == 1 {
			prefix := "init:"
			cts, err := l.db.ReadWriteTransaction(context.Background(),
				func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
					var q strings.Builder
					fmt.Fprintf(&q, "insert %s ", l.table)
					fmt.Fprintf(&q, "(name, heartbeat, token, writer) ")
					fmt.Fprintf(&q, "values (")
					fmt.Fprintf(&q, "'%s',", l.name)
					fmt.Fprintf(&q, "PENDING_COMMIT_TIMESTAMP(),")
					fmt.Fprintf(&q, "PENDING_COMMIT_TIMESTAMP(),")
					fmt.Fprintf(&q, "'%s')", l.id)
					_, err := txn.Update(ctx, spanner.Statement{SQL: q.String()})
					return err
				},
			)

			if err == nil {
				l.setToken(&cts)
				l.logger.Printf("%v got the lock with token %v", prefix, l.token())
				return
			}

			initial.Store(0)
		}

		// For the succeeding lock attempts.
		if initial.Load() == 0 {
			prefix := "next:"
			token, _, err := l.getCurrentToken()
			if err != nil {
				l.logger.Printf("%v getCurrentToken failed: %v", prefix, err)
				return
			}

			if token == 0 {
				l.logger.Printf("%v read token failed: empty", prefix)
				return
			}

			// Attempt to grab the next lock. Multiple nodes could potentially do this successfully.
			nts, err := l.db.ReadWriteTransaction(context.Background(),
				func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
					nxt := fmt.Sprintf("%v_%v", l.name, token)
					var q strings.Builder
					fmt.Fprintf(&q, "insert %s ", l.table)
					fmt.Fprintf(&q, "(name) ")
					fmt.Fprintf(&q, "values ('%s')", nxt)
					_, err := txn.Update(ctx, spanner.Statement{SQL: q.String()})
					return err
				},
			)

			if err == nil {
				// We got the lock. Attempt to update the current token to this commit timestamp.
				_, err := l.db.ReadWriteTransaction(context.Background(),
					func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
						var q strings.Builder
						fmt.Fprintf(&q, "update %s set ", l.table)
						fmt.Fprintf(&q, "heartbeat = PENDING_COMMIT_TIMESTAMP(), ")
						fmt.Fprintf(&q, "token = @token, ")
						fmt.Fprintf(&q, "writer = @writer ")
						fmt.Fprintf(&q, "where name = @name")
						stmt := spanner.Statement{
							SQL: q.String(),
							Params: map[string]any{
								"name":   l.name,
								"token":  nts,
								"writer": l.id,
							},
						}

						_, err := txn.Update(ctx, stmt)
						return err
					},
				)

				if err != nil {
					l.logger.Printf("%v update token failed: %v", prefix, err)
					return
				}

				l.setToken(&nts) // doesn't mean we're leader
				l.logger.Printf("%v got the lock with token %v", prefix, l.token())
			}
		}
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(l.duration))
	quit := context.WithValue(ctx, struct{}{}, nil)
	first := make(chan struct{}, 1)
	first <- struct{}{}

	go func() {
		for {
			select {
			case <-first: // immediately before 1st tick
				go attemptLeader()
			case <-tick.C: // duration heartbeat
				if active.Load() == 1 {
					continue
				}

				go attemptLeader()
			case <-quit.Done():
				if len(done) > 0 {
					done[0] <- nil
				}

				l.active.Store(0) // global
				tick.Stop()
				return
			}
		}
	}()

	return nil
}

// HasLock returns true if this instance got the lock, together with the lock token.
func (l *Lock) HasLock() (bool, uint64) {
	if l.active.Load() == 0 {
		return false, 0
	}

	token, _, err := l.getCurrentToken()
	if err != nil {
		return false, token
	}

	if token == l.token() {
		return true, token
	}

	return false, token
}

// HasLock2 is the same as HasLock but returns the leader id as well. Recommended
// instead of calling both HasLock() and Leader() functions.
func (l *Lock) HasLock2() (bool, string, uint64) {
	if l.active.Load() == 0 {
		return false, "", 0
	}

	token, w, err := l.getCurrentToken()
	if err != nil {
		return false, "", 0
	}

	if token == l.token() {
		return true, w, token
	}

	return false, w, token
}

// Leader returns the current leader id.
func (l *Lock) Leader() (string, error) {
	if l.active.Load() == 0 {
		return "", ErrNotRunning
	}

	_, w, err := l.getCurrentToken()
	return w, err
}

// Duration returns the duration in main loop in milliseconds.
func (l *Lock) Duration() int64 { return l.duration }

// Iterations returns the number of iterations done by the main loop.
func (l *Lock) Iterations() int64 { return l.iter.Load() }

// Client returns the Spanner client.
func (l *Lock) Client() *spanner.Client { return l.db }

func (l *Lock) token() uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if l.ttoken == nil {
		return 0
	}

	v := (*l.ttoken).UTC().Format(time.RFC3339Nano)
	return xxhash.Sum64String(v)
}

func (l *Lock) setToken(v *time.Time) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.ttoken = v
}

type diffT struct {
	Diff  spanner.NullInt64
	Token spanner.NullTime
}

func (l *Lock) checkLock() (uint64, int64, error) {
	var token string
	var diff int64

	err := func() error {
		var q strings.Builder
		fmt.Fprintf(&q, "select ")
		fmt.Fprintf(&q, "timestamp_diff(current_timestamp(), heartbeat, millisecond) as diff, ")
		fmt.Fprintf(&q, "token ")
		fmt.Fprintf(&q, "from %s ", l.table)
		fmt.Fprintf(&q, "where name = @name")
		stmt := spanner.Statement{
			SQL:    q.String(),
			Params: map[string]any{"name": l.name},
		}

		var retErr error
		iter := l.db.Single().Query(context.Background(), stmt)
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}

			if err != nil {
				retErr = err
				break
			}

			var v diffT
			err = row.ToStruct(&v)
			if err != nil {
				return err
			}

			diff = v.Diff.Int64
			token = v.Token.Time.UTC().Format(time.RFC3339Nano)
		}

		return retErr
	}()

	return xxhash.Sum64String(token), diff, err
}

type tokenT struct {
	Token  spanner.NullTime
	Writer spanner.NullString
}

func (l *Lock) getCurrentToken() (uint64, string, error) {
	var q strings.Builder
	fmt.Fprintf(&q, "select token, writer from %s ", l.table)
	fmt.Fprintf(&q, "where name = @name")
	stmt := spanner.Statement{
		SQL:    q.String(),
		Params: map[string]any{"name": l.name},
	}

	var token, writer string
	iter := l.db.Single().Query(context.Background(), stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return 0, writer, err
		}

		var v tokenT
		err = row.ToStruct(&v)
		if err != nil {
			return 0, writer, err
		}

		token = v.Token.Time.UTC().Format(time.RFC3339Nano)
		if v.Writer.Valid {
			writer = v.Writer.String()
		}
	}

	return xxhash.Sum64String(token), writer, nil
}

func (l *Lock) heartbeat() {
	l.db.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var q strings.Builder
			fmt.Fprintf(&q, "update %s ", l.table)
			fmt.Fprintf(&q, "set heartbeat = PENDING_COMMIT_TIMESTAMP() ")
			fmt.Fprintf(&q, "where name = @name")
			stmt := spanner.Statement{
				SQL:    q.String(),
				Params: map[string]any{"name": l.name},
			}

			_, err := txn.Update(ctx, stmt)
			if err != nil {
				l.logger.Printf("heartbeat failed: id=%v, err=%v", l.id, err)
			}

			// Best-effort cleanup.
			rm := fmt.Sprintf("%v_", l.name)
			q.Reset()
			fmt.Fprintf(&q, "delete from %s ", l.table)
			fmt.Fprintf(&q, "where starts_with(name, '%s')", rm)
			txn.Update(ctx, spanner.Statement{SQL: q.String()})
			return err
		},
	)
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
