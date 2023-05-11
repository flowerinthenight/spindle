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

var (
	ErrNotRunning = fmt.Errorf("spindle: not running")
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
	iter     int64
	token    *time.Time
	mtx      *sync.Mutex
	logger   *log.Logger
	active   int32
}

// Run starts the main lock loop which can be canceled using the input context. You can
// provide an optional done channel if you want to be notified when the loop is done.
func (l *Lock) Run(ctx context.Context, done ...chan error) error {
	ticker := time.NewTicker(time.Millisecond * time.Duration(l.duration))
	first := make(chan struct{}, 1)
	first <- struct{}{} // trigger immediately
	quit := context.WithValue(ctx, struct{}{}, nil)

	go func() {
		var initial int32 = 1
		var active int32

		locked := func() bool {
			// See if there is an active leased lock (could be us, could be somebody else).
			tokenLocked, diff, err := l.checkLock()
			if err != nil {
				l.logger.Println(err)
				return true // err on safer side
			}

			if l.tokenString() != "" && l.tokenString() == tokenLocked {
				l.logger.Println("leader active (me)")
				l.heartbeat()
				return true
			}

			if diff > 0 {
				var ok bool
				switch {
				case diff <= l.duration: // ideally
					ok = true
				case diff > l.duration:
					// Sometimes, its going to go beyond duration+drift, even in normal
					// situations. In that case, we will allow a new leader for now.
					ovr := float64((diff - l.duration))
					ok = ovr <= 2000 // allow 2s drift
				}

				if ok {
					l.logger.Println("leader active (not me)")
					return true
				}
			}

			return false // lock available
		}

		attemptLeader := func() {
			defer func(begin time.Time) {
				l.logger.Printf("duration=%v, n=%v", time.Since(begin), l.Iterations())
				atomic.StoreInt32(&l.active, 1) // global
				atomic.StoreInt32(&active, 0)   // local
				atomic.AddInt64(&l.iter, 1)
			}(time.Now())

			atomic.StoreInt32(&active, 1) // local
			if yes := locked(); yes {
				return
			}

			// Attempt first ever lock. The return commit timestamp will be our fencing token.
			// Only one node should be able to do this successfully.
			if atomic.LoadInt32(&initial) == 1 {
				prefix := "[init]"
				cts, err := l.db.ReadWriteTransaction(context.Background(),
					func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
						sql := `
insert ` + l.table + `
  (name, heartbeat, token, writer)
values (
  '` + l.name + `',
  PENDING_COMMIT_TIMESTAMP(),
  PENDING_COMMIT_TIMESTAMP(),
  '` + l.id + `'
)`

						n, err := txn.Update(ctx, spanner.Statement{SQL: sql})
						l.logger.Printf("%v insert: n=%v, err=%v", prefix, n, err)
						return err
					},
				)

				if err == nil {
					l.setToken(&cts)
					l.logger.Printf("%v leadertoken: %v", prefix, l.tokenString())
					return
				}

				l.logger.Printf("%v leader attempt failed: %v", prefix, err)
				atomic.StoreInt32(&initial, 0)
			}

			// For the succeeding lock attempts.
			if atomic.LoadInt32(&initial) == 0 {
				prefix := "[next]"
				token, _, err := l.getCurrentTokenAndId()
				if err != nil {
					l.logger.Printf("%v getCurrentTokenAndId failed: %v", prefix, err)
					return
				}

				if token == "" {
					l.logger.Printf("%v read token failed: empty", prefix)
					return
				}

				// Attempt to grab the next lock. Multiple nodes could potentially do this successfully.
				nxtcts, err := l.db.ReadWriteTransaction(context.Background(),
					func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
						nxtname := fmt.Sprintf("%v_%v", l.name, token)
						sql := `
insert ` + l.table + ` (name)
values ('` + nxtname + `')`

						n, err := txn.Update(ctx, spanner.Statement{SQL: sql})
						l.logger.Printf("%v insert: name=%v, n=%v, err=%v", prefix, nxtname, n, err)
						return err
					})

				if err == nil {
					// We got the lock. Attempt to update the current token to this commit timestamp.
					_, err := l.db.ReadWriteTransaction(context.Background(),
						func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
							sql := `
update ` + l.table + ` set
  heartbeat = PENDING_COMMIT_TIMESTAMP(),
  token = @token,
  writer = @writer
where name = @name`

							stmt := spanner.Statement{
								SQL: sql,
								Params: map[string]interface{}{
									"name":   l.name,
									"token":  nxtcts,
									"writer": l.id,
								},
							}

							_, err := txn.Update(ctx, stmt)
							return err
						})

					if err != nil {
						l.logger.Printf("%v update token failed: %v", prefix, err)
						return
					}

					l.setToken(&nxtcts) // doesn't mean we're leader
				}
			}
		}

		for {
			select {
			case <-first: // immediately before 1st tick
				go attemptLeader()
			case <-ticker.C: // duration heartbeat
				if atomic.LoadInt32(&active) == 1 {
					continue
				}

				go attemptLeader()
			case <-quit.Done():
				if len(done) > 0 {
					done[0] <- nil
				}

				atomic.StoreInt32(&l.active, 0) // global
				ticker.Stop()
				return
			}
		}
	}()

	return nil
}

// HasLock returns true if this instance got the lock, together with the lock token.
func (l *Lock) HasLock() (bool, string) {
	if atomic.LoadInt32(&l.active) == 0 {
		return false, ""
	}

	token, _, err := l.getCurrentTokenAndId()
	if err != nil {
		return false, token
	}

	if token != "" && token == l.tokenString() {
		return true, token
	}

	return false, token
}

// Leader returns the current leader id.
func (l *Lock) Leader() (string, error) {
	if atomic.LoadInt32(&l.active) == 0 {
		return "", ErrNotRunning
	}

	_, w, err := l.getCurrentTokenAndId()
	return w, err
}

// Duration returns the duration in main loop in milliseconds.
func (l *Lock) Duration() int64 { return l.duration }

// Iterations returns the number of iterations done by the main loop.
func (l *Lock) Iterations() int64 { return atomic.LoadInt64(&l.iter) }

// Client returns the Spanner client.
func (l *Lock) Client() *spanner.Client { return l.db }

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

type diffT struct {
	Diff  spanner.NullInt64
	Token spanner.NullTime
}

func (l *Lock) checkLock() (string, int64, error) {
	var tokenLocked string
	var diff int64

	err := func() error {
		sql := `
select
  timestamp_diff(current_timestamp(), heartbeat, millisecond) as diff,
  token
from ` + l.table + `
where name = @name`

		stmt := spanner.Statement{
			SQL:    sql,
			Params: map[string]interface{}{"name": l.name},
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
			tokenLocked = v.Token.Time.UTC().Format(time.RFC3339Nano)
			l.logger.Printf("diff=%v, token=%v", v.Diff.Int64, tokenLocked)
		}

		return retErr
	}()

	return tokenLocked, diff, err
}

type tokenT struct {
	Token  spanner.NullTime
	Writer spanner.NullString
}

func (l *Lock) getCurrentTokenAndId() (string, string, error) {
	sql := ` select token, writer from ` + l.table + ` where name = @name`
	stmt := spanner.Statement{
		SQL:    sql,
		Params: map[string]interface{}{"name": l.name},
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
			return "", "", err
		}

		var v tokenT
		err = row.ToStruct(&v)
		if err != nil {
			return "", "", err
		}

		token = v.Token.Time.UTC().Format(time.RFC3339Nano)
		if v.Writer.Valid {
			writer = v.Writer.String()
		}
	}

	return token, writer, nil
}

func (l *Lock) heartbeat() {
	ctx := context.Background()
	l.db.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		sql := `
update ` + l.table + `
set heartbeat = PENDING_COMMIT_TIMESTAMP()
where name = @name`

		stmt := spanner.Statement{
			SQL:    sql,
			Params: map[string]interface{}{"name": l.name},
		}

		_, err := txn.Update(ctx, stmt)
		l.logger.Printf("heartbeat: id=%v, err=%v", l.id, err)

		// Best-effort cleanup.
		delname := fmt.Sprintf("%v_", l.name)
		sql = fmt.Sprintf("delete from %v where starts_with(name, '%v')", l.table, delname)
		txn.Update(ctx, spanner.Statement{SQL: sql})
		return err
	})
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

	return lock
}
