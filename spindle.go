package spindle

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

type Option interface {
	Apply(*Lock)
}

type withDuration int64

func (w withDuration) Apply(o *Lock) { o.duration = int64(w) }

func WithDuration(v int64) Option { return withDuration(v) }

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *Lock) { o.logger = w.l }

func WithLogger(v *log.Logger) Option { return withLogger{v} }

type Lock struct {
	db       *spanner.Client
	table    string // table name
	name     string // lock name
	id       string // unique id for this instance
	duration int64  // lock duration in ms
	leader   int32  // 1 = leader, 0 = !leader
	logger   *log.Logger
}

func (l *Lock) Run(ctx context.Context, done ...chan error) error {
	ticker := time.NewTicker(time.Millisecond * time.Duration(l.duration))
	quit := context.WithValue(ctx, struct{}{}, nil)

	go func() {
		var token string
		initial := true
		var cnt int64

		for {
			l.logger.Println("-->", cnt)
			cnt++

			select {
			case <-ticker.C:
				var tokenlocked string
				var diff int64

				l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
					stmt := spanner.Statement{
						SQL:    fmt.Sprintf("select timestamp_diff(current_timestamp(), heartbeat, millisecond), token from %v where name = @name", l.table),
						Params: map[string]interface{}{"name": l.name},
					}

					iter := txn.Query(ctx, stmt)
					defer iter.Stop()
					for {
						row, err := iter.Next()
						if err == iterator.Done || err != nil {
							break
						}

						var v spanner.NullInt64
						var t spanner.NullTime
						row.Columns(&v, &t)

						diff = v.Int64
						tokenlocked = t.Time.UTC().Format(time.RFC3339Nano)
						l.logger.Printf("diff=%v, fence=%v", v.Int64, tokenlocked)
					}

					return nil
				})

				if token != "" && token == tokenlocked {
					l.logger.Println("leader active (me)")
					atomic.StoreInt32(&l.leader, 1)
					l.heartbeat()
					break // switch
				}

				if diff > 0 && diff < l.duration {
					l.logger.Println("leader active (not me)")
					atomic.StoreInt32(&l.leader, 0)
					break // switch
				}

				if initial {
					// Attempt first ever lock; use insert instead of update. Only one client should be able to do this.
					// The return commit timestamp will be our fencing token (string version).
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
						// We got the first ever lock!
						token = cts.UTC().Format(time.RFC3339Nano)
						l.logger.Printf("leadertoken: %v", token)
						atomic.StoreInt32(&l.leader, 1)
						break // switch
					}

					l.logger.Printf("leader attempt failed: %v", err)
					initial = false
				}

				if !initial {
					var curtoken string
					_, err := l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
						stmt := spanner.Statement{
							SQL:    fmt.Sprintf("select token from %v where name = @name", l.table),
							Params: map[string]interface{}{"name": l.name},
						}

						var reterr error
						iter := txn.Query(ctx, stmt)
						defer iter.Stop()
						for {
							row, err := iter.Next()
							if err == iterator.Done || err != nil {
								reterr = err
								break
							}

							var t spanner.NullTime
							row.Columns(&t)
							curtoken = t.Time.UTC().Format(time.RFC3339Nano)
						}

						return reterr
					})

					if curtoken == "" {
						l.logger.Printf("read token failed: %v", err)
						break // switch
					}

					cts, err := l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
						nxtname := fmt.Sprintf("%v_%v", l.name, curtoken)
						stmt := spanner.Statement{
							SQL: fmt.Sprintf("insert %v (name) values ('%s')", l.table, nxtname),
						}

						_, err := txn.Update(ctx, stmt)
						l.logger.Println("insert:", err, nxtname)
						return err
					})

					if err == nil {
						token = cts.UTC().Format(time.RFC3339Nano)
						_, err := l.db.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
							ts := "PENDING_COMMIT_TIMESTAMP()"
							stmt := spanner.Statement{
								SQL:    fmt.Sprintf("update %v set heartbeat = %v, token = @token where name = @name", l.table, ts),
								Params: map[string]interface{}{"name": l.name, "token": cts},
							}

							_, err := txn.Update(ctx, stmt)
							return err
						})

						if err != nil {
							l.logger.Println(err)
						}
					}
				}
			case <-quit.Done():
				if len(done) > 0 {
					done[0] <- nil
				}

				return
			}
		}
	}()

	return nil
}

func (l *Lock) HasLock() bool { return atomic.LoadInt32(&l.leader) == 1 }

func (l *Lock) heartbeat() {
	ctx := context.Background()
	l.db.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL:    fmt.Sprintf("update %v set heartbeat = PENDING_COMMIT_TIMESTAMP() where name = @name", l.table),
			Params: map[string]interface{}{"name": l.name},
		}

		_, err := txn.Update(ctx, stmt)
		l.logger.Println("heartbeat:", err, l.id)

		delname := fmt.Sprintf("%v_", l.name)
		stmt = spanner.Statement{
			SQL:    fmt.Sprintf("delete from %v where starts_with(name, '%v')", l.table, delname),
			Params: map[string]interface{}{"name": l.name},
		}

		txn.Update(ctx, stmt)
		return err
	})
}

func New(db *spanner.Client, table, name string, o ...Option) *Lock {
	l := &Lock{
		db:       db,
		table:    table,
		name:     name,
		id:       uuid.New().String(),
		duration: 10000,
	}

	for _, opt := range o {
		opt.Apply(l)
	}

	if l.logger == nil {
		prefix := fmt.Sprintf("[spindle][%v] ", l.id)
		l.logger = log.New(os.Stdout, prefix, 0)
	}

	return l
}
