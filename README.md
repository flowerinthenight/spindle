![main](https://github.com/flowerinthenight/spindle/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/spindle.svg)](https://pkg.go.dev/github.com/flowerinthenight/spindle)

## spindle
A distributed locking library built on top of [Cloud Spanner](https://cloud.google.com/spanner/). It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking mechanism.

## Usage
At the moment, the table needs to be created beforehand using the following DDL (`locktable` is just an example):
```SQL
CREATE TABLE locktable (
    name STRING(MAX) NOT NULL,
    heartbeat TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    writer STRING(MAX),
) PRIMARY KEY (name)
```

This library doesn't use the usual synchronous "lock", "do protected work", "unlock" sequence. For that, you can check out the included [lock](./lock) package. Instead, after instantiating the lock object, you will call the `Run(...)` function which will attempt to acquire a named lock at a regular interval (lease duration) until cancelled. A `HasLock()` function is provided which returns true (along with the lock token) if the lock is successfully acquired. Something like:

```go
db, _ := spanner.NewClient(context.Background(), "your/database")
defer db.Close()

done := make(chan error, 1) // notify me when done (optional)
quit, cancel := context.WithCancel(context.Background()) // for cancel

// Instantiate the lock object using a 5s lease duration using locktable above.
lock := spindle.New(db, "locktable", "mylock", spindle.WithDuration(5000))

lock.Run(quit, done) // start the main loop, async

time.Sleep(time.Second * 20)
locked, token := lock.HasLock()
log.Println("HasLock:", locked, token)
time.Sleep(time.Second * 20)

cancel()
<-done
```

## How it works
The initial lock (the lock record doesn't exist in the table yet) is acquired by a process using an SQL `INSERT`. Once the record is created (by one process), all other `INSERT` attempts will fail. In this phase, the commit timestamp of the locking process' transaction will be equal to the timestamp stored in the `token` column. This will serve as our fencing token in situations where multiple processes are somehow able to acquire a lock. Using this token, the real lock holder will start sending heartbeats by updating the `heartbeat` column.

When a lock is active, all participating processes will detect if the lease has expired by checking the heartbeat against Spanner's current timestamp. If so (say, the active locker has crashed, or cancelled), another round of SQL `INSERT` is attempted, this time, using the name format `<lockname_current-lock-token>`. The process that gets the lock this time will then attempt to update the `token` column using its commit timestamp, thus, updating the fencing token. In the event that the original locker process recovers (if crashed), or continues after a stop-the-world GC pause, the latest token should invalidate its locking claim (its token is already outdated).

A simple [code](./examples/simple/main.go) is provided to demonstrate the mechanism through logs. You can try running multiple binaries in multiple terminals or in a single terminal, like:

```bash
$ cd examples/simple/
$ go build -v
$ for num in 1 2 3; do ./simple &; done
```
