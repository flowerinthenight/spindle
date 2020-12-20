![main](https://github.com/flowerinthenight/spindle/workflows/main/badge.svg)

## spindle
A [Spanner](https://cloud.google.com/spanner/)-based distributed locking library. It uses Spanner's support for [transactions](https://cloud.google.com/spanner/docs/transactions) and [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) to achieve its locking mechanism.

## Usage
At the moment, the table needs to be created beforehand using the following DDL (`locktable` is just an example):
```SQL
CREATE TABLE locktable (
	name STRING(MAX) NOT NULL,
	heartbeat TIMESTAMP OPTIONS (allow_commit_timestamp=true),
	token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (name)
```

This library doesn't use the usual "lock", "do protected work", "unlock" sequence. Instead, after instantiating the lock object, you will call the `Run(...)` function which will continuously attempt to acquire a named lock until canceled. A `HasLock()` function is provided that returns true (along with the lock token) if the lock is successfully acquired. Something like

```go
db, _ := spanner.NewClient(context.Background(), "your/database")
defer db.Close()

done := make(chan error, 1)
quit, cancel := context.WithCancel(context.Background())
lock := spindle.New(db, "locktable", "mylock", spindle.WithDuration(5000))
lock.Run(quit, done)

time.Sleep(time.Second * 20)

locked, token := lock.HasLock()
log.Println(">>>>> HasLock:", locked, token)

time.Sleep(time.Second * 20)
cancel()
<-done
```
