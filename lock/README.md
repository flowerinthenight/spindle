This package provides a blocking `Lock(...)` / `Unlock()` function pair. The basic usage will look something like:
```golang
ctx := context.Background()
db, _ := spanner.NewClient(ctx, "your/database")
defer db.Close()

lock := dlock.NewSpindleLock(&dlock.SpindleLockOptions{
    Client:   db,
    Table:    "testlease",
    Name:     "dlock",
    Duration: 1000,
})

start := time.Now()
lock.Lock(ctx)
log.Printf("lock acquired after %v, do protected work...", time.Since(start))
time.Sleep(time.Second * 5)
lock.Unlock()
```
