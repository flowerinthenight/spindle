This package provides a blocking `Lock(...)` / `Unlock()` function pair. The basic usage will look something like:
```golang
ctx := context.Background()
client, _ := spanner.NewClient(ctx, "your/database")
defer db.Close()

slock := lock.NewSpindleLock(&lock.SpindleLockOptions{
    Client:   client,
    Table:    "testlease",
    Name:     "dlock",
    Duration: 1000,
})

start := time.Now()
slock.Lock(ctx)
log.Printf("lock acquired after %v, do protected work...", time.Since(start))
time.Sleep(time.Second * 5)
slock.Unlock()
```
