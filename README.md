![main](https://github.com/flowerinthenight/spindle/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/spindle.svg)](https://pkg.go.dev/github.com/flowerinthenight/spindle/v2)

(This repo is mirrored to [https://codeberg.org/flowerinthenight/spindle](https://codeberg.org/flowerinthenight/spindle)).

## spindle
A distributed locking library built on [Cloud Spanner](https://cloud.google.com/spanner/). It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking mechanism.

> [!IMPORTANT]
> **Note on v3.x**: This branch (`v3.x`) is a big departure from the `v2.x` branch in terms of locking logic and correctness. Although `v2.x` is heavily used in Alphaus' production and has stood the test of time, please be aware of the critical changes from `v2.x` to `v3.x` when upgrading.
> 
> ### Key Differences: `v2.x` vs `v3.x`
> 
> **`v2.x`**:
> * **Non-atomic Acquisitions**: Required multiple network calls (an `INSERT` followed by an `UPDATE`) to acquire a lock.
> * **Loose Heartbeats**: Unconditionally updated the heartbeat column without validating the current token.
> * **Schema**: Relied on both a `heartbeat` column and a separate `token` column.
> 
> **`v3.x`**:
> * **Atomic Operations**: Lock acquisition and takeover now execute in a single atomic network call via `spanner.InsertOrUpdate` within a ReadWrite transaction.
> * **Strict TrueTime**: Lease expirations are verified directly inside the transaction using Spanner's `CURRENT_TIMESTAMP()`, avoiding any local clock drift issues.
> * **Optimistic Concurrency**: Heartbeats utilize a check-and-set mechanism (`WHERE token = @oldToken`) to ensure the lock is still held.
> * **Context Cancellation**: Leader callbacks provide a context that is automatically cancelled upon lease loss or token mismatch, preventing split-brain scenarios.
> * **Monotonic Fencing Token**: The heartbeat strictly advances the token (using Spanner's `PENDING_COMMIT_TIMESTAMP()`). Downstream services can enforce monotonic ordering by accepting writes where `current_token >= callback_token`.
> * **Simplified Schema**: Consolidates lock tracking into a single `token` column.

Port(s):
* [spindle-rs](https://github.com/flowerinthenight/spindle-rs) - a port written in Rust
* [spindle-cb](https://github.com/flowerinthenight/spindle-cb) - relies on [aws/clock-bound](https://github.com/aws/clock-bound) and PostgreSQL (storage)

Similar projects:
* [DistributedLock](https://github.com/madelson/DistributedLock) - .NET
* [Amazon DynamoDB Lock Client](https://github.com/awslabs/amazon-dynamodb-lock-client) - Java
* [distributed-lock](https://github.com/alturkovic/distributed-lock) - Java
* [redlock-rb](https://github.com/leandromoreira/redlock-rb) - Ruby
* [lockgate](https://github.com/werf/lockgate) - Go
* [kettle](https://github.com/flowerinthenight/kettle) (via Redis) - Go

> [!NOTE]
> **A note on costs**: The smallest Spanner instance you can provision in GCP is 100 PUs (processing units), which is more than enough for this library. Without discounts, that's around ~$127/month (Tokyo region). At [Alphaus](https://www.alphaus.cloud/), we buy discounts for Spanner; we only pay around ~$60/month. Currently, it’s the cheapest way, surprisingly, to do distributed locking that we’ve tried so far, compared to Redis-based locking (which we used before), and way cheaper than the usual 3-node requirements for the likes of etcd, Zookeeper, Consul, etc.

## Use cases
One use case for this library is [leader election](https://en.wikipedia.org/wiki/Leader_election). If you want one host/node/pod to be the leader within a cluster/group, you can achieve that with this library. When the leader fails, it will fail over to another host/node/pod within a specific timeout. That said, you might want to check out [hedge](https://github.com/flowerinthenight/hedge), which is a memberlist tracking library built on top of this library.

## Usage
At the moment, the table needs to be created beforehand using the following DDL (`locktable` is just an example):
```SQL
CREATE TABLE locktable (
    name STRING(MAX) NOT NULL,
    token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    owner STRING(MAX)
) PRIMARY KEY (name)
```

After creating the lock object, you will call the `Run(...)` function which will attempt to acquire a named lock at a regular interval (lease duration) until cancelled. You can provide a leader callback via `WithLeaderCallback(...)` which will be called when leadership is acquired or lost. Something like:

```go
import (
    ...
    "github.com/flowerinthenight/spindle/v3"
)

func main() {
    db, _ := spanner.NewClient(ctx, "your/database")
    defer db.Close()
    dbAdmin, _ := admin.NewDatabaseAdminClient(ctx)
    defer dbAdmin.Close()

    quit, cancel := context.WithCancel(ctx)
    lock := spindle.New(
        db, "locktable", "mylock",
        spindle.WithDuration(10000),
        spindle.WithDatabaseAdminClient(dbAdmin, "your/database"),
        spindle.WithLeaderCallback(nil,
            func(d any, leader bool, token int64, ctx context.Context) {
            if !leader {
                return // lost leadership
            }

            // Do leader work using ctx; cancelled when leadership is lost.
            // Use token as a fencing token for downstream conditional writes.
            // NOTE: Use the token monotonically (e.g. >= token) as heartbeats
            // will advance the token stored in the database.
            // IMPORTANT: You must honor 'ctx' to avoid split-brain scenarios.
            go func() {
                for {
                    select {
                    case <-ctx.Done():
                        return // leadership lost, stop working
                    case <-time.After(1 * time.Second):
                        // Do leader work here...
                    }
                }
            }()
        }),
    )

    done := make(chan error, 1)
    lock.Run(quit, done) // start main loop

    // On signal, cancel triggers lock release if this node is the leader.
    sigch := make(chan os.Signal, 1)
    signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
    <-sigch
    cancel()
    <-done
}
```

## How it works
The initial lock (the lock record doesn't exist in the table yet) is acquired by a process using an SQL `INSERT` (via `InsertOrUpdate`). Once the record is created (by one process), all other attempts will see the active lock. In this phase, **the commit timestamp of the locking process' transaction will be equal to the timestamp stored in the** `token` **column (being able to do this in one atomic network call is a crucial part of the algorithm)**. This will serve as our fencing token in situations where multiple processes are somehow able to acquire a lock. Using this token, the real lock holder will start sending heartbeats by updating the `token` column with a new commit timestamp. **Note that the heartbeat process advances the database token; downstream systems should use the callback token as a monotonic epoch (accepting writes where `current_token >= callback_token`).**

When a lock is active, all participating processes will detect if the lease has expired by checking the lock's `token` against Spanner's `CURRENT_TIMESTAMP()` within a read-write transaction. This ensures that the time check strictly uses Spanner's TrueTime, avoiding any issues with local clock drift. If the lease has expired (say, the active locker has crashed, or cancelled), the first process to successfully commit an `InsertOrUpdate` will take over the lock (**still using a single, atomic network call**). This takeover updates the `token` column using its new commit timestamp, thus, updating the fencing token. In the event that the original locker process recovers (if crashed), or continues after a stop-the-world GC pause, the latest token should invalidate its locking claim (its token is already outdated).

## Example
A simple [code](./examples/simple/main.go) is provided to demonstrate the mechanism through logs. You can try running multiple processes in multiple terminals.

```bash
# Update flags with your values as needed:
$ cd examples/simple/
$ go build -v
$ ./simple -db projects/{v}/instances/{v}/databases/{v} -table mytable -name mylock
```

The leader process should output something like `leader active (me)`. You can then try to stop (Ctrl+C) that process and observe another one taking over as leader.

## License

This library is licensed under the [Apache 2.0 License](./LICENSE).
