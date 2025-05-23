![main](https://github.com/flowerinthenight/spindle/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/spindle.svg)](https://pkg.go.dev/github.com/flowerinthenight/spindle/v2)

(This repo is mirrored to [https://codeberg.org/flowerinthenight/spindle](https://codeberg.org/flowerinthenight/spindle)).

## spindle
A distributed locking library built on [Cloud Spanner](https://cloud.google.com/spanner/). It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking mechanism.

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

### A note on costs

The smallest Spanner instance you can provision in GCP is 100 PUs (processing units), which is more than enough for this library. Without discounts, that's around ~$127/month (Tokyo region). At [Alphaus](https://www.alphaus.cloud/), we buy discounts for Spanner; we only pay around ~$60/month. Currently, it’s the cheapest way, surprisingly, to do distributed locking that we’ve tried so far, compared to Redis-based locking (which we used before), and way cheaper than the usual 3-node requirements for the likes of etcd, Zookeeper, Consul, etc.

## Use cases
One use case for this library is [leader election](https://en.wikipedia.org/wiki/Leader_election). If you want one host/node/pod to be the leader within a cluster/group, you can achieve that with this library. When the leader fails, it will fail over to another host/node/pod within a specific timeout. That said, you might want to check out [hedge](https://github.com/flowerinthenight/hedge), which is a memberlist tracking library built on top of this library.

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

After creating the lock object, you will call the `Run(...)` function which will attempt to acquire a named lock at a regular interval (lease duration) until cancelled. A `HasLock()` function is provided which returns true (along with the lock token) if the lock is successfully acquired. Something like:

```go
import (
    ...
    "github.com/flowerinthenight/spindle/v2"
)

func main() {
    db, _ := spanner.NewClient(context.Background(), "your/database")
    defer db.Close()
    
    done := make(chan error, 1) // notify me when done (optional)
    quit, cancel := context.WithCancel(context.Background()) // for cancel
    
    // Create the lock object using a 5s lease duration using locktable above.
    lock := spindle.New(db, "locktable", "mylock", spindle.WithDuration(5000))
    
    lock.Run(quit, done) // start the main loop, async
    
    time.Sleep(time.Second * 20)
    locked, token := lock.HasLock()
    log.Println("HasLock:", locked, token)
    time.Sleep(time.Second * 20)
    
    cancel()
    <-done
}
```

## How it works
The initial lock (the lock record doesn't exist in the table yet) is acquired by a process using an SQL `INSERT`. Once the record is created (by one process), all other `INSERT` attempts will fail. In this phase, **the commit timestamp of the locking process' transaction will be equal to the timestamp stored in the** `token` **column (being able to do this in one atomic network call is a crucial part of the algorithm)**. This will serve as our fencing token in situations where multiple processes are somehow able to acquire a lock. Using this token, the real lock holder will start sending heartbeats by updating the `heartbeat` column.

When a lock is active, all participating processes will detect if the lease has expired by checking the heartbeat against Spanner's current timestamp. If so (say, the active locker has crashed, or cancelled), another round of SQL `INSERT` is attempted, this time, using the name format `<lockname_current-lock-token>`. The process that gets the lock (**still using a single, atomic network call**) during this phase will then attempt to update the `token` column using its commit timestamp, thus, updating the fencing token. In the event that the original locker process recovers (if crashed), or continues after a stop-the-world GC pause, the latest token should invalidate its locking claim (its token is already outdated).

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
