package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/flowerinthenight/spindle/v3"
)

func main() {
	dbstr := flag.String("db", "", "db, fmt: projects/{v}/instances/{v}/databases/{v}")
	table := flag.String("table", "testlease", "table name")
	name := flag.String("name", "mylock", "lock name")
	dbg := flag.Bool("dbg", false, "enable verbose debug logging")
	flag.Parse()

	// To run, update the database name, table name, and, optionally, the lock name.
	// Auth depends on environment's ADC.
	ctx := context.Background()
	db, err := spanner.NewClient(ctx, *dbstr)
	if err != nil {
		log.Println(err)
		return
	}

	defer db.Close()
	dbAdminClient, err := admin.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Println(err)
		return
	}

	defer dbAdminClient.Close()
	quit, cancel := context.WithCancel(ctx)

	// Try running multiple instances of this binary in separate terminals
	// pointing to the same database, table, and lock name. You should see:
	// - Only one instance logs "doing leader work" at a time.
	// - When you Ctrl+C the leader, it releases the lock and shuts down.
	// - Another instance picks up leadership and starts doing work.
	id := fmt.Sprintf("node-%d", os.Getpid())
	lock, err := spindle.New(
		db,
		*table,
		*name,
		spindle.WithId(id),
		spindle.WithDuration(10),
		spindle.WithDatabaseAdminClient(dbAdminClient),
		spindle.WithDebug(*dbg),
		spindle.WithLeaderCallback(nil, func(ctx context.Context, d any, leader bool, token int64) {
			if !leader {
				log.Printf("[%s] lost leadership, stopping work", id)
				return
			}

			log.Printf("[%s] became leader, token=%v", id, token)

			// Do leader work using ctx; cancelled when leadership is lost.
			// Use token as a fencing token for downstream conditional writes.
			go func() {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						log.Printf("[%s] leader context cancelled", id)
						return
					case <-ticker.C:
						log.Printf("[%s] doing leader work (token=%v)", id, token)
					}
				}
			}()
		}),
	)
	if err != nil {
		log.Println(err)
		return
	}

	done := make(chan error, 1)
	lock.Run(quit, done) // start main loop

	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		<-sigch
		cancel() // triggers lock release if this node is the leader
	}()

	err = <-done
	if err != nil {
		log.Println(err)
	}

	log.Printf("[%s] shut down", id)
}
