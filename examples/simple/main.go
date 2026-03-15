package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/flowerinthenight/spindle/v2"
)

func main() {
	dbstr := flag.String("db", "", "db, fmt: projects/{v}/instances/{v}/databases/{v}")
	table := flag.String("table", "testlease", "table name")
	name := flag.String("name", "mylock", "lock name")
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
	lock := spindle.New(
		db,
		*table,
		*name,
		spindle.WithDuration(10000),
		spindle.WithDatabaseAdminClient(dbAdminClient, *dbstr),
		spindle.WithLeaderCallback(nil, func(d any, leader bool, token int64) {
			log.Printf("callback: leader=%v, token=%v", leader, token)
		}),
	)

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

	log.Println("lock released, shutting down")
}
