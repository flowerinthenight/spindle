package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/spindle/v2"
)

func main() {
	dbstr := flag.String("db", "", "db, fmt: projects/{v}/instances/{v}/databases/{v}")
	table := flag.String("table", "testlease", "table name")
	name := flag.String("name", "mylock", "lock name")
	flag.Parse()

	// To run, update the database name, table name, and, optionally, the lock name.
	// It is assumed that your environment is able to authenticate to Spanner via
	// GOOGLE_APPLICATION_CREDENTIALS environment variable.
	ctx := context.Background()
	db, err := spanner.NewClient(ctx, *dbstr)
	if err != nil {
		log.Println(err)
		return
	}

	defer db.Close()
	quit, cancel := context.WithCancel(ctx)
	lock := spindle.New(db, *table, *name, spindle.WithDuration(10000))

	done := make(chan error, 1)
	lock.Run(quit, done) // start main loop

	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		<-sigch
		cancel()
	}()

	<-done
}
