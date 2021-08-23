package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/spindle"
)

func main() {
	// To run, update the database name, table name, and, optionally, the lock name.
	// It is assumed that your environment is able to authenticate to Spanner via
	// GOOGLE_APPLICATION_CREDENTIALS environment variable.
	db, err := spanner.NewClient(
		context.Background(),
		"projects/{project}/instances/{instance}/databases/{db}",
	)

	if err != nil {
		log.Println(err)
		return
	}

	defer db.Close()

	done := make(chan error, 1)
	quit, cancel := context.WithCancel(context.Background())
	lock := spindle.New(db, "testlease", "mylock", spindle.WithDuration(5000))

	// Start the main loop.
	lock.Run(quit, done)

	time.Sleep(time.Second * 20)
	locked, token := lock.HasLock()
	log.Println(">>>>> HasLock:", locked, token)
	time.Sleep(time.Second * 20)
	cancel()
	<-done
}
