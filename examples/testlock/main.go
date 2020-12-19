package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/spindle"
)

func main() {
	db, err := spanner.NewClient(
		context.Background(),
		"projects/mobingi-main/instances/alphaus-prod/databases/main",
	)

	if err != nil {
		log.Println(err)
		return
	}

	defer db.Close()

	done := make(chan error, 1)
	quit, cancel := context.WithCancel(context.Background())
	spindle.New(db, "testlease", "mylock", spindle.WithDuration(5000)).Run(quit, done)

	time.Sleep(time.Second * 40)
	cancel()
	<-done
}
