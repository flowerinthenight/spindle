package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	spindlelock "github.com/flowerinthenight/spindle/lock"
)

func main() {
	db, err := spanner.NewClient(
		context.Background(),
		"projects/{project}/instances/{instance}/databases/{db}",
	)

	if err != nil {
		log.Println(err)
		return
	}

	defer db.Close()

	var w sync.WaitGroup
	for i := 0; i < 3; i++ {
		w.Add(1)
		go func(n int) {
			defer w.Done()
			lockname := fmt.Sprintf("testlock/1111-22")
			lock := spindlelock.NewSpindleLock(&spindlelock.SpindleLockOptions{
				Client:   db,
				Table:    "curmxdlock",
				Name:     lockname,
				Duration: 2000,
				Logger:   log.New(ioutil.Discard, "", 0), // don't set if you want to see spindle logs
			})

			ctx := context.Background()
			if n > 1 {
				ctx, _ = context.WithTimeout(context.Background(), time.Second*2)
			}

			err = lock.Lock(ctx)
			if err != nil {
				log.Printf("[%v] lock failed: %v", n, err)
				return
			}

			log.Printf("[%v] locked", n)
			time.Sleep(time.Second * 5)
			lock.Unlock()
		}(i)
	}

	w.Wait()
}
