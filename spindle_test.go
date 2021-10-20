package spindle

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	gaxv2 "github.com/googleapis/gax-go/v2"
)

const (
	db = "projects/test-project/instances/test-instance/databases/testdb"
)

func TestLock(t *testing.T) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		t.Error(err)
		return
	}

	defer client.Close()
	done := make(chan error, 1)
	quit, cancel := context.WithCancel(ctx)
	lock := New(client, "locktable", "mylock", WithDuration(5000))

	lock.Run(quit, done)

	var cnt int
	bo := gaxv2.Backoff{
		Initial:    time.Second,
		Max:        time.Second * 30,
		Multiplier: 2,
	}

	for {
		cnt++
		locked, token := lock.HasLock()
		switch {
		case locked:
			t.Logf("lock obtained, token=%v", token)
			break
		default:
			t.Log("lock not obtained, retry")
			time.Sleep(bo.Pause())
			continue
		}

		if cnt >= 10 {
			t.Fatalf("can't get lock")
		}

		break
	}

	cancel()
	<-done
}
