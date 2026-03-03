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
		Initial: time.Second,
		Max:     time.Second * 30,
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

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected string
	}{
		{"Exactly Seconds", 2 * time.Second, "2s"},
		{"Exactly Minutes", 10 * time.Minute, "10m"},
		{"Exactly Hours", 5 * time.Hour, "5h"},
		{"Minutes and Seconds", 10*time.Minute + 30*time.Second, "10m30s"},
		{"Hours and Minutes", 2*time.Hour + 15*time.Minute, "2h15m"},
		{"Hours and Seconds", 1*time.Hour + 45*time.Second, "1h45s"},
		{"Full Combo", 1*time.Hour + 30*time.Minute + 15*time.Second, "1h30m15s"},
		{"Large Duration", 25 * time.Hour, "25h"},
		{"Sub-second Rounding", 2*time.Second + 500*time.Millisecond, "3s"},
		{"Zero Duration", 0, "0s"},
		{"Negative Duration", -5 * time.Second, "0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := formatDuration(tt.input)
			if actual != tt.expected {
				t.Errorf("formatDuration(%v) = %s; want %s", tt.input, actual, tt.expected)
			}
		})
	}
}
