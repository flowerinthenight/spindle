package spindle

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	leaderCh := make(chan int64, 1)
	lock := New(client, "locktable", "mylock",
		WithDuration(5000),
		WithLeaderCallback(nil, func(d any, leader bool, token int64) {
			if leader {
				select {
				case leaderCh <- token:
				default:
				}
			}
		}),
	)

	lock.Run(quit, done)

	select {
	case token := <-leaderCh:
		t.Logf("lock obtained, token=%v", token)
	case <-time.After(30 * time.Second):
		t.Fatalf("can't get lock")
	}

	cancel()
	<-done
}

func TestNewDefaults(t *testing.T) {
	lock := New(nil, "mytable", "mylock")
	if lock.duration != 10000 {
		t.Errorf("default duration = %d; want 10000", lock.duration)
	}
	if lock.table != "mytable" {
		t.Errorf("table = %s; want mytable", lock.table)
	}
	if lock.name != "mylock" {
		t.Errorf("name = %s; want mylock", lock.name)
	}
	if lock.id == "" {
		t.Error("id should be auto-generated")
	}
	if lock.logger == nil {
		t.Error("logger should be set by default")
	}
}

func TestNewWithOptions(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	lock := New(nil, "mytable", "mylock",
		WithId("custom-id"),
		WithDuration(5000),
		WithLogger(logger),
	)
	if lock.id != "custom-id" {
		t.Errorf("id = %s; want custom-id", lock.id)
	}
	if lock.duration != 5000 {
		t.Errorf("duration = %d; want 5000", lock.duration)
	}
	if lock.logger != logger {
		t.Error("logger should match provided logger")
	}
}

func TestNewMinDuration(t *testing.T) {
	lock := New(nil, "t", "n", WithDuration(100))
	if lock.duration != 1000 {
		t.Errorf("duration = %d; want 1000 (minimum)", lock.duration)
	}
}

func TestTokenSetGet(t *testing.T) {
	lock := New(nil, "t", "n")

	if lock.token() != 0 {
		t.Errorf("initial token = %d; want 0", lock.token())
	}

	now := time.Now()
	lock.setToken(&now)
	if lock.token() != now.UnixNano() {
		t.Errorf("token = %d; want %d", lock.token(), now.UnixNano())
	}

	lock.setToken(nil)
	if lock.token() != 0 {
		t.Errorf("nil token = %d; want 0", lock.token())
	}
}

func TestTokenConcurrency(t *testing.T) {
	lock := New(nil, "t", "n")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			ts := time.Now()
			lock.setToken(&ts)
		}()
		go func() {
			defer wg.Done()
			lock.token()
		}()
	}
	wg.Wait()
}

func TestErrAlreadyExists(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", fmt.Errorf("something else"), false},
		{"grpc AlreadyExists", status.Error(codes.AlreadyExists, "exists"), true},
		{"contains already exists", fmt.Errorf("Table ALREADY EXISTS in schema"), true},
		{"contains duplicate name", fmt.Errorf("Duplicate name in schema: foo"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errAlreadyExists(tt.err); got != tt.want {
				t.Errorf("errAlreadyExists(%v) = %v; want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestLeaderCallback(t *testing.T) {
	type event struct {
		leader bool
		token  int64
	}

	var mu sync.Mutex
	var events []event
	lock := New(nil, "t", "n",
		WithLeaderCallback(nil, func(d any, leader bool, token int64) {
			mu.Lock()
			events = append(events, event{leader, token})
			mu.Unlock()
		}),
	)

	if lock.cbLeader == nil {
		t.Fatal("callback should be set")
	}

	ts := time.Now()
	lock.setToken(&ts)
	lock.cbLeader(lock.cbLeaderData, true, lock.token())
	lock.cbLeader(lock.cbLeaderData, false, 0)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("got %d events; want 2", len(events))
	}
	if !events[0].leader || events[0].token != ts.UnixNano() {
		t.Errorf("event[0] = %+v; want leader=true token=%d", events[0], ts.UnixNano())
	}
	if events[1].leader || events[1].token != 0 {
		t.Errorf("event[1] = %+v; want leader=false token=0", events[1])
	}
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
