package spindle

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewDefaults(t *testing.T) {
	lock, err := New(nil, "mytable", "mylock")
	if err != nil {
		t.Fatal(err)
	}
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
	lock, err := New(nil, "mytable", "mylock",
		WithId("custom-id"),
		WithDuration(5000),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatal(err)
	}
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

func TestNewInvalidTableName(t *testing.T) {
	for _, name := range []string{"", "1abc", "my table", "foo;bar", "DROP TABLE x"} {
		_, err := New(nil, name, "n")
		if err != ErrInvalidTableName {
			t.Errorf("New(nil, %q, ...) err = %v; want ErrInvalidTableName", name, err)
		}
	}
}

func TestNewMinDuration(t *testing.T) {
	lock, err := New(nil, "t", "n", WithDuration(100))
	if err != nil {
		t.Fatal(err)
	}
	if lock.duration != 3000 {
		t.Errorf("duration = %d; want 3000 (minimum)", lock.duration)
	}
}

func TestTokenSetGet(t *testing.T) {
	lock, err := New(nil, "t", "n")
	if err != nil {
		t.Fatal(err)
	}

	if lock.token() != 0 {
		t.Errorf("initial token = %d; want 0", lock.token())
	}

	now := time.Now()
	lock.setToken(&now)
	if lock.token() != now.UnixNano() {
		t.Errorf("token = %d; want %d", lock.token(), now.UnixNano())
	}
}

func TestTokenConcurrency(t *testing.T) {
	lock, err := New(nil, "t", "n")
	if err != nil {
		t.Fatal(err)
	}
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
	lock, err := New(nil, "t", "n",
		WithLeaderCallback(nil, func(ctx context.Context, d any, leader bool, token int64) {
			mu.Lock()
			events = append(events, event{leader, token})
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if lock.cbLeader == nil {
		t.Fatal("callback should be set")
	}

	ts := time.Now()
	lock.setToken(&ts)
	lock.cbLeader(context.Background(), lock.cbLeaderData, true, lock.token())
	lock.cbLeader(context.Background(), lock.cbLeaderData, false, 0)

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
