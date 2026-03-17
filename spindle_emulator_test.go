package spindle

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSpannerEmulatorFailover(t *testing.T) {
	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		t.Skip("SPANNER_EMULATOR_HOST is not set. Skipping emulator integration test.")
	}

	ctx := context.Background()
	conn, err := grpc.NewClient(emulatorHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial emulator: %v", err)
	}
	defer conn.Close()

	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create instance admin client: %v", err)
	}
	defer instanceAdminClient.Close()

	projectID := "test-project"
	instanceID := "test-instance"
	dbID := "test-db"

	createInstanceOp, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: "Emulator Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		t.Logf("CreateInstance err (might already exist): %v", err)
	} else {
		_, err = createInstanceOp.Wait(ctx)
		if err != nil {
			t.Logf("CreateInstance wait err: %v", err)
		}
	}

	dbAdminClient, err := admin.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create database admin client: %v", err)
	}
	defer dbAdminClient.Close()

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbID)
	createDbOp, err := dbAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbID),
		ExtraStatements: []string{
			`CREATE TABLE test_lock_table (
				name STRING(MAX) NOT NULL,
				token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
				owner STRING(MAX)
			) PRIMARY KEY (name)`,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if _, err := createDbOp.Wait(ctx); err != nil {
		t.Fatalf("CreateDatabase wait failed: %v", err)
	}

	spannerClient, err := spanner.NewClient(ctx, dbPath, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	tableName := "test_lock_table"
	lockName := "my_test_lock"
	leaseDuration := int64(5) // 5 seconds for faster testing

	var mu sync.Mutex
	var leaderB bool

	nodeABecameLeader := make(chan struct{})
	nodeBBecameLeader := make(chan struct{})

	// Setup Node A.
	quitA, cancelA := context.WithCancel(ctx)
	defer cancelA() // ensure cleanup
	doneA := make(chan error, 1)

	var onceA sync.Once
	lockA, err := New(spannerClient, tableName, lockName,
		WithDuration(leaseDuration),
		WithId("node-A"),
		WithLogger(log.New(os.Stdout, "[Node-A] ", log.LstdFlags)),
		WithLeaderCallback(nil, func(lctx context.Context, state LeaderState) {
			if state.Leader {
				t.Logf("Node A is leader, token: %d", state.Token)
				onceA.Do(func() {
					close(nodeABecameLeader)
				})
			} else {
				t.Log("Node A lost leadership")
			}
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create lockA: %v", err)
	}

	// Start Node A.
	lockA.Run(quitA, doneA)

	// Wait for Node A to become leader.
	select {
	case <-nodeABecameLeader:
	case <-time.After(10 * time.Second):
		t.Fatal("Node A did not become leader in time")
	}

	// Setup Node B.
	quitB, cancelB := context.WithCancel(ctx)
	defer cancelB() // ensure cleanup
	doneB := make(chan error, 1)

	var onceB sync.Once
	lockB, err := New(spannerClient, tableName, lockName,
		WithDuration(leaseDuration),
		WithId("node-B"),
		WithLogger(log.New(os.Stdout, "[Node-B] ", log.LstdFlags)),
		WithLeaderCallback(nil, func(lctx context.Context, state LeaderState) {
			mu.Lock()
			leaderB = state.Leader
			mu.Unlock()
			if state.Leader {
				t.Logf("Node B is leader, token: %d", state.Token)
				onceB.Do(func() {
					close(nodeBBecameLeader)
				})
			} else {
				t.Log("Node B lost leadership")
			}
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create lockB: %v", err)
	}

	// Start Node B.
	lockB.Run(quitB, doneB)

	// Wait a bit to ensure B doesn't take over prematurely.
	time.Sleep(time.Duration(leaseDuration) * time.Second)

	mu.Lock()
	if leaderB {
		mu.Unlock()
		t.Fatal("Node B became leader while Node A is still active")
	}
	mu.Unlock()

	t.Log("Cancelling Node A...")
	cancelA()
	<-doneA

	// Wait for Node B to become leader after A's lease expires.
	select {
	case <-nodeBBecameLeader:
		t.Log("Failover successful: Node B became leader")
	case <-time.After(time.Duration(leaseDuration)*2*time.Second + 10*time.Second):
		t.Fatal("Node B did not become leader after Node A was cancelled")
	}

	cancelB()
	<-doneB
}
