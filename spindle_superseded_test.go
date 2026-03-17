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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestSpannerEmulatorTokenSuperseded(t *testing.T) {
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
	dbID := fmt.Sprintf("test-db-super-%d", time.Now().UnixNano())

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
		if status.Code(err) != codes.AlreadyExists {
			t.Logf("CreateInstance err: %v", err)
		}
	} else {
		_, _ = createInstanceOp.Wait(ctx)
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
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("Failed to create database: %v", err)
		}
	} else {
		if _, err := createDbOp.Wait(ctx); err != nil {
			t.Fatalf("CreateDatabase wait failed: %v", err)
		}
	}

	spannerClient, err := spanner.NewClient(ctx, dbPath, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	tableName := "test_lock_table"
	lockName := "my_test_superseded_lock"
	leaseDuration := int64(5) // 5 seconds

	var mu sync.Mutex
	var leaderToken int64
	var nodeALostLeadership bool

	nodeALostLeadershipChan := make(chan struct{})

	quitA, cancelA := context.WithCancel(ctx)
	defer cancelA() // ensure cleanup
	doneA := make(chan error, 1)

	lockA, err := New(spannerClient, tableName, lockName,
		WithDuration(leaseDuration),
		WithId("node-A"),
		WithLogger(log.New(os.Stdout, "[Node-A] ", log.LstdFlags)),
		WithLeaderCallback(nil, func(lctx context.Context, state LeaderState) {
			mu.Lock()
			defer mu.Unlock()
			if state.Leader {
				t.Logf("Node A is leader, token: %d", state.Token)
				leaderToken = state.Token
			} else {
				t.Log("Node A lost leadership")
				if leaderToken > 0 && !nodeALostLeadership {
					nodeALostLeadership = true
					close(nodeALostLeadershipChan)
				}
			}
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create lockA: %v", err)
	}

	// Start Node A.
	lockA.Run(quitA, doneA)

	// Wait for Node A to become leader.
	requireLeader := func() {
		for i := 0; i < 50; i++ { // wait up to 5 seconds
			mu.Lock()
			hasLeader := leaderToken > 0
			mu.Unlock()
			if hasLeader {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("Node A did not become leader in time")
	}
	requireLeader()

	// Simulate rogue process artificially advancing token (superseding it)
	t.Log("Manually advancing the lock token to simulate being superseded...")
	_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL:    fmt.Sprintf("UPDATE %s SET token = PENDING_COMMIT_TIMESTAMP() WHERE name = @name", tableName),
			Params: map[string]any{"name": lockName},
		}
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		t.Fatalf("Failed to manually update token: %v", err)
	}

	// The next heartbeat for Node A will fail with ErrTokenSuperseded
	// It should lose leadership immediately.
	select {
	case <-nodeALostLeadershipChan:
		t.Log("Success: Node A lost leadership after its token was superseded")
	case <-time.After(time.Duration(leaseDuration)*time.Second + 2*time.Second):
		t.Fatalf("Node A did not lose leadership in time after token was superseded")
	}

	cancelA()
	<-doneA
}
