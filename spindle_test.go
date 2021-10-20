package spindle

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
)

func TestInstance(t *testing.T) {
	ctx := context.Background()
	dbstr := "projects/test-project/instances/test-instance/databases/testdb"
	client, err := spanner.NewClient(ctx, dbstr)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("client=%v", client)
	client.Close()
}
