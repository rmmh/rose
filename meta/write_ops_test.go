package meta

import (
	"context"
	"fmt"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestWriteOpIdempotentKeyAndVlogLease(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	op, err := db.CreateWriteOp(ctx, "op-1", "/file")
	if err != nil {
		t.Fatal(err)
	}
	// The same client key is the same operation, not a second append plan.
	again, err := db.CreateWriteOp(ctx, "op-1", "/file")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != op.ID {
		t.Fatalf("duplicate key created op %d, want %d", again.ID, op.ID)
	}
	if _, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.ClaimVlogLease(ctx, 1, op.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.ClaimVlogLease(ctx, 1, op.ID, 1); err == nil {
		t.Fatal("second lease of same vlog succeeded")
	}
	if err := db.ReleaseWriteOpLeases(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.ClaimVlogLease(ctx, 1, op.ID, 1); err != nil {
		t.Fatal(err)
	}
}

func TestCreateWriteOpRejectsNamespaceRoot(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i, path := range []string{"", "/"} {
		if _, err := db.CreateWriteOp(context.Background(), fmt.Sprintf("root-op-%d", i), path); err == nil {
			t.Errorf("CreateWriteOp path %q succeeded", path)
		}
	}
	var count int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM write_op").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rejected root operations left %d rows", count)
	}
}
