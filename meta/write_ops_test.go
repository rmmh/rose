package meta

import (
	"context"
	"testing"
)

func TestWriteOpPersistsProgressAndVlogLease(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	op, err := db.CreateWriteOp(ctx, "op-1", "/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	// The same client key is the same operation, not a second append plan.
	again, err := db.CreateWriteOp(ctx, "op-1", "/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != op.ID {
		t.Fatalf("duplicate key created op %d, want %d", again.ID, op.ID)
	}
	if err := db.UpdateWriteOpStream(ctx, op.ID, 4096, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	var offset int64
	var tail []byte
	if err := db.db.QueryRowContext(ctx, "SELECT acknowledged_offset, tail FROM write_op WHERE id = ?", op.ID).Scan(&offset, &tail); err != nil {
		t.Fatal(err)
	}
	if offset != 4096 || string(tail) != "tail" {
		t.Fatalf("stream state = (%d, %q)", offset, tail)
	}
	if _, err := db.MakeVlog(ctx, "NONE", 1, 0); err != nil {
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
