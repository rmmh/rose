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

func TestWriteOpChunkBatchHelpers(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	op, err := db.CreateWriteOp(ctx, "op-batch", "/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []WriteOpChunk{
		{Index: 0, Data: []byte("a"), Hash: []byte("hash-a"), VlogID: 1, VaddrOffset: 0, LogicalLen: 1},
		{Index: 1, Data: []byte("b"), Hash: []byte("hash-b"), VlogID: 1, VaddrOffset: 1, LogicalLen: 1},
	}
	if err := db.AppendWriteOpChunks(ctx, op.ID, chunks); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkWriteOpChunksDurable(ctx, op.ID, []int{0, 1}); err != nil {
		t.Fatal(err)
	}
	got, err := db.WriteOpChunks(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d chunks, want 2", len(got))
	}
	for i, chunk := range got {
		if chunk.State != WriteChunkDurable {
			t.Fatalf("chunk %d state = %q, want %q", i, chunk.State, WriteChunkDurable)
		}
	}
	// Re-marking an already durable batch should be idempotent.
	if err := db.MarkWriteOpChunksDurable(ctx, op.ID, []int{0, 1}); err != nil {
		t.Fatal(err)
	}
}
