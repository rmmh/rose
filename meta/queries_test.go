package meta

import (
	"bytes"
	"context"
	"testing"
)

func testChunkPlacement(fill byte, logicalLen int, vlogID uint32, vaddrOffset int64, compressedLen int) ChunkPlacement {
	return ChunkPlacement{
		Hash:          bytes.Repeat([]byte{fill}, 15),
		VlogID:        vlogID,
		VaddrOffset:   vaddrOffset,
		LogicalLen:    logicalLen,
		CompressedLen: compressedLen,
	}
}

func chunkRefcount(t *testing.T, db *DB, hash []byte) int {
	t.Helper()
	var refcount int
	if err := db.db.QueryRow("SELECT refcount FROM chunk WHERE hash = ?", hash).Scan(&refcount); err != nil {
		t.Fatalf("load refcount for %x: %v", hash, err)
	}
	return refcount
}

func TestCommitFileTransfersChunkRefs(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	a := testChunkPlacement('A', 10, 1, 100, 10)
	b := testChunkPlacement('B', 11, 1, 200, 11)
	c := testChunkPlacement('C', 12, 2, 300, 12)

	if _, err := db.CommitFile(ctx, "bucket/file", 1, []ChunkPlacement{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CommitFile(ctx, "bucket/file", 2, []ChunkPlacement{a, c}); err != nil {
		t.Fatal(err)
	}

	if got := chunkRefcount(t, db, a.Hash); got != 1 {
		t.Fatalf("refcount(A) = %d, want 1", got)
	}
	if got := chunkRefcount(t, db, b.Hash); got != 0 {
		t.Fatalf("refcount(B) = %d, want 0", got)
	}
	if got := chunkRefcount(t, db, c.Hash); got != 1 {
		t.Fatalf("refcount(C) = %d, want 1", got)
	}
}

func TestCommitFileCountsRepeatedChunkRefs(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	a := testChunkPlacement('A', 10, 7, 123, 10)
	if _, err := db.CommitFile(ctx, "bucket/file", 1, []ChunkPlacement{a, a}); err != nil {
		t.Fatal(err)
	}

	if got := chunkRefcount(t, db, a.Hash); got != 2 {
		t.Fatalf("refcount(A) = %d, want 2", got)
	}
	if got, ok, err := db.ChunkByHash(ctx, a.Hash); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("ChunkByHash did not find committed chunk")
	} else if got.VlogID != a.VlogID || got.VaddrOffset != a.VaddrOffset || got.LogicalLen != a.LogicalLen || got.CompressedLen != a.CompressedLen {
		t.Fatalf("ChunkByHash() = %+v, want %+v", got, a)
	}
}
