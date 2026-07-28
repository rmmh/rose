package meta

import (
	"bytes"
	"context"
	"math"
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
	badHash := a
	badHash.Hash = badHash.Hash[:len(badHash.Hash)-1]
	if _, err := db.CommitFile(ctx, "bucket/bad-hash", 1, []ChunkPlacement{badHash}); err == nil {
		t.Fatal("commit with a short chunk hash succeeded")
	}
	badLength := a
	badLength.LogicalLen = -1
	if _, err := db.CommitFile(ctx, "bucket/bad-length", 1, []ChunkPlacement{badLength}); err == nil {
		t.Fatal("commit with a negative logical chunk length succeeded")
	}
	badOffset := a
	badOffset.VaddrOffset = -1
	if _, err := db.CommitFile(ctx, "bucket/bad-offset", 1, []ChunkPlacement{badOffset}); err == nil {
		t.Fatal("commit with a negative virtual-log offset succeeded")
	}
	badVlog := a
	badVlog.VlogID = 0
	if _, err := db.CommitFile(ctx, "bucket/bad-vlog", 1, []ChunkPlacement{badVlog}); err == nil {
		t.Fatal("commit with the zero virtual-log id succeeded")
	}
	badCompressed := a
	badCompressed.CompressedLen = -1
	if _, err := db.CommitFile(ctx, "bucket/bad-compressed", 1, []ChunkPlacement{badCompressed}); err == nil {
		t.Fatal("commit with a negative compressed chunk length succeeded")
	}
	zeroLength := a
	zeroLength.LogicalLen = 0
	if _, err := db.CommitFile(ctx, "bucket/zero-length", 1, []ChunkPlacement{zeroLength}); err == nil {
		t.Fatal("commit with a zero-length chunk placement succeeded")
	}
	zeroCompressed := a
	zeroCompressed.CompressedLen = 0
	if _, err := db.CommitFile(ctx, "bucket/zero-compressed", 1, []ChunkPlacement{zeroCompressed}); err == nil {
		t.Fatal("commit with a zero compressed chunk length succeeded")
	}
	overflowingRange := a
	overflowingRange.VaddrOffset = math.MaxInt64
	if _, err := db.CommitFile(ctx, "bucket/overflowing-range", 1, []ChunkPlacement{overflowingRange}); err == nil {
		t.Fatal("commit with an overflowing virtual-log range succeeded")
	}

	if _, err := db.CommitFile(ctx, "bucket/file", 1, []ChunkPlacement{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CommitFile(ctx, "bucket/file", 2, []ChunkPlacement{a, c}); err != nil {
		t.Fatal(err)
	}
	conflictingA := a
	conflictingA.LogicalLen++
	if _, err := db.CommitFile(ctx, "bucket/hash-collision", 3, []ChunkPlacement{conflictingA}); err == nil {
		t.Fatal("commit reused a hash with conflicting placement geometry")
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
