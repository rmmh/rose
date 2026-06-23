package server

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func collectChunkSizes(t *testing.T, chunker *chunkers.Chunker) []int {
	t.Helper()
	var sizes []int
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			t.Fatalf("chunker.Next(): %v", err)
		}
		if len(chunk) > 0 {
			sizes = append(sizes, len(chunk))
		}
		if err == io.EOF {
			return sizes
		}
	}
}

func TestFileHandleChunkerResetReusesChunker(t *testing.T) {
	first := bytes.Repeat([]byte("abcdef0123456789"), 96*1024)
	second := bytes.Repeat([]byte("zyxwvu9876543210"), 80*1024)

	h := &FileHandle{}
	ch1, err := h.chunkerForData(first)
	if err != nil {
		t.Fatal(err)
	}
	firstSizes := collectChunkSizes(t, ch1)

	ch2, err := h.chunkerForData(second)
	if err != nil {
		t.Fatal(err)
	}
	if ch1 != ch2 {
		t.Fatal("chunkerForData allocated a new chunker instead of resetting the existing one")
	}
	reusedSizes := collectChunkSizes(t, ch2)

	var freshInput bytes.Reader
	freshInput.Reset(second)
	fresh, err := chunkers.NewChunker("fastcdc", &freshInput, &chunkers.ChunkerOpts{
		MinSize:    chunkMinSize,
		NormalSize: chunkNormalSize,
		MaxSize:    chunkMaxSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	freshSizes := collectChunkSizes(t, fresh)

	if !bytes.Equal(intsToBytes(reusedSizes), intsToBytes(freshSizes)) {
		t.Fatalf("reset chunk sizes = %v, want %v", reusedSizes, freshSizes)
	}
	if len(firstSizes) == 0 || len(reusedSizes) == 0 {
		t.Fatal("expected non-empty chunk output")
	}
}

func intsToBytes(v []int) []byte {
	out := make([]byte, 0, len(v)*4)
	for _, n := range v {
		out = append(out, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return out
}

func TestSealChunksWritesAdjacentPendingChunksAsVectors(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewServerWithDataDir(db, filepath.Join(dir, "plogs"))
	ctx := context.Background()
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.StopMaintenanceDriver()

	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/f", OperationKey: "seal-adjacent"})
	if err != nil {
		t.Fatal(err)
	}
	h := s.handles[open.GetHandle()]

	vlogID, v, err := s.activeVlogForAppend(ctx, bucketOf(h.path()), 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.ClaimVlogLease(ctx, vlogID, h.writeOpID, 0); err != nil {
		t.Fatal(err)
	}
	start := v.Length()
	chunks := []pendingChunk{
		{vlogID: vlogID, vaddr: start, data: []byte("hello")},
		{vlogID: vlogID, vaddr: start + 5, data: []byte("world")},
		{vlogID: vlogID, vaddr: start + 10, data: []byte("!!")},
	}
	if err := s.sealChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	got, err := v.Read(ctx, start, 12)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("helloworld!!")) {
		t.Fatalf("sealed bytes = %q, want %q", got, []byte("helloworld!!"))
	}
}
