package server_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

// newServer builds an isolated in-process server backed by a temp meta DB and
// local plogs, for direct (non-gRPC) write-cache exercise.
func newServer(t *testing.T) *server.Server {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := server.NewServerWithDataDir(db, filepath.Join(dir, "plogs"))
	return s
}

var opKeySeq int

// writeAt opens a keyed write handle, applies the writes (offset,data) in the
// given order, optionally truncates first, then closes -- publishing one version.
func writeAt(t *testing.T, s *server.Server, path string, truncate int64, writes [][2]any) {
	t.Helper()
	ctx := context.Background()
	opKeySeq++
	key := fmt.Sprintf("test-op-%s-%d", path, opKeySeq)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if truncate >= 0 {
		if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: open.GetHandle(), Size: truncate}); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range writes {
		off := int64(w[0].(int))
		data := w[1].([]byte)
		if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: off, Buffer: data}); err != nil {
			t.Fatalf("write at %d: %v", off, err)
		}
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: key}); err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, s *server.Server, path string) []byte {
	t.Helper()
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()})
	return res.GetBuffer()
}

func TestWriteCacheOutOfOrderOverlapping(t *testing.T) {
	s := newServer(t)
	// Write three 4 KiB blocks out of order, with a final overlapping write that
	// must win on its overlapped bytes (last-writer-wins).
	a := bytes.Repeat([]byte("A"), 4096)
	b := bytes.Repeat([]byte("B"), 4096)
	c := bytes.Repeat([]byte("C"), 4096)
	overlap := bytes.Repeat([]byte("X"), 2048)
	writeAt(t, s, "/f", 0, [][2]any{
		{8192, c},
		{0, a},
		{4096, b},
		{4096, overlap}, // overwrites first half of block b
	})
	want := append(append(append([]byte{}, a...), overlap...), b[2048:]...)
	want = append(want, c...)
	if got := readAll(t, s, "/f"); !bytes.Equal(got, want) {
		t.Fatalf("assembled %d bytes, want %d (first mismatch region)", len(got), len(want))
	}
}

func TestWriteCacheOverwriteMiddle(t *testing.T) {
	s := newServer(t)
	base := bytes.Repeat([]byte("0123456789"), 4096) // 40 KiB, multiple chunks
	writeAt(t, s, "/f", 0, [][2]any{{0, base}})

	// Overwrite a middle range in place (no truncate: tail must be preserved).
	patch := bytes.Repeat([]byte("Z"), 1000)
	writeAt(t, s, "/f", -1, [][2]any{{20000, patch}})

	want := append([]byte{}, base...)
	copy(want[20000:], patch)
	if got := readAll(t, s, "/f"); !bytes.Equal(got, want) {
		t.Fatalf("overwrite-middle mismatch: got %d bytes want %d", len(got), len(want))
	}
}

func TestWriteCacheExtendAndTruncate(t *testing.T) {
	s := newServer(t)
	writeAt(t, s, "/f", 0, [][2]any{{0, []byte("hello")}})

	// Extend past EOF with a gap: the hole must read back as zero bytes.
	writeAt(t, s, "/f", -1, [][2]any{{10, []byte("world")}})
	want := []byte("hello\x00\x00\x00\x00\x00world")
	if got := readAll(t, s, "/f"); !bytes.Equal(got, want) {
		t.Fatalf("extend got %q want %q", got, want)
	}

	// Grow via truncate: trailing hole reads zero.
	ctx := context.Background()
	open, _ := s.Open(ctx, &pb.OpenRequest{Path: "/f", OperationKey: "grow"})
	if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: open.GetHandle(), Size: 20}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: "grow"}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/f"); len(got) != 20 || !bytes.Equal(got[:15], want) || !bytes.Equal(got[15:], make([]byte, 5)) {
		t.Fatalf("grow-truncate got %q (len %d)", got, len(got))
	}

	// Shrink via truncate.
	open, _ = s.Open(ctx, &pb.OpenRequest{Path: "/f", OperationKey: "shrink"})
	if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: open.GetHandle(), Size: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: "shrink"}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/f"); !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("shrink-truncate got %q", got)
	}
}

func TestWriteCacheReadYourWrites(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/f", OperationKey: "ryw"})
	if err != nil {
		t.Fatal(err)
	}
	h := open.GetHandle()
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h, Offset: 0, Buffer: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h, Offset: 5, Buffer: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	// Read on the still-open write handle sees the uncommitted overlay.
	res, err := s.Read(ctx, &pb.ReadRequest{Handle: h, Offset: 0, Length: 11})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(res.GetBuffer(), []byte("firstsecond")) {
		t.Fatalf("read-your-writes got %q", res.GetBuffer())
	}
	_, _ = s.Close(ctx, &pb.CloseRequest{Handle: h, IdempotencyKey: "ryw"})
}

// TestWriteCacheLargeAppendSpills writes a file larger than the spill threshold
// as many kernel-sized chunks, so the cache spills durable chunks mid-stream and
// the final splice stitches the settled prefix to the in-memory tail. The content
// must round-trip exactly.
func TestWriteCacheLargeAppendSpills(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	const total = 12 << 20 // 12 MiB > spill threshold
	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i*1103515245 + 12345)
	}
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/big", OperationKey: "big"})
	if err != nil {
		t.Fatal(err)
	}
	h := open.GetHandle()
	const block = 128 << 10 // emulate go-fuse's split WRITE size
	for off := 0; off < total; off += block {
		end := off + block
		if end > total {
			end = total
		}
		if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h, Offset: int64(off), Buffer: data[off:end]}); err != nil {
			t.Fatalf("write at %d: %v", off, err)
		}
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h, IdempotencyKey: "big"}); err != nil {
		t.Fatal(err)
	}
	read, err := s.Open(ctx, &pb.OpenRequest{Path: "/big"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(ctx, &pb.ReadRequest{Handle: read.GetHandle(), Offset: 0, Length: total})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(res.GetBuffer(), data) {
		t.Fatalf("large append round-trip mismatch: got %d bytes want %d", len(res.GetBuffer()), len(data))
	}
}

// TestWriteCacheSpliceDedup verifies that editing one region of a multi-chunk
// file reuses the untouched chunks (no new vlog bytes for them): only the
// modified window's bytes are newly stored.
func TestWriteCacheSpliceDedup(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	// Random, incompressible content so FastCDC yields several distinct chunks.
	base := make([]byte, 256<<10)
	for i := range base {
		base[i] = byte(i*2654435761 + i>>3)
	}
	writeAt(t, s, "/f", 0, [][2]any{{0, base}})

	usageBefore, err := s.GetDB().VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var liveBefore int64
	for _, u := range usageBefore {
		liveBefore += u.LiveBytes
	}

	// Edit a small 64-byte window near the middle, preserving everything else.
	patch := bytes.Repeat([]byte("!"), 64)
	writeAt(t, s, "/f", -1, [][2]any{{128 << 10, patch}})

	want := append([]byte{}, base...)
	copy(want[128<<10:], patch)
	if got := readAll(t, s, "/f"); !bytes.Equal(got, want) {
		t.Fatalf("spliced content mismatch: got %d bytes want %d", len(got), len(want))
	}

	// After the edit (before GC), live bytes should have grown only by roughly the
	// re-chunked window, not the whole file: the untouched chunks were reused by
	// hash rather than rewritten.
	usageAfter, err := s.GetDB().VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var liveAfter int64
	for _, u := range usageAfter {
		liveAfter += u.LiveBytes
	}
	added := liveAfter - liveBefore
	if added <= 0 || added > int64(len(base)/2) {
		t.Fatalf("edit added %d live bytes; expected a small window re-chunk well under half the %d-byte file", added, len(base))
	}
}
