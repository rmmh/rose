package server_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
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

func TestFlushHandlePublishesWithoutClosing(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/f", OperationKey: "flush-first"})
	if err != nil {
		t.Fatal(err)
	}
	h := open.GetHandle()
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h, Offset: 0, Buffer: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushHandle(ctx, h); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/f"); !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("after flush got %q, want %q", got, "hello")
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h, Offset: 5, Buffer: []byte(" world")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/f"); !bytes.Equal(got, []byte("hello world")) {
		t.Fatalf("after close got %q, want %q", got, "hello world")
	}
}

func TestFlushRetryHandleConvergesAfterPeerCommit(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	first, err := s.Open(ctx, &pb.OpenRequest{Path: "/peer-flush", OperationKey: "peer-flush"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Open(ctx, &pb.OpenRequest{Path: "/peer-flush", OperationKey: "peer-flush"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: first.GetHandle(), Buffer: []byte("committed")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: first.GetHandle(), IdempotencyKey: "peer-flush"}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushHandle(ctx, second.GetHandle()); err != nil {
		t.Fatal(err)
	}
	read, err := s.Read(ctx, &pb.ReadRequest{Handle: second.GetHandle(), Length: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), []byte("committed")) {
		t.Fatalf("retry handle after flush read %q, want committed", read.GetBuffer())
	}
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
	// Sized to span several ~1MB chunks so a small edit re-chunks only a window.
	base := make([]byte, 8<<20)
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
	writeAt(t, s, "/f", -1, [][2]any{{4 << 20, patch}})

	want := append([]byte{}, base...)
	copy(want[4<<20:], patch)
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

// TestGetattrHandleReflectsUncommittedWrites verifies that a stat against an
// open write handle reports the uncommitted length (read-your-writes), while a
// path-only stat still sees the last committed version until Close.
func TestGetattrHandleReflectsUncommittedWrites(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	const path = "/dir/file"
	opKeySeq++
	key := fmt.Sprintf("test-op-%s-%d", path, opKeySeq)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: key})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 4096)
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: 0, Buffer: data}); err != nil {
		t.Fatal(err)
	}

	// Path-only stat: nothing committed yet, so size is 0.
	if resp, err := s.Getattr(ctx, &pb.GetattrRequest{Path: path}); err == nil && resp.GetSize() != 0 {
		t.Fatalf("path-only getattr size = %d, want 0 (uncommitted)", resp.GetSize())
	}

	// Handle stat: must reflect the bytes written into the cache.
	resp, err := s.Getattr(ctx, &pb.GetattrRequest{Path: path, Handle: open.GetHandle()})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSize() != int64(len(data)) {
		t.Fatalf("handle getattr size = %d, want %d", resp.GetSize(), len(data))
	}

	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: key}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, path); len(got) != len(data) {
		t.Fatalf("after close size = %d, want %d", len(got), len(data))
	}
}

// TestRenameOpenUncommittedHandle reproduces rsync's sequence: create a temp
// file, write into it, then rename it into place *before* closing the write
// handle. The source has no committed file head yet, so the rename must retarget
// the live handle rather than failing with EIO; Close then publishes the data at
// the destination path.
func TestRenameOpenUncommittedHandle(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	const tmp = "/.b.mp4.tmp"
	const final = "/b.mp4"
	opKeySeq++
	key := fmt.Sprintf("test-op-%s-%d", tmp, opKeySeq)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: tmp, OperationKey: key})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("z"), 559906)
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: 0, Buffer: data}); err != nil {
		t.Fatal(err)
	}

	// Rename before Close: the temp path has no committed head, yet this must
	// succeed by retargeting the open handle.
	if _, err := s.Rename(ctx, &pb.RenameRequest{OldPath: tmp, NewPath: final}); err != nil {
		t.Fatalf("rename of open uncommitted file: %v", err)
	}

	// Closing the (retargeted) handle publishes the data at the destination.
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: key}); err != nil {
		t.Fatal(err)
	}

	if got := readAll(t, s, final); !bytes.Equal(got, data) {
		t.Fatalf("destination size = %d, want %d", len(got), len(data))
	}
	// The temp path must not be resurrected by the Close.
	if _, ok, err := statPath(t, s, tmp); err == nil && ok {
		t.Fatalf("temp path %s still exists after rename+close", tmp)
	}
}

func TestUnlinkOpenWriterDoesNotRepublishPathOnClose(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	const path = "/deleted-while-open"
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: "unlink-open-writer"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{
		Handle: open.GetHandle(),
		Buffer: []byte("must not be relinked"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: path}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{
		Handle:         open.GetHandle(),
		IdempotencyKey: "unlink-open-writer",
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := statPath(t, s, path); err == nil && ok {
		t.Fatal("Close republished a path unlinked while its writer was open")
	}
}

func TestRmdirDoesNotLetOpenDescendantRecreateSubtree(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	if _, err := s.Mkdir(ctx, &pb.MkdirRequest{Path: "/removed-dir"}); err != nil {
		t.Fatal(err)
	}
	open, err := s.Open(ctx, &pb.OpenRequest{
		Path:         "/removed-dir/pending",
		OperationKey: "rmdir-open-descendant",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{
		Handle: open.GetHandle(),
		Buffer: []byte("pending"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rmdir(ctx, &pb.RmdirRequest{Path: "/removed-dir"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/removed-dir", "/removed-dir/pending"} {
		if _, ok, err := statPath(t, s, path); err == nil && ok {
			t.Fatalf("Close recreated removed path %q", path)
		}
	}
}

func TestRenameDirectoryRetargetsOpenDescendantHandle(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	if _, err := s.Mkdir(ctx, &pb.MkdirRequest{Path: "/old"}); err != nil {
		t.Fatal(err)
	}
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/old/file", OperationKey: "rename-dir-open"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: []byte("pending")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rename(ctx, &pb.RenameRequest{OldPath: "/old", NewPath: "/new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/new/file"); !bytes.Equal(got, []byte("pending")) {
		t.Fatalf("renamed descendant got %q, want %q", got, "pending")
	}
	if _, ok, _ := statPath(t, s, "/old/file"); ok {
		t.Fatal("close resurrected open descendant under old directory")
	}
}

func TestWriteRejectsInvalidRangesWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/ranges"})
	if err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{-1, math.MaxInt64} {
		if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: off, Buffer: []byte("xx")}); err == nil {
			t.Fatalf("write offset %d succeeded", off)
		}
	}
}

func TestReadRejectsInvalidRangesWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	writeAt(t, s, "/ranges-read", -1, [][2]any{{0, []byte("contents")}})
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/ranges-read"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []pb.ReadRequest{
		{Handle: open.GetHandle(), Offset: -1, Length: 1},
		{Handle: open.GetHandle(), Offset: 0, Length: -1},
		{Handle: open.GetHandle(), Offset: math.MaxInt64, Length: 2},
	}
	for _, req := range tests {
		if _, err := s.Read(ctx, &req); err == nil {
			t.Fatalf("read offset=%d length=%d succeeded", req.GetOffset(), req.GetLength())
		}
	}
}

func TestHugeReadLengthStopsAtCommittedEOF(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	writeAt(t, s, "/huge-read", -1, [][2]any{{0, []byte("contents")}})
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/huge-read"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, &pb.ReadRequest{
		Handle: open.GetHandle(),
		Length: math.MaxInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetBuffer(), []byte("contents")) {
		t.Fatalf("huge read returned %q, want contents", got.GetBuffer())
	}
}

func TestZeroLengthWriteDoesNotExtendFile(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	writeAt(t, s, "/zero-write", -1, [][2]any{{0, []byte("base")}})

	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/zero-write"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	attr, err := s.Getattr(ctx, &pb.GetattrRequest{Path: "/zero-write", Handle: open.GetHandle()})
	if err != nil {
		t.Fatal(err)
	}
	if attr.GetSize() != 4 {
		t.Fatalf("size after zero-length write = %d, want 4", attr.GetSize())
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/zero-write"); !bytes.Equal(got, []byte("base")) {
		t.Fatalf("content after zero-length write = %q, want base", got)
	}
}

func TestWriteCacheDeterministicStateMachine(t *testing.T) {
	const (
		steps   = 400
		maxSize = 512
	)
	ctx := context.Background()
	s := newServer(t)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/state-machine", OperationKey: "state-machine-initial"})
	if err != nil {
		t.Fatal(err)
	}
	handle := open.GetHandle()
	rng := rand.New(rand.NewSource(0x5eed))
	var oracle []byte
	var history []string

	for step := 0; step < steps; step++ {
		switch rng.Intn(4) {
		case 0, 1:
			off := rng.Intn(maxSize)
			data := make([]byte, rng.Intn(33))
			if _, err := rng.Read(data); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(ctx, &pb.WriteRequest{Handle: handle, Offset: int64(off), Buffer: data}); err != nil {
				t.Fatalf("step %d write: %v", step, err)
			}
			history = append(history, fmt.Sprintf("%d: write off=%d len=%d", step, off, len(data)))
			if len(data) > 0 {
				end := off + len(data)
				if end > len(oracle) {
					oracle = append(oracle, make([]byte, end-len(oracle))...)
				}
				copy(oracle[off:end], data)
			}
		case 2:
			size := rng.Intn(maxSize + 1)
			if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: handle, Size: int64(size)}); err != nil {
				t.Fatalf("step %d truncate: %v", step, err)
			}
			history = append(history, fmt.Sprintf("%d: truncate size=%d", step, size))
			if size < len(oracle) {
				oracle = oracle[:size]
			} else {
				oracle = append(oracle, make([]byte, size-len(oracle))...)
			}
		case 3:
			off := rng.Intn(maxSize)
			length := rng.Intn(65)
			got, err := s.Read(ctx, &pb.ReadRequest{Handle: handle, Offset: int64(off), Length: int64(length)})
			if err != nil {
				t.Fatalf("step %d read: %v", step, err)
			}
			end := min(off+length, len(oracle))
			var want []byte
			if off < len(oracle) {
				want = oracle[off:end]
			}
			if !bytes.Equal(got.GetBuffer(), want) {
				t.Fatalf("step %d read [%d,%d): got %x, want %x", step, off, off+length, got.GetBuffer(), want)
			}
			history = append(history, fmt.Sprintf("%d: read off=%d len=%d", step, off, length))
		}
		if step%23 == 22 {
			if err := s.FlushHandle(ctx, handle); err != nil {
				t.Fatalf("step %d flush: %v", step, err)
			}
			history = append(history, fmt.Sprintf("%d: flush", step))
		}
		got, err := s.Read(ctx, &pb.ReadRequest{Handle: handle, Length: int64(len(oracle) + 1)})
		if err != nil {
			t.Fatalf("step %d verify read: %v", step, err)
		}
		if !bytes.Equal(got.GetBuffer(), oracle) {
			start := max(0, len(history)-20)
			t.Fatalf("step %d full content mismatch: got len=%d, want len=%d\ntrace:\n%s",
				step, len(got.GetBuffer()), len(oracle), strings.Join(history[start:], "\n"))
		}
		attr, err := s.Getattr(ctx, &pb.GetattrRequest{Path: "/state-machine", Handle: handle})
		if err != nil {
			t.Fatalf("step %d getattr: %v", step, err)
		}
		if attr.GetSize() != int64(len(oracle)) {
			t.Fatalf("step %d size = %d, want %d", step, attr.GetSize(), len(oracle))
		}
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: handle}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/state-machine"); !bytes.Equal(got, oracle) {
		t.Fatalf("reopened content mismatch: got %x, want %x", got, oracle)
	}
}

func TestConcurrentFirstWritesShareInitializedOperation(t *testing.T) {
	const writers = 32
	s := newServer(t)
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/concurrent-first-write"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Write(ctx, &pb.WriteRequest{
				Handle: open.GetHandle(),
				Offset: int64(i),
				Buffer: []byte{byte(i + 1)},
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, writers)
	for i := range want {
		want[i] = byte(i + 1)
	}
	if got := readAll(t, s, "/concurrent-first-write"); !bytes.Equal(got, want) {
		t.Fatalf("concurrent content = %x, want %x", got, want)
	}
}

func TestConcurrentFirstWriteAndReadSynchronizeCache(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		open, err := s.Open(ctx, &pb.OpenRequest{Path: fmt.Sprintf("/write-read-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: []byte("x")})
			errs <- err
		}()
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Length: 1})
			errs <- err
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTruncateThroughSpilledChunkPreservesPrefix(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/spill-truncate"})
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 5<<20)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	for off := 0; off < len(data); off += 128 << 10 {
		end := min(off+(128<<10), len(data))
		if _, err := s.Write(ctx, &pb.WriteRequest{
			Handle: open.GetHandle(),
			Offset: int64(off),
			Buffer: data[off:end],
		}); err != nil {
			t.Fatal(err)
		}
	}
	const truncated = 2_000_003
	if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: open.GetHandle(), Size: truncated}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Length: truncated})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetBuffer(), data[:truncated]) {
		t.Fatalf("read after truncating spilled data does not preserve prefix")
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "/spill-truncate"); !bytes.Equal(got, data[:truncated]) {
		t.Fatal("committed truncate lost prefix bytes")
	}
}

// statPath reports whether a path resolves, via Getattr.
func statPath(t *testing.T, s *server.Server, path string) (int64, bool, error) {
	t.Helper()
	resp, err := s.Getattr(context.Background(), &pb.GetattrRequest{Path: path})
	if err != nil {
		return 0, false, err
	}
	return resp.GetSize(), true, nil
}
