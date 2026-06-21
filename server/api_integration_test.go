package server_test

import (
	"bytes"
	"context"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newClient(t *testing.T) pb.RoseClient {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterRoseServer(grpcServer, server.NewServerWithDataDir(db, filepath.Join(dir, "plogs")))
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///rose", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewRoseClient(conn)
}

func writeFile(t *testing.T, client pb.RoseClient, path string, data []byte) {
	t.Helper()
	ctx := context.Background()
	open, err := client.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
}

func readHandle(t *testing.T, client pb.RoseClient, handle int64) []byte {
	t.Helper()
	res, err := client.Read(context.Background(), &pb.ReadRequest{Handle: handle, Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetBuffer()
}

func TestFileSnapshotNamespaceLifecycleOverGRPC(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	writeFile(t, client, "/alpha", []byte("first version"))
	snapshot, err := client.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "before-rewrite"})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, client, "/alpha", []byte("second version"))

	snapOpen, err := client.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshot.GetSnapshotId(), Path: "/alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if got := readHandle(t, client, snapOpen.GetHandle()); !bytes.Equal(got, []byte("first version")) {
		t.Fatalf("snapshot read = %q, want first version", got)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: snapOpen.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Rename(ctx, &pb.RenameRequest{OldPath: "/alpha", NewPath: "/beta"}); err != nil {
		t.Fatal(err)
	}
	live, err := client.Open(ctx, &pb.OpenRequest{Path: "/beta"})
	if err != nil {
		t.Fatal(err)
	}
	if got := readHandle(t, client, live.GetHandle()); !bytes.Equal(got, []byte("second version")) {
		t.Fatalf("live read = %q, want second version", got)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: live.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Unlink(ctx, &pb.UnlinkRequest{Path: "/beta"}); err != nil {
		t.Fatal(err)
	}
	retained, err := client.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshot.GetSnapshotId(), Path: "/alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if got := readHandle(t, client, retained.GetHandle()); !bytes.Equal(got, []byte("first version")) {
		t.Fatalf("snapshot after unlink = %q, want first version", got)
	}
	if _, err := client.Write(ctx, &pb.WriteRequest{Handle: retained.GetHandle(), Buffer: []byte("forbidden")}); err == nil {
		t.Fatal("snapshot write unexpectedly succeeded")
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: retained.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{SnapshotId: snapshot.GetSnapshotId()}); err != nil {
		t.Fatal(err)
	}
}

func TestPlogCommitOverGRPC(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	plog, err := client.MakePlog(ctx, &pb.MakePlogRequest{DiskId: 1})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("durable plog payload")
	write, err := client.WritePlog(ctx, &pb.WritePlogRequest{PlogId: plog.GetPlogId(), TxnId: 7, Buffer: data})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CommitPlog(ctx, &pb.CommitPlogRequest{TxnId: 7}); err != nil {
		t.Fatal(err)
	}
	read, err := client.ReadPlog(ctx, &pb.ReadPlogRequest{PlogId: plog.GetPlogId(), Offset: write.GetOffset(), Length: uint32(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), data) {
		t.Fatalf("plog read = %q, want %q", read.GetBuffer(), data)
	}
}

func TestVlogCommitOverGRPC(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	vlog, err := client.MakeVlog(ctx, &pb.MakeVlogRequest{ProtectionScheme: "DUPLICATE", DataShards: 1})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("durable vlog payload")
	write, err := client.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: vlog.GetVlogId(), TxnId: 8, Buffer: data})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 8}); err != nil {
		t.Fatal(err)
	}
	read, err := client.ReadVlog(ctx, &pb.ReadVlogRequest{VlogId: vlog.GetVlogId(), Offset: write.GetOffset(), Length: uint32(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), data) {
		t.Fatalf("vlog read = %q, want %q", read.GetBuffer(), data)
	}
}

func TestRecoverReopensPersistedVlogs(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	first := server.NewServerWithDataDir(db, filepath.Join(dir, "disk1"))
	opened, err := first.Open(ctx, &pb.OpenRequest{Path: "/recovered"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write(ctx, &pb.WriteRequest{Handle: opened.GetHandle(), Buffer: []byte("survives restart")}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Close(ctx, &pb.CloseRequest{Handle: opened.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	restarted := server.NewServerWithDataDir(db, filepath.Join(dir, "disk1"))
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := restarted.Open(ctx, &pb.OpenRequest{Path: "/recovered"})
	if err != nil {
		t.Fatal(err)
	}
	read, err := restarted.Read(ctx, &pb.ReadRequest{Handle: reopened.GetHandle(), Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if got := read.GetBuffer(); !bytes.Equal(got, []byte("survives restart")) {
		t.Fatalf("recovered read = %q", got)
	}
}

func TestGCReclaimsOnlyUnreferencedChunks(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := server.NewServerWithDataDir(db, filepath.Join(dir, "disk"))
	ctx := context.Background()

	writeServerFile(t, s, "/a", []byte("alpha content"))
	snap, err := s.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "snap"})
	if err != nil {
		t.Fatal(err)
	}
	// Overwrite the live head; the old chunk is now reachable only via snapshot.
	writeServerFile(t, s, "/a", []byte("beta content"))

	// Nothing is collectable: old chunk pinned by the snapshot, new by the head.
	if n, err := s.GC(ctx); err != nil || n != 0 {
		t.Fatalf("premature GC collected %d (err=%v), want 0", n, err)
	}
	// Both the snapshot and live versions still read correctly.
	if got := readSnapshotFile(t, s, snap.GetSnapshotId(), "/a"); !bytes.Equal(got, []byte("alpha content")) {
		t.Fatalf("snapshot read = %q", got)
	}
	if got := readServerFile(t, s, "/a"); !bytes.Equal(got, []byte("beta content")) {
		t.Fatalf("live read = %q", got)
	}

	// Releasing the snapshot drops the old chunk to refcount zero.
	if _, err := s.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{SnapshotId: snap.GetSnapshotId()}); err != nil {
		t.Fatal(err)
	}
	n, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("GC collected nothing after snapshot release")
	}
	// The live version is untouched, and GC is idempotent.
	if got := readServerFile(t, s, "/a"); !bytes.Equal(got, []byte("beta content")) {
		t.Fatalf("live read after GC = %q", got)
	}
	if again, err := s.GC(ctx); err != nil || again != 0 {
		t.Fatalf("second GC collected %d (err=%v), want 0", again, err)
	}
}

func writeServerFile(t *testing.T, s *server.Server, path string, data []byte) {
	t.Helper()
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
}

func readServerFile(t *testing.T, s *server.Server, path string) []byte {
	t.Helper()
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetBuffer()
}

func readSnapshotFile(t *testing.T, s *server.Server, snapshotID uint64, path string) []byte {
	t.Helper()
	ctx := context.Background()
	open, err := s.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshotID, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetBuffer()
}

func TestScrubFlagsCorruptionAndReplicaServesGoodData(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	disk1 := filepath.Join(dir, "disk1")
	disk2 := filepath.Join(dir, "disk2")
	s := server.NewServerWithDiskRoots(db, map[uint32]string{1: disk1, 2: disk2})
	ctx := context.Background()

	// A payload larger than one hash-protected block (>1MB) so its first sectors
	// are sealed with durable on-disk hashes that scrub can validate.
	payload := make([]byte, 1<<20+128*1024)
	rng := rand.New(rand.NewSource(7))
	rng.Read(payload)

	opened, err := s.Open(ctx, &pb.OpenRequest{Path: "/big"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: opened.GetHandle(), Buffer: payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: opened.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	// A clean scrub before corruption.
	clean, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range clean {
		for _, shard := range v.Shards {
			if !shard.Result.Healthy() {
				t.Fatalf("pre-corruption scrub unhealthy: %+v", shard)
			}
		}
	}

	// Corrupt the first replica's plog on disk.
	entries, err := os.ReadDir(disk1)
	if err != nil || len(entries) == 0 {
		t.Fatalf("read disk1: %v", err)
	}
	corruptFileByte(t, filepath.Join(disk1, entries[0].Name()), 100)

	scrubbed, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	var sawCorruption bool
	for _, v := range scrubbed {
		for _, shard := range v.Shards {
			if len(shard.Result.CorruptSectors) > 0 {
				sawCorruption = true
			}
		}
	}
	if !sawCorruption {
		t.Fatal("scrub did not detect injected corruption")
	}

	// The duplicate replica still serves correct data despite the corruption.
	reopened, err := s.Open(ctx, &pb.OpenRequest{Path: "/big"})
	if err != nil {
		t.Fatal(err)
	}
	read, err := s.Read(ctx, &pb.ReadRequest{Handle: reopened.GetHandle(), Offset: 0, Length: 256})
	if err != nil {
		t.Fatalf("read after corruption: %v", err)
	}
	if !bytes.Equal(read.GetBuffer(), payload[:256]) {
		t.Fatal("read after corruption returned wrong data")
	}
}

func corruptFileByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func TestDuplicatePlacementUsesEveryConfiguredDisk(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	disk1 := filepath.Join(dir, "disk1")
	disk2 := filepath.Join(dir, "disk2")
	s := server.NewServerWithDiskRoots(db, map[uint32]string{1: disk1, 2: disk2})
	ctx := context.Background()
	opened, err := s.Open(ctx, &pb.OpenRequest{Path: "/replicated"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: opened.GetHandle(), Buffer: []byte("replicated across disks")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: opened.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{disk1, disk2} {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read disk root %s: %v", root, err)
		}
		if len(entries) == 0 {
			t.Fatalf("disk root %s has no replica plog", root)
		}
	}
	restarted := server.NewServerWithDiskRoots(db, map[uint32]string{1: disk1, 2: disk2})
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	opened, err = restarted.Open(ctx, &pb.OpenRequest{Path: "/replicated"})
	if err != nil {
		t.Fatal(err)
	}
	read, err := restarted.Read(ctx, &pb.ReadRequest{Handle: opened.GetHandle(), Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), []byte("replicated across disks")) {
		t.Fatalf("recovered replicated read = %q", read.GetBuffer())
	}
}
