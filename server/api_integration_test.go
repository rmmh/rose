package server_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	"github.com/rmmh/rose/storage"
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

func TestWriteOperationRetriesOpenWriteAndClose(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	data := bytes.Repeat([]byte("retryable-stream-data"), 600)
	first, err := client.Open(ctx, &pb.OpenRequest{Path: "/retry", OperationKey: "op-retry"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Open(ctx, &pb.OpenRequest{Path: "/retry", OperationKey: "op-retry"})
	if err != nil {
		t.Fatal(err)
	}
	if first.GetAcknowledgedOffset() != 0 || second.GetAcknowledgedOffset() != 0 {
		t.Fatal("new operation acknowledged data")
	}
	write := &pb.WriteRequest{Handle: first.GetHandle(), Offset: 0, Buffer: data}
	result, err := client.Write(ctx, write)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetAcknowledgedOffset() != int64(len(data)) {
		t.Fatalf("ack = %d", result.GetAcknowledgedOffset())
	}
	result, err = client.Write(ctx, write)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetAcknowledgedOffset() != int64(len(data)) {
		t.Fatalf("retry ack = %d", result.GetAcknowledgedOffset())
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: first.GetHandle(), IdempotencyKey: "op-retry"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: first.GetHandle(), IdempotencyKey: "op-retry"}); err != nil {
		t.Fatal(err)
	}
	read, err := client.Open(ctx, &pb.OpenRequest{Path: "/retry"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Read(ctx, &pb.ReadRequest{Handle: read.GetHandle(), Length: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetBuffer(), data) {
		t.Fatal("retried operation published different content")
	}
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

// TestBucketECFileRoundTrip writes a file into a bucket configured for 3+1
// erasure coding and reads it back, exercising the file API end to end over EC
// rather than the default mirror. EC is deferred: the write lands in a replicated
// staging vlog and only becomes coded once the maintenance promotion pass packs
// it into whole stripe rows, so the test promotes explicitly and then asserts the
// promoted vlog really is EC 3+1 with its four shards spread across four distinct
// nodes (NodeLevelDurability), and that the file still reads back from it.
func TestBucketECFileRoundTrip(t *testing.T) {
	defer storage.SetECColumnBytesForTest(16 << 10)() // small stripe so 200 KB promotes
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	// Four disks, each its own node (disk id == node id), so EC 3+1's four shards
	// can land on four distinct nodes.
	roots := map[uint32]string{}
	for id := uint32(1); id <= 4; id++ {
		roots[id] = filepath.Join(dir, fmt.Sprintf("disk-%d", id))
	}
	srv := server.NewServerWithDiskRoots(db, roots)
	srv.SetMaintenanceInterval(0)
	if err := srv.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.StopMaintenanceDriver)

	if err := srv.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 200_000)
	if _, err := rand.New(rand.NewSource(99)).Read(data); err != nil {
		t.Fatal(err)
	}

	open, err := srv.Open(ctx, &pb.OpenRequest{Path: "/ec/file1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	// Promote the staged chunks into a coded EC vlog, the deferred half of EC.
	if _, err := srv.PromoteStaging(ctx); err != nil {
		t.Fatalf("promote staging: %v", err)
	}

	reopen, err := srv.Open(ctx, &pb.OpenRequest{Path: "/ec/file1"})
	if err != nil {
		t.Fatal(err)
	}
	read, err := srv.Read(ctx, &pb.ReadRequest{Handle: reopen.GetHandle(), Offset: 0, Length: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), data) {
		t.Fatalf("EC file round-trip mismatch: got %d bytes, want %d", len(read.GetBuffer()), len(data))
	}

	// After promotion the file's bytes live in an EC 3+1 vlog, spread across four nodes.
	vlogs, err := db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sawEC := false
	for _, v := range vlogs {
		if v.ProtectionScheme != "EC" {
			continue
		}
		sawEC = true
		if v.DataShards != 3 || v.ParityShards != 1 {
			t.Fatalf("vlog %d = EC %d+%d, want 3+1", v.ID, v.DataShards, v.ParityShards)
		}
		shards, err := db.VlogShardDisks(ctx, v.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(shards) != 4 {
			t.Fatalf("EC vlog %d has %d shards, want 4", v.ID, len(shards))
		}
		nodes := map[uint32]bool{}
		for _, sh := range shards {
			nodes[sh.DiskID] = true // disk id == node id here
		}
		if len(nodes) != 4 {
			t.Fatalf("EC vlog %d shards span %d nodes, want 4 (NodeLevelDurability)", v.ID, len(nodes))
		}
	}
	if !sawEC {
		t.Fatal("no EC vlog provisioned; bucket policy did not take effect")
	}
}

// TestVlogECRealPlogWholeRows drives an EC 3+1 vlog backed by real plogs through
// the low-level vlog API, isolating the real-plog EC path from the file/chunk
// layer. EC vlogs only accept whole stripe rows (the deferred-EC model packs
// chunks into rows during promotion), so the writes here are row-aligned; a
// shrunk stripe keeps the fixtures small.
func TestVlogECRealPlogWholeRows(t *testing.T) {
	defer storage.SetECColumnBytesForTest(4096)() // stripe row = 4096*3 = 12288 bytes
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	roots := map[uint32]string{}
	for id := uint32(1); id <= 4; id++ {
		roots[id] = filepath.Join(dir, fmt.Sprintf("disk-%d", id))
	}
	srv := server.NewServerWithDiskRoots(db, roots)
	srv.SetMaintenanceInterval(0)
	if err := srv.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.StopMaintenanceDriver)

	mk, err := srv.MakeVlog(ctx, &pb.MakeVlogRequest{ProtectionScheme: "EC", DataShards: 3, ParityShards: 1})
	if err != nil {
		t.Fatal(err)
	}
	vlogID := mk.GetVlogId()

	rng := rand.New(rand.NewSource(3))
	sw := int(storage.ECStripeWidth(3))
	lengths := []int{sw, 2 * sw, sw, 3 * sw} // whole stripe rows of varying size
	offs := make([]uint32, len(lengths))
	payloads := make([][]byte, len(lengths))
	for i, n := range lengths {
		data := make([]byte, n)
		rng.Read(data)
		payloads[i] = data
		w, err := srv.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: vlogID, TxnId: 1, Buffer: data})
		if err != nil {
			t.Fatalf("write %d (len %d): %v", i, n, err)
		}
		offs[i] = w.GetOffset()
	}
	if _, err := srv.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 1}); err != nil {
		t.Fatal(err)
	}
	for i := range lengths {
		r, err := srv.ReadVlog(ctx, &pb.ReadVlogRequest{VlogId: vlogID, Offset: offs[i], Length: uint32(lengths[i])})
		if err != nil {
			t.Fatalf("read %d (offset %d len %d): %v", i, offs[i], lengths[i], err)
		}
		if !bytes.Equal(r.GetBuffer(), payloads[i]) {
			t.Fatalf("chunk %d real-plog EC mismatch at offset %d len %d", i, offs[i], lengths[i])
		}
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

func TestCompactionRewritesVlogAndReclaimsSpace(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	diskRoot := filepath.Join(dir, "disk")
	s := server.NewServerWithDataDir(db, diskRoot)
	ctx := context.Background()

	// Distinct payloads land in one active vlog. Keep /keep, delete the rest.
	keep := make([]byte, 64*1024)
	rand.New(rand.NewSource(1)).Read(keep)
	writeServerFile(t, s, "/keep", keep)
	for i := 0; i < 4; i++ {
		dead := make([]byte, 64*1024)
		rand.New(rand.NewSource(int64(100 + i))).Read(dead)
		writeServerFile(t, s, fmt.Sprintf("/dead%d", i), dead)
		if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: fmt.Sprintf("/dead%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	usages, err := db.VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(usages) != 1 || usages[0].DeadBytes() == 0 {
		t.Fatalf("expected one vlog with dead space, got %+v", usages)
	}
	sourceVlog := usages[0].VlogID

	// Aggressive policy so the single wasteful vlog is selected.
	n, err := s.Compact(ctx, server.CompactionPolicy{MinWasteRatio: 0.1, MinDeadBytes: 1, MaxJobs: 4})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("compacted %d vlogs, want 1", n)
	}

	// The source vlog is retired; a fresh one holds only the live chunk.
	after, err := db.VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range after {
		if u.VlogID == sourceVlog {
			t.Fatalf("source vlog %d still present after compaction", sourceVlog)
		}
		if u.DeadBytes() != 0 {
			t.Fatalf("compacted vlog %d still has %d dead bytes", u.VlogID, u.DeadBytes())
		}
	}

	// Survivor data is intact, both live and after a restart that re-mounts the
	// rewritten vlog.
	if got := readServerFile(t, s, "/keep"); !bytes.Equal(got, keep) {
		t.Fatal("kept file corrupted by compaction")
	}
	restarted := server.NewServerWithDataDir(db, diskRoot)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got := readServerFile(t, restarted, "/keep"); !bytes.Equal(got, keep) {
		t.Fatal("kept file unreadable after restart")
	}
}

func TestCompactionResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	diskRoot := filepath.Join(dir, "disk")
	s := server.NewServerWithDataDir(db, diskRoot)
	ctx := context.Background()

	keep := make([]byte, 32*1024)
	rand.New(rand.NewSource(5)).Read(keep)
	writeServerFile(t, s, "/keep", keep)
	dead := make([]byte, 32*1024)
	rand.New(rand.NewSource(6)).Read(dead)
	writeServerFile(t, s, "/dead", dead)
	if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: "/dead"}); err != nil {
		t.Fatal(err)
	}

	usages, err := db.VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sourceVlog := usages[0].VlogID

	// Simulate a crash after a compaction job was durably recorded but before
	// any work ran: a fresh server must pick it up during recovery and finish.
	if _, err := db.GetOrCreateCompactionJob(ctx, sourceVlog); err != nil {
		t.Fatal(err)
	}

	restarted := server.NewServerWithDataDir(db, diskRoot)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatalf("recover should resume compaction: %v", err)
	}

	after, err := db.VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range after {
		if u.VlogID == sourceVlog {
			t.Fatal("resumed recovery did not retire the source vlog")
		}
	}
	if got := readServerFile(t, restarted, "/keep"); !bytes.Equal(got, keep) {
		t.Fatal("kept file unreadable after resumed compaction")
	}
	jobs, err := db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no running jobs after resume, got %d", len(jobs))
	}
}

func TestMaintenancePassDrivesGCAndCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	diskRoot := filepath.Join(dir, "disk")
	s := server.NewServerWithDataDir(db, diskRoot)
	ctx := context.Background()

	// One live file and several deleted ones pile dead space into a single vlog,
	// the same setup the explicit-compaction test uses.
	keep := make([]byte, 64*1024)
	rand.New(rand.NewSource(11)).Read(keep)
	writeServerFile(t, s, "/keep", keep)
	for i := 0; i < 4; i++ {
		dead := make([]byte, 64*1024)
		rand.New(rand.NewSource(int64(200 + i))).Read(dead)
		writeServerFile(t, s, fmt.Sprintf("/dead%d", i), dead)
		if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: fmt.Sprintf("/dead%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	usages, err := db.VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(usages) != 1 || usages[0].DeadBytes() == 0 {
		t.Fatalf("expected one vlog with dead space, got %+v", usages)
	}
	sourceVlog := usages[0].VlogID

	// Drive reclamation through the maintenance pass rather than an explicit call:
	// an aggressive policy makes the single wasteful vlog a candidate.
	s.SetCompactionPolicy(server.CompactionPolicy{MinWasteRatio: 0.1, MinDeadBytes: 1, MaxJobs: 4})
	if err := s.RunMaintenanceOnce(ctx); err != nil {
		t.Fatalf("maintenance pass: %v", err)
	}

	// Compaction retired the source vlog and the survivor carries no dead bytes.
	after, err := db.VlogUsages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range after {
		if u.VlogID == sourceVlog {
			t.Fatalf("source vlog %d still present after maintenance pass", sourceVlog)
		}
		if u.DeadBytes() != 0 {
			t.Fatalf("compacted vlog %d still has %d dead bytes", u.VlogID, u.DeadBytes())
		}
	}
	// GC already ran in the pass: a follow-up GC finds nothing left to collect.
	if n, err := s.GC(ctx); err != nil || n != 0 {
		t.Fatalf("follow-up GC collected %d (err=%v), want 0 -- maintenance pass should have GC'd", n, err)
	}
	if got := readServerFile(t, s, "/keep"); !bytes.Equal(got, keep) {
		t.Fatal("kept file corrupted by maintenance pass")
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

// TestScrubAndRepairHealsBitrot writes an EC 3+1 file, flips a byte in one
// shard's plog (bitrot), then runs ScrubAndRepair and asserts the shard is
// rebuilt from the surviving redundancy: the repair reports one shard healed, a
// follow-up scrub is clean, and the file still reads back byte-identical.
func TestScrubAndRepairHealsBitrot(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	// Four disks on four nodes so EC 3+1's four shards land on distinct nodes.
	roots := map[uint32]string{}
	for id := uint32(1); id <= 4; id++ {
		roots[id] = filepath.Join(dir, fmt.Sprintf("disk-%d", id))
	}
	srv := server.NewServerWithDiskRoots(db, roots)
	srv.SetMaintenanceInterval(0)
	if err := srv.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.StopMaintenanceDriver)

	if err := srv.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	// Large enough that every EC shard plog seals several hash-protected blocks,
	// so scrub validates persisted sectors rather than the open trailing buffer.
	data := make([]byte, 6<<20)
	if _, err := rand.New(rand.NewSource(11)).Read(data); err != nil {
		t.Fatal(err)
	}
	open, err := srv.Open(ctx, &pb.OpenRequest{Path: "/ec/file1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	// Flip a sealed sector byte in one shard's plog. disk-1 backs exactly one
	// shard of the single EC vlog, so its lone plog file is that shard.
	entries, err := os.ReadDir(roots[1])
	if err != nil || len(entries) == 0 {
		t.Fatalf("read disk-1: %v", err)
	}
	corruptFileByte(t, filepath.Join(roots[1], entries[0].Name()), 4096+100)

	// Confirm the corruption is detectable before repairing it.
	scrubbed, err := srv.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	sawCorruption := false
	for _, v := range scrubbed {
		for _, sh := range v.Shards {
			if !sh.Result.Healthy() {
				sawCorruption = true
			}
		}
	}
	if !sawCorruption {
		t.Fatal("scrub did not detect injected bitrot")
	}

	res, err := srv.ScrubAndRepair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ShardsRepaired != 1 {
		t.Fatalf("ScrubAndRepair healed %d shards, want 1 (result %+v)", res.ShardsRepaired, res)
	}
	if len(res.Unrepairable) != 0 {
		t.Fatalf("ScrubAndRepair reported unrepairable shards: %+v", res.Unrepairable)
	}

	// A scrub after repair must be clean.
	clean, err := srv.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range clean {
		for _, sh := range v.Shards {
			if !sh.Result.Healthy() {
				t.Fatalf("scrub still unhealthy after repair: vlog %d shard %d %+v", v.VlogID, sh.Shard, sh.Result)
			}
		}
	}

	// The file still reads back byte-identical.
	reopen, err := srv.Open(ctx, &pb.OpenRequest{Path: "/ec/file1"})
	if err != nil {
		t.Fatal(err)
	}
	read, err := srv.Read(ctx, &pb.ReadRequest{Handle: reopen.GetHandle(), Offset: 0, Length: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), data) {
		t.Fatal("EC file mismatch after scrub-repair")
	}
}

// TestScrubAndRepairHealsDuplicate confirms the repair path for DUPLICATE vlogs:
// a corrupt mirror is rebuilt from a surviving copy (readSurvivingCopyLocked),
// not EC reconstruct.
func TestScrubAndRepairHealsDuplicate(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	disk1 := filepath.Join(dir, "disk1")
	disk2 := filepath.Join(dir, "disk2")
	srv := server.NewServerWithDiskRoots(db, map[uint32]string{1: disk1, 2: disk2})
	srv.SetMaintenanceInterval(0)
	if err := srv.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.StopMaintenanceDriver)

	// Default policy is 2-copy DUPLICATE across the two nodes.
	data := make([]byte, 3<<20)
	if _, err := rand.New(rand.NewSource(13)).Read(data); err != nil {
		t.Fatal(err)
	}
	open, err := srv.Open(ctx, &pb.OpenRequest{Path: "/big"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(disk1)
	if err != nil || len(entries) == 0 {
		t.Fatalf("read disk1: %v", err)
	}
	corruptFileByte(t, filepath.Join(disk1, entries[0].Name()), 4096+100)

	res, err := srv.ScrubAndRepair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ShardsRepaired != 1 || len(res.Unrepairable) != 0 {
		t.Fatalf("DUPLICATE repair = %+v, want 1 healed, 0 unrepairable", res)
	}

	clean, err := srv.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range clean {
		for _, sh := range v.Shards {
			if !sh.Result.Healthy() {
				t.Fatalf("DUPLICATE scrub unhealthy after repair: vlog %d shard %d", v.VlogID, sh.Shard)
			}
		}
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
