package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

// newNodeServer builds a recovered server whose disks are grouped onto nodes per
// the diskNodes map (disk id -> node id), so the node-level fault domain has more
// than the default one-disk-per-node topology to enforce.
func newNodeServer(t *testing.T, diskNodes map[uint32]uint32) *Server {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	roots := make(map[uint32]string, len(diskNodes))
	for d := range diskNodes {
		roots[d] = filepath.Join(dir, fmt.Sprintf("disk-%d", d))
		if err := os.MkdirAll(roots[d], 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := NewServerWithDiskRoots(db, roots)
	for d, n := range diskNodes {
		s.SetDiskNode(d, n)
	}
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func assertDistinctDisks(t *testing.T, s *Server, vlogID uint32) {
	t.Helper()
	seen := make(map[uint32]int) // disk -> shard index already there
	for _, sh := range mustShards(t, s, vlogID) {
		if other, ok := seen[sh.DiskID]; ok {
			t.Fatalf("vlog %d shards %d and %d both on disk %d", vlogID, other, sh.ShardIndex, sh.DiskID)
		}
		seen[sh.DiskID] = sh.ShardIndex
	}
}

// TestNodeFailureDropsDisksFromLiveSet exercises DiskLive folding in node
// liveness: a failed node's disks stop counting toward commit/read durability
// without their disk_state changing, and the loss reverses when the node returns.
func TestNodeFailureDropsDisksFromLiveSet(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3) // disks 1..3, each its own node
	vlogID := provision(t, s, "DUPLICATE", 1, 0)

	ready := func() bool {
		r, err := s.CommitReady(ctx, vlogID)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	readable := func() bool {
		r, err := s.Readable(ctx, vlogID)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	if !ready() {
		t.Fatal("3 live copies should commit")
	}
	if err := s.SetNodeState(ctx, 3, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if !ready() {
		t.Fatal("2 live copies still meets minCopies")
	}
	if err := s.SetNodeState(ctx, 2, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if ready() {
		t.Fatal("1 live copy < minCopies: should refuse new commits")
	}
	if !readable() {
		t.Fatal("the surviving copy should still be readable")
	}
	// A failed node never touched disk_state: the loss is transient.
	if got := s.DiskStates()[2]; got != meta.DiskActive {
		t.Fatalf("disk 2 state = %q, want active (node failure must not change disk_state)", got)
	}
	// Node 2 returns: its copy is live again and commits resume.
	if err := s.SetNodeState(ctx, 2, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	if !ready() {
		t.Fatal("commit readiness should be restored when the node returns")
	}
}

// TestDiskFaultDomainSpreadsShards checks placement's disk fault domain: no two
// shards of a vlog share a disk, while multiple disks on one node may all
// participate.
func TestDiskFaultDomainSpreadsShards(t *testing.T) {
	ctx := context.Background()
	// Four disks but only two node fault domains.
	s := newNodeServer(t, map[uint32]uint32{1: 10, 2: 10, 3: 20, 4: 20})

	// EC 2+1 needs three disks and may use several disks on one node.
	s.vlogMu.Lock()
	ecID, _, err := s.provisionVlogLocked(ctx, "EC", 2, 1)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatalf("EC 2+1 with three available disks: %v", err)
	}
	assertDistinctDisks(t, s, ecID)

	// DUPLICATE places one copy per active disk.
	dupID := provision(t, s, "DUPLICATE", 1, 0)
	if got := mustShards(t, s, dupID); len(got) != 4 {
		t.Fatalf("DUPLICATE vlog has %d copies, want 4 (one per disk)", len(got))
	}
	assertDistinctDisks(t, s, dupID)

	// EC 1+1 uses two different disks.
	ecID = provision(t, s, "EC", 1, 1)
	assertDistinctDisks(t, s, ecID)
}

func TestSingleNodeMultiDiskEC(t *testing.T) {
	ctx := context.Background()
	s := newNodeServer(t, map[uint32]uint32{1: 10, 2: 10, 3: 10, 4: 10})

	s.vlogMu.Lock()
	vlogID, _, err := s.provisionVlogLocked(ctx, "EC", 3, 1)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatalf("EC 3+1 on four disks in one node: %v", err)
	}
	assertDistinctDisks(t, s, vlogID)
}

// TestDrainHonorsDiskFaultDomain checks that relocating a shard never places two
// shards of a vlog onto one disk, while a same-node spare disk is legal.
func TestDrainHonorsDiskFaultDomain(t *testing.T) {
	ctx := context.Background()
	// disks 1,2 on distinct nodes initially; disk 3 is attached later as a spare
	// sharing node 20 with disk 2.
	s := newNodeServer(t, map[uint32]uint32{1: 10, 2: 20})
	vlogID := provision(t, s, "DUPLICATE", 1, 0) // copies on nodes 10 and 20
	writeVlog(t, s, vlogID, []byte("fault domain payload"))
	spare := filepath.Join(t.TempDir(), "disk-3")
	if err := os.MkdirAll(spare, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.AttachDiskOnNode(ctx, 3, 20, spare, 0); err != nil {
		t.Fatal(err)
	}

	if err := s.DrainDisk(ctx, 1); err != nil {
		t.Fatal(err)
	}
	assertDistinctDisks(t, s, vlogID)
}

// TestNodeReturnCancelsReprotect checks the user-facing requirement: a node
// coming back online abandons a reprotect its outage triggered, restoring the
// disks rather than finishing pointless regeneration.
func TestNodeReturnCancelsReprotect(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3) // disk id == node id

	// The node carrying disk 1 goes offline and the disk is declared failed, and a
	// reprotect is started (a running job row) but not finished.
	if err := s.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.GetOrCreateReprotectJob(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// The node comes back: the reprotect is unnecessary (disk 1's bytes survived).
	if err := s.SetNodeState(ctx, 1, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}

	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("node return left %d running jobs, want 0 (reprotect should be cancelled)", len(jobs))
	}
	if got := s.DiskStates()[1]; got != meta.DiskActive {
		t.Fatalf("disk 1 state = %q, want active (restored when the node returned)", got)
	}
}

func TestNodeFailureTakesMountedPlogsOffline(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 1)
	made, err := s.MakeVlog(ctx, &pb.MakeVlogRequest{
		ProtectionScheme: "NONE",
		DataShards:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("node failure must cut off mounted storage")
	written, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: made.GetVlogId(), TxnId: 1, Buffer: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 1}); err != nil {
		t.Fatal(err)
	}

	if err := s.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: made.GetVlogId(), Offset: written.GetOffset(), Length: uint32(len(payload)),
	}); err == nil {
		t.Fatal("ReadVlog served bytes from a failed node's mounted plog")
	}

	if err := s.SetNodeState(ctx, 1, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: made.GetVlogId(), Offset: written.GetOffset(), Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatalf("read after node return = %q, want %q", read.GetBuffer(), payload)
	}
}

func TestDuplicateVlogWritesWithMinimumCopiesAfterNodeFailure(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	made, err := s.MakeVlog(ctx, &pb.MakeVlogRequest{
		ProtectionScheme: "DUPLICATE",
		DataShards:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	ready, err := s.CommitReady(ctx, made.GetVlogId())
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("two surviving mirrors should meet the configured commit threshold")
	}

	payload := []byte("write through the surviving mirrors")
	written, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: made.GetVlogId(), TxnId: 2, Buffer: payload,
	})
	if err != nil {
		t.Fatalf("commit-ready DUPLICATE write failed with one node down: %v", err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 2}); err != nil {
		t.Fatalf("commit-ready DUPLICATE commit failed with one node down: %v", err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: made.GetVlogId(), Offset: written.GetOffset(), Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatalf("degraded read = %q, want %q", read.GetBuffer(), payload)
	}
}

// TestNodeStatePersistsAcrossRecover checks node liveness survives a restart, so
// a node failed before a crash keeps its disks out of the live set afterward.
func TestNodeStatePersistsAcrossRecover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	roots := map[uint32]string{1: filepath.Join(dir, "d1"), 2: filepath.Join(dir, "d2")}
	for _, r := range roots {
		if err := os.MkdirAll(r, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	s1 := NewServerWithDiskRoots(db, roots)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s1.SetNodeState(ctx, 2, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}

	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s2.NodeStates()[2]; got != meta.NodeFailed {
		t.Fatalf("recovered node 2 state = %q, want %q", got, meta.NodeFailed)
	}
	if got := s2.NodeStates()[1]; got != meta.NodeWorking {
		t.Fatalf("recovered node 1 state = %q, want %q", got, meta.NodeWorking)
	}
}

func TestNodeReturnRetryReopensEveryVlogAfterPartialReturn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root := filepath.Join(dir, "disk-1")
	s1 := NewServerWithDiskRoots(db, map[uint32]string{1: root})
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer s1.StopMaintenanceDriver()
	first := provision(t, s1, "NONE", 1, 0)
	second := provision(t, s1, "NONE", 1, 0)
	firstData := []byte("first vlog must be remounted after the retry")
	secondData := []byte("second vlog returns later")
	firstOff := writeVlog(t, s1, first, firstData)
	secondOff := writeVlog(t, s1, second, secondData)
	if err := s1.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	plogs, err := db.ListPlogs(ctx)
	if err != nil || len(plogs) != 2 {
		t.Fatalf("plogs = %v, err = %v", plogs, err)
	}
	missing := filepath.Join(root, fmt.Sprintf("plog-%05d", plogs[1].ID))
	offline := missing + ".offline"
	if err := os.Rename(missing, offline); err != nil {
		t.Fatal(err)
	}

	s2 := NewServerWithDiskRoots(db, map[uint32]string{1: root})
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer s2.StopMaintenanceDriver()
	if err := s2.SetNodeState(ctx, 1, meta.NodeWorking); err == nil {
		t.Fatal("partial node return succeeded with a missing plog")
	}
	if err := os.Rename(offline, missing); err != nil {
		t.Fatal(err)
	}
	if err := s2.SetNodeState(ctx, 1, meta.NodeWorking); err != nil {
		t.Fatalf("node return retry: %v", err)
	}
	for _, tc := range []struct {
		vlog uint32
		off  int64
		data []byte
	}{{first, firstOff, firstData}, {second, secondOff, secondData}} {
		got, err := s2.vlogs[tc.vlog].Read(ctx, tc.off, len(tc.data))
		if err != nil {
			t.Fatalf("read vlog %d after node return: %v", tc.vlog, err)
		}
		if string(got) != string(tc.data) {
			t.Fatalf("vlog %d data = %q, want %q", tc.vlog, got, tc.data)
		}
	}
}
