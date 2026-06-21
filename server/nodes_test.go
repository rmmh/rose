package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
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

func assertDistinctNodes(t *testing.T, s *Server, vlogID uint32) {
	t.Helper()
	seen := make(map[uint32]int) // node -> shard index already there
	for _, sh := range mustShards(t, s, vlogID) {
		n := s.nodeOf(sh.DiskID)
		if other, ok := seen[n]; ok {
			t.Fatalf("vlog %d shards %d and %d both on node %d (disk %d)", vlogID, other, sh.ShardIndex, n, sh.DiskID)
		}
		seen[n] = sh.ShardIndex
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

// TestNodeFaultDomainSpreadsShards checks PlacementAllowed's NodeLevelDurability:
// no two shards of a vlog share a node, and provisioning fails when there are not
// enough distinct nodes for the scheme.
func TestNodeFaultDomainSpreadsShards(t *testing.T) {
	ctx := context.Background()
	// Four disks but only two node fault domains.
	s := newNodeServer(t, map[uint32]uint32{1: 10, 2: 10, 3: 20, 4: 20})

	// EC 2+1 wants three distinct-node disks; only two nodes exist.
	s.vlogMu.Lock()
	_, _, err := s.provisionVlogLocked(ctx, "EC", 2, 1)
	s.vlogMu.Unlock()
	if err == nil {
		t.Fatal("EC 2+1 should fail with only two node fault domains")
	}

	// DUPLICATE places one copy per node: two copies on distinct nodes, not four.
	dupID := provision(t, s, "DUPLICATE", 1, 0)
	if got := mustShards(t, s, dupID); len(got) != 2 {
		t.Fatalf("DUPLICATE vlog has %d copies, want 2 (one per node)", len(got))
	}
	assertDistinctNodes(t, s, dupID)

	// EC 1+1 fits the two nodes, one shard each.
	ecID := provision(t, s, "EC", 1, 1)
	assertDistinctNodes(t, s, ecID)
}

// TestDrainHonorsNodeFaultDomain checks that relocating a shard never collapses
// two shards of a vlog onto one node, even when a spare disk shares a node with
// an existing shard.
func TestDrainHonorsNodeFaultDomain(t *testing.T) {
	ctx := context.Background()
	// disks 1,2 on distinct nodes; disk 3 shares node 20 with disk 2.
	s := newNodeServer(t, map[uint32]uint32{1: 10, 2: 20, 3: 20})
	vlogID := provision(t, s, "DUPLICATE", 1, 0) // copies on nodes 10 and 20
	writeVlog(t, s, vlogID, []byte("fault domain payload"))

	// The copy on node 10 (disk 1) can only legally move to a disk on a node that
	// does not already hold the other copy. Node 20 is taken by the other copy, so
	// disk 3 is not allowed, and there is no other node: drain must fail.
	if err := s.DrainDisk(ctx, 1); err == nil {
		t.Fatal("drain succeeded though the only spare disk shares a node with the other copy")
	}
	if got := s.DiskStates()[1]; got != meta.DiskDraining {
		t.Fatalf("disk 1 state = %q, want draining (stuck without a legal destination)", got)
	}
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
