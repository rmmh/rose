package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
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

func TestDiskFailureTakesMountedPlogsOffline(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 1)
	made, err := s.MakeVlog(ctx, &pb.MakeVlogRequest{
		ProtectionScheme: "NONE",
		DataShards:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("failed disks must stop serving mounted storage")
	written, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: made.GetVlogId(), TxnId: 1, Buffer: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 1}); err != nil {
		t.Fatal(err)
	}

	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: made.GetVlogId(), Offset: written.GetOffset(), Length: uint32(len(payload)),
	}); err == nil {
		t.Fatal("ReadVlog served bytes from a failed disk's mounted plog")
	}

	if err := s.SetDiskState(ctx, 1, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: made.GetVlogId(), Offset: written.GetOffset(), Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatalf("read after disk return = %q, want %q", read.GetBuffer(), payload)
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

func TestNodeReturnCatchesUpDuplicateWritesCommittedDuringOutage(t *testing.T) {
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
	payload := []byte("committed while node one was offline")
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: made.GetVlogId(), TxnId: 7, Buffer: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 7}); err != nil {
		t.Fatal(err)
	}

	if err := s.SetNodeState(ctx, 1, meta.NodeWorking); err != nil {
		t.Fatalf("node could not return after degraded writes: %v", err)
	}
	if got := s.NodeStates()[1]; got != meta.NodeWorking {
		t.Fatalf("returned node state = %q, want working", got)
	}
	if err := s.SetNodeState(ctx, 2, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 3, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: made.GetVlogId(), Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatalf("read after node catch-up = %q, want %q", read.GetBuffer(), payload)
	}
}

func TestNodeReturnReplacesSameLengthUncommittedTail(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	s.SetMaintenanceInterval(0)
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	mappings, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	var stalePlog uint32
	for _, mapping := range mappings {
		if mapping.DiskID == 3 {
			stalePlog = mapping.PlogID
		}
	}
	if stalePlog == 0 {
		t.Fatal("vlog has no replica on disk 3")
	}

	if err := s.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 2, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	stale := bytes.Repeat([]byte{0x51}, storage.SectorSize)
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: vlogID, TxnId: 51, Buffer: stale,
	}); err == nil {
		t.Fatal("single surviving replica unexpectedly met the write quorum")
	}
	deadline := time.Now().Add(time.Second)
	for s.plogs[stalePlog].LogicalLength() != int64(len(stale)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.plogs[stalePlog].LogicalLength(); got != int64(len(stale)) {
		t.Fatalf("failed write left stale replica length %d, want %d", got, len(stale))
	}

	if err := s.SetNodeState(ctx, 3, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 1, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 2, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	committed := bytes.Repeat([]byte{0x52}, storage.SectorSize)
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: vlogID, TxnId: 52, Buffer: committed,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 52}); err != nil {
		t.Fatal(err)
	}

	if err := s.SetNodeState(ctx, 3, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	got, err := s.plogs[stalePlog].Read(0, len(committed))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, committed) {
		t.Fatalf("returned replica retained stale bytes %q, want %q", got, committed)
	}
}

func TestNodeReturnRepairsReplicaBeforePublishingWorkingState(t *testing.T) {
	ctx := context.Background()
	before := newControlPlaneServer(t, 3)
	before.SetMaintenanceInterval(0)
	vlogID := provision(t, before, "DUPLICATE", 1, 0)
	mappings, err := before.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	var stalePlog uint32
	for _, mapping := range mappings {
		if mapping.DiskID == 3 {
			stalePlog = mapping.PlogID
		}
	}
	if stalePlog == 0 {
		t.Fatal("vlog has no replica on disk 3")
	}
	if err := before.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := before.SetNodeState(ctx, 2, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	stale := bytes.Repeat([]byte{0x61}, storage.SectorSize)
	if _, err := before.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: vlogID, TxnId: 61, Buffer: stale,
	}); err == nil {
		t.Fatal("single replica unexpectedly met write quorum")
	}
	deadline := time.Now().Add(time.Second)
	for before.plogs[stalePlog].LogicalLength() != int64(len(stale)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := before.plogs[stalePlog].LogicalLength(); got != int64(len(stale)) {
		t.Fatalf("failed write left stale replica length %d, want %d", got, len(stale))
	}
	if err := before.SetNodeState(ctx, 3, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := before.SetNodeState(ctx, 1, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	if err := before.SetNodeState(ctx, 2, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	committed := bytes.Repeat([]byte{0x62}, storage.SectorSize)
	if _, err := before.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: vlogID, TxnId: 62, Buffer: committed,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := before.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 62}); err != nil {
		t.Fatal(err)
	}

	// A catalog write failure models a dead or unwritable metadata disk at the
	// publication boundary. Returned bytes must already be caught up before the
	// working state is attempted, so a process crash cannot expose this stale
	// replica on the next recovery.
	if _, err := before.db.GetDB().ExecContext(ctx, `
		CREATE TRIGGER fail_node_return
		BEFORE UPDATE OF state ON node
		WHEN OLD.id = 3 AND NEW.state = 'working'
		BEGIN
			SELECT RAISE(ABORT, 'injected node-state write failure');
		END
	`); err != nil {
		t.Fatal(err)
	}
	if err := before.SetNodeState(ctx, 3, meta.NodeWorking); err == nil {
		t.Fatal("node return unexpectedly survived injected catalog write failure")
	}
	nodes, err := before.db.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if node.ID == 3 && node.State != meta.NodeFailed {
			t.Fatalf("durable node 3 state = %q, want failed", node.State)
		}
	}
	repaired, err := storage.OpenExistingPlog(before.plogPath(3, stalePlog), stalePlog)
	if err != nil {
		t.Fatal(err)
	}
	got, err := repaired.Read(0, len(committed))
	closeErr := repaired.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if !bytes.Equal(got, committed) {
		t.Fatal("node-state failure happened before returned replica was repaired")
	}
	if _, err := before.db.GetDB().ExecContext(ctx, `DROP TRIGGER fail_node_return`); err != nil {
		t.Fatal(err)
	}
	if err := before.SetNodeState(ctx, 3, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
}

func TestNodeFailurePreservesConcurrentSuccessfulWrite(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	ctx := context.Background()
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	mappings, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	started := make(chan struct{}, 2)
	clients := make([]storage.PlogClient, len(mappings))
	for i, mapping := range mappings {
		local := &localPlogClient{plog: s.plogs[mapping.PlogID]}
		if mapping.DiskID == 3 {
			clients[i] = local
		} else {
			clients[i] = &writeFaultClient{block: block, started: started, local: local}
		}
	}
	vlog, err := storage.NewVlog(vlogID, "DUPLICATE", 1, 0, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := vlog.SetWriteQuorum(2); err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	s.vlogs[vlogID] = vlog
	s.vlogMu.Unlock()

	payload := bytes.Repeat([]byte{0x75}, 2*storage.SectorSize)
	writeDone := make(chan error, 1)
	go func() {
		_, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
			VlogId: vlogID, TxnId: 75, Buffer: payload,
		})
		writeDone <- err
	}()
	<-started
	<-started

	stateDone := make(chan error, 1)
	go func() {
		stateDone <- s.SetNodeState(ctx, 3, meta.NodeFailed)
	}()
	select {
	case err := <-stateDone:
		t.Fatalf("node failure remounted during the affected write: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(block)
	if err := <-stateDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 75}); err != nil {
		t.Fatal(err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: vlogID, Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatal("node failure lost the concurrent successful write")
	}
}

func TestNodeReturnUsesAllReturnedDisksForMirrorAgreement(t *testing.T) {
	s := newNodeServer(t, map[uint32]uint32{1: 10, 2: 10})
	ctx := context.Background()
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	payload := bytes.Repeat([]byte{0x76}, 2*storage.SectorSize)
	writeVlog(t, s, vlogID, payload)

	if err := s.SetNodeState(ctx, 10, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 10, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: vlogID, Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatal("multi-disk node return changed committed mirror bytes")
	}
}

func TestNodeReturnAuthenticatesCompleteNonQuorumMirror(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	ctx := context.Background()
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	mappings, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	late := mappings[2]
	block := make(chan struct{})
	written := make(chan struct{}, 1)
	clients := make([]storage.PlogClient, len(mappings))
	for i, mapping := range mappings {
		local := &localPlogClient{plog: s.plogs[mapping.PlogID]}
		if mapping.PlogID == late.PlogID {
			clients[i] = &writeFaultClient{
				blockAfter: block, afterStart: written, local: local,
			}
		} else {
			clients[i] = local
		}
	}
	vlog, err := storage.NewVlog(vlogID, "DUPLICATE", 1, 0, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := vlog.SetWriteQuorum(2); err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	s.vlogs[vlogID] = vlog
	s.vlogMu.Unlock()

	payload := bytes.Repeat([]byte{0x77}, 2*storage.SectorSize)
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: vlogID, TxnId: 79, Buffer: payload,
	}); err != nil {
		t.Fatal(err)
	}
	<-written
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 79}); err != nil {
		t.Fatal(err)
	}
	close(block)
	deadline := time.Now().Add(time.Second)
	for s.plogs[late.PlogID].LogicalLength() != int64(len(payload)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.plogs[late.PlogID].LogicalLength(); got != int64(len(payload)) {
		t.Fatalf("late mirror length = %d, want %d", got, len(payload))
	}

	if err := s.SetNodeState(ctx, late.DiskID, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, late.DiskID, meta.NodeWorking); err != nil {
		t.Fatal(err)
	}
	got, err := s.plogs[late.PlogID].Read(0, len(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("returned complete non-quorum mirror changed bytes")
	}
}

func TestNodeReturnPreservesConcurrentSuccessfulWrite(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	ctx := context.Background()
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	if err := s.SetNodeState(ctx, 3, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	mappings, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	started := make(chan struct{}, 2)
	clients := make([]storage.PlogClient, len(mappings))
	for i, mapping := range mappings {
		if mapping.DiskID == 3 {
			clients[i] = offlinePlogClient{plogID: mapping.PlogID}
			continue
		}
		clients[i] = &writeFaultClient{
			block: block, started: started,
			local: &localPlogClient{plog: s.plogs[mapping.PlogID]},
		}
	}
	vlog, err := storage.NewVlog(vlogID, "DUPLICATE", 1, 0, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := vlog.SetWriteQuorum(2); err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	s.vlogs[vlogID] = vlog
	s.vlogMu.Unlock()

	payload := bytes.Repeat([]byte{0x78}, 2*storage.SectorSize)
	writeDone := make(chan error, 1)
	go func() {
		_, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
			VlogId: vlogID, TxnId: 80, Buffer: payload,
		})
		writeDone <- err
	}()
	<-started
	<-started
	stateDone := make(chan error, 1)
	go func() {
		stateDone <- s.SetNodeState(ctx, 3, meta.NodeWorking)
	}()
	select {
	case err := <-stateDone:
		t.Fatalf("node return remounted during the affected write: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(block)
	if err := <-stateDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 80}); err != nil {
		t.Fatal(err)
	}
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: vlogID, Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatal("node return lost the concurrent successful write")
	}
}

type readFaultClient struct {
	data    []byte
	slow    bool
	block   <-chan struct{}
	started chan<- struct{}
}

func (c *readFaultClient) Write(context.Context, int64, []byte) (int64, error) {
	return 0, fmt.Errorf("unused")
}

func (c *readFaultClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if c.block != nil {
		if c.started != nil {
			c.started <- struct{}{}
		}
		<-c.block
	}
	if c.slow {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return append([]byte(nil), c.data[offset:offset+int64(length)]...), nil
	}
}

func TestReadVlogDoesNotWaitForSlowDuplicate(t *testing.T) {
	payload := []byte("healthy duplicate")
	vlog, err := storage.NewVlog(99, "DUPLICATE", 1, 0, []storage.PlogClient{
		&readFaultClient{slow: true},
		&readFaultClient{data: payload},
	}, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{vlogs: map[uint32]*storage.Vlog{99: vlog}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: 99, Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatalf("healthy duplicate was hidden behind a slow copy: %v", err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatalf("read = %q, want %q", read.GetBuffer(), payload)
	}
}

func TestSlowVlogReadDoesNotBlockUnrelatedVlog(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	slow, err := storage.NewVlog(101, "NONE", 1, 0, []storage.PlogClient{
		&readFaultClient{data: []byte{'x'}, block: block, started: started},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("healthy")
	healthy, err := storage.NewVlog(102, "NONE", 1, 0, []storage.PlogClient{
		&readFaultClient{data: payload},
	}, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{vlogs: map[uint32]*storage.Vlog{101: slow, 102: healthy}}
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		_, _ = s.ReadVlog(context.Background(), &pb.ReadVlogRequest{VlogId: 101, Length: 1})
	}()
	<-started
	time.AfterFunc(50*time.Millisecond, func() { close(block) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{
		VlogId: 102, Length: uint32(len(payload)),
	})
	if err != nil {
		t.Fatalf("slow unrelated vlog blocked healthy read: %v", err)
	}
	if !bytes.Equal(read.GetBuffer(), payload) {
		t.Fatalf("read = %q, want %q", read.GetBuffer(), payload)
	}
	<-slowDone
}

type ecReadFaultClient struct {
	data []byte
	fail bool
	slow bool
}

func (c *ecReadFaultClient) Write(context.Context, int64, []byte) (int64, error) {
	return 0, fmt.Errorf("unused")
}

func (c *ecReadFaultClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if c.fail {
		return nil, fmt.Errorf("disk read failed")
	}
	if c.slow {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return append([]byte(nil), c.data[offset:offset+int64(length)]...), nil
}

func TestECReadDoesNotWaitForSlowShardAfterReconstructionQuorum(t *testing.T) {
	defer storage.SetECColumnBytesForTest(32)()
	shards := [][]byte{
		bytes.Repeat([]byte{'a'}, 32),
		bytes.Repeat([]byte{'b'}, 32),
		make([]byte, 32),
		make([]byte, 32),
	}
	encoder, err := reedsolomon.New(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(shards); err != nil {
		t.Fatal(err)
	}
	vlog, err := storage.NewVlog(105, "EC", 2, 2, []storage.PlogClient{
		&ecReadFaultClient{fail: true},
		&ecReadFaultClient{data: shards[1]},
		&ecReadFaultClient{data: shards[2]},
		&ecReadFaultClient{slow: true},
	}, 64)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{vlogs: map[uint32]*storage.Vlog{105: vlog}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{VlogId: 105, Length: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("EC reconstruction waited for a slow shard after reaching data quorum: %v", err)
	}
	if !bytes.Equal(read.GetBuffer(), shards[0][:8]) {
		t.Fatalf("read = %q, want %q", read.GetBuffer(), shards[0][:8])
	}
}

type writeFaultClient struct {
	slow        bool
	block       <-chan struct{}
	started     chan<- struct{}
	blockAfter  <-chan struct{}
	afterStart  chan<- struct{}
	local       *localPlogClient
	readBlock   <-chan struct{}
	readStarted chan<- struct{}
}

func (c *writeFaultClient) Write(context.Context, int64, []byte) (int64, error) {
	return 0, fmt.Errorf("positioned writes expected")
}

func (c *writeFaultClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if c.readBlock != nil {
		if c.readStarted != nil {
			select {
			case c.readStarted <- struct{}{}:
			default:
			}
		}
		<-c.readBlock
	}
	if c.local != nil {
		return c.local.Read(ctx, offset, length)
	}
	return nil, fmt.Errorf("unused")
}

func (c *writeFaultClient) EnsureAppend(ctx context.Context, offset int64, data []byte) error {
	if c.blockAfter != nil {
		err := c.local.EnsureAppend(ctx, offset, data)
		if c.afterStart != nil {
			c.afterStart <- struct{}{}
		}
		<-c.blockAfter
		return err
	}
	if c.block != nil {
		if c.started != nil {
			c.started <- struct{}{}
		}
		<-c.block
	}
	if c.slow {
		<-ctx.Done()
		return ctx.Err()
	}
	if c.local != nil {
		return c.local.EnsureAppend(ctx, offset, data)
	}
	return ctx.Err()
}

func (c *writeFaultClient) Commit(ctx context.Context, _ int64) error {
	if c.slow {
		<-ctx.Done()
		return ctx.Err()
	}
	if c.local != nil {
		return c.local.Commit(ctx, 0)
	}
	return nil
}

func (c *writeFaultClient) Scrub() (storage.ScrubResult, error) {
	if c.local == nil {
		return storage.ScrubResult{}, fmt.Errorf("unused")
	}
	return c.local.Scrub()
}

func (c *writeFaultClient) TruncateTo(logical int64) error {
	if c.local == nil {
		return fmt.Errorf("unused")
	}
	return c.local.TruncateTo(logical)
}

func TestWriteVlogDoesNotWaitForSlowCopyAfterQuorum(t *testing.T) {
	clients := []storage.PlogClient{
		&writeFaultClient{},
		&writeFaultClient{},
		&writeFaultClient{slow: true},
	}
	vlog, err := storage.NewVlog(100, "DUPLICATE", 1, 0, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := vlog.SetWriteQuorum(2); err != nil {
		t.Fatal(err)
	}
	s := newControlPlaneServer(t, 1)
	s.vlogs = map[uint32]*storage.Vlog{100: vlog}
	if _, err := s.db.GetDB().Exec("INSERT INTO vlog (id, protection_scheme, data_shards, parity_shards) VALUES (?, 'NONE', 1, 0)", 100); err != nil {
		t.Fatal(err)
	}

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelWrite()
	if _, err := s.WriteVlog(writeCtx, &pb.WriteVlogRequest{
		VlogId: 100, TxnId: 1, Buffer: []byte("quorum"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeCtx.Err(); err != nil {
		t.Fatalf("write waited for slow copy after reaching quorum: %v", err)
	}

	commitCtx, cancelCommit := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCommit()
	if _, err := s.CommitVlog(commitCtx, &pb.CommitVlogRequest{TxnId: 1}); err != nil {
		t.Fatal(err)
	}
	if err := commitCtx.Err(); err != nil {
		t.Fatalf("commit waited for slow copy after reaching quorum: %v", err)
	}
}

func TestConcurrentWriteVlogCannotCrossAddressBoundary(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	vlog, err := storage.NewVlog(105, "NONE", 1, 0, []storage.PlogClient{
		&writeFaultClient{block: block, started: started},
	}, MaxVlogBytes-1)
	if err != nil {
		t.Fatal(err)
	}
	s := newControlPlaneServer(t, 1)
	s.vlogs = map[uint32]*storage.Vlog{105: vlog}
	if _, err := s.db.GetDB().Exec("INSERT INTO vlog (id, protection_scheme, data_shards, parity_shards) VALUES (?, 'NONE', 1, 0)", 105); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	results := make(chan error, 2)
	go func() {
		_, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: 105, Buffer: []byte{1}})
		results <- err
	}()
	<-started // the first append holds writeMu inside the backing-node write
	go func() {
		_, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: 105, Buffer: []byte{2}})
		results <- err
	}()
	close(block)

	var succeeded, failed int
	for range 2 {
		if err := <-results; err != nil {
			failed++
		} else {
			succeeded++
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent boundary writes: %d succeeded, %d failed; want 1 and 1", succeeded, failed)
	}
	if got := vlog.Length(); got != MaxVlogBytes {
		t.Fatalf("vlog length = %d, want %d", got, MaxVlogBytes)
	}
}

func TestCommitVlogWaitsForConcurrentWrite(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	vlog, err := storage.NewVlog(106, "NONE", 1, 0, []storage.PlogClient{
		&writeFaultClient{block: block, started: started},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s := newControlPlaneServer(t, 1)
	s.vlogs = map[uint32]*storage.Vlog{106: vlog}
	if _, err := s.db.GetDB().Exec("INSERT INTO vlog (id, protection_scheme, data_shards, parity_shards) VALUES (?, 'NONE', 1, 0)", 106); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	writeDone := make(chan error, 1)
	go func() {
		_, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
			VlogId: 106, TxnId: 1, Buffer: []byte("slow write"),
		})
		writeDone <- err
	}()
	<-started

	commitDone := make(chan error, 1)
	go func() {
		_, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 1})
		commitDone <- err
	}()
	select {
	case err := <-commitDone:
		t.Fatalf("commit returned before concurrent write completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(block)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-commitDone; err != nil {
		t.Fatal(err)
	}
}

func TestDiskRoundTripPreservesLeasedWriteTail(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{
		Path: "/disk-round-trip-write", OperationKey: "disk-round-trip-write",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("leased-tail"), (5<<20)/len("leased-tail")+1)
	payload = payload[:5<<20]
	if _, err := s.Write(ctx, &pb.WriteRequest{
		Handle: open.GetHandle(), Buffer: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{
		Handle: open.GetHandle(), IdempotencyKey: "disk-round-trip-write",
	}); err != nil {
		t.Fatal(err)
	}
	read, err := s.Open(ctx, &pb.OpenRequest{Path: "/disk-round-trip-write"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, &pb.ReadRequest{
		Handle: read.GetHandle(), Length: int64(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetBuffer(), payload) {
		t.Fatalf("file after disk round trip has %d bytes, want %d", len(got.GetBuffer()), len(payload))
	}
}

func TestSlowVlogWriteDoesNotBlockUnrelatedVlog(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	slow, err := storage.NewVlog(103, "NONE", 1, 0, []storage.PlogClient{
		&writeFaultClient{block: block, started: started},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := storage.NewVlog(104, "NONE", 1, 0, []storage.PlogClient{
		&writeFaultClient{},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	s.vlogs = map[uint32]*storage.Vlog{103: slow, 104: healthy}
	for _, id := range []uint32{103, 104} {
		if _, err := s.db.GetDB().Exec("INSERT INTO vlog (id, protection_scheme, data_shards, parity_shards) VALUES (?, 'NONE', 1, 0)", id); err != nil {
			t.Fatal(err)
		}
	}
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		_, _ = s.WriteVlog(context.Background(), &pb.WriteVlogRequest{
			VlogId: 103, TxnId: 1, Buffer: []byte("slow"),
		})
	}()
	<-started
	time.AfterFunc(50*time.Millisecond, func() { close(block) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{
		VlogId: 104, TxnId: 2, Buffer: []byte("healthy"),
	}); err != nil {
		t.Fatalf("slow unrelated vlog blocked healthy write: %v", err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("healthy write returned after its deadline: %v", err)
	}
	<-slowDone
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

func TestNodeReturnRetryDoesNotPublishPlogRejectedDuringRemount(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 1)
	s.SetMaintenanceInterval(0)
	vlogID := provision(t, s, "NONE", 1, 0)
	writeVlog(t, s, vlogID, bytes.Repeat([]byte("durable before media damage"), 300))
	mappings, err := s.db.ListVlogPlogs(ctx, vlogID)
	if err != nil || len(mappings) != 1 {
		t.Fatalf("vlog mappings = %v, err = %v", mappings, err)
	}
	plogID := mappings[0].PlogID
	path := s.plogPath(1, plogID)

	if err := s.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}
	// The node comes back, but its disk lost the committed body while offline.
	// The header remains readable, so rejection happens during vlog remount
	// rather than while opening the returned plog.
	if err := os.Truncate(path, storage.SectorSize); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := s.SetNodeState(ctx, 1, meta.NodeWorking); err == nil {
			t.Fatalf("damaged node return attempt %d succeeded", attempt)
		}
		if got := s.NodeStates()[1]; got != meta.NodeFailed {
			t.Fatalf("node state after rejected return %d = %q, want failed", attempt, got)
		}
		if !s.offlinePlogs[plogID] {
			t.Fatalf("rejected return %d removed plog %d from offline set", attempt, plogID)
		}
		if _, ok := s.plogs[plogID]; ok {
			t.Fatalf("rejected return %d published plog %d", attempt, plogID)
		}
	}
}

func TestNodeReturnRejectsSubstitutedPlogIdentity(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 1)
	s.SetMaintenanceInterval(0)
	first := provision(t, s, "NONE", 1, 0)
	second := provision(t, s, "NONE", 1, 0)
	writeVlog(t, s, first, []byte("first-vlog"))
	writeVlog(t, s, second, []byte("other-vlog"))
	firstMapping, err := s.db.ListVlogPlogs(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	secondMapping, err := s.db.ListVlogPlogs(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState(ctx, 1, meta.NodeFailed); err != nil {
		t.Fatal(err)
	}

	firstPath := s.plogPath(1, firstMapping[0].PlogID)
	secondPath := s.plogPath(1, secondMapping[0].PlogID)
	substitute, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstPath, substitute, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := s.SetNodeState(ctx, 1, meta.NodeWorking); err == nil {
		t.Fatal("node return mounted a substituted plog")
	}
	if got := s.NodeStates()[1]; got != meta.NodeFailed {
		t.Fatalf("node state after rejected return = %q, want failed", got)
	}
	if !s.offlinePlogs[firstMapping[0].PlogID] {
		t.Fatal("substituted plog left the offline set")
	}
}
