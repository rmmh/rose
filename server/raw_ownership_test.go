package server

import (
	"context"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestMaintenanceDestinationFencedBeforeJobAssignment(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	s.vlogMu.Lock()
	id, _, err := s.provisionVlogInDomainLocked(ctx, "DUPLICATE", 1, 0, nil, true)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.db.GetVlog(ctx, id)
	if err != nil || !info.MaintenanceOwned {
		t.Fatalf("destination not fenced at creation: %v", err)
	}
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: id, Buffer: []byte("unowned append")}); err == nil {
		t.Fatal("raw write admitted before maintenance job assignment")
	}
}

func TestRawTransactionsCannotMutateOrPublishFileOwnedLogs(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "file-owner"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: make([]byte, 5<<20)}); err != nil {
		t.Fatal(err)
	}
	op, err := s.db.WriteOpByKey(ctx, "file-owner")
	if err != nil {
		t.Fatal(err)
	}
	leases, err := s.db.WriteOpLeases(ctx, op.ID)
	if err != nil || len(leases) == 0 {
		t.Fatalf("leases=%v err=%v", leases, err)
	}
	id := leases[0]
	before, err := s.db.GetVlog(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if s.vlogs[id].Length() <= before.Length {
		t.Fatal("fixture has no unpublished storage tail")
	}
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: id, Buffer: []byte("intrusion")}); err == nil {
		t.Fatal("raw write admitted to leased file log")
	}
	if _, err := s.CommitPlog(ctx, &pb.CommitPlogRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{}); err != nil {
		t.Fatal(err)
	}
	after, err := s.db.GetVlog(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if before.Length != after.Length {
		t.Fatal("raw transaction published a file owner's storage prefix")
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: id, Buffer: []byte("after release")}); err == nil {
		t.Fatal("raw write admitted after file lease release")
	}
}

func TestMaintenanceOwnershipSurvivesCompletionAndRemount(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	source := provision(t, s, "DUPLICATE", 1, 0)
	dest := provision(t, s, "DUPLICATE", 1, 0)
	if err := s.db.SetJobDest(ctx, 1<<60, dest); err == nil {
		t.Fatal("destination accepted without a running job")
	}
	unclaimed, err := s.db.GetVlog(ctx, dest)
	if err != nil || unclaimed.MaintenanceOwned {
		t.Fatalf("failed job claim leaked ownership=%v err=%v", unclaimed.MaintenanceOwned, err)
	}

	job, err := s.db.GetOrCreateCompactionJob(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.SetJobDest(ctx, job.ID, dest); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint32{source, dest} {
		if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: id, Buffer: []byte("foreign append")}); err == nil {
			t.Fatalf("raw write admitted to maintenance vlog %d", id)
		}
	}
	info, err := s.db.GetVlog(ctx, dest)
	if err != nil || !info.MaintenanceOwned {
		t.Fatalf("persisted maintenance ownership=%v err=%v", info.MaintenanceOwned, err)
	}
	// Mount uses the durable owner, not the process-local setup done by compaction.
	s.vlogMu.Lock()
	v, err := s.mountVlogLocked(ctx, info)
	if err == nil {
		s.vlogs[dest] = v
	}
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.GetDB().ExecContext(ctx, "UPDATE job SET state='done' WHERE id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: dest, Buffer: []byte("after completion")}); err == nil {
		t.Fatal("completed destination reverted to raw ownership")
	}
	shards, err := s.db.VlogShardDisks(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, "failed"); err != nil {
		t.Fatal(err)
	}
	// Internal maintenance writes still need all three copies after remount.
	if _, err := s.vlogs[dest].Write(ctx, job.ID, []byte("internal append")); err == nil {
		t.Fatal("remounted maintenance log silently used a two-copy quorum")
	}
}
