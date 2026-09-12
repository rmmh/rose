package server

import (
	"context"
	pb "github.com/rmmh/rose/proto"
	"testing"
)

func TestGCCollectsInternalJobsAndKeepsPublicStatus(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	source := provision(t, s, "NONE", 1, 0)
	internal, err := s.db.GetOrCreateScrubRepairJob(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.MarkJobDone(ctx, internal.ID); err != nil {
		t.Fatal(err)
	}
	public, err := s.db.GetOrCreateDrainJob(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.MarkJobDone(ctx, public.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GC(ctx); err != nil || n != 0 {
		t.Fatalf("GC changed chunk count: %d %v", n, err)
	}
	var count int
	if err := s.db.GetDB().QueryRow("SELECT count(*) FROM job WHERE id=?", internal.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("internal job retained: %d %v", count, err)
	}
	status, err := s.GetMaintenanceJob(ctx, &pb.GetMaintenanceJobRequest{JobId: uint64(public.ID)})
	if err != nil || status.State != pb.MaintenanceJobState_MAINTENANCE_JOB_STATE_COMPLETED {
		t.Fatalf("public terminal status lost: %v %v", status, err)
	}
}
