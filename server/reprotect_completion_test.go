package server

import (
	"context"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestEmptyFailedDiskDoesNotAccumulateReprotectJobs(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 10; pass++ {
		if err := s.ReprotectDisk(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.db.GetDB().QueryRow("SELECT COUNT(*) FROM job WHERE kind='reprotect'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("empty passes created %d jobs: %v", count, err)
	}
	// Recovery of a final-step interruption must still finish the existing job.
	job, err := s.db.GetOrCreateReprotectJob(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 10; pass++ {
		if err := s.ReprotectDisk(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.db.GetJob(ctx, job.ID)
	if err != nil || got.State != meta.JobDone {
		t.Fatalf("existing job unfinished: %v %v", got, err)
	}
	if err := s.db.GetDB().QueryRow("SELECT COUNT(*) FROM job WHERE kind='reprotect'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("completed passes created %d jobs: %v", count, err)
	}
}

func TestReprotectRPCDoesNotReusePriorFailureCompletion(t *testing.T) {
	s := newControlPlaneServer(t, 4)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	for _, disk := range []uint32{3, 4} {
		if err := s.SetDiskState(ctx, disk, meta.DiskDraining); err != nil {
			t.Fatal(err)
		}
	}
	id := provision(t, s, "DUPLICATE", 1, 0)
	writeVlog(t, s, id, []byte("first failure"))
	if err := s.SetDiskState(ctx, 3, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	first, err := s.StartReprotect(ctx, &pb.StartReprotectRequest{DiskId: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	secondVlog := provision(t, s, "DUPLICATE", 1, 0)
	writeVlog(t, s, secondVlog, []byte("second failure"))
	if err := s.SetDiskState(ctx, 4, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.db.PlogsOnDisk(ctx, 1)
	if err != nil || len(remaining) == 0 {
		t.Fatalf("fixture lacks new failed shards: %v %v", remaining, err)
	}
	second, err := s.StartReprotect(ctx, &pb.StartReprotectRequest{DiskId: 1})
	if err != nil {
		t.Fatal(err)
	}
	if second.JobId == first.JobId {
		t.Fatal("RPC reused completion from a previous disk failure")
	}
	remaining, err = s.db.PlogsOnDisk(ctx, 1)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("new failure was not reprotected: %v %v", remaining, err)
	}
	retry, err := s.StartReprotect(ctx, &pb.StartReprotectRequest{DiskId: 1})
	if err != nil || retry.GetJobId() != second.JobId {
		t.Fatalf("current completion retry=%v err=%v", retry, err)
	}
	for vlog, want := range map[uint32]string{id: "first failure", secondVlog: "second failure"} {
		got, err := s.vlogs[vlog].Read(ctx, 0, len(want))
		if err != nil || string(got) != want {
			t.Fatalf("vlog %d lost bytes across repeated failure: %v", vlog, err)
		}
	}
}
