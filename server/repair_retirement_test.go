package server

import (
	"context"
	"testing"

	"github.com/rmmh/rose/meta"
)

func TestCompactionReleasesSupersededRepair(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	source := provision(t, s, "DUPLICATE", 1, 0)
	job, err := s.db.GetOrCreateScrubRepairJob(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompactVlog(ctx, source); err != nil {
		t.Fatal(err)
	}
	current, err := s.db.GetJob(ctx, job.ID)
	if err != nil || current.State != meta.JobCancelled {
		t.Fatalf("retired source retained repair: %v err=%v", current, err)
	}
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("recovery would resume obsolete jobs: %v", jobs)
	}
}
