package server

import (
	"context"
	"os"
	"testing"

	"github.com/rmmh/rose/meta"
)

// An interrupted rewrite has assigned a destination but has not copied its
// first chunk. Another maintenance pass must not retire that apparently empty
// output, or the original durable job can never resume.
func TestRunningDestinationDefersRewrite(t *testing.T) {
	for _, action := range []string{"compact", "promote"} {
		t.Run(action, func(t *testing.T) {
			s := newControlPlaneServer(t, 2)
			s.StopMaintenanceDriver()
			defer s.CloseStorage()
			ctx := context.Background()
			s.vlogMu.Lock()
			source, _, err := s.provisionStagingVlogLocked(ctx, 1, 1)
			s.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			info, err := s.db.GetVlog(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			job, err := s.db.GetOrCreateCompactionJob(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			s.vlogMu.Lock()
			dest, _, err := s.provisionCompactionDestinationLocked(ctx, info)
			if err == nil {
				err = s.assignMaintenanceDestinationLocked(ctx, job.ID, dest)
			}
			s.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			shards, err := s.db.VlogShardDisks(ctx, dest)
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				if action == "compact" {
					err = s.CompactVlog(ctx, dest)
				} else {
					var promoted bool
					promoted, err = s.PromoteStagingVlog(ctx, dest)
					if promoted {
						t.Fatal("promoted another running job's destination")
					}
				}
				if err != nil {
					t.Fatalf("deferred rewrite: %v", err)
				}
				if _, err := s.db.GetVlog(ctx, dest); err != nil {
					t.Fatalf("running destination retired: %v", err)
				}
				for _, shard := range shards {
					if _, err := os.Stat(s.plogPath(shard.DiskID, shard.PlogID)); err != nil {
						t.Fatalf("owned destination file removed: %v", err)
					}
				}
			}
			if _, err := s.db.RetireVlog(ctx, dest); err == nil {
				t.Fatal("catalog accepted retirement of running destination")
			}
			if err := s.CompactVlog(ctx, source); err != nil {
				t.Fatalf("original job cannot resume: %v", err)
			}
			finished, err := s.db.GetJob(ctx, job.ID)
			if err != nil || finished.State != meta.JobDone {
				t.Fatalf("original job=%v err=%v", finished, err)
			}
			if err := s.CompactVlog(ctx, dest); err != nil {
				t.Fatalf("completed destination remains held: %v", err)
			}
		})
	}
}
