package server

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestMaintenanceAssignmentErrorCleanup(t *testing.T) {
	for _, outcome := range []string{"unassigned", "cancelled", "committed-reply-lost"} {
		t.Run(outcome, func(t *testing.T) {
			s := newControlPlaneServer(t, 2)
			s.StopMaintenanceDriver()
			defer s.CloseStorage()
			ctx := context.Background()
			source := provision(t, s, "DUPLICATE", 1, 0)
			info, err := s.db.GetVlog(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			job, err := s.db.GetOrCreateCompactionJob(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected assignment boundary failure")
			for attempt := 0; attempt < 5; attempt++ {
				s.vlogMu.Lock()
				dest, _, err := s.provisionCompactionDestinationLocked(ctx, info)
				s.vlogMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				shards, err := s.db.VlogShardDisks(ctx, dest)
				if err != nil {
					t.Fatal(err)
				}
				callCtx, cancel := context.WithCancel(ctx)
				s.maintenanceFault = func(stage string) error {
					if stage == "maintenance-provisioned" {
						if outcome == "cancelled" {
							cancel()
							return nil
						}
						if outcome == "unassigned" {
							return injected
						}
					}
					if stage == "maintenance-assigned" && outcome == "committed-reply-lost" {
						return injected
					}
					return nil
				}
				s.vlogMu.Lock()
				err = s.assignMaintenanceDestinationLocked(callCtx, job.ID, dest)
				s.vlogMu.Unlock()
				cancel()
				s.maintenanceFault = nil
				wantErr := injected
				if outcome == "cancelled" {
					wantErr = context.Canceled
				}
				if !errors.Is(err, wantErr) {
					t.Fatalf("assignment outcome=%v want=%v", err, wantErr)
				}
				logs, err := s.db.ListVlogs(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if outcome == "committed-reply-lost" {
					if len(logs) != 2 || s.vlogs[dest] == nil {
						t.Fatal("ambiguous committed assignment was reclaimed")
					}
					for _, shard := range shards {
						if _, err := os.Stat(s.plogPath(shard.DiskID, shard.PlogID)); err != nil {
							t.Fatalf("committed destination file removed: %v", err)
						}
					}
					s.vlogMu.Lock()
					err = s.assignMaintenanceDestinationLocked(ctx, job.ID, dest)
					s.vlogMu.Unlock()
					if err != nil {
						t.Fatalf("identical assignment retry failed: %v", err)
					}
					break
				}
				if len(logs) != 1 || len(s.vlogs) != 1 {
					t.Fatalf("attempt %d leaked destination: catalog=%d mounted=%d", attempt, len(logs), len(s.vlogs))
				}
				for _, shard := range shards {
					if s.plogs[shard.PlogID] != nil {
						t.Fatal("reclaimed destination retained open plog")
					}
					if _, err := os.Stat(s.plogPath(shard.DiskID, shard.PlogID)); !os.IsNotExist(err) {
						t.Fatalf("reclaimed destination file remains: %v", err)
					}
				}
			}
		})
	}
}
