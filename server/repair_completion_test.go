package server

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

func TestIncompleteRepairPreservesJobUntilRecovery(t *testing.T) {
	for _, mode := range []string{"scrub", "offline"} {
		t.Run(mode, func(t *testing.T) {
			s := newControlPlaneServer(t, 2)
			s.StopMaintenanceDriver()
			defer s.CloseStorage()
			ctx := context.Background()
			id := provision(t, s, "DUPLICATE", 1, 0)
			payload := bytes.Repeat([]byte{0x6b}, 3*storage.SectorSize)
			if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: id, TxnId: 92, Buffer: payload}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{TxnId: 92}); err != nil {
				t.Fatal(err)
			}
			shards, err := s.db.VlogShardDisks(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			victim, peer := shards[0], shards[1]
			if mode == "scrub" {
				file, err := os.OpenFile(s.plogPath(victim.DiskID, victim.PlogID), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				var b [1]byte
				if _, err := file.ReadAt(b[:], storage.CalcPhysical(100)); err != nil {
					file.Close()
					t.Fatal(err)
				}
				b[0] ^= 0xff
				if _, err := file.WriteAt(b[:], storage.CalcPhysical(100)); err != nil {
					file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				s.offlinePlogs[victim.PlogID] = true
			}
			s.vlogMu.Lock()
			err = s.setDiskStateLocked(ctx, peer.DiskID, "failed")
			s.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			repair := func() (ScrubRepairResult, error) {
				if mode == "offline" {
					return s.RepairOfflineShards(ctx)
				}
				return s.RepairVlog(ctx, id)
			}
			var jobID int64
			for attempt := 0; attempt < 3; attempt++ {
				result, err := repair()
				if err != nil || len(result.Unrepairable) != 1 || result.ShardsRepaired != 0 {
					t.Fatalf("incomplete repair=%+v err=%v", result, err)
				}
				jobs, err := s.db.RunningJobs(ctx)
				if err != nil || len(jobs) != 1 || jobs[0].Kind != meta.JobScrubRepair {
					t.Fatalf("incomplete repair lost durable job: %v err=%v", jobs, err)
				}
				if attempt == 0 {
					jobID = jobs[0].ID
				}
				if jobs[0].ID != jobID {
					t.Fatal("retry replaced incomplete job")
				}
			}
			s.vlogMu.Lock()
			err = s.setDiskStateLocked(ctx, peer.DiskID, "active")
			s.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			result, err := repair()
			if err != nil || len(result.Unrepairable) != 0 || result.ShardsRepaired != 1 {
				t.Fatalf("recovered repair=%+v err=%v", result, err)
			}
			job, err := s.db.GetJob(ctx, jobID)
			if err != nil || job.State != meta.JobDone {
				t.Fatalf("recovered job=%v err=%v", job, err)
			}
			read, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{VlogId: id, Length: uint32(len(payload))})
			if err != nil || !bytes.Equal(read.GetBuffer(), payload) {
				t.Fatalf("repaired bytes differ: %v", err)
			}
		})
	}
}
