package server

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

func promotionCrashPayload(seed int64) []byte {
	// Each record occupies exactly twenty test stripe rows; both source chunks
	// are selected, so the relocation checkpoint cuts a genuinely partial move.
	return publicationCrashBytes(seed)[:192*20-storage.ChunkHeaderSize]
}

func TestPromotionCrashChild(t *testing.T) {
	stage := os.Getenv("ROSE_PROMOTION_CRASH_STAGE")
	if stage == "" {
		return
	}
	defer storage.SetECColumnBytesForTest(64)()
	s, _ := openPublicationCrashServer(t, os.Getenv("ROSE_PROMOTION_CRASH_DIR"), 4)
	s.maintenanceFault = func(at string) error {
		if at == stage {
			os.Exit(73)
		}
		return nil
	}
	_, err := s.PromoteStaging(context.Background())
	t.Fatalf("promotion crash boundary not reached: %v", err)
}

func assertPromotedCrashFiles(t *testing.T, s *Server, snapshot uint64) {
	t.Helper()
	ctx := context.Background()
	for i, path := range []string{"bucket/file", "bucket/other"} {
		want := promotionCrashPayload(int64(11 + i))
		var handle int64
		if snapshot == 0 {
			h, err := s.Open(ctx, &pb.OpenRequest{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			handle = h.Handle
		} else {
			h, err := s.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{Path: path, SnapshotId: snapshot})
			if err != nil {
				t.Fatal(err)
			}
			handle = h.Handle
		}
		read, err := s.Read(ctx, &pb.ReadRequest{Handle: handle, Length: int64(len(want) + 1)})
		if err != nil || !bytes.Equal(read.GetBuffer(), want) {
			t.Fatalf("promotion changed %s snapshot=%d: %v", path, snapshot, err)
		}
		if _, err := s.Close(ctx, &pb.CloseRequest{Handle: handle}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPromotionProcessCrashRecovery(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)()
	for _, stage := range []string{"promotion-destination", "ec-rows-synced", "ec-prefix-recorded", "ec-chunk-relocated", "promotion-job-done", "promotion-source-retired"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			s, closeServer := openPublicationCrashServer(t, dir, 4)
			ctx := context.Background()
			if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "bucket", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
				t.Fatal(err)
			}
			for i, path := range []string{"bucket/file", "bucket/other"} {
				auditWrite(t, s, path, promotionCrashPayload(int64(11+i)))
			}
			staging := stagingVlogID(t, s)
			chunks, err := s.db.LiveChunksInVlog(ctx, staging)
			if err != nil || len(chunks) != 2 {
				t.Fatalf("fixture chunks=%d err=%v", len(chunks), err)
			}
			if count, _ := promotablePrefix(chunks, storage.ECStripeWidth(3)); count != 2 {
				t.Fatal("fixture does not promote both chunks")
			}
			snap, err := s.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "before-promotion"})
			if err != nil {
				t.Fatal(err)
			}
			closeServer()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPromotionCrashChild$")
			cmd.Env = append(os.Environ(), "ROSE_PROMOTION_CRASH_STAGE="+stage, "ROSE_PROMOTION_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				log := filepath.Join(dir, "child.log")
				_ = os.WriteFile(log, output, 0600)
				t.Fatalf("child did not reach %s: %v; details in %s", stage, err, log)
			}
			s, closeServer = openPublicationCrashServer(t, dir, 4)
			assertPromotedCrashFiles(t, s, 0)
			assertPromotedCrashFiles(t, s, snap.SnapshotId)
			// Recover resumes running jobs. A job-done crash may still leave
			// empty staging to be retired by the next regular maintenance pass.
			if _, err := s.PromoteStaging(ctx); err != nil {
				t.Fatal(err)
			}
			if s.vlogs[staging] != nil {
				t.Fatal("promotion source survived completed recovery")
			}
			jobs, err := s.db.RunningJobs(ctx)
			if err != nil || len(jobs) != 0 {
				t.Fatalf("unfinished recovered jobs=%d err=%v", len(jobs), err)
			}
			for _, path := range []string{"bucket/file", "bucket/other"} {
				p := auditPlacement(t, s, path)
				info, err := s.db.GetVlog(ctx, p.VlogID)
				if err != nil || info.ProtectionScheme != "EC" {
					t.Fatalf("recovered destination is not EC: %v", err)
				}
				if err := s.verifyProtectedChunk(ctx, s.vlogs[p.VlogID], p.VlogID, meta.ChunkLoc{Hash: p.Hash, VaddrOffset: p.VaddrOffset, LogicalLen: p.LogicalLen}); err != nil {
					t.Fatalf("recovered EC placement lacks verified protection: %v", err)
				}
			}
			closeServer()
			s, closeServer = openPublicationCrashServer(t, dir, 4)
			defer closeServer()
			assertPromotedCrashFiles(t, s, 0)
			assertPromotedCrashFiles(t, s, snap.SnapshotId)
		})
	}
}
