package server

import (
	"bytes"
	"context"
	"errors"
	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRepairCrashChild(t *testing.T) {
	stage := os.Getenv("ROSE_REPAIR_CRASH_STAGE")
	if stage == "" {
		return
	}
	s, _ := openPublicationCrashServer(t, os.Getenv("ROSE_REPAIR_CRASH_DIR"), 2)
	placement := auditPlacement(t, s, "file")
	shards, err := s.db.VlogShardDisks(context.Background(), placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	s.maintenanceFault = func(at string) error {
		if at == stage {
			os.Exit(73)
		}
		return nil
	}
	s.vlogMu.Lock()
	err = s.regenerateShardLocked(context.Background(), placement.VlogID, shards[0].ShardIndex, shards[0].PlogID, shards[0].DiskID)
	s.vlogMu.Unlock()
	t.Fatalf("repair crash hook not reached: %v", err)
}

func TestRepairProcessCrashRecovery(t *testing.T) {
	for _, stage := range []string{"repair-destination-created", "repair-before-repoint", "repair-after-repoint"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			s, closeServer := openPublicationCrashServer(t, dir, 2)
			want := bytes.Repeat([]byte("repair crash bytes"), 100)
			auditWrite(t, s, "file", want)
			// Raw plogs are legitimately unassigned and must not be treated as repairs.
			u := uid.New()
			raw, err := s.db.MakePlog(ctx, u, 1)
			if err != nil {
				t.Fatal(err)
			}
			h, err := s.basePlogHeader(ctx, raw, 1, u)
			if err != nil {
				t.Fatal(err)
			}
			rawPath := s.plogPath(1, raw)
			p, err := storage.OpenPlog(rawPath, raw, storage.WithHeader(h))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Write(0, []byte("raw bytes")); err != nil {
				t.Fatal(err)
			}
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
			p.Close()
			rawBytes, err := os.ReadFile(rawPath)
			if err != nil {
				t.Fatal(err)
			}
			placement := auditPlacement(t, s, "file")
			before, err := s.db.VlogShardDisks(ctx, placement.VlogID)
			if err != nil {
				t.Fatal(err)
			}
			closeServer()
			cmd := exec.Command(os.Args[0], "-test.run=^TestRepairCrashChild$")
			cmd.Env = append(os.Environ(), "ROSE_REPAIR_CRASH_STAGE="+stage, "ROSE_REPAIR_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				path := filepath.Join(dir, "child.log")
				_ = os.WriteFile(path, output, 0600)
				t.Fatalf("child did not reach %s: %v; details in %s", stage, err, path)
			}
			// Capture the allocated ID before recovery deletes its row, if unpublished.
			catalog, err := meta.Open(filepath.Join(dir, "meta.db"))
			if err != nil {
				t.Fatal(err)
			}
			var repairID uint32
			if err := catalog.GetDB().QueryRow("SELECT max(id) FROM plog").Scan(&repairID); err != nil {
				catalog.Close()
				t.Fatal(err)
			}
			catalog.Close()
			repairPath := s.plogPath(before[0].DiskID, repairID)
			dbs, finish := openPublicationCrashServer(t, dir, 2)
			defer finish()
			var orphans int
			if err := dbs.db.GetDB().QueryRow("SELECT count(*) FROM plog WHERE repair_owned=1 AND NOT EXISTS(SELECT 1 FROM vlog_plog WHERE plog_id=plog.id)").Scan(&orphans); err != nil || orphans != 0 {
				t.Fatalf("repair orphan after recovery=%d %v", orphans, err)
			}
			_, fileErr := os.Stat(repairPath)
			if stage == "repair-after-repoint" {
				if fileErr != nil {
					t.Fatalf("published repair file missing: %v", fileErr)
				}
			} else if !os.IsNotExist(fileErr) {
				t.Fatalf("abandoned repair file survived recovery: %v", fileErr)
			}
			after, err := dbs.db.VlogShardDisks(ctx, placement.VlogID)
			if err != nil {
				t.Fatal(err)
			}
			if (after[0].PlogID != before[0].PlogID) != (stage == "repair-after-repoint") {
				t.Fatalf("unexpected recovered mapping: %v -> %v", before, after)
			}
			if got := readServerFileInternal(t, dbs, "file"); !bytes.Equal(got, want) {
				t.Fatal("recovery changed published bytes")
			}
			got, err := os.ReadFile(rawPath)
			if err != nil || !bytes.Equal(got, rawBytes) {
				t.Fatalf("recovery changed raw plog: %v", err)
			}
			rows, err := dbs.db.ListPlogs(ctx)
			if err != nil || len(rows) != 3 {
				t.Fatalf("recovered catalog plogs=%v %v", rows, err)
			}
			// A crash after repoint leaves the old source file. The existing stray sweep
			// handles that catalog-first cleanup and must preserve the published copy.
			if _, err := dbs.SweepStrayPlogFiles(ctx); err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if _, err := os.Stat(dbs.plogPath(row.DiskID, row.ID)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRepairPostRepointErrorCompletesRemount(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	want := []byte("committed repair must remain mounted after a lost response")
	auditWrite(t, s, "file", want)
	placement := auditPlacement(t, s, "file")
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected post-repoint error")
	s.maintenanceFault = func(at string) error {
		if at == "repair-after-repoint" {
			return injected
		}
		return nil
	}
	s.vlogMu.Lock()
	err = s.regenerateShardLocked(ctx, placement.VlogID, shards[0].ShardIndex, shards[0].PlogID, shards[0].DiskID)
	s.vlogMu.Unlock()
	s.maintenanceFault = nil
	if !errors.Is(err, injected) {
		t.Fatalf("completion error=%v", err)
	}
	current, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil || current[0].PlogID == shards[0].PlogID {
		t.Fatalf("repair did not publish: %v %v", current, err)
	}
	if s.plogs[shards[0].PlogID] != nil || s.plogs[current[0].PlogID] == nil {
		t.Fatal("mounted plogs disagree with committed mapping")
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("post-repoint error changed readable bytes")
	}
}
