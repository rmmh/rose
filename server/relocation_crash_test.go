package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRelocationCrashChild(t *testing.T) {
	stage := os.Getenv("ROSE_RELOCATION_CRASH_STAGE")
	if stage == "" {
		return
	}
	s, _ := openPublicationCrashServer(t, os.Getenv("ROSE_RELOCATION_CRASH_DIR"), 3)
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
	err = s.migratePlogLocked(context.Background(), shards[0].PlogID, placement.VlogID, shards[0].DiskID, 3)
	s.vlogMu.Unlock()
	t.Fatalf("relocation crash hook not reached: %v", err)
}

func TestRelocationProcessCrashRecovery(t *testing.T) {
	for _, stage := range []string{"relocation-before-repoint", "relocation-after-repoint"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			s, closeServer := openPublicationCrashServer(t, dir, 2)
			want := bytes.Repeat([]byte("relocation crash preserves acknowledged bytes"), 100)
			auditWrite(t, s, "file", want)
			placement := auditPlacement(t, s, "file")
			shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
			if err != nil {
				t.Fatal(err)
			}
			source := shards[0]
			if err := s.AttachDiskOnNode(ctx, 3, 3, filepath.Join(dir, "disk-3"), 0); err != nil {
				t.Fatal(err)
			}
			oldPath, newPath := s.plogPath(source.DiskID, source.PlogID), s.plogPath(3, source.PlogID)
			closeServer()
			cmd := exec.Command(os.Args[0], "-test.run=^TestRelocationCrashChild$")
			cmd.Env = append(os.Environ(), "ROSE_RELOCATION_CRASH_STAGE="+stage, "ROSE_RELOCATION_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				log := filepath.Join(dir, "child.log")
				_ = os.WriteFile(log, output, 0600)
				t.Fatalf("child did not reach %s: %v; details in %s", stage, err, log)
			}
			// Neither invocation cleanup nor CloseStorage runs in the child.
			for _, path := range []string{oldPath, newPath} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("crash did not retain candidate %s: %v", path, err)
				}
			}
			recovered, finish := openPublicationCrashServer(t, dir, 3)
			defer finish()
			expectedDisk, strayPath, authoritativePath := source.DiskID, newPath, oldPath
			if stage == "relocation-after-repoint" {
				expectedDisk, strayPath, authoritativePath = 3, oldPath, newPath
			}
			if got := diskOf(t, recovered, placement.VlogID, source.ShardIndex); got != expectedDisk {
				t.Fatalf("recovered placement = %d, want %d", got, expectedDisk)
			}
			if got := readServerFileInternal(t, recovered, "file"); !bytes.Equal(got, want) {
				t.Fatal("recovery changed acknowledged bytes")
			}
			if removed, err := recovered.SweepStrayPlogFiles(ctx); err != nil || removed != 1 {
				t.Fatalf("stray sweep = %d, %v", removed, err)
			}
			if _, err := os.Stat(strayPath); !os.IsNotExist(err) {
				t.Fatalf("stray survived: %v", err)
			}
			if _, err := os.Stat(authoritativePath); err != nil {
				t.Fatalf("authoritative file removed: %v", err)
			}
			if removed, err := recovered.SweepStrayPlogFiles(ctx); err != nil || removed != 0 {
				t.Fatalf("repeat sweep = %d, %v", removed, err)
			}
			if got := readServerFileInternal(t, recovered, "file"); !bytes.Equal(got, want) {
				t.Fatal("sweep changed acknowledged bytes")
			}
		})
	}
}

func TestRelocationPostRepointErrorCompletesRemount(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	want := []byte("relocation must remount even when its response is lost")
	auditWrite(t, s, "file", want)
	placement := auditPlacement(t, s, "file")
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	source := shards[0]
	if err := s.AttachDiskOnNode(ctx, 3, 3, filepath.Join(t.TempDir(), "disk3"), 0); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("lost relocation response")
	s.maintenanceFault = func(at string) error {
		if at == "relocation-after-repoint" {
			return injected
		}
		return nil
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, source.PlogID, placement.VlogID, source.DiskID, 3)
	s.vlogMu.Unlock()
	s.maintenanceFault = nil
	if !errors.Is(err, injected) {
		t.Fatalf("completion error = %v", err)
	}
	if got := diskOf(t, s, placement.VlogID, source.ShardIndex); got != 3 {
		t.Fatalf("published disk = %d", got)
	}
	if _, err := os.Stat(s.plogPath(source.DiskID, source.PlogID)); !os.IsNotExist(err) {
		t.Fatalf("old file survived completion: %v", err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("completion error changed readable bytes")
	}
}
