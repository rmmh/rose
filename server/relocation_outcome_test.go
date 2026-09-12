package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestRelocationUncertainOutcome(t *testing.T) {
	for _, fault := range []string{"applied", "reconciliation read fails", "destination returned", "remount and rollback fail"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			s, closeServer := openPublicationCrashServer(t, dir, 2)
			defer closeServer()
			want := bytes.Repeat([]byte("outcome reconciliation preserves committed bytes"), 50)
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
			injected := errors.New("injected relocation outcome failure")
			s.maintenanceFault = func(at string) error {
				if at == "relocation-commit-result" && fault != "remount and rollback fail" {
					if fault == "destination returned" {
						if _, err := s.db.GetDB().Exec("UPDATE disk SET state='failed' WHERE id=3; UPDATE disk SET state='active' WHERE id=3"); err != nil {
							t.Fatal(err)
						}
					}
					return injected
				}
				if at == "relocation-reconcile" && fault == "reconciliation read fails" {
					return injected
				}
				if at == "relocation-before-remount" && fault == "remount and rollback fail" {
					// Preserve the source bytes but invalidate the pre-I/O rollback token.
					if _, err := s.db.GetDB().Exec("UPDATE disk SET state='failed' WHERE id=?", source.DiskID); err != nil {
						t.Fatal(err)
					}
					if _, err := s.db.GetDB().Exec("UPDATE disk SET state='active' WHERE id=?", source.DiskID); err != nil {
						t.Fatal(err)
					}
					return injected
				}
				return nil
			}
			s.vlogMu.Lock()
			err = s.migratePlogLocked(ctx, source.PlogID, placement.VlogID, source.DiskID, 3)
			s.vlogMu.Unlock()
			s.maintenanceFault = nil
			if !errors.Is(err, injected) {
				t.Fatalf("outcome error = %v", err)
			}
			if got := diskOf(t, s, placement.VlogID, source.ShardIndex); got != 3 {
				t.Fatalf("catalog disk=%d", got)
			}
			oldPath, newPath := s.plogPath(source.DiskID, source.PlogID), s.plogPath(3, source.PlogID)
			if _, err := os.Stat(newPath); err != nil {
				t.Fatalf("authoritative destination removed after error: %v", err)
			}
			if fault == "applied" {
				if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
					t.Fatalf("resolved source retained: %v", err)
				}
				if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
					t.Fatal("resolved bytes changed")
				}
				return
			}
			for _, path := range []string{oldPath, newPath} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("unresolved candidate removed: %v", err)
				}
			}
			if s.vlogs[placement.VlogID] != nil || s.plogs[source.PlogID] != nil || s.quarantinedVlogs[placement.VlogID] == nil {
				t.Fatal("unresolved placement still mounted")
			}
			if _, err := s.ReadVlog(ctx, &pb.ReadVlogRequest{VlogId: placement.VlogID, Length: 1}); err == nil {
				t.Fatal("unresolved read admitted")
			}
			if _, err := s.WriteVlog(ctx, &pb.WriteVlogRequest{VlogId: placement.VlogID, Buffer: []byte("unsafe")}); err == nil {
				t.Fatal("unresolved write admitted")
			}
			s.vlogMu.Lock()
			remountErr := s.remountVlogLocked(ctx, placement.VlogID)
			retryErr := s.migratePlogLocked(ctx, source.PlogID, placement.VlogID, 3, source.DiskID)
			s.vlogMu.Unlock()
			if remountErr == nil || retryErr == nil {
				t.Fatal("unresolved placement escaped recovery fence")
			}
			// Quiescent recovery on the same server must clear the actual fence,
			// not merely rely on a new Server starting with an empty map.
			s.CloseStorage()
			if err := s.Recover(ctx); err != nil {
				t.Fatalf("quiescent recovery failed: %v", err)
			}
			s.StopMaintenanceDriver()
			if s.quarantinedVlogs[placement.VlogID] != nil {
				t.Fatal("recovered mount remains quarantined")
			}
			if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
				t.Fatal("quiescent recovery changed bytes")
			}
			closeServer()
			recovered, finish := openPublicationCrashServer(t, dir, 3)
			defer finish()
			if got := readServerFileInternal(t, recovered, "file"); !bytes.Equal(got, want) {
				t.Fatal("restart changed acknowledged bytes")
			}
			if removed, err := recovered.SweepStrayPlogFiles(ctx); err != nil || removed != 1 {
				t.Fatalf("recovery sweep=%d %v", removed, err)
			}
			if _, err := os.Stat(newPath); err != nil {
				t.Fatalf("authoritative destination lost: %v", err)
			}
		})
	}
}

func TestRelocationRejectedCommitPreservesSource(t *testing.T) {
	ctx := context.Background()
	s, finish := openPublicationCrashServer(t, t.TempDir(), 2)
	defer finish()
	want := []byte("failed commit must resolve to durable source")
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
	if _, err := s.db.GetDB().Exec(`CREATE TABLE relocation_commit_fault (id INTEGER REFERENCES node(id) DEFERRABLE INITIALLY DEFERRED);
 CREATE TRIGGER fail_relocation_commit AFTER UPDATE OF disk_id ON plog BEGIN INSERT INTO relocation_commit_fault VALUES(999); END;`); err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, source.PlogID, placement.VlogID, source.DiskID, 3)
	s.vlogMu.Unlock()
	if err == nil {
		t.Fatal("deferred commit constraint accepted")
	}
	if got := diskOf(t, s, placement.VlogID, source.ShardIndex); got != source.DiskID {
		t.Fatalf("failed commit changed placement: %d", got)
	}
	if _, err := os.Stat(s.plogPath(3, source.PlogID)); !os.IsNotExist(err) {
		t.Fatalf("known unpublished copy retained: %v", err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("failed commit changed source bytes")
	}
	if _, err := s.db.GetDB().Exec("DROP TRIGGER fail_relocation_commit"); err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, source.PlogID, placement.VlogID, source.DiskID, 3)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatalf("resolved rollback prevents retry: %v", err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("retry changed bytes")
	}
}
