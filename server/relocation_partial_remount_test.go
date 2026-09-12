package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/storage"
)

func TestRelocationPartialRemountRollbackRequiresRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, finish := openPublicationCrashServer(t, dir, 2)
	defer finish()
	want := bytes.Repeat([]byte("partial remount preserves the acknowledged prefix"), 40)
	auditWrite(t, s, "file", want)
	placement := auditPlacement(t, s, "file")
	info, err := s.db.GetVlog(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil || len(shards) != 2 {
		t.Fatalf("mirror fixture: %v %v", shards, err)
	}
	source, peer := shards[0], shards[1]
	if err := s.AttachDiskOnNode(ctx, 3, 3, filepath.Join(dir, "disk-3"), 0); err != nil {
		t.Fatal(err)
	}
	var copied *storage.Plog
	s.maintenanceFault = func(at string) error {
		if at != "relocation-before-remount" {
			return nil
		}
		copied = s.plogs[source.PlogID]
		// Both later physical tails are unpublished. Reconciliation must actually
		// trim the first shard before the second shard encounters its I/O failure.
		for _, p := range []*storage.Plog{copied, s.plogs[peer.PlogID]} {
			if _, err := p.Write(0, []byte("unpublished tail")); err != nil {
				t.Fatal(err)
			}
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		// A closed descriptor causes real storage I/O errors without destroying the
		// durable peer prefix. Reopening during quiescent recovery restores access.
		if err := s.plogs[peer.PlogID].Close(); err != nil {
			t.Fatal(err)
		}
		return nil
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, source.PlogID, placement.VlogID, source.DiskID, 3)
	s.vlogMu.Unlock()
	s.maintenanceFault = nil
	if err == nil {
		t.Fatal("partial reconciliation did not fail")
	}
	if copied == nil || copied.LogicalLength() != info.Length {
		t.Fatal("first shard was not reconciled before the later failure")
	}
	if s.plogs[peer.PlogID].LogicalLength() <= info.Length {
		t.Fatal("later shard was unexpectedly reconciled")
	}
	if got := diskOf(t, s, placement.VlogID, source.ShardIndex); got != source.DiskID {
		t.Fatalf("rollback did not restore source: %d", got)
	}
	if s.vlogs[placement.VlogID] != nil || s.quarantinedVlogs[placement.VlogID] == nil {
		t.Fatal("failed rollback remount retained an unchecked mounted vlog")
	}
	for _, path := range []string{s.plogPath(source.DiskID, source.PlogID), s.plogPath(3, source.PlogID)} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("candidate removed before recovery: %v", err)
		}
	}
	s.CloseStorage()
	if err := s.Recover(ctx); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	s.StopMaintenanceDriver()
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("recovery changed acknowledged bytes")
	}
	if removed, err := s.SweepStrayPlogFiles(ctx); err != nil || removed < 1 {
		t.Fatalf("stray recovery cleanup=%d %v", removed, err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("cleanup changed acknowledged bytes")
	}
}
