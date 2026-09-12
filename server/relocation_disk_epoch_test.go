package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRelocationRejectsReturnedDestination(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	want := []byte("relocation must reject a returned destination")
	auditWrite(t, s, "file", want)
	placement := auditPlacement(t, s, "file")
	if err := s.AttachDiskOnNode(ctx, 3, 3, filepath.Join(t.TempDir(), "disk3"), 0); err != nil {
		t.Fatal(err)
	}
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	p := shards[0]
	s.maintenanceFault = func(at string) error {
		if at != "relocation-before-repoint" {
			return nil
		}
		_, err := s.db.GetDB().Exec("UPDATE disk SET state='failed' WHERE id=3; UPDATE disk SET state='active' WHERE id=3")
		return err
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, p.PlogID, placement.VlogID, p.DiskID, 3)
	s.vlogMu.Unlock()
	s.maintenanceFault = nil
	if err == nil {
		t.Fatal("stale disk relocation accepted")
	}
	if _, err := os.Stat(s.plogPath(3, p.PlogID)); !os.IsNotExist(err) {
		t.Fatalf("rejected copy retained: %v", err)
	}
	if diskOf(t, s, placement.VlogID, p.ShardIndex) != p.DiskID {
		t.Fatal("rejected relocation changed source")
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, p.PlogID, placement.VlogID, p.DiskID, 3)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatalf("fresh relocation failed: %v", err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("relocation changed published bytes")
	}
}
