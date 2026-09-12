package server

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestRelocationRejectsSameDiskBeforeCopy(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	want := []byte("same disk relocation must not truncate the source")
	auditWrite(t, s, "file", want)
	ctx := context.Background()
	placement := auditPlacement(t, s, "file")
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	p := shards[0]
	path := s.plogPath(p.DiskID, p.PlogID)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	err = s.migratePlogLocked(ctx, p.PlogID, placement.VlogID, p.DiskID, p.DiskID)
	s.vlogMu.Unlock()
	if err == nil {
		t.Fatal("same-disk relocation accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("source changed on invalid relocation: %v", err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("published source unreadable")
	}
}
