package server

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRepairRejectsPlacementChangeBeforeRepoint(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	want := bytes.Repeat([]byte("preserve repaired content"), 100)
	auditWrite(t, s, "file", want)
	placement := auditPlacement(t, s, "file")
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	victim := shards[0]
	before, err := s.db.ListPlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.maintenanceFault = func(stage string) error {
		if stage != "repair-before-repoint" {
			return nil
		}
		// Simulate a placement event completing while reconstruction was in
		// flight. Returning to the same state must not validate the old work.
		if _, err := s.db.GetDB().Exec("UPDATE disk SET state='failed' WHERE id=?", victim.DiskID); err != nil {
			return err
		}
		_, err := s.db.GetDB().Exec("UPDATE disk SET state='active' WHERE id=?", victim.DiskID)
		return err
	}
	s.vlogMu.Lock()
	err = s.regenerateShardLocked(ctx, placement.VlogID, victim.ShardIndex, victim.PlogID, victim.DiskID)
	s.vlogMu.Unlock()
	s.maintenanceFault = nil
	if err == nil || !strings.Contains(err.Error(), "stale generation") {
		t.Fatalf("stale repair completion=%v", err)
	}
	after, err := s.db.ListPlogs(ctx)
	if err != nil || len(after) != len(before) {
		t.Fatalf("rejected repair leaked plogs: %v %v", after, err)
	}
	current, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil || len(current) != len(shards) || current[0].PlogID != victim.PlogID {
		t.Fatalf("stale completion changed source mapping: %v %v", current, err)
	}
	s.vlogMu.Lock()
	err = s.regenerateShardLocked(ctx, placement.VlogID, victim.ShardIndex, victim.PlogID, victim.DiskID)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatalf("fresh repair attempt failed: %v", err)
	}
	if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
		t.Fatal("repair changed published bytes")
	}
}
