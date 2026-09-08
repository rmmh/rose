package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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

func TestRepairRejectsDestinationChangeBeforeRepoint(t *testing.T) {
	for _, change := range []string{"disk", "node"} {
		t.Run(change, func(t *testing.T) {
			s := newControlPlaneServer(t, 2)
			s.StopMaintenanceDriver()
			defer s.CloseStorage()
			ctx := context.Background()
			want := bytes.Repeat([]byte("preserve repaired content"), 100)
			auditWrite(t, s, "file", want)
			placement := auditPlacement(t, s, "file")
			s.vlogMu.Lock()
			err := s.attachDiskOnNodeLocked(ctx, 3, 3, filepath.Join(t.TempDir(), "disk-3"), 0)
			s.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			beforeFiles, err := os.ReadDir(s.diskRoots[3])
			if err != nil {
				t.Fatal(err)
			}
			initial, err := s.db.GetVlog(ctx, placement.VlogID)
			if err != nil {
				t.Fatal(err)
			}
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
				if change == "node" {
					if _, err := s.db.GetDB().Exec("UPDATE node SET state='failed' WHERE id=3"); err != nil {
						return err
					}
					_, err := s.db.GetDB().Exec("UPDATE node SET state='working' WHERE id=3")
					return err
				}
				// Simulate a placement event completing while reconstruction was in
				// flight. Returning to the same state must not validate the old work.
				if _, err := s.db.GetDB().Exec("UPDATE disk SET state='failed' WHERE id=?", uint32(3)); err != nil {
					return err
				}
				_, err := s.db.GetDB().Exec("UPDATE disk SET state='active' WHERE id=?", uint32(3))
				return err
			}
			s.vlogMu.Lock()
			err = s.regenerateShardLocked(ctx, placement.VlogID, victim.ShardIndex, victim.PlogID, 3)
			s.vlogMu.Unlock()
			s.maintenanceFault = nil
			if err == nil || !strings.Contains(err.Error(), "stale generation") {
				t.Fatalf("stale repair completion=%v", err)
			}
			afterFiles, err := os.ReadDir(s.diskRoots[3])
			if err != nil || len(afterFiles) != len(beforeFiles) {
				t.Fatalf("rejected repair leaked destination files: %v %v", afterFiles, err)
			}
			unchanged, err := s.db.GetVlog(ctx, placement.VlogID)
			if err != nil || unchanged.PlacementEpoch != initial.PlacementEpoch {
				t.Fatalf("destination event changed source epoch: %v", err)
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
			err = s.regenerateShardLocked(ctx, placement.VlogID, victim.ShardIndex, victim.PlogID, 3)
			s.vlogMu.Unlock()
			if err != nil {
				t.Fatalf("fresh repair attempt failed: %v", err)
			}
			if got := readServerFileInternal(t, s, "file"); !bytes.Equal(got, want) {
				t.Fatal("repair changed published bytes")
			}
		})
	}
}
