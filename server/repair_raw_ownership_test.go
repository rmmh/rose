package server

import (
	"context"
	"errors"
	pb "github.com/rmmh/rose/proto"
	"testing"
)

func TestRawPlogRejectsRepairWhoseCleanupFailed(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	auditWrite(t, s, "file", []byte("keep published source"))
	placement := auditPlacement(t, s, "file")
	shards, err := s.db.VlogShardDisks(ctx, placement.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	s.maintenanceFault = func(at string) error {
		if at != "repair-before-repoint" {
			return nil
		}
		if _, err := s.db.GetDB().Exec("CREATE TRIGGER fail_repair_cleanup BEFORE DELETE ON plog WHEN OLD.repair_owned=1 BEGIN SELECT RAISE(ABORT,'cleanup failed'); END"); err != nil {
			return err
		}
		return errors.New("reject completion")
	}
	s.vlogMu.Lock()
	err = s.regenerateShardLocked(ctx, placement.VlogID, shards[0].ShardIndex, shards[0].PlogID, shards[0].DiskID)
	s.vlogMu.Unlock()
	s.maintenanceFault = nil
	if err == nil {
		t.Fatal("fixture did not fail repair")
	}
	var id uint32
	if err := s.db.GetDB().QueryRow("SELECT id FROM plog WHERE repair_owned=1").Scan(&id); err != nil {
		t.Fatal(err)
	}
	stranded := s.plogs[id]
	if stranded == nil {
		t.Fatal("fixture did not retain mounted destination")
	}
	before := stranded.LogicalLength()
	if _, err := s.WritePlog(ctx, &pb.WritePlogRequest{PlogId: id, TxnId: 99, Buffer: []byte("must not acknowledge raw data")}); err == nil {
		t.Error("raw write accepted repair-owned destination")
	}
	if stranded.LogicalLength() != before {
		t.Error("rejected raw write changed repair bytes")
	}
	// Closing this protected client makes an accidental Commit observable. Valid
	// raw plogs must still be committed by the same batch RPC.
	if err := stranded.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := s.MakePlog(ctx, &pb.MakePlogRequest{DiskId: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WritePlog(ctx, &pb.WritePlogRequest{PlogId: raw.PlogId, TxnId: 100, Buffer: []byte("raw")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitPlog(ctx, &pb.CommitPlogRequest{TxnId: 100}); err != nil {
		t.Fatalf("raw commit touched repair destination: %v", err)
	}
	if err := s.plogs[raw.PlogId].Verify(); err != nil {
		t.Fatal(err)
	}
}
