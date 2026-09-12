package meta

import (
	"context"

	"github.com/rmmh/rose/uid"
	"path/filepath"
	"testing"
)

func TestDiskEpochFencesReturnedDestination(t *testing.T) {
	for _, change := range []string{"disk", "node", "identity", "reinsert", "node reinsert"} {
		t.Run(change, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "meta.db")
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			ctx := context.Background()
			exec := func(q string) {
				t.Helper()
				if _, err := db.db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'),(2,'two','two'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,2,1,0)")
			v, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			p, err := db.MakeAssignedPlog(ctx, uid.New(), 1, v, 0)
			if err != nil {
				t.Fatal(err)
			}
			source, err := db.CapturePlogRelocation(ctx, p, v, 1)
			if err != nil {
				t.Fatal(err)
			}
			captured := diskEpoch(t, db, 2)
			switch change {
			case "disk":
				exec("UPDATE disk SET state='failed' WHERE id=2")
				exec("UPDATE disk SET state='active' WHERE id=2")
			case "node":
				exec("UPDATE node SET state='failed' WHERE id=2")
				exec("UPDATE node SET state='working' WHERE id=2")
			case "identity":
				exec("UPDATE disk SET uid=X'01' WHERE id=2")
				exec("UPDATE disk SET uid=X'' WHERE id=2")
			case "reinsert":
				exec("DELETE FROM disk WHERE id=2")
				exec("INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(2,2,1,0)")
			case "node reinsert":
				exec("DELETE FROM node WHERE id=2")
				exec("INSERT INTO node(id,mac,hostname) VALUES(2,'two','two'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(2,2,1,0)")
			}
			current := diskEpoch(t, db, 2)
			if current <= captured {
				t.Fatal("destination generation did not advance")
			}
			unchanged, err := db.CapturePlogRelocation(ctx, p, v, 1)
			if err != nil || unchanged != source {
				t.Fatal("test changed source generations")
			}
			if _, err := db.MovePlogToDisk(ctx, p, v, 1, 2, source, captured); err == nil {
				t.Fatal("stale disk completion accepted")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := diskEpoch(t, db, 2); got != current {
				t.Fatal("disk epoch lost on reopen")
			}
			originalDisk := diskEpoch(t, db, 1)
			moved, err := db.MovePlogToDisk(ctx, p, v, 1, 2, source, current)
			if err != nil {
				t.Fatalf("fresh disk completion rejected: %v", err)
			}
			exec("UPDATE disk SET state='failed' WHERE id=1; UPDATE disk SET state='active' WHERE id=1")
			if _, err := db.MovePlogToDisk(ctx, p, v, 2, 1, moved, originalDisk); err == nil {
				t.Fatal("rollback accepted stale original disk")
			}
			if _, err := db.MovePlogToDisk(ctx, p, v, 2, 1, moved, diskEpoch(t, db, 1)); err != nil {
				t.Fatalf("fresh rollback disk rejected: %v", err)
			}
		})
	}
}
func TestDiskEpochOverflowRollsBack(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.db.Exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0); UPDATE placement_clock SET epoch=9223372036854775807"); err != nil {
		t.Fatal(err)
	}
	before := diskEpoch(t, db, 1)
	if _, err := db.db.Exec("UPDATE disk SET state='failed' WHERE id=1"); err == nil {
		t.Fatal("disk clock overflow accepted")
	}
	var state string
	if err := db.db.QueryRow("SELECT state FROM disk WHERE id=1").Scan(&state); err != nil || state != "active" || diskEpoch(t, db, 1) != before {
		t.Fatal("overflow did not roll back disk mutation")
	}
}
