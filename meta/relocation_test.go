package meta

import (
	"context"
	"github.com/rmmh/rose/uid"
	"testing"
)

func TestRelocationFencesSourceAndPlacement(t *testing.T) {
	for _, kind := range []string{"missing source", "same disk", "wrong source", "missing disk", "failed disk", "failed node", "colocated", "returned source", "valid rollback", "vlog prefix", "vlog lease", "shared source"} {
		t.Run(kind, func(t *testing.T) {
			db, err := OpenEphemeral()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			exec := func(q string, args ...any) {
				t.Helper()
				if _, err := db.db.Exec(q, args...); err != nil {
					t.Fatal(err)
				}
			}
			exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'),(2,'two','two'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,1,1,0),(3,2,1,0)")
			v, err := db.MakeVlog(ctx, uid.New(), "DUPLICATE", 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			id, err := db.MakeAssignedPlog(ctx, uid.New(), 1, v, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.MakeAssignedPlog(ctx, uid.New(), 2, v, 1); err != nil {
				t.Fatal(err)
			}
			epoch, err := db.CapturePlogRelocation(ctx, id, v, 1)
			if err != nil {
				t.Fatal(err)
			}
			source, dest := uint32(1), uint32(3)
			target := id
			switch kind {
			case "missing source":
				target = 999
			case "same disk":
				dest = 1
			case "wrong source":
				source = 2
			case "missing disk":
				dest = 999
			case "failed disk":
				exec("UPDATE disk SET state='failed' WHERE id=3")
			case "failed node":
				exec("UPDATE node SET state='failed' WHERE id=2")
			case "colocated":
				dest = 2
			case "returned source":
				exec("UPDATE disk SET state='failed' WHERE id=1")
				exec("UPDATE disk SET state='active' WHERE id=1")
			case "vlog prefix":
				exec("UPDATE vlog SET length=1 WHERE id=?", v)
				exec("UPDATE vlog SET length=0 WHERE id=?", v)
			case "vlog lease":
				op, err := db.CreateWriteOp(ctx, "relocation-op", "file")
				if err != nil {
					t.Fatal(err)
				}
				if err := db.ClaimVlogLease(ctx, v, op.ID, 0); err != nil {
					t.Fatal(err)
				}
				exec("DELETE FROM vlog_lease WHERE vlog_id=?", v)
			case "shared source":
				other, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.AssignPlogToVlog(ctx, other, 0, id); err != nil {
					t.Fatal(err)
				}
				if _, err := db.CapturePlogRelocation(ctx, id, v, 1); err == nil {
					t.Error("captured shared relocation source")
				}
				// Isolate the commit-time ownership check from stale epochs.
				epoch.Plog, err = db.PlogPlacementEpoch(ctx, id, 1)
				if err != nil {
					t.Fatal(err)
				}
				epoch.Vlog = replacementEpoch(t, db, v)
			case "valid rollback":
				exec("UPDATE disk SET state='draining' WHERE id=1")
				epoch, err = db.CapturePlogRelocation(ctx, id, v, 1)
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "vlog prefix" || kind == "vlog lease" {
				current, err := db.PlogPlacementEpoch(ctx, id, 1)
				if err != nil || current != epoch.Plog {
					t.Fatal("fixture failed to isolate vlog generation")
				}
			}
			next, err := db.MovePlogToDisk(ctx, target, v, source, dest, epoch, diskEpoch(t, db, dest))
			if kind != "valid rollback" {
				if err == nil {
					t.Fatal("invalid relocation accepted")
				}
				if _, err := db.PlogPlacementEpoch(ctx, id, 1); err != nil {
					t.Fatal("rejection moved source")
				}
				return
			}
			if err != nil || next.Plog <= epoch.Plog || next.Vlog <= epoch.Vlog {
				t.Fatalf("valid relocation=%v %v", next, err)
			}
			if _, err := db.MovePlogToDisk(ctx, id, v, 3, 1, epoch, diskEpoch(t, db, 1)); err == nil {
				t.Fatal("stale rollback accepted")
			}
			if _, err := db.MovePlogToDisk(ctx, id, v, 3, 1, RelocationEpochs{Plog: next.Plog, Vlog: epoch.Vlog}, diskEpoch(t, db, 1)); err == nil {
				t.Fatal("stale vlog rollback accepted")
			}
			if _, err := db.MovePlogToDisk(ctx, id, v, 3, 1, next, diskEpoch(t, db, 1)); err != nil {
				t.Fatalf("rollback to draining source: %v", err)
			}
		})
	}
}

func diskEpoch(t *testing.T, db *DB, id uint32) int64 {
	t.Helper()
	if id == 999 {
		return 0
	}
	token, err := db.CaptureDiskPlacement(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return token.Epoch
}
