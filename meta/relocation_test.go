package meta

import (
	"context"
	"github.com/rmmh/rose/uid"
	"testing"
)

func TestRelocationFencesSourceAndPlacement(t *testing.T) {
	for _, kind := range []string{"missing source", "same disk", "wrong source", "missing disk", "failed disk", "failed node", "colocated", "returned source", "valid rollback"} {
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
			epoch, err := db.PlogPlacementEpoch(ctx, id, 1)
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
			case "valid rollback":
				exec("UPDATE disk SET state='draining' WHERE id=1")
				epoch, err = db.PlogPlacementEpoch(ctx, id, 1)
				if err != nil {
					t.Fatal(err)
				}
			}
			next, err := db.MovePlogToDisk(ctx, target, source, dest, epoch)
			if kind != "valid rollback" {
				if err == nil {
					t.Fatal("invalid relocation accepted")
				}
				if _, err := db.PlogPlacementEpoch(ctx, id, 1); err != nil {
					t.Fatal("rejection moved source")
				}
				return
			}
			if err != nil || next <= epoch {
				t.Fatalf("valid relocation=%d %v", next, err)
			}
			if _, err := db.MovePlogToDisk(ctx, id, 3, 1, epoch); err == nil {
				t.Fatal("stale rollback accepted")
			}
			if _, err := db.MovePlogToDisk(ctx, id, 3, 1, next); err != nil {
				t.Fatalf("rollback to draining source: %v", err)
			}
		})
	}
}
