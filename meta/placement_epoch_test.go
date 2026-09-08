package meta

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestPlacementEpochFencesReturnedState(t *testing.T) {
	for _, change := range []string{"disk", "node", "mapping", "physical disk", "prefix", "lease"} {
		t.Run(change, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.db")
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			ctx := context.Background()
			for _, stmt := range []string{
				"INSERT INTO node(id,mac,hostname) VALUES(1,'one','one')",
				"INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,1,1,0)",
			} {
				if _, err := db.db.Exec(stmt); err != nil {
					t.Fatal(err)
				}
			}
			v, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			old, err := db.MakeAssignedPlog(ctx, uid.New(), 1, v, 0)
			if err != nil {
				t.Fatal(err)
			}
			dest, err := db.MakePlog(ctx, uid.New(), 2)
			if err != nil {
				t.Fatal(err)
			}
			captured := replacementEpoch(t, db, v)
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := db.db.Exec(query, args...); err != nil {
					t.Fatal(err)
				}
			}
			switch change {
			case "disk":
				exec("UPDATE disk SET state='failed' WHERE id=1")
				exec("UPDATE disk SET state='active' WHERE id=1")
			case "node":
				exec("UPDATE node SET state='failed' WHERE id=1")
				exec("UPDATE node SET state='working' WHERE id=1")
			case "mapping":
				exec("UPDATE vlog_plog SET plog_id=? WHERE vlog_id=?", dest, v)
				exec("UPDATE vlog_plog SET plog_id=? WHERE vlog_id=?", old, v)
			case "physical disk":
				exec("UPDATE plog SET disk_id=2 WHERE id=?", old)
				exec("UPDATE plog SET disk_id=1 WHERE id=?", old)
			case "prefix":
				exec("UPDATE vlog SET length=1 WHERE id=?", v)
				exec("UPDATE vlog SET length=0 WHERE id=?", v)
			case "lease":
				op, err := db.CreateWriteOp(ctx, "op", "file")
				if err != nil {
					t.Fatal(err)
				}
				if err := db.ClaimVlogLease(ctx, v, op.ID, 0); err != nil {
					t.Fatal(err)
				}
				exec("DELETE FROM vlog_lease WHERE vlog_id=?", v)
			}
			current := replacementEpoch(t, db, v)
			if current <= captured {
				t.Fatal("placement changes did not advance generation")
			}
			if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, captured); err == nil {
				t.Fatal("stale completion accepted after state returned")
			}
			if got := replacementEpoch(t, db, v); got != current {
				t.Fatal("rejected completion changed generation")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := replacementEpoch(t, db, v); got != current {
				t.Fatalf("restart generation=%d want=%d", got, current)
			}
			if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, current); err != nil {
				t.Fatalf("current generation rejected: %v", err)
			}
			if got := replacementEpoch(t, db, v); got <= current {
				t.Fatal("replacement did not advance generation")
			}
		})
	}
}

func TestPlacementEpochRollsBackAndCannotOverflow(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	v, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	epoch := replacementEpoch(t, db, v)
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE vlog SET length=10 WHERE id=?", v); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := replacementEpoch(t, db, v); got != epoch {
		t.Fatal("rolled-back mutation changed epoch")
	}
	if _, err := db.db.Exec("UPDATE vlog SET placement_epoch=9223372036854775807 WHERE id=?", v); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec("UPDATE vlog SET length=10 WHERE id=?", v); err == nil {
		t.Fatal("generation overflow accepted")
	}
	info, err := db.GetVlog(ctx, v)
	if err != nil || info.Length != 0 || info.PlacementEpoch != 9223372036854775807 {
		t.Fatalf("overflow did not roll back: %v %v", info, err)
	}
}
