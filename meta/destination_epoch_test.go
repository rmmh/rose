package meta

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestRepairDestinationFencesLifecycle(t *testing.T) {
	for _, change := range []string{"disk return", "node return", "disk identity", "disk move", "ownership", "disk reinsert", "node reinsert", "failed", "draining", "detached", "node failed"} {
		t.Run(change, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.db")
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			ctx := context.Background()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := db.db.Exec(query, args...); err != nil {
					t.Fatal(err)
				}
			}
			exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'),(2,'two','two')")
			exec("INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,2,1,0),(3,2,1,0)")
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
			source := replacementEpoch(t, db, v)
			captured := destinationEpoch(t, db, dest)
			switch change {
			case "disk return":
				exec("UPDATE disk SET state='failed' WHERE id=2")
				exec("UPDATE disk SET state='active' WHERE id=2")
			case "node return":
				exec("UPDATE node SET state='failed' WHERE id=2")
				exec("UPDATE node SET state='working' WHERE id=2")
			case "disk identity":
				exec("UPDATE disk SET uid=X'01' WHERE id=2")
				exec("UPDATE disk SET uid=X'' WHERE id=2")
			case "disk move":
				exec("UPDATE plog SET disk_id=3 WHERE id=?", dest)
				exec("UPDATE plog SET disk_id=2 WHERE id=?", dest)
			case "ownership":
				other, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.AssignPlogToVlog(ctx, other, 0, dest); err != nil {
					t.Fatal(err)
				}
				exec("DELETE FROM vlog_plog WHERE vlog_id=?", other)
			case "disk reinsert":
				exec("DELETE FROM disk WHERE id=2")
				exec("INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(2,2,1,0)")
			case "node reinsert":
				exec("DELETE FROM node WHERE id=2")
				exec("INSERT INTO node(id,mac,hostname) VALUES(2,'two','two')")
				exec("INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(2,2,1,0)")
			case "failed", "draining", "detached":
				exec("UPDATE disk SET state=? WHERE id=2", change)
			case "node failed":
				exec("UPDATE node SET state='failed' WHERE id=2")
			}
			if got := replacementEpoch(t, db, v); got != source {
				t.Fatal("destination-only change altered source epoch; test does not isolate destination fence")
			}
			var current int64
			if err := db.db.QueryRow("SELECT placement_epoch FROM plog WHERE id=?", dest).Scan(&current); err != nil {
				t.Fatal(err)
			}
			if current <= captured {
				t.Fatal("destination lifecycle did not advance epoch")
			}
			if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, source, captured); err == nil {
				t.Fatal("stale destination completion accepted")
			}
			unavailable := change == "failed" || change == "draining" || change == "detached" || change == "node failed"
			if unavailable {
				if _, err := db.RepairDestinationEpoch(ctx, dest); err == nil {
					t.Fatal("unavailable destination admitted")
				}
				if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, source, current); err == nil {
					t.Fatal("current but unavailable destination accepted")
				}
				exec("UPDATE disk SET state='active' WHERE id=2")
				exec("UPDATE node SET state='working' WHERE id=2")
				current = destinationEpoch(t, db, dest)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := destinationEpoch(t, db, dest); got != current {
				t.Fatal("destination epoch lost across reopen")
			}
			if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, source, current); err != nil {
				t.Fatalf("fresh destination rejected: %v", err)
			}
		})
	}
}

func TestDestinationEpochOverflowRollsBackLifecycle(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.db.Exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0); INSERT INTO plog(id,disk_id,placement_epoch) VALUES(1,1,9223372036854775807)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec("UPDATE disk SET state='failed' WHERE id=1"); err == nil {
		t.Fatal("destination epoch overflow accepted")
	}
	var state string
	if err := db.db.QueryRow("SELECT state FROM disk WHERE id=1").Scan(&state); err != nil || state != "active" {
		t.Fatalf("overflow did not roll back disk state: %s %v", state, err)
	}
}
