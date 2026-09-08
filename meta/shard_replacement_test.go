package meta

import (
	"context"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestShardReplacementRejectsInvalidDestination(t *testing.T) {
	for _, kind := range []string{"same", "zero", "missing", "owned", "colocated"} {
		t.Run(kind, func(t *testing.T) {
			db, err := OpenEphemeral()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			if _, err := db.db.Exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,1,1,0)"); err != nil {
				t.Fatal(err)
			}
			v, err := db.MakeVlog(ctx, uid.New(), "DUPLICATE", 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			old, err := db.MakeAssignedPlog(ctx, uid.New(), 1, v, 0)
			if err != nil {
				t.Fatal(err)
			}
			peer, err := db.MakeAssignedPlog(ctx, uid.New(), 2, v, 1)
			if err != nil {
				t.Fatal(err)
			}
			var dest uint32
			switch kind {
			case "same":
				dest = old
			case "zero":
				dest = 0
			case "missing":
				dest = 999
			case "owned":
				dest = peer
			case "colocated":
				dest, err = db.MakePlog(ctx, uid.New(), 2)
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := db.ListPlogs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var epoch int64 = 1
			if kind != "zero" && kind != "missing" {
				if err := db.db.QueryRow("SELECT placement_epoch FROM plog WHERE id=?", dest).Scan(&epoch); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, replacementEpoch(t, db, v), epoch); err == nil {
				t.Fatalf("accepted %s replacement", kind)
			}
			mappings, err := db.ListVlogPlogs(ctx, v)
			if err != nil || len(mappings) != 2 || mappings[0].PlogID != old || mappings[1].PlogID != peer {
				t.Fatalf("rejection changed mappings: %v %v", mappings, err)
			}
			after, err := db.ListPlogs(ctx)
			if err != nil || len(after) != len(before) {
				t.Fatalf("rejection changed plog rows: %v %v", after, err)
			}
		})
	}
}

func TestShardReplacementPreservesOtherSourceOwners(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.db.Exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,1,1,0)"); err != nil {
		t.Fatal(err)
	}
	v, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	other, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	old, err := db.MakeAssignedPlog(ctx, uid.New(), 1, v, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AssignPlogToVlog(ctx, other, 0, old); err != nil {
		t.Fatal(err)
	}
	dest, err := db.MakePlog(ctx, uid.New(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, replacementEpoch(t, db, v), 1); err == nil {
		t.Fatal("accepted shared source replacement")
	}
	var exists bool
	if err := db.db.QueryRow("SELECT EXISTS(SELECT 1 FROM plog WHERE id=?)", old).Scan(&exists); err != nil || !exists {
		t.Fatalf("rejection removed shared source: %v", err)
	}
	for _, owner := range []uint32{v, other} {
		m, err := db.ListVlogPlogs(ctx, owner)
		if err != nil || len(m) != 1 || m[0].PlogID != old {
			t.Fatalf("shared source mapping changed: %v %v", m, err)
		}
	}
	if _, err := db.db.Exec("DELETE FROM vlog_plog WHERE vlog_id=?", other); err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceShardPlog(ctx, v, 0, old, dest, replacementEpoch(t, db, v), 1); err != nil {
		t.Fatalf("exclusive source replacement failed: %v", err)
	}
}

func replacementEpoch(t *testing.T, db *DB, id uint32) int64 {
	t.Helper()
	info, err := db.GetVlog(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return info.PlacementEpoch
}
