package meta

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestMaintenanceOrphanRetirementPreservesOwnedOrNonemptyLogs(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	logs := map[string]uint32{}
	for _, name := range []string{"orphan", "ordinary", "job", "length", "chunk", "lease"} {
		id, err := db.MakeVlogInDomain(ctx, uid.New(), "NONE", 1, 0, 0, 0, nil, 1, name != "ordinary")
		if err != nil {
			t.Fatal(err)
		}
		logs[name] = id
	}
	job, err := db.GetOrCreateCompactionJob(ctx, logs["ordinary"])
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetJobDest(ctx, job.ID, logs["job"]); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkJobDone(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.SetVlogLength(ctx, logs["length"], 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`INSERT INTO chunk(hash,refcount,vlog_id,vaddr_offset,logical_len,compressed_len) VALUES (?,0,?,0,1,1)`, bytes.Repeat([]byte{1}, 15), logs["chunk"]); err != nil {
		t.Fatal(err)
	}
	op, err := db.CreateWriteOp(ctx, "lease", "file")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ClaimVlogLease(ctx, logs["lease"], op.ID, 0); err != nil {
		t.Fatal(err)
	}
	for repeat := 0; repeat < 2; repeat++ {
		if _, err := db.RetireUnassignedMaintenanceVlogs(ctx); err != nil {
			t.Fatal(err)
		}
		for name, id := range logs {
			_, err := db.GetVlog(ctx, id)
			if name == "orphan" {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("orphan survived: %v", err)
				}
			} else if err != nil {
				t.Fatalf("retired protected %s vlog: %v", name, err)
			}
		}
	}
}
