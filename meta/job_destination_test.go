package meta

import (
	"context"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestJobDestinationOwnershipIsImmutableAndExclusive(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	var logs []uint32
	for i := 0; i < 4; i++ {
		id, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, id)
	}
	first, err := db.GetOrCreateCompactionJob(ctx, logs[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.GetOrCreateCompactionJob(ctx, logs[1])
	if err != nil {
		t.Fatal(err)
	}
	for retry := 0; retry < 2; retry++ {
		if err := db.SetJobDest(ctx, first.ID, logs[2]); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetJobDest(ctx, first.ID, logs[3]); err == nil {
		t.Fatal("replaced established destination")
	}
	if err := db.SetJobDest(ctx, second.ID, logs[2]); err == nil {
		t.Fatal("shared destination across jobs")
	}
	if err := db.SetJobDest(ctx, second.ID, logs[1]); err == nil {
		t.Fatal("claimed source as destination")
	}
	if err := db.SetJobDest(ctx, second.ID, 0); err == nil {
		t.Fatal("claimed zero destination")
	}
	if err := db.SetJobDest(ctx, second.ID, 1<<30); err == nil {
		t.Fatal("claimed missing destination")
	}
	if err := db.MarkJobDone(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.SetJobDest(ctx, second.ID, logs[2]); err == nil {
		t.Fatal("stole completed job's output")
	}
	if err := db.SetJobDest(ctx, first.ID, logs[3]); err == nil {
		t.Fatal("terminal job acquired destination")
	}
	// Read independent rows to ensure rejected claims also rolled back the
	// maintenance-owned marker and preserved both job destinations.
	for i, id := range logs {
		var owned bool
		if err := db.db.QueryRow(`SELECT maintenance_owned FROM vlog WHERE id=?`, id).Scan(&owned); err != nil {
			t.Fatal(err)
		}
		if owned != (i == 2) {
			t.Fatalf("rejected claim changed vlog %d ownership to %v", id, owned)
		}
	}
	for _, test := range []struct {
		id   int64
		want uint32
	}{{first.ID, logs[2]}, {second.ID, 0}} {
		var got uint32
		if err := db.db.QueryRow(`SELECT dest_vlog FROM job WHERE id=?`, test.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("job %d destination=%d want=%d", test.id, got, test.want)
		}
	}
	if err := db.SetJobDest(ctx, second.ID, logs[3]); err != nil {
		t.Fatalf("valid independent destination rejected: %v", err)
	}
}
