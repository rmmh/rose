package meta

import (
	"context"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestRetirementCancelsSupersededRepairAtomically(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	source, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.GetOrCreateScrubRepairJob(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`CREATE TRIGGER fail_repair_cancel BEFORE UPDATE OF state ON job BEGIN SELECT RAISE(ABORT,'injected repair cancellation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetireVlog(ctx, source); err == nil {
		t.Fatal("retirement ignored repair cancellation failure")
	}
	if _, err := db.GetVlog(ctx, source); err != nil {
		t.Fatalf("failed cancellation retired source: %v", err)
	}
	current, err := db.GetJob(ctx, job.ID)
	if err != nil || current.State != JobRunning {
		t.Fatalf("failed transaction job=%v err=%v", current, err)
	}
	if _, err := db.db.Exec("DROP TRIGGER fail_repair_cancel"); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		if _, err := db.RetireVlog(ctx, source); err != nil {
			t.Fatal(err)
		}
		current, err := db.GetJob(ctx, job.ID)
		if err != nil || current.State != JobCancelled {
			t.Fatalf("retired source repair=%v err=%v", current, err)
		}
	}
}
