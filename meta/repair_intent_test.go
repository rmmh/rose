package meta

import (
	"context"

	"github.com/rmmh/rose/uid"
	"testing"
)

func TestRepairIntentRetirement(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	raw, err := db.MakePlog(ctx, uid.New(), 1)
	if err != nil {
		t.Fatal(err)
	}
	abandoned, err := db.MakeRepairPlog(ctx, uid.New(), 1)
	if err != nil {
		t.Fatal(err)
	}
	published, err := db.MakeRepairPlog(ctx, uid.New(), 1)
	if err != nil {
		t.Fatal(err)
	}
	v, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AssignPlogToVlog(ctx, v, 0, published); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec("CREATE TRIGGER reject_repair_retirement BEFORE DELETE ON plog BEGIN SELECT RAISE(ABORT,'injected failure'); END"); err != nil {
		t.Fatal(err)
	}
	retired, err := db.RetireUnassignedRepairPlogs(ctx)
	if err == nil || len(retired) != 0 {
		t.Fatalf("failed transaction returned cleanup candidates: %v %v", retired, err)
	}
	if _, err := db.db.Exec("DROP TRIGGER reject_repair_retirement"); err != nil {
		t.Fatal(err)
	}
	retired, err = db.RetireUnassignedRepairPlogs(ctx)
	if err != nil || len(retired) != 1 || retired[0].ID != abandoned {
		t.Fatalf("retired=%v err=%v", retired, err)
	}
	for _, id := range []uint32{raw, published} {
		var n int
		if err := db.db.QueryRow("SELECT count(*) FROM plog WHERE id=?", id).Scan(&n); err != nil || n != 1 {
			t.Fatalf("protected plog %d missing: %v", id, err)
		}
	}
	retired, err = db.RetireUnassignedRepairPlogs(ctx)
	if err != nil || len(retired) != 0 {
		t.Fatalf("repeat retirement=%v %v", retired, err)
	}
}
