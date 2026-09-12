package meta

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/uid"
)

func TestRelocationCommitFailureReconcilesDurableState(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		if _, err := db.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0),(2,1,1,0)")
	v, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	p, err := db.MakeAssignedPlog(ctx, uid.New(), 1, v, 0)
	if err != nil {
		t.Fatal(err)
	}
	before, err := db.CapturePlogRelocation(ctx, p, v, 1)
	if err != nil {
		t.Fatal(err)
	}
	dest, err := db.CaptureDiskPlacement(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	// UPDATE succeeds, but COMMIT fails its deferred constraint. SQLite retains
	// the transaction until rollback or connection close; a same-session read
	// must not be mistaken for durable publication.
	exec(`CREATE TABLE relocation_commit_fault (id INTEGER REFERENCES node(id) DEFERRABLE INITIALLY DEFERRED);
 CREATE TRIGGER fail_relocation_commit AFTER UPDATE OF disk_id ON plog BEGIN INSERT INTO relocation_commit_fault VALUES(999); END;`)
	after, err := db.MovePlogToDisk(ctx, p, v, 1, 2, before, dest.Epoch)
	var uncertain *RelocationCommitError
	if !errors.As(err, &uncertain) {
		t.Fatalf("expected commit error, got %v", err)
	}
	if after.Plog == 0 || after.Vlog == 0 {
		t.Fatal("attempted epochs were lost")
	}
	resolved, err := db.ResolvePlogRelocation(ctx, p, v, 1, 2, before, after)
	if err != nil || resolved != 1 {
		t.Fatalf("failed commit resolved disk=%d err=%v; want durable source", resolved, err)
	}
	if _, err := db.db.Exec("INSERT INTO relocation_commit_fault VALUES(999)"); err == nil {
		t.Fatal("replacement connection lost foreign-key enforcement")
	}
	var synchronous int
	if err := db.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
		t.Fatalf("replacement connection synchronous=%d err=%v", synchronous, err)
	}
	exec("DROP TRIGGER fail_relocation_commit")
	after, err = db.MovePlogToDisk(ctx, p, v, 1, 2, before, dest.Epoch)
	if err != nil {
		t.Fatalf("retry after failed commit: %v", err)
	}
	resolved, err = db.ResolvePlogRelocation(ctx, p, v, 1, 2, before, after)
	if err != nil || resolved != 2 {
		t.Fatalf("successful commit resolved disk=%d err=%v", resolved, err)
	}
	exec("UPDATE disk SET state='failed' WHERE id=2; UPDATE disk SET state='active' WHERE id=2")
	if disk, err := db.ResolvePlogRelocation(ctx, p, v, 1, 2, before, after); err == nil || disk != 0 {
		t.Fatalf("returned placement accepted: disk=%d err=%v", disk, err)
	}
}
