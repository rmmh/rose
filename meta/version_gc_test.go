package meta

import (
	"context"
	"testing"
)

func TestVersionCollectionPreservesDurableOwners(t *testing.T) {
	for _, owner := range []string{"head", "snapshot", "retry", "committed", "expired", "none"} {
		t.Run(owner, func(t *testing.T) {
			db, err := OpenEphemeral()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			var id int64
			if owner == "retry" || owner == "committed" || owner == "expired" {
				op, err := db.CreateWriteOp(ctx, "key", "file")
				if err != nil {
					t.Fatal(err)
				}
				if owner == "committed" {
					id, err = db.CommitWriteOpVersion(ctx, op.ID, "file", 1, nil)
				} else {
					id, err = db.CommitWriteOpVersionWithRetention(ctx, op.ID, "file", 1, nil, 100)
				}
				if err != nil {
					t.Fatal(err)
				}
			} else {
				id, err = db.CommitFile(ctx, "file", 1, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			if owner == "snapshot" {
				if _, err := db.CreateSnapshot(ctx, "snapshot", 1); err != nil {
					t.Fatal(err)
				}
			}
			if owner != "head" {
				if err := db.UnlinkFile(ctx, "file"); err != nil {
					t.Fatal(err)
				}
			}
			if owner == "expired" {
				if _, err := db.ExpireWriteResults(ctx, 100); err != nil {
					t.Fatal(err)
				}
			}
			wantRemoved := owner == "expired" || owner == "none"
			n, err := db.GCFileVersions(ctx, 10)
			if err != nil || (n == 1) != wantRemoved {
				t.Fatalf("collected=%d err=%v owner=%s", n, err, owner)
			}
			var exists bool
			if err := db.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file WHERE id=?)", id).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if exists == wantRemoved {
				t.Fatalf("version existence=%v owner=%s", exists, owner)
			}
			if issues, err := db.CheckCatalog(ctx); err != nil || len(issues) != 0 {
				t.Fatalf("issues=%v err=%v", issues, err)
			}
		})
	}
}

func TestVersionCollectionBoundAndRollback(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := db.CommitFile(ctx, "file", int64(i), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.db.Exec(`CREATE TRIGGER reject_version_gc BEFORE DELETE ON file WHEN OLD.mtime=1 BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GCFileVersions(ctx, 2); err == nil {
		t.Fatal("ignored deletion failure")
	}
	var count int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM file").Scan(&count); err != nil || count != 5 {
		t.Fatalf("partial collection count=%d err=%v", count, err)
	}
	if _, err := db.db.Exec("DROP TRIGGER reject_version_gc"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int{2, 2, 0} {
		if n, err := db.GCFileVersions(ctx, 2); err != nil || n != want {
			t.Fatalf("collected=%d want=%d err=%v", n, want, err)
		}
	}
}
