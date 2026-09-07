package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSnapshotDirectoriesSurviveCatalogReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	ctx := context.Background()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Mkdir(ctx, "empty/nested", 77); err != nil {
		t.Fatal(err)
	}
	id, err := db.CreateSnapshot(ctx, "persist", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entries, err := db.ListSnapshotDir(ctx, id, "empty")
	if err != nil || len(entries) != 1 || entries[0].Name != "nested" || !entries[0].IsDir || entries[0].Mtime != 77 {
		t.Fatalf("persisted entries=%v err=%v", entries, err)
	}
	if _, err := db.ListSnapshotDir(ctx, id+1, ""); err == nil {
		t.Fatal("unknown snapshot returned a root")
	}
}
