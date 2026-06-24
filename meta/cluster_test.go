package meta

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

func TestClusterIdentitySingleton(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	clusterUID, version, flags, err := db.ClusterInfo(ctx)
	if err != nil {
		t.Fatalf("cluster info: %v", err)
	}
	if clusterUID.IsZero() {
		t.Fatal("cluster uid is zero")
	}
	if version != storage.PlogFormatVersion {
		t.Fatalf("format version = %d, want %d", version, storage.PlogFormatVersion)
	}
	if flags != 0 {
		t.Fatalf("feature flags = %d, want 0", flags)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening must adopt the existing identity, never regenerate it.
	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	again, _, _, err := db2.ClusterInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again != clusterUID {
		t.Fatalf("cluster uid changed across reopen: %s -> %s", clusterUID, again)
	}
}

func TestMakeVlogPlogPersistUID(t *testing.T) {
	ctx := context.Background()
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.RegisterDisk(ctx, 1, 1, uid.New()); err != nil {
		t.Fatal(err)
	}

	vlogUID := uid.New()
	vlogID, err := db.MakeVlog(ctx, vlogUID, "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	vlogs, err := db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, v := range vlogs {
		if v.ID == vlogID {
			found = true
			if v.UID != vlogUID {
				t.Fatalf("vlog uid = %s, want %s", v.UID, vlogUID)
			}
		}
	}
	if !found {
		t.Fatal("vlog not listed")
	}

	plogUID := uid.New()
	plogID, err := db.MakePlog(ctx, plogUID, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.PlogUID(ctx, plogID)
	if err != nil {
		t.Fatal(err)
	}
	if got != plogUID {
		t.Fatalf("plog uid = %s, want %s", got, plogUID)
	}

	gotDisk, err := db.DiskUID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if gotDisk.IsZero() {
		t.Fatal("disk uid is zero")
	}
}
