package meta

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func checkerFixture(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog ?.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, stmt := range []string{
		"INSERT INTO vlog(id,length,protection_scheme,data_shards,parity_shards,required_shards) VALUES(1,256,'NONE',1,0,1)",
		"INSERT INTO plog(id,disk_id) VALUES(1,1)",
		"INSERT INTO vlog_plog(vlog_id,shard_idx,plog_id) VALUES(1,0,1)",
	} {
		if _, err := db.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	chunk := ChunkPlacement{Hash: bytes.Repeat([]byte{1}, 15), VlogID: 1, VaddrOffset: 0, LogicalLen: 3, CompressedLen: 3}
	if _, err := db.CommitFile(context.Background(), "dir/file", 123, []ChunkPlacement{chunk, chunk}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateSnapshot(context.Background(), "snapshot", 456); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestCatalogCheckerFindsInjectedCorruption(t *testing.T) {
	for _, tc := range []struct{ name, sql, code string }{
		{"namespace index", "UPDATE file_head SET name='wrong'", "namespace_index"},
		{"parent loss", "DELETE FROM dir", "namespace_parent"},
		{"job ownership", "INSERT INTO job(kind,state,target_vlog,dest_vlog,created_at) VALUES('compact','running',1,1,0)", "job_destination"},
		{"counts", "UPDATE chunk SET refcount=refcount+1", "reference_count"},
		{"multiplicity", "UPDATE chunk SET refcount=2", "reference_count"},
		{"bounds", "UPDATE vlog SET length=2", "extent_bounds"},
		{"length", "UPDATE chunk SET logical_len=4", "extent_length"},
		{"missing chunk", "DELETE FROM chunk", "missing_chunk"},
		{"partial extent", "UPDATE file SET chunks=CAST(chunks||X'00' AS BLOB)", "malformed_extents"},
		{"mapping loss", "DELETE FROM vlog_plog", "protection_count"},
		{"missing plog", "DELETE FROM plog", "missing_plog"},
		{"shard index", "UPDATE vlog_plog SET shard_idx=2", "shard_index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := checkerFixture(t)
			if issues, err := db.CheckCatalog(context.Background()); err != nil || len(issues) != 0 {
				t.Fatalf("healthy=%v err=%v", issues, err)
			}
			if _, err := db.db.Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			issues, err := db.CheckCatalog(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, issue := range issues {
				if issue.Code == tc.code {
					return
				}
			}
			t.Fatalf("did not detect %s: %v", tc.code, issues)
		})
	}
}

func TestCatalogFileCheckerDoesNotCreateOrRepair(t *testing.T) {
	db, path := checkerFixture(t)
	// Open's namespace backfill would rewrite this field. The checker must not.
	if _, err := db.db.Exec("UPDATE file_head SET name='' "); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec("UPDATE chunk SET refcount=9"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	issues, err := CheckCatalogFile(context.Background(), path)
	if err != nil || len(issues) == 0 {
		t.Fatalf("issues=%v err=%v", issues, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only checker modified the database")
	}
	var name string
	if err := db.db.QueryRow("SELECT name FROM file_head").Scan(&name); err != nil || name != "" {
		t.Fatal("checker performed namespace repair")
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	if _, err := CheckCatalogFile(context.Background(), missing); err == nil {
		t.Fatal("missing catalog accepted")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("checker created missing database: %v", err)
	}
}
