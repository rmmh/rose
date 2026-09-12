package meta

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rmmh/rose/uid"
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

func TestCatalogCheckerAcceptsProtectionGeometry(t *testing.T) {
	for _, tc := range []struct {
		scheme                                 string
		data, parity, targetData, targetParity int32
		required                               int
	}{
		{"NONE", 1, 0, 0, 0, 1}, {"DUPLICATE", 1, 0, 0, 0, 3}, {"EC", 2, 1, 0, 0, 3}, {"DUPLICATE", 1, 0, 2, 1, 2},
	} {
		db, err := OpenEphemeral()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.MakeVlogInDomain(context.Background(), uid.New(), tc.scheme, tc.data, tc.parity, tc.targetData, tc.targetParity, nil, tc.required); err != nil {
			db.Close()
			t.Fatal(err)
		}
		issues, err := db.CheckCatalog(context.Background())
		db.Close()
		if err != nil || len(issues) != 0 {
			t.Fatalf("valid geometry=%+v issues=%v err=%v", tc, issues, err)
		}
	}
}

func TestCatalogCheckerRecordOverlap(t *testing.T) {
	for _, offset := range []int64{0, 1, 66, 67} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			db, _ := checkerFixture(t)
			// The fixture already references its first record twice in both a
			// head and snapshot. Multiplicity is valid; a distinct stored record
			// must start beyond that record's 64-byte header plus 3-byte payload.
			p := ChunkPlacement{Hash: bytes.Repeat([]byte{2}, 15), VlogID: 1, VaddrOffset: offset, LogicalLen: 3, CompressedLen: 3}
			if _, err := db.CommitFile(context.Background(), "other", 1, []ChunkPlacement{p}); err != nil {
				t.Fatal(err)
			}
			issues, err := db.CheckCatalog(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, issue := range issues {
				if issue.Code == "overlapping_records" {
					found = true
				}
			}
			if found != (offset < 67) {
				t.Fatalf("offset=%d overlap=%v issues=%v", offset, found, issues)
			}
			if offset == 67 && len(issues) != 0 {
				t.Fatalf("adjacent records rejected: %v", issues)
			}
		})
	}
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
		{"unknown protection", "UPDATE vlog SET protection_scheme='unknown'", "protection_geometry"},
		{"missing disk clock", "DELETE FROM placement_clock", "placement_clock"},
		{"invalid disk epoch", "INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0); PRAGMA ignore_check_constraints=ON; UPDATE disk SET placement_epoch=0", "placement_epoch"},
		{"disk clock rollback", "INSERT INTO node(id,mac,hostname) VALUES(1,'one','one'); INSERT INTO disk(id,node_id,total_bytes,used_bytes) VALUES(1,1,1,0); UPDATE placement_clock SET epoch=1", "placement_epoch"},
		{"invalid plog epoch", "PRAGMA ignore_check_constraints=ON; UPDATE plog SET placement_epoch=0", "placement_epoch"},
		{"invalid epoch", "PRAGMA ignore_check_constraints=ON; UPDATE vlog SET placement_epoch=0", "placement_epoch"},
		{"mirror geometry", "UPDATE vlog SET protection_scheme='DUPLICATE',data_shards=2", "protection_geometry"},
		{"EC requirement", "UPDATE vlog SET protection_scheme='EC',data_shards=2,parity_shards=1", "protection_geometry"},
		{"staging target", "UPDATE vlog SET protection_scheme='DUPLICATE',target_data_shards=2", "staging_geometry"},
		{"staging copies", "UPDATE vlog SET protection_scheme='DUPLICATE',target_data_shards=2,target_parity_shards=1", "staging_geometry"},
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
