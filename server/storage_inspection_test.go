package server

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

func TestReadOnlyStorageInspection(t *testing.T) {
	for _, fault := range []string{"healthy", "payload", "header", "trailer", "identity", "missing", "orphan", "short prefix", "journal", "wrong key", "wrong address"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			catalog := filepath.Join(dir, "meta.db")
			root := filepath.Join(dir, "disk")
			db, err := meta.Open(catalog)
			if err != nil {
				t.Fatal(err)
			}
			s := NewServerWithDiskRoots(db, map[uint32]string{1: root})
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			s.StopMaintenanceDriver()
			auditWrite(t, s, "file", []byte("inspect the stored ciphertext without changing it"))
			placement := auditPlacement(t, s, "file")
			plogs, err := db.ListPlogs(ctx)
			if err != nil || len(plogs) != 1 {
				t.Fatalf("plogs=%v err=%v", plogs, err)
			}
			path := s.plogPath(1, plogs[0].ID)
			if fault == "short prefix" {
				info, err := db.GetVlog(ctx, placement.VlogID)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.SetVlogLength(ctx, info.ID, info.Length+1); err != nil {
					t.Fatal(err)
				}
			}
			s.CloseStorage()
			if fault == "wrong address" {
				// Preserve the 64 bits used for stream selection, changing only
				// the canonical full hash. Header decryption still succeeds.
				hash := append([]byte(nil), placement.Hash...)
				hash[14] ^= 1
				var extents []byte
				if err := db.GetDB().QueryRow("SELECT chunks FROM file").Scan(&extents); err != nil {
					t.Fatal(err)
				}
				if len(extents) != 19 {
					t.Fatalf("expected one extent, got %d bytes", len(extents))
				}
				copy(extents[:15], hash)
				if _, err := db.GetDB().Exec("UPDATE chunk SET hash=? WHERE hash=?", hash, placement.Hash); err != nil {
					t.Fatal(err)
				}
				if _, err := db.GetDB().Exec("UPDATE file SET chunks=?", extents); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "wrong key" {
				key := uid.New()
				if _, err := db.GetDB().Exec("UPDATE cluster SET encryption_key=?", key[:]); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "journal":
				if err := os.WriteFile(path+".undo", []byte("interrupted journal"), 0600); err != nil {
					t.Fatal(err)
				}
			case "payload":
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				offset := storage.CalcPhysical(placement.VaddrOffset + storage.ChunkHeaderSize)
				b := []byte{0}
				if _, err := f.ReadAt(b, offset); err != nil {
					t.Fatal(err)
				}
				b[0] ^= 1
				if _, err := f.WriteAt(b, offset); err != nil {
					t.Fatal(err)
				}
				f.Close()
			case "header":
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte{0}, 0); err != nil {
					t.Fatal(err)
				}
				f.Close()
			case "trailer":
				end := placement.VaddrOffset + storage.ChunkHeaderSize + int64(placement.LogicalLen)
				if err := os.Truncate(path, storage.CalcPhysical(end)); err != nil {
					t.Fatal(err)
				}
			case "identity":
				if err := os.WriteFile(filepath.Join(root, diskUIDMarker), []byte(uid.New().String()), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "plog-99999"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.ReadFile(path)
			issues, err := meta.CheckStorageFiles(ctx, catalog, map[uint32]string{1: root})
			if err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("inspection modified physical evidence")
			}
			if fault == "missing" {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("inspection recreated a missing file")
				}
			}
			if fault == "healthy" {
				if len(issues) != 0 {
					t.Fatalf("healthy inspection=%v", issues)
				}
				return
			}
			want := map[string]string{"payload": "plog_integrity", "header": "plog_unreadable", "trailer": "plog_integrity", "identity": "disk_identity", "missing": "plog_unreadable", "orphan": "uncataloged_plog", "short prefix": "plog_short_prefix", "journal": "plog_recovery_journal", "wrong key": "chunk_plaintext", "wrong address": "chunk_plaintext"}[fault]
			for _, issue := range issues {
				if issue.Code == want {
					return
				}
			}
			t.Fatalf("did not detect %s: %v", want, issues)
		})
	}
}

func TestReadOnlyPlaintextInspectionOfMirrorAndEC(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)()
	for _, scheme := range []string{"DUPLICATE", "EC"} {
		t.Run(scheme, func(t *testing.T) {
			ctx := context.Background()
			catalog := filepath.Join(t.TempDir(), "catalog.db")
			db, err := meta.Open(catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			roots := map[uint32]string{}
			for id := uint32(1); id <= 4; id++ {
				roots[id] = t.TempDir()
			}
			s := NewServerWithDiskRoots(db, roots)
			s.SetMaintenanceInterval(0)
			defer s.CloseStorage()
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			policy := meta.BucketPolicy{Name: "bucket", ProtectionScheme: scheme, DataShards: 1}
			if scheme == "EC" {
				policy.DataShards, policy.ParityShards = 3, 1
			}
			if err := s.SetBucketPolicy(ctx, policy); err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, 4000)
			rand.New(rand.NewSource(5)).Read(payload)
			writeServerFileInternal(t, s, "bucket/file", payload)
			if scheme == "EC" {
				if n, err := s.PromoteStaging(ctx); err != nil || n != 1 {
					t.Fatalf("promotion=%d err=%v", n, err)
				}
			}
			plogs, err := db.ListPlogs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s.CloseStorage()
			before := map[string][]byte{}
			for _, p := range plogs {
				path := s.plogPath(p.DiskID, p.ID)
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				before[path] = b
			}
			issues, err := meta.CheckStorageFiles(ctx, catalog, roots)
			if err != nil || len(issues) != 0 {
				t.Fatalf("valid %s plaintext rejected: %v %v", scheme, issues, err)
			}
			for path, b := range before {
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(b, after) {
					t.Fatalf("inspection changed file %s: %v", path, err)
				}
			}
		})
	}
}
