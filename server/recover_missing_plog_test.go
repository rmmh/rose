package server

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
)

// TestRecoverStubsMissingPlogFile covers true media loss on a still-reachable
// disk: a shard's backing file is gone (deleted out from under the catalog,
// which has not yet marked the disk failed). Recover must boot the server
// degraded -- stubbing the absent shard offline so the surviving DUPLICATE copy
// carries reads -- rather than failing startup on the first missing file, or
// letting OpenPlog's O_CREATE silently resurrect it as an empty shard.
func TestRecoverStubsMissingPlogFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Two disks on two nodes: the default policy mirrors one copy onto each
	// (2-copy DUPLICATE), so a single lost shard is recoverable from its sibling.
	roots := map[uint32]string{1: filepath.Join(dir, "disk1"), 2: filepath.Join(dir, "disk2")}
	s1 := NewServerWithDiskRoots(db, roots)
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 4096)
	rand.New(rand.NewSource(7)).Read(payload)
	writeServerFileInternal(t, s1, "/mirror/file", payload)

	// Find a shard of the file's vlog to lose. Any non-staging DUPLICATE vlog
	// with at least two copies will do; delete its shard-0 plog file.
	vlogs, err := db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var lostPlogID, lostDiskID uint32
	for _, v := range vlogs {
		if v.ProtectionScheme != "DUPLICATE" || v.IsStaging() {
			continue
		}
		shardDisks, err := db.VlogShardDisks(ctx, v.ID)
		if err != nil {
			t.Fatal(err)
		}
		mappings, err := db.ListVlogPlogs(ctx, v.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(mappings) < 2 {
			continue
		}
		lostPlogID = mappings[0].PlogID
		lostDiskID = shardDisks[0].DiskID
		break
	}
	if lostPlogID == 0 {
		t.Fatal("no DUPLICATE data vlog with a sibling copy found")
	}
	lostPath := s1.plogPath(lostDiskID, lostPlogID)

	s1.CloseStorage()

	// Simulate media loss: the file vanishes while the catalog still considers the
	// disk reachable (active, on a working node).
	if err := os.Remove(lostPath); err != nil {
		t.Fatal(err)
	}

	// Recover on a fresh server. This must succeed despite the absent file.
	s2 := NewServerWithDiskRoots(db, roots)
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover with a missing shard file should boot degraded, got: %v", err)
	}
	defer s2.CloseStorage()

	// The lost shard is stubbed offline, not opened...
	if !s2.offlinePlogs[lostPlogID] {
		t.Fatalf("plog %d should be stubbed offline after its file went missing", lostPlogID)
	}
	if _, ok := s2.plogs[lostPlogID]; ok {
		t.Fatalf("plog %d should not have an open handle after its file went missing", lostPlogID)
	}
	// ...and it was not resurrected as an empty file by an O_CREATE open.
	if _, err := os.Stat(lostPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing plog file should stay absent after recovery, stat err = %v", err)
	}

	// The data still reads correctly: the surviving mirror carries the read.
	if got := readServerFileInternal(t, s2, "/mirror/file"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed after recovering with a lost shard")
	}
}

// TestRecoverStubsWhollyMissingDisk is the disk-granularity case: an entire disk
// root is gone (the disk is unplugged, not yet marked failed in the catalog).
// Every shard on it must stub offline -- the absent parent directory yields the
// same fs.ErrNotExist as an absent file -- and the server must boot degraded
// rather than fail or recreate the directory and empty plogs underneath it.
func TestRecoverStubsWhollyMissingDisk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	disk1 := filepath.Join(dir, "disk1")
	disk2 := filepath.Join(dir, "disk2")
	roots := map[uint32]string{1: disk1, 2: disk2}
	s1 := NewServerWithDiskRoots(db, roots)
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 4096)
	rand.New(rand.NewSource(11)).Read(payload)
	writeServerFileInternal(t, s1, "/mirror/file", payload)
	s1.CloseStorage()

	// Which plogs lived on disk1? They must all come back stubbed offline.
	var onDisk1 []uint32
	plogs, err := db.ListPlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range plogs {
		if p.DiskID == 1 {
			onDisk1 = append(onDisk1, p.ID)
		}
	}
	if len(onDisk1) == 0 {
		t.Fatal("expected at least one plog on disk1")
	}

	// The whole disk vanishes -- root directory and all.
	if err := os.RemoveAll(disk1); err != nil {
		t.Fatal(err)
	}

	s2 := NewServerWithDiskRoots(db, roots)
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover with a wholly missing disk should boot degraded, got: %v", err)
	}
	defer s2.CloseStorage()

	for _, id := range onDisk1 {
		if !s2.offlinePlogs[id] {
			t.Fatalf("plog %d on the missing disk should be stubbed offline", id)
		}
	}
	// The disk's root was not recreated underneath the catalog.
	if _, err := os.Stat(disk1); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing disk root should stay absent after recovery, stat err = %v", err)
	}
	// Reads still resolve from the mirror on the surviving disk.
	if got := readServerFileInternal(t, s2, "/mirror/file"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed after recovering with a wholly missing disk")
	}
}
