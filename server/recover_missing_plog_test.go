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

// TestRecoverStubsSingleMissingPlogFile covers an individual shard file removed
// out-of-band while the rest of the disk's directory is intact (e.g. tearing
// down a bucket's vlogs). Recover must leave the disk active -- not condemn the
// whole disk for one file -- stub just that shard offline, and boot degraded with
// reads served from the surviving mirror, rather than failing startup or letting
// OpenPlog's O_CREATE silently resurrect the shard as an empty file.
func TestRecoverStubsSingleMissingPlogFile(t *testing.T) {
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

	// The disk stays active: a single out-of-band file deletion must not condemn
	// the whole disk, which keeps serving its other shards.
	if got := s2.DiskStates()[lostDiskID]; got != meta.DiskActive {
		t.Fatalf("disk %d state = %q after one missing shard file, want %q", lostDiskID, got, meta.DiskActive)
	}
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

// TestRecoverStubbedShardGetsRepaired closes the loop for the missing-file case:
// a shard whose file vanished on an otherwise-active disk is stubbed offline at
// recovery (the disk is NOT condemned), and the next maintenance pass regenerates
// it from the surviving mirror onto a placement-allowed disk, clearing the
// offline stub and restoring full redundancy with no operator action. This is the
// gap the disk-keyed reprotect path could not reach.
func TestRecoverStubbedShardGetsRepaired(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	disk1 := filepath.Join(dir, "disk1")
	disk2 := filepath.Join(dir, "disk2")
	disk3 := filepath.Join(dir, "disk3")
	// Mirror across disks 1 and 2 (2-copy DUPLICATE); disk 3 is a free spare the
	// repair can relocate the regenerated shard onto.
	roots := map[uint32]string{1: disk1, 2: disk2, 3: disk3}
	s1 := NewServerWithDiskRoots(db, roots)
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4096)
	rand.New(rand.NewSource(31)).Read(payload)
	writeServerFileInternal(t, s1, "/mirror/file", payload)

	// Pick a non-staging DUPLICATE vlog and the shard-0 plog to lose.
	vlogs, err := db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var vlogID, lostPlogID, lostDiskID uint32
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
		vlogID = v.ID
		lostPlogID = mappings[0].PlogID
		lostDiskID = shardDisks[0].DiskID
		break
	}
	if vlogID == 0 {
		t.Fatal("no DUPLICATE data vlog with a sibling copy found")
	}
	lostPath := s1.plogPath(lostDiskID, lostPlogID)
	s1.CloseStorage()

	// The shard's file vanishes while its disk stays reachable (active, working
	// node): a genuine single-file media loss, not a whole-disk unplug.
	if err := os.Remove(lostPath); err != nil {
		t.Fatal(err)
	}

	s2 := NewServerWithDiskRoots(db, roots)
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover with a missing shard file should boot degraded, got: %v", err)
	}
	defer s2.CloseStorage()
	if got := s2.DiskStates()[lostDiskID]; got != meta.DiskActive {
		t.Fatalf("disk %d state = %q after one missing shard file, want %q", lostDiskID, got, meta.DiskActive)
	}
	if !s2.offlinePlogs[lostPlogID] {
		t.Fatalf("plog %d should be stubbed offline after its file went missing", lostPlogID)
	}

	// One maintenance pass regenerates the offline shard from the surviving mirror.
	if err := s2.RunMaintenanceOnce(ctx); err != nil {
		t.Fatalf("maintenance pass: %v", err)
	}

	// The offline stub is cleared and the vlog no longer references the lost plog:
	// its copy was regenerated onto a fresh plog, restoring two healthy copies.
	if s2.offlinePlogs[lostPlogID] {
		t.Fatalf("plog %d should no longer be offline after repair", lostPlogID)
	}
	mappings, err := db.ListVlogPlogs(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if len(mappings) < 2 {
		t.Fatalf("vlog %d should still have two copies after repair, got %d", vlogID, len(mappings))
	}
	for _, m := range mappings {
		if m.PlogID == lostPlogID {
			t.Fatalf("vlog %d still references the lost plog %d after repair", vlogID, lostPlogID)
		}
	}
	// Every surviving shard is backed by an open, reachable plog (none offline).
	for _, m := range mappings {
		if _, ok := s2.plogs[m.PlogID]; !ok {
			t.Fatalf("vlog %d shard %d plog %d has no open handle after repair", vlogID, m.ShardIndex, m.PlogID)
		}
	}
	if got := readServerFileInternal(t, s2, "/mirror/file"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed after repairing the offline shard")
	}
}

// TestRecoverFailsWhollyMissingDisk is the disk-granularity case: an entire disk
// root is gone (the disk is unplugged, not yet marked failed in the catalog).
// The disk must be marked failed and every shard on it stubbed offline -- the
// absent parent directory yields the same stat error as an absent file -- and
// the server must boot degraded rather than fail or recreate the directory and
// empty plogs underneath it.
func TestRecoverFailsWhollyMissingDisk(t *testing.T) {
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

	if got := s2.DiskStates()[1]; got != meta.DiskFailed {
		t.Fatalf("disk 1 state = %q after its root vanished, want %q", got, meta.DiskFailed)
	}
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

// TestRecoverFailedDiskGetsReprotected closes the loop end to end: a disk lost at
// boot is marked failed, and the very next maintenance pass regenerates the
// shards it held onto a healthy spare disk, restoring full redundancy without any
// operator action.
func TestRecoverFailedDiskGetsReprotected(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	disk1 := filepath.Join(dir, "disk1")
	disk2 := filepath.Join(dir, "disk2")
	disk3 := filepath.Join(dir, "disk3")

	// Provision the mirror across just disks 1 and 2 (2-copy DUPLICATE), leaving
	// disk 3 unconfigured for now so it is free as a reprotect destination later.
	s1 := NewServerWithDiskRoots(db, map[uint32]string{1: disk1, 2: disk2})
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4096)
	rand.New(rand.NewSource(23)).Read(payload)
	writeServerFileInternal(t, s1, "/mirror/file", payload)

	var vlogID uint32
	vlogs, err := db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vlogs {
		if v.ProtectionScheme == "DUPLICATE" && !v.IsStaging() {
			vlogID = v.ID
			break
		}
	}
	if vlogID == 0 {
		t.Fatal("no DUPLICATE data vlog provisioned")
	}
	s1.CloseStorage()

	// Disk 1 is unplugged, then a spare (disk 3) is brought online on restart.
	if err := os.RemoveAll(disk1); err != nil {
		t.Fatal(err)
	}
	s2 := NewServerWithDiskRoots(db, map[uint32]string{1: disk1, 2: disk2, 3: disk3})
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer s2.CloseStorage()
	if got := s2.DiskStates()[1]; got != meta.DiskFailed {
		t.Fatalf("disk 1 state = %q after its root vanished, want %q", got, meta.DiskFailed)
	}

	// One maintenance pass reprotects the failed disk onto the spare.
	if err := s2.RunMaintenanceOnce(ctx); err != nil {
		t.Fatalf("maintenance pass: %v", err)
	}

	// The mirror no longer references the failed disk; its copy now lives on the
	// spare, restoring two healthy copies.
	shardDisks, err := db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	disks := map[uint32]bool{}
	for _, sd := range shardDisks {
		disks[sd.DiskID] = true
	}
	if disks[1] {
		t.Fatalf("vlog %d still references failed disk 1 after reprotect: %v", vlogID, disks)
	}
	if !disks[2] || !disks[3] {
		t.Fatalf("vlog %d should be mirrored on disks 2 and 3 after reprotect, got %v", vlogID, disks)
	}
	if got := readServerFileInternal(t, s2, "/mirror/file"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed after reprotect")
	}
}
