package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/uid"
)

func TestDiskUIDMarkerPersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	diskRoot := filepath.Join(dir, "disk-1")
	roots := map[uint32]string{1: diskRoot}

	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := NewServerWithDiskRoots(db, roots)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// The marker exists and matches the catalog UID.
	markerBytes, err := os.ReadFile(filepath.Join(diskRoot, diskUIDMarker))
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	markerUID, err := uid.Parse(strings.TrimSpace(string(markerBytes)))
	if err != nil {
		t.Fatalf("parse marker: %v", err)
	}
	catalogUID, err := db.DiskUID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if markerUID != catalogUID {
		t.Fatalf("marker UID %s != catalog UID %s", markerUID, catalogUID)
	}
	if catalogUID.IsZero() {
		t.Fatal("catalog disk UID is zero")
	}

	// Restart: the existing marker must be adopted, not regenerated.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	markerBytes2, err := os.ReadFile(filepath.Join(diskRoot, diskUIDMarker))
	if err != nil {
		t.Fatal(err)
	}
	markerUID2, err := uid.Parse(strings.TrimSpace(string(markerBytes2)))
	if err != nil {
		t.Fatal(err)
	}
	if markerUID2 != markerUID {
		t.Fatalf("disk UID changed across restart: %s -> %s", markerUID, markerUID2)
	}
	catalogUID2, err := db.DiskUID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if catalogUID2 != catalogUID {
		t.Fatalf("catalog disk UID changed across restart: %s -> %s", catalogUID, catalogUID2)
	}
}

// TestDiskRebindsByUIDOnRelocation proves the UID is authoritative: when two
// disks are physically swapped between configured roots, each disk_id follows its
// disk's marker to the new directory rather than staying glued to the mount path.
func TestDiskRebindsByUIDOnRelocation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rootA := filepath.Join(dir, "slotA")
	rootB := filepath.Join(dir, "slotB")

	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := NewServerWithDiskRoots(db, map[uint32]string{1: rootA, 2: rootB})
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	uid1, err := db.DiskUID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	uid2, err := db.DiskUID(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Physically swap the media: the disk that was in slotA is now in slotB.
	swap := filepath.Join(dir, "swap")
	if err := os.Rename(rootA, swap); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(rootB, rootA); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(swap, rootB); err != nil {
		t.Fatal(err)
	}

	// Restart with the same config (slot 1 -> rootA, slot 2 -> rootB). The ids must
	// follow the markers: disk 1 now resolves to rootB, disk 2 to rootA.
	s2 := NewServerWithDiskRoots(db, map[uint32]string{1: rootA, 2: rootB})
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s2.diskRoot(1); got != rootB {
		t.Fatalf("disk 1 should follow its marker to %s, bound to %s", rootB, got)
	}
	if got := s2.diskRoot(2); got != rootA {
		t.Fatalf("disk 2 should follow its marker to %s, bound to %s", rootA, got)
	}
	// Catalog UIDs are unchanged: identity is stable, only the binding moved.
	if again, _ := db.DiskUID(ctx, 1); again != uid1 {
		t.Fatalf("disk 1 uid changed: %s -> %s", uid1, again)
	}
	if again, _ := db.DiskUID(ctx, 2); again != uid2 {
		t.Fatalf("disk 2 uid changed: %s -> %s", uid2, again)
	}
}

func TestUnknownReplacementMediaDoesNotImpersonateKnownDisk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root1 := filepath.Join(dir, "slot1")
	root2 := filepath.Join(dir, "slot2")
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s1 := NewServerWithDiskRoots(db, map[uint32]string{1: root1, 2: root2})
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("foreign media must not inherit disk identity"), 200)
	writeServerFileInternal(t, s1, "/identity/file", payload)
	s1.CloseStorage()

	oldMedia := filepath.Join(dir, "removed-disk1")
	if err := os.Rename(root1, oldMedia); err != nil {
		t.Fatal(err)
	}
	if _, err := diskUIDForRoot(root1); err != nil {
		t.Fatal(err)
	}

	s2 := NewServerWithDiskRoots(db, map[uint32]string{1: root1, 2: root2})
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer s2.CloseStorage()
	if got := s2.DiskStates()[1]; got != meta.DiskFailed {
		t.Fatalf("foreign media in disk-1 slot left catalog disk active: %q", got)
	}
	if got := readServerFileInternal(t, s2, "/identity/file"); !bytes.Equal(got, payload) {
		t.Fatal("surviving mirror changed after foreign-media substitution")
	}
}

func TestCorruptDiskUIDMarkerDoesNotBlockHealthyMirrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root1 := filepath.Join(dir, "slot1")
	root2 := filepath.Join(dir, "slot2")
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s1 := NewServerWithDiskRoots(db, map[uint32]string{1: root1, 2: root2})
	s1.SetMaintenanceInterval(0)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("healthy mirror survives a corrupt disk marker"), 200)
	writeServerFileInternal(t, s1, "/identity/corrupt-marker", payload)
	s1.CloseStorage()
	if err := os.WriteFile(filepath.Join(root1, diskUIDMarker), []byte("not-a-uid\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := NewServerWithDiskRoots(db, map[uint32]string{1: root1, 2: root2})
	s2.SetMaintenanceInterval(0)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("corrupt marker on one disk blocked degraded recovery: %v", err)
	}
	defer s2.CloseStorage()
	if got := s2.DiskStates()[1]; got != meta.DiskFailed {
		t.Fatalf("disk with corrupt marker state = %q, want failed", got)
	}
	if got := readServerFileInternal(t, s2, "/identity/corrupt-marker"); !bytes.Equal(got, payload) {
		t.Fatal("surviving mirror changed after disk marker corruption")
	}
}
