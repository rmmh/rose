package server

import (
	"context"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestSnapshotNamespaceRetainsEmptyDirectoriesAndMetadata(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	if err := s.db.Mkdir(ctx, "tree/empty", 123); err != nil {
		t.Fatal(err)
	}
	auditWrite(t, s, "tree/file", []byte("frozen contents"))
	snap, err := s.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "complete"})
	if err != nil {
		t.Fatal(err)
	}
	id := snap.SnapshotId
	if _, err := s.Rmdir(ctx, &pb.RmdirRequest{Path: "tree/empty"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rename(ctx, &pb.RenameRequest{OldPath: "tree/file", NewPath: "tree/moved"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Mkdir(ctx, &pb.MkdirRequest{Path: "tree/new"}); err != nil {
		t.Fatal(err)
	}
	entries, err := s.ListDir(ctx, &pb.ListDirRequest{Path: "/tree", SnapshotId: id})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries.Entries) != 2 || entries.Entries[0].Name != "empty" || !entries.Entries[0].IsDir || entries.Entries[0].Mtime != 123 || entries.Entries[1].Name != "file" {
		t.Fatalf("snapshot children=%v", entries.Entries)
	}
	empty, err := s.ListDir(ctx, &pb.ListDirRequest{Path: "tree/empty", SnapshotId: id})
	if err != nil || len(empty.GetEntries()) != 0 {
		t.Fatalf("empty directory=%v err=%v", empty, err)
	}
	attr, err := s.Getattr(ctx, &pb.GetattrRequest{Path: "tree/empty", SnapshotId: id})
	if err != nil || !attr.GetIsDir() || attr.GetMtime() != 123 {
		t.Fatalf("frozen directory=%v err=%v", attr, err)
	}
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{Path: "tree/new", SnapshotId: id}); err == nil {
		t.Fatal("snapshot acquired a later directory")
	}
	again, err := s.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "complete"})
	if err != nil || again.GetSnapshotId() != id {
		t.Fatalf("retry snapshot=%v err=%v", again, err)
	}
	h, err := s.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: id, Path: "tree/file"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{SnapshotId: id, Handle: h.Handle}); err == nil {
		t.Fatal("ambiguous snapshot/handle stat accepted")
	}
	if _, err := s.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{SnapshotId: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListDir(ctx, &pb.ListDirRequest{SnapshotId: id}); err == nil {
		t.Fatal("deleted snapshot appeared as empty root")
	}
	var dirs int
	if err := s.db.GetDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM snapshot_dir WHERE snapshot_id=?", id).Scan(&dirs); err != nil || dirs != 0 {
		t.Fatalf("deleted directory rows=%d err=%v", dirs, err)
	}
	read, err := s.Read(ctx, &pb.ReadRequest{Handle: h.Handle, Length: 100})
	if err != nil || string(read.GetBuffer()) != "frozen contents" {
		t.Fatalf("retained snapshot reader=%q err=%v", read.GetBuffer(), err)
	}
}
