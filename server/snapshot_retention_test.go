package server

import (
	"context"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestSnapshotRetentionReleasesReferencesButPreservesOpenReader(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	now := time.Now()
	s.now = func() time.Time { return now }
	auditWrite(t, s, "file", []byte("snapshot reader survives expiration"))
	p := auditPlacement(t, s, "file")
	snap, err := s.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "expire"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := s.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snap.SnapshotId, Path: "file"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: "file"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSnapshotRetention(ctx, &meta.SnapshotRetention{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	expired, err := s.ExpireSnapshots(ctx)
	if err != nil || len(expired) != 1 || expired[0] != snap.SnapshotId {
		t.Fatalf("expired=%v err=%v", expired, err)
	}
	var refs int
	if err := s.db.GetDB().QueryRowContext(ctx, "SELECT refcount FROM chunk WHERE hash=?", p.Hash).Scan(&refs); err != nil || refs != 0 {
		t.Fatalf("remaining references=%d err=%v", refs, err)
	}
	if _, err := s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	read, err := s.Read(ctx, &pb.ReadRequest{Handle: h.Handle, Length: 100})
	if err != nil || string(read.GetBuffer()) != "snapshot reader survives expiration" {
		t.Fatalf("pinned read=%q err=%v", read.GetBuffer(), err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	var chunks int
	if err := s.db.GetDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM chunk WHERE hash=?", p.Hash).Scan(&chunks); err != nil || chunks != 0 {
		t.Fatalf("unpinned chunks=%d err=%v", chunks, err)
	}
}
