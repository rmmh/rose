package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/rmmh/rose/proto"
)

func TestIdleHandleExpiryRenewsFencesAndReleasesResources(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	now := time.Now()
	s.now = func() time.Time { return now }
	auditWrite(t, s, "bucket/file", []byte("reader pin"))
	reader, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file"})
	if err != nil {
		t.Fatal(err)
	}
	stale := s.handles[reader.Handle]
	now = now.Add(40 * time.Minute)
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{Handle: reader.Handle}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(40 * time.Minute)
	if _, err := s.ReapAbandonedWriteOps(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if !s.handleStillRegistered(reader.Handle, stale) {
		t.Fatal("renewed reader expired")
	}
	if len(s.pinnedChunks[stale.pinOwner]) == 0 {
		t.Fatal("renewed reader lost pins")
	}
	now = now.Add(21 * time.Minute)
	if _, err := s.ReapAbandonedWriteOps(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if s.handleStillRegistered(reader.Handle, stale) {
		t.Fatal("idle reader remains registered")
	}
	if len(s.pinnedChunks[stale.pinOwner]) != 0 {
		t.Fatal("expired reader retained pins")
	}
	if err := s.setHandleMtime(ctx, reader.Handle, 1, stale); err == nil {
		t.Fatal("request paused after lookup revived expired handle")
	}
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{Handle: reader.Handle, Path: "bucket/file"}); err == nil {
		t.Fatal("expired handle fell back to namespace lookup")
	}

	writer, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/pending", OperationKey: "idle-writer"})
	if err != nil {
		t.Fatal(err)
	}
	// Enough bytes to spill to leased storage rather than only the write cache.
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: writer.Handle, Buffer: make([]byte, 5<<20)}); err != nil {
		t.Fatal(err)
	}
	var leases int
	if err := s.db.GetDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM vlog_lease").Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases == 0 {
		t.Fatal("fixture did not acquire a lease")
	}
	now = now.Add(61 * time.Minute)
	n, err := s.ReapAbandonedWriteOps(ctx, time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("reaped=%d err=%v", n, err)
	}
	if err := s.db.GetDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM vlog_lease").Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases != 0 {
		t.Fatal("expired writer retained leases")
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: writer.Handle}); err == nil {
		t.Fatal("expired writer published")
	}
}

func TestLocalOwnershipAndSameKeyRenewalProtectLiveOperation(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	now := time.Now()
	s.now = func() time.Time { return now }
	a, err := s.Open(ctx, &pb.OpenRequest{Path: "pending", OperationKey: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Open(ctx, &pb.OpenRequest{Path: "pending", OperationKey: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.RetainHandle(b.Handle)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if n, err := s.ReapAbandonedWriteOps(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("reaped=%d err=%v", n, err)
	}
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{Handle: a.Handle}); err == nil {
		t.Fatal("unretained idle handle survived")
	}
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{Handle: b.Handle}); err != nil {
		t.Fatal(err)
	}
	release()
	release() // The adapter may repeat cleanup after a failed Close.
	now = now.Add(30 * time.Minute)
	if n, err := s.ReapAbandonedWriteOps(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("renewed operation reaped=%d err=%v", n, err)
	}
	now = now.Add(31 * time.Minute)
	if n, err := s.ReapAbandonedWriteOps(ctx, time.Hour); err != nil || n != 1 {
		t.Fatalf("released operation reaped=%d err=%v", n, err)
	}
}
