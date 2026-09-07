package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestRetainedRetrySurvivesNamespaceGCAndRestart(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer func() { s.CloseStorage() }()
	now := time.Now()
	s.now = func() time.Time { return now }
	if err := s.SetRetryRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("historical result"), 5000)
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "stable"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: want}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	p := auditPlacement(t, s, "bucket/file")
	auditWrite(t, s, "bucket/file", []byte("new head"))
	if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: "bucket/file"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactVlog(ctx, p.VlogID); err != nil {
		t.Fatal(err)
	}
	old := s
	old.CloseStorage()
	s = NewServerWithDiskRoots(old.db, old.diskRoots)
	s.SetMaintenanceInterval(0)
	s.now = func() time.Time { return now }
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	retry, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "stable"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, &pb.ReadRequest{Handle: retry.Handle, Length: int64(len(want))})
	if err != nil || !bytes.Equal(got.GetBuffer(), want) {
		t.Fatalf("historical result unreadable: %v", err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: retry.Handle, Buffer: want}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: retry.Handle}); err != nil {
		t.Fatal(err)
	}
	if id, err := s.db.OpenFile(ctx, "bucket/file"); err != nil || id != 0 {
		t.Fatalf("retry resurrected name: id=%d err=%v", id, err)
	}
	// A same-length conflicting payload is rejected against the retained result.
	conflict, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "stable"})
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), want...)
	bad[0] ^= 1
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: conflict.Handle, Buffer: bad}); err == nil {
		if _, err := s.Close(ctx, &pb.CloseRequest{Handle: conflict.Handle}); err == nil {
			t.Fatal("conflicting historical retry succeeded")
		}
	}
	if err := s.AbortHandle(ctx, conflict.Handle); err != nil {
		t.Fatal(err)
	}
	reader, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "stable"})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if _, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "stable"}); err == nil {
		t.Fatal("expired key admitted without maintenance")
	}
	if _, err := s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = s.Read(ctx, &pb.ReadRequest{Handle: reader.Handle, Length: int64(len(want))})
	if err != nil || !bytes.Equal(got.GetBuffer(), want) {
		t.Fatalf("expiry lost active reader bytes: %v", err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: reader.Handle}); err == nil {
		t.Fatal("expired handle returned a retry result")
	}
	if err := s.AbortHandle(ctx, reader.Handle); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GC(ctx); err != nil || n == 0 {
		t.Fatalf("expired unpinned result not reclaimed: %d %v", n, err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: -1, IdempotencyKey: "stable"}); err == nil {
		t.Fatal("expired key acknowledged through missing-handle Close")
	}
	if issues, err := s.db.CheckCatalog(ctx); err != nil || len(issues) != 0 {
		t.Fatalf("catalog issues=%v err=%v", issues, err)
	}
}

func TestRetryRetentionDefaultDeadlineAndCloseAdmission(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	now := time.Now()
	s.now = func() time.Time { return now }
	if err := s.SetRetryRetention(0); err == nil {
		t.Fatal("zero retention accepted")
	}
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "retained", OperationKey: "deadline"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	var deadline int64
	if err := s.db.GetDB().QueryRow("SELECT expires_at FROM write_result_root").Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	if deadline != now.Add(DefaultRetryRetention).UnixNano() {
		t.Fatal("default deadline differs from contract")
	}
	if err := s.SetRetryRetention(48 * time.Hour); err != nil {
		t.Fatal(err)
	}
	now = now.Add(DefaultRetryRetention)
	// No Open or maintenance call intervenes: Close itself must enforce the
	// original deadline, even after the configured period has increased.
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle, IdempotencyKey: "deadline"}); err == nil {
		t.Fatal("Close returned an expired result")
	}
	op, err := s.db.WriteOpByKey(ctx, "deadline")
	if err != nil || op.State != meta.WriteOpExpired {
		t.Fatalf("expiry state=%v err=%v", op, err)
	}
}
