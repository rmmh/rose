package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestReapAbandonedWriteOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := NewServerWithDataDir(db, filepath.Join(dir, "plogs"))
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// 1. Create a prepared write op that is older than the threshold.
	s.SetWriteOpExpiry(50 * time.Millisecond)

	// Open a handle to create a write op.
	openResp, err := s.Open(ctx, &pb.OpenRequest{Path: "/f1", OperationKey: "op-1"})
	if err != nil {
		t.Fatal(err)
	}
	h1 := openResp.GetHandle()

	// Verify it's created and active.
	ops, err := s.db.ListPreparedWriteOps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 prepared write op, got %d", len(ops))
	}

	// Wait 100ms so it exceeds the 50ms expiry.
	time.Sleep(100 * time.Millisecond)

	// Since there is an active handle h1, it should NOT be reaped.
	reaped, err := s.ReapAbandonedWriteOps(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if reaped != 0 {
		t.Fatalf("expected 0 reaped ops (since handle is active), got %d", reaped)
	}

	// Close the handle without committing by removing it from the server's handles map.
	s.handlesMu.Lock()
	delete(s.handles, h1)
	s.handlesMu.Unlock()

	// Now there is no active handle, and the op is older than 50ms.
	// It should be reaped.
	reaped, err = s.ReapAbandonedWriteOps(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if reaped != 1 {
		t.Fatalf("expected 1 reaped op, got %d", reaped)
	}

	// Verify the DB state is now abandoned.
	ops, err = s.db.ListPreparedWriteOps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected 0 prepared write ops, got %d", len(ops))
	}
}

func TestReapStartupGracePeriod(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := NewServerWithDataDir(db, filepath.Join(dir, "plogs"))
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// Create a write op.
	openResp, err := s.Open(ctx, &pb.OpenRequest{Path: "/f2", OperationKey: "op-2"})
	if err != nil {
		t.Fatal(err)
	}
	h2 := openResp.GetHandle()

	// Simulate client crash/disconnect (remove from in-memory handles).
	s.handlesMu.Lock()
	delete(s.handles, h2)
	s.handlesMu.Unlock()

	// Set writeOpExpiry to 1 hour, and simulate that the write op was created
	// before the server started (e.g. op.CreatedAt is 30 minutes before the simulated s.startTime).
	s.SetWriteOpExpiry(1 * time.Hour)

	// Update the creation time of the write op in DB to be 120 minutes ago.
	oneTwentyMinsAgo := time.Now().Add(-120 * time.Minute).UnixNano()
	_, err = db.GetDB().ExecContext(ctx, "UPDATE write_op SET created_at = ?", oneTwentyMinsAgo)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's in the DB.
	ops, err := s.db.ListPreparedWriteOps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 prepared op, got %d", len(ops))
	}

	// The server just started (s.startTime is now), so it has been running for 0 seconds.
	// Since 0 seconds < 1 hour (expiry threshold), the startup grace period has NOT expired.
	// Therefore, it should NOT be reaped!
	reaped, err := s.ReapAbandonedWriteOps(ctx, 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if reaped != 0 {
		t.Fatalf("expected 0 reaped ops due to startup grace period, got %d", reaped)
	}

	// Now simulate that the server has been running for 90 minutes.
	s.startTime = time.Now().Add(-90 * time.Minute)

	// Since 90 minutes > 1 hour, the startup grace period has expired.
	// Since the write op is also older than 1 hour (created 30m before 90m ago = 120m ago),
	// it should be reaped now!
	reaped, err = s.ReapAbandonedWriteOps(ctx, 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if reaped != 1 {
		t.Fatalf("expected 1 reaped op after grace period, got %d", reaped)
	}
}

func TestReapReleaseLeaseAndCompaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Create a server with 1 disk.
	roots := map[uint32]string{1: filepath.Join(dir, "disk-1")}
	if err := os.MkdirAll(roots[1], 0o755); err != nil {
		t.Fatal(err)
	}
	s := NewServerWithDiskRoots(db, roots)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// Provision a vlog
	s.vlogMu.Lock()
	vlogID, vlog, err := s.provisionVlogLocked(ctx, "NONE", 1, 0)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Create a write op.
	openResp, err := s.Open(ctx, &pb.OpenRequest{Path: "/f3", OperationKey: "op-3"})
	if err != nil {
		t.Fatal(err)
	}
	h3 := openResp.GetHandle()

	// Claim vlog lease
	err = s.db.ClaimVlogLease(ctx, vlogID, h3, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify lease exists.
	leased, err := s.db.VlogLeased(ctx, vlogID)
	if err != nil || !leased {
		t.Fatalf("expected vlog to be leased, got leased=%v, err=%v", leased, err)
	}

	// Compaction should skip it because it is leased.
	// Write some dummy data so it qualifies for compaction.
	data := []byte("hello compaction")
	_, err = vlog.Write(ctx, 0, data)
	if err != nil {
		t.Fatal(err)
	}
	err = vlog.Commit(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = s.db.SetVlogLength(ctx, vlogID, vlog.Length())
	if err != nil {
		t.Fatal(err)
	}

	// CompactVlog should return nil (noop/skipped) without compacting because vlog is leased.
	err = s.CompactVlog(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}

	// Verify vlog is still mounted and not retired.
	s.vlogMu.Lock()
	_, mounted := s.vlogs[vlogID]
	s.vlogMu.Unlock()
	if !mounted {
		t.Fatal("vlog was retired despite being leased")
	}

	// Simulate client disconnect by deleting the handle from the map.
	s.handlesMu.Lock()
	delete(s.handles, h3)
	s.handlesMu.Unlock()

	// Reap it!
	s.SetWriteOpExpiry(0) // immediate expiry
	reaped, err := s.ReapAbandonedWriteOps(ctx, 0)
	if err != nil || reaped != 1 {
		t.Fatalf("expected 1 reaped op, got %d, err=%v", reaped, err)
	}

	// Verify lease is gone.
	leased, err = s.db.VlogLeased(ctx, vlogID)
	if err != nil || leased {
		t.Fatalf("expected lease to be gone, got leased=%v, err=%v", leased, err)
	}

	// Now CompactVlog should be able to run and not be skipped.
	err = s.CompactVlog(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}

	// Verify vlog is now retired (no longer in s.vlogs).
	s.vlogMu.Lock()
	_, mounted = s.vlogs[vlogID]
	s.vlogMu.Unlock()
	if mounted {
		t.Fatal("expected vlog to be retired after compaction")
	}
}
