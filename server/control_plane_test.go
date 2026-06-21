package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
)

// newControlPlaneServer builds a recovered server with diskCount independent
// local disks, the multi-disk shape the storage control plane operates on.
func newControlPlaneServer(t *testing.T, diskCount int) *Server {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	roots := make(map[uint32]string, diskCount)
	for i := 1; i <= diskCount; i++ {
		roots[uint32(i)] = filepath.Join(dir, fmt.Sprintf("disk-%d", i))
		if err := os.MkdirAll(roots[uint32(i)], 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := NewServerWithDiskRoots(db, roots)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// provision creates a vlog under the server lock, as the RPC entry points do.
func provision(t *testing.T, s *Server, scheme string, data, parity int) uint32 {
	t.Helper()
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	id, _, err := s.provisionVlogLocked(context.Background(), scheme, data, parity)
	if err != nil {
		t.Fatalf("provision %s vlog: %v", scheme, err)
	}
	return id
}

func TestCommitThreshold(t *testing.T) {
	s := &Server{minCopies: 2}
	cases := []struct {
		scheme       string
		data, parity int
		total        int
		wantCommit   int
		wantReadGate int
	}{
		{"NONE", 1, 0, 1, 1, 1},
		{"DUPLICATE", 1, 0, 1, 1, 1}, // single disk: commit on the one copy
		{"DUPLICATE", 1, 0, 3, 2, 1}, // three copies: need minCopies live
		{"EC", 2, 1, 3, 3, 2},        // all shards to commit, data shards to read
		{"EC", 4, 2, 6, 6, 4},
	}
	for _, c := range cases {
		info := meta.VlogInfo{ProtectionScheme: c.scheme, DataShards: int32(c.data), ParityShards: int32(c.parity)}
		if got := s.commitThreshold(info, c.total); got != c.wantCommit {
			t.Errorf("%s commitThreshold(total=%d) = %d, want %d", c.scheme, c.total, got, c.wantCommit)
		}
		if got := s.readThreshold(info); got != c.wantReadGate {
			t.Errorf("%s readThreshold = %d, want %d", c.scheme, got, c.wantReadGate)
		}
	}
}

func TestDuplicateCommitGateDegradesToReadOnly(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	vlogID := provision(t, s, "DUPLICATE", 1, 0) // one copy per disk -> 3 copies

	mustReady := func(want bool) {
		t.Helper()
		got, err := s.CommitReady(ctx, vlogID)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("CommitReady = %v, want %v", got, want)
		}
	}
	mustReadable := func(want bool) {
		t.Helper()
		got, err := s.Readable(ctx, vlogID)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("Readable = %v, want %v", got, want)
		}
	}

	mustReady(true) // 3 live copies >= minCopies(2)
	if err := s.SetDiskState(ctx, 3, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	mustReady(true) // 2 live copies still meets the gate
	if err := s.SetDiskState(ctx, 2, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	mustReady(false)   // 1 live copy < minCopies(2): refuse new commits
	mustReadable(true) // but the surviving copy can still be served
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	mustReadable(false) // last copy gone: unreadable
}

func TestECCommitGateRequiresAllShards(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	vlogID := provision(t, s, "EC", 2, 1) // 3 shards across 3 disks

	ready, err := s.CommitReady(ctx, vlogID)
	if err != nil || !ready {
		t.Fatalf("CommitReady = %v, %v; want true", ready, err)
	}
	if err := s.SetDiskState(ctx, 3, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	ready, err = s.CommitReady(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("CommitReady = true with a failed shard; EC must require all shards to commit")
	}
	// Losing one of three shards still leaves data_shards(2) readable.
	readable, err := s.Readable(ctx, vlogID)
	if err != nil || !readable {
		t.Fatalf("Readable = %v, %v; want true (data shards survive)", readable, err)
	}
}

func TestPlacementSkipsNonActiveDisks(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)

	if err := s.SetDiskState(ctx, 2, meta.DiskDraining); err != nil {
		t.Fatal(err)
	}
	s.vlogMu.Lock()
	active := s.activeDiskIDs()
	s.vlogMu.Unlock()
	if len(active) != 2 || active[0] != 1 || active[1] != 3 {
		t.Fatalf("activeDiskIDs = %v, want [1 3]", active)
	}

	// A new DUPLICATE vlog lands only on the two active disks.
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 2 {
		t.Fatalf("new vlog has %d shards, want 2 (drained disk excluded)", len(shards))
	}
	for _, sh := range shards {
		if sh.DiskID == 2 {
			t.Fatalf("shard placed on draining disk 2: %+v", shards)
		}
	}
}

func TestDiskStatePersistsAcrossRecover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	roots := map[uint32]string{1: filepath.Join(dir, "d1"), 2: filepath.Join(dir, "d2")}
	for _, r := range roots {
		if err := os.MkdirAll(r, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	s1 := NewServerWithDiskRoots(db, roots)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s1.SetDiskState(ctx, 2, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}

	// A fresh server over the same catalog must re-adopt the failed state.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s2.DiskStates()[2]; got != meta.DiskFailed {
		t.Fatalf("recovered disk 2 state = %q, want %q", got, meta.DiskFailed)
	}
	if got := s2.DiskStates()[1]; got != meta.DiskActive {
		t.Fatalf("recovered disk 1 state = %q, want %q", got, meta.DiskActive)
	}
}
