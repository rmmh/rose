package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
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

func TestMakeVlogRejectsInvalidShardGeometryWithoutPanicking(t *testing.T) {
	s := newControlPlaneServer(t, 4)
	ctx := context.Background()
	tests := []pb.MakeVlogRequest{
		{ProtectionScheme: "EC", DataShards: -1, ParityShards: 1},
		{ProtectionScheme: "EC", DataShards: 1, ParityShards: -1},
		{ProtectionScheme: "EC", DataShards: 0, ParityShards: 1},
		{ProtectionScheme: "EC", DataShards: 1, ParityShards: 0},
		{ProtectionScheme: "bogus", DataShards: 1, ParityShards: 1},
	}
	for _, req := range tests {
		if _, err := s.MakeVlog(ctx, &req); err == nil {
			t.Errorf("MakeVlog(%+v) succeeded", req)
		}
	}
}

type unexpectedReadClient struct{}

func (unexpectedReadClient) Write(context.Context, int64, []byte) (int64, error) {
	return 0, fmt.Errorf("unexpected write")
}

func (unexpectedReadClient) Read(context.Context, int64, int) ([]byte, error) {
	return nil, fmt.Errorf("read client reached")
}

func TestReadVlogRejectsUnboundedUnaryResultBeforeStorage(t *testing.T) {
	vlog, err := storage.NewVlog(99, "NONE", 1, 0, []storage.PlogClient{
		unexpectedReadClient{},
	}, maxUnaryReadBytes+1)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{vlogs: map[uint32]*storage.Vlog{99: vlog}}
	if _, err := s.ReadVlog(context.Background(), &pb.ReadVlogRequest{
		VlogId: 99,
		Length: uint32(maxUnaryReadBytes + 1),
	}); err == nil || !strings.Contains(err.Error(), "exceeds unary limit") {
		t.Fatalf("unbounded vlog read error = %v, want unary limit rejection", err)
	}
}

func TestSetBucketPolicyRejectsInvalidGeometry(t *testing.T) {
	s := newControlPlaneServer(t, 4)
	ctx := context.Background()
	tests := []meta.BucketPolicy{
		{Name: "bad-negative", ProtectionScheme: "EC", DataShards: -1, ParityShards: 1},
		{Name: "bad-no-data", ProtectionScheme: "EC", DataShards: 0, ParityShards: 1},
		{Name: "bad-no-parity", ProtectionScheme: "EC", DataShards: 1, ParityShards: 0},
		{Name: "bad-mirror", ProtectionScheme: "DUPLICATE", DataShards: 2},
		{Name: "bad-none", ProtectionScheme: "NONE", DataShards: 1, ParityShards: 1},
		{Name: "bad-scheme", ProtectionScheme: "bogus", DataShards: 1},
	}
	for _, policy := range tests {
		if err := s.SetBucketPolicy(ctx, policy); err == nil {
			t.Errorf("SetBucketPolicy(%+v) succeeded", policy)
		}
		if _, ok, err := s.GetDB().GetBucketPolicy(ctx, policy.Name); err != nil || ok {
			t.Errorf("invalid policy %q persisted: ok=%v err=%v", policy.Name, ok, err)
		}
	}
}

func TestSetBucketPolicyCanonicalizesAndValidatesName(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	ctx := context.Background()
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{
		Name:             "//canonical/",
		ProtectionScheme: "DUPLICATE",
		DataShards:       1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetDB().GetBucketPolicy(ctx, "canonical"); err != nil || !ok {
		t.Fatalf("canonical policy missing: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.GetDB().GetBucketPolicy(ctx, "//canonical/"); err != nil || ok {
		t.Fatalf("noncanonical policy key persisted: ok=%v err=%v", ok, err)
	}
	nested := meta.BucketPolicy{
		Name:             "nested/bucket",
		ProtectionScheme: "DUPLICATE",
		DataShards:       1,
	}
	if err := s.SetBucketPolicy(ctx, nested); err == nil {
		t.Fatal("nested bucket policy name succeeded")
	}
	if _, ok, err := s.GetDB().GetBucketPolicy(ctx, nested.Name); err != nil || ok {
		t.Fatalf("nested policy persisted: ok=%v err=%v", ok, err)
	}
}

func TestRawPlogRPCsSynchronizeRegistryAccess(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := s.MakePlog(ctx, &pb.MakePlogRequest{DiskId: 1})
			errs <- err
		}()
		go func() {
			defer wg.Done()
			_, err := s.CommitPlog(ctx, &pb.CommitPlogRequest{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRawVlogRPCsSynchronizeRegistryAccess(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := s.MakeVlog(ctx, &pb.MakeVlogRequest{
				ProtectionScheme: "DUPLICATE",
				DataShards:       1,
			})
			errs <- err
		}()
		go func() {
			defer wg.Done()
			_, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
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
	if _, err := s.MakePlog(ctx, &pb.MakePlogRequest{DiskId: 2}); err == nil {
		t.Fatal("MakePlog placed a new shard on a draining disk")
	}
	before, err := s.db.ListPlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	originalRoot := s.diskRoots[1]
	blockedRoot := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedRoot, []byte("block mkdir"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.diskRoots[1] = blockedRoot
	if _, err := s.MakePlog(ctx, &pb.MakePlogRequest{DiskId: 1}); err == nil {
		t.Fatal("MakePlog with an unusable disk root succeeded")
	}
	s.diskRoots[1] = originalRoot
	after, err := s.db.ListPlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("failed MakePlog left %d catalog rows, had %d", len(after), len(before))
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

func TestVlogProvisioningRollsBackPartialFailure(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	blockedRoot := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedRoot, []byte("block mkdir"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.diskRoots[2] = blockedRoot

	if _, err := s.MakeVlog(ctx, &pb.MakeVlogRequest{
		ProtectionScheme: "DUPLICATE",
		DataShards:       1,
	}); err == nil {
		t.Fatal("MakeVlog with a failed shard creation succeeded")
	}
	vlogs, err := s.db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plogs, err := s.db.ListPlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(vlogs) != 0 || len(plogs) != 0 {
		t.Fatalf("failed MakeVlog left catalog state: %d vlogs, %d plogs", len(vlogs), len(plogs))
	}
	if len(s.vlogs) != 0 || len(s.plogs) != 0 {
		t.Fatalf("failed MakeVlog left memory state: %d vlogs, %d plogs", len(s.vlogs), len(s.plogs))
	}
	files, err := filepath.Glob(filepath.Join(s.diskRoots[1], "plog-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("failed MakeVlog left %d physical plog files: %v", len(files), files)
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
