package server

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

func writeServerFileInternal(t *testing.T, s *Server, path string, data []byte) {
	t.Helper()
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
}

func readServerFileInternal(t *testing.T, s *Server, path string) []byte {
	t.Helper()
	ctx := context.Background()
	open, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetBuffer()
}

// stagingVlogID returns the single staging vlog the server provisioned for an EC
// bucket, failing if there is not exactly one.
func stagingVlogID(t *testing.T, s *Server) uint32 {
	t.Helper()
	vlogs, err := s.db.ListVlogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found uint32
	for _, v := range vlogs {
		if v.IsStaging() {
			if found != 0 {
				t.Fatalf("expected one staging vlog, found %d and %d", found, v.ID)
			}
			found = v.ID
		}
	}
	if found == 0 {
		t.Fatal("no staging vlog provisioned")
	}
	return found
}

// ecVlogs returns the non-staging EC vlogs in the catalog.
func ecVlogs(t *testing.T, s *Server) []meta.VlogInfo {
	t.Helper()
	vlogs, err := s.db.ListVlogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []meta.VlogInfo
	for _, v := range vlogs {
		if v.ProtectionScheme == "EC" && !v.IsStaging() {
			out = append(out, v)
		}
	}
	return out
}

// TestPromoteStagingMovesChunksIntoEC writes enough data into an EC bucket to
// fill several stripe rows, promotes the replicated staging vlog, and confirms
// the chunks now live in an EC vlog and read back unchanged.
func TestPromoteStagingMovesChunksIntoEC(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)() // stripe row = 64*3 = 192 bytes
	ctx := context.Background()
	s := newControlPlaneServer(t, 4) // EC 3+1 needs 4 nodes; staging needs 2
	s.SetMaintenanceInterval(0)
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 4000)
	rand.New(rand.NewSource(5)).Read(payload)
	writeServerFileInternal(t, s, "/ec/big", payload)

	staging := stagingVlogID(t, s)
	if got := ecVlogs(t, s); len(got) != 0 {
		t.Fatalf("EC vlog provisioned before promotion: %+v", got)
	}
	liveBefore, err := s.db.LiveChunksInVlog(ctx, staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(liveBefore) == 0 {
		t.Fatal("no chunks landed in staging")
	}

	promoted, err := s.PromoteStaging(ctx)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("promoted %d staging vlogs, want 1", promoted)
	}

	ec := ecVlogs(t, s)
	if len(ec) != 1 {
		t.Fatalf("got %d EC vlogs after promotion, want 1", len(ec))
	}
	if ec[0].DataShards != 3 || ec[0].ParityShards != 1 {
		t.Fatalf("EC vlog scheme = %d+%d, want 3+1", ec[0].DataShards, ec[0].ParityShards)
	}
	if ec[0].Length%storage.ECStripeWidth(3) != 0 {
		t.Fatalf("EC vlog length %d is not a whole number of stripe rows", ec[0].Length)
	}

	ecLive, err := s.db.LiveChunksInVlog(ctx, ec[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ecLive) != len(liveBefore) {
		t.Fatalf("EC vlog holds %d chunks after promotion, want %d", len(ecLive), len(liveBefore))
	}

	if got := readServerFileInternal(t, s, "/ec/big"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed across promotion")
	}

	// The promotion job is finished, leaving nothing to resume.
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("promotion left %d running jobs", len(jobs))
	}
}

// TestPromoteStagingLeavesSubRowRemainder confirms that when less than a full
// stripe row of chunks has accumulated, promotion is a no-op and the data stays
// in the replicated staging vlog.
func TestPromoteStagingLeavesSubRowRemainder(t *testing.T) {
	defer storage.SetECColumnBytesForTest(1 << 20)() // stripe row = 3 MiB, far above the write
	ctx := context.Background()
	s := newControlPlaneServer(t, 4)
	s.SetMaintenanceInterval(0)
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	small := []byte("not even close to a full stripe row")
	writeServerFileInternal(t, s, "/ec/small", small)
	staging := stagingVlogID(t, s)

	promoted, err := s.PromoteStaging(ctx)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promoted != 0 {
		t.Fatalf("promoted %d, want 0 for sub-row data", promoted)
	}
	if got := ecVlogs(t, s); len(got) != 0 {
		t.Fatalf("EC vlog provisioned for sub-row data: %+v", got)
	}
	live, err := s.db.LiveChunksInVlog(ctx, staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) == 0 {
		t.Fatal("sub-row data left staging unexpectedly")
	}
	if got := readServerFileInternal(t, s, "/ec/small"); !bytes.Equal(got, small) {
		t.Fatal("sub-row payload changed")
	}
}

// TestPromoteResumesAfterRestart simulates a crash right after a promotion job
// was created but before any chunk was reparented: a fresh server's Recover must
// finish the promotion from the running job, leaving the chunks coded in an EC
// vlog and nothing to resume.
func TestPromoteResumesAfterRestart(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)() // stripe row = 192 bytes
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	roots := map[uint32]string{}
	for i := uint32(1); i <= 4; i++ {
		roots[i] = filepath.Join(dir, "disk", string(rune('0'+i)))
	}

	s1 := NewServerWithDiskRoots(db, roots)
	if err := s1.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	s1.SetMaintenanceInterval(0)
	if err := s1.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 4000)
	rand.New(rand.NewSource(11)).Read(payload)
	writeServerFileInternal(t, s1, "/ec/big", payload)
	staging := stagingVlogID(t, s1)

	// Simulate a crash right after StartPromote: the durable job exists, but no
	// rows have been written and no chunk reparented yet.
	if _, err := db.GetOrCreatePromoteJob(ctx, staging); err != nil {
		t.Fatal(err)
	}
	s1.CloseStorage()

	// A fresh server resumes the promotion from the running job during Recover.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover should complete the promotion: %v", err)
	}
	s2.SetMaintenanceInterval(0)

	if got := ecVlogs(t, s2); len(got) != 1 {
		t.Fatalf("got %d EC vlogs after resumed promotion, want 1", len(got))
	}
	if got := readServerFileInternal(t, s2, "/ec/big"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed across resumed promotion")
	}
	jobs, err := db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("resumed promotion left %d running jobs", len(jobs))
	}
}

func TestPromotablePrefix(t *testing.T) {
	mk := func(lens ...int) []meta.ChunkLoc {
		out := make([]meta.ChunkLoc, len(lens))
		for i, n := range lens {
			out[i] = meta.ChunkLoc{LogicalLen: n}
		}
		return out
	}
	cases := []struct {
		name       string
		lens       []int
		sw         int64
		wantCount  int
		wantPadded int64
	}{
		{"below one row", []int{10, 20}, 100, 0, 0},
		{"exact single row", []int{100}, 100, 1, 100},
		{"exact multi row", []int{60, 40, 100}, 100, 3, 200},
		{"pads final row", []int{150}, 100, 1, 200},
		{"prefers zero-pad cut and drains more", []int{50, 50, 30}, 100, 2, 100},
		{"min pad over several prefixes", []int{90, 90, 90}, 100, 2, 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotCount, gotPadded := promotablePrefix(mk(c.lens...), c.sw)
			if gotCount != c.wantCount || gotPadded != c.wantPadded {
				t.Fatalf("promotablePrefix = (%d, %d), want (%d, %d)", gotCount, gotPadded, c.wantCount, c.wantPadded)
			}
		})
	}
}
