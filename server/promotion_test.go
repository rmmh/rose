package server

import (
	"bytes"
	"context"
	"fmt"
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
	if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: open.GetHandle(), Size: 0}); err != nil {
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

// TestRecoverTruncatesOrphanPlogTail reproduces the sharp resume window where a
// crash lands after writeChunksAsRows seals new stripe rows to the EC dest's plog
// files (dest.Commit reaches disk) but before SetVlogLength records the new length.
// The metadata DB still holds the old vlog length while each backing plog's file
// has grown, so on restart the plog reloads an inflated length (the new write
// overwrote the previous open-block trailer, so reload falls back to trusting the
// file bytes). Left unreconciled, the plog cursor sits past where the vlog expects
// the next append, so a subsequent EC write is misplaced relative to where reads
// look. Recover must reconcile each shard down to the committed vlog length.
func TestRecoverTruncatesOrphanPlogTail(t *testing.T) {
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

	// Promote a payload into an EC vlog so it holds several committed stripe rows.
	payload := make([]byte, 4000)
	rand.New(rand.NewSource(13)).Read(payload)
	writeServerFileInternal(t, s1, "/ec/big", payload)
	if _, err := s1.PromoteStaging(ctx); err != nil {
		t.Fatalf("promote: %v", err)
	}
	ec := ecVlogs(t, s1)
	if len(ec) != 1 {
		t.Fatalf("got %d EC vlogs after promotion, want 1", len(ec))
	}
	ecID := ec[0].ID
	committed := ec[0].Length

	// Simulate the crash window: append one more stripe row to the dest and make it
	// durable on the plog files, but do NOT call SetVlogLength. The plog files now
	// extend past the length the DB records for the vlog.
	dest := s1.vlogs[ecID]
	sw := storage.ECStripeWidth(3)
	orphanRow := make([]byte, sw)
	rand.New(rand.NewSource(101)).Read(orphanRow)
	if _, err := dest.Write(ctx, 999, orphanRow); err != nil {
		t.Fatalf("write orphan row: %v", err)
	}
	if err := dest.Commit(ctx, 999); err != nil {
		t.Fatalf("commit orphan row: %v", err)
	}
	s1.CloseStorage()

	// Recover on a fresh server: each plog reloads its inflated file length while
	// the DB still reports the committed vlog length.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	s2.SetMaintenanceInterval(0)

	dest2, ok := s2.vlogs[ecID]
	if !ok {
		t.Fatalf("EC vlog %d not mounted after recovery", ecID)
	}
	if dest2.Length() != committed {
		t.Fatalf("recovered vlog length = %d, want %d", dest2.Length(), committed)
	}

	// Every backing plog's committed length must agree with the vlog: committed
	// logical bytes spread over dataShards columns.
	perShard := committed / int64(ec[0].DataShards)
	mappings, err := db.ListVlogPlogs(ctx, ecID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range mappings {
		p, ok := s2.plogs[m.PlogID]
		if !ok {
			t.Fatalf("plog %d not open after recovery", m.PlogID)
		}
		if got := p.LogicalLength(); got != perShard {
			t.Fatalf("shard %d plog length = %d, want %d (orphan tail not truncated)", m.ShardIndex, got, perShard)
		}
	}

	// Existing data still reads back correctly.
	if got := readServerFileInternal(t, s2, "/ec/big"); !bytes.Equal(got, payload) {
		t.Fatal("payload changed across recovery")
	}

	// A subsequent write must land where reads look. Use different content than the
	// orphan row so an append placed at the stale (inflated) cursor would read back
	// the wrong bytes.
	newRow := make([]byte, sw)
	rand.New(rand.NewSource(202)).Read(newRow)
	base, err := dest2.Write(ctx, 1000, newRow)
	if err != nil {
		t.Fatalf("subsequent write: %v", err)
	}
	if base != committed {
		t.Fatalf("subsequent write offset = %d, want %d", base, committed)
	}
	if err := dest2.Commit(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	got, err := dest2.Read(ctx, base, int(sw))
	if err != nil {
		t.Fatalf("read subsequent write: %v", err)
	}
	if !bytes.Equal(got, newRow) {
		t.Fatal("subsequent write misplaced: read did not return the bytes just written")
	}
}

// unlinkServerFile deletes a path, dropping its chunks' refcounts.
func unlinkServerFile(t *testing.T, s *Server, path string) {
	t.Helper()
	if _, err := s.Unlink(context.Background(), &pb.UnlinkRequest{Path: path}); err != nil {
		t.Fatal(err)
	}
}

// TestPromoteRetiresFullyDrainedStaging writes chunks that pack into whole stripe
// rows with nothing left over, so promotion drains every live chunk out of the
// staging vlog. The now-empty replica must be retired rather than left lingering.
func TestPromoteRetiresFullyDrainedStaging(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)() // stripe row = 64*3 = 192 bytes
	ctx := context.Background()
	s := newControlPlaneServer(t, 4)
	s.SetMaintenanceInterval(0)
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	// Six 200-byte chunks: the whole-row prefix that minimizes padding consumes
	// all of them, leaving the staging vlog empty after promotion.
	for i := 0; i < 6; i++ {
		payload := make([]byte, 200)
		rand.New(rand.NewSource(int64(20 + i))).Read(payload)
		writeServerFileInternal(t, s, fmt.Sprintf("/ec/f%d", i), payload)
	}
	staging := stagingVlogID(t, s)

	if _, err := s.PromoteStaging(ctx); err != nil {
		t.Fatalf("promote: %v", err)
	}

	// The drained staging vlog is gone from the catalog and the in-memory mount.
	vlogs, err := s.db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vlogs {
		if v.ID == staging {
			t.Fatalf("drained staging vlog %d still in catalog", staging)
		}
	}
	if _, ok := s.vlogs[staging]; ok {
		t.Fatalf("drained staging vlog %d still mounted", staging)
	}
	if got := ecVlogs(t, s); len(got) != 1 {
		t.Fatalf("got %d EC vlogs after promotion, want 1", len(got))
	}
	for i := 0; i < 6; i++ {
		path := fmt.Sprintf("/ec/f%d", i)
		want := make([]byte, 200)
		rand.New(rand.NewSource(int64(20 + i))).Read(want)
		if got := readServerFileInternal(t, s, path); !bytes.Equal(got, want) {
			t.Fatalf("%s changed across promotion", path)
		}
	}
}

// TestPromoteRetiresEmptyStagingAfterDelete confirms a staging vlog holding only
// a sub-row remainder is retired once that remainder is deleted: the data never
// completed a row, so it stayed replicated, and when it goes away the empty
// replica must not linger.
func TestPromoteRetiresEmptyStagingAfterDelete(t *testing.T) {
	defer storage.SetECColumnBytesForTest(1 << 20)() // stripe row = 3 MiB, far above the write
	ctx := context.Background()
	s := newControlPlaneServer(t, 4)
	s.SetMaintenanceInterval(0)
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	writeServerFileInternal(t, s, "/ec/small", []byte("well under a stripe row"))
	staging := stagingVlogID(t, s)

	// Sub-row data: promotion is a no-op and leaves it replicated in staging.
	if promoted, err := s.PromoteStaging(ctx); err != nil || promoted != 0 {
		t.Fatalf("promote sub-row = (%d, %v), want (0, nil)", promoted, err)
	}
	if _, ok := s.vlogs[staging]; !ok {
		t.Fatalf("staging vlog %d retired while it still held live data", staging)
	}

	// Deleting the only file leaves the staging vlog all dead; the next pass
	// retires it instead of waiting for the dead-byte floor.
	unlinkServerFile(t, s, "/ec/small")
	if _, err := s.PromoteStaging(ctx); err != nil {
		t.Fatalf("promote after delete: %v", err)
	}
	if _, ok := s.vlogs[staging]; ok {
		t.Fatalf("empty staging vlog %d not retired", staging)
	}
	vlogs, err := s.db.ListVlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(vlogs) != 0 {
		t.Fatalf("expected no vlogs after draining and deleting, got %+v", vlogs)
	}
}

// TestCompactECVlogRepacksRowsAndReclaimsSpace promotes chunks into an EC vlog,
// deletes most of them to leave dead holes between stripe rows, and confirms EC
// compaction repacks the survivors into a fresh EC vlog (whole rows only) and
// retires the wasteful one.
func TestCompactECVlogRepacksRowsAndReclaimsSpace(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)() // stripe row = 192 bytes
	ctx := context.Background()
	s := newControlPlaneServer(t, 4)
	s.SetMaintenanceInterval(0)
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}

	keep := make([]byte, 200)
	rand.New(rand.NewSource(1)).Read(keep)
	writeServerFileInternal(t, s, "/ec/keep", keep)
	for i := 0; i < 5; i++ {
		dead := make([]byte, 200)
		rand.New(rand.NewSource(int64(100 + i))).Read(dead)
		writeServerFileInternal(t, s, fmt.Sprintf("/ec/dead%d", i), dead)
	}

	if _, err := s.PromoteStaging(ctx); err != nil {
		t.Fatalf("promote: %v", err)
	}
	ec := ecVlogs(t, s)
	if len(ec) != 1 {
		t.Fatalf("got %d EC vlogs after promotion, want 1", len(ec))
	}
	ecID := ec[0].ID

	for i := 0; i < 5; i++ {
		unlinkServerFile(t, s, fmt.Sprintf("/ec/dead%d", i))
	}
	liveBefore, err := s.db.LiveChunksInVlog(ctx, ecID)
	if err != nil {
		t.Fatal(err)
	}
	if len(liveBefore) != 1 {
		t.Fatalf("EC vlog holds %d live chunks before compaction, want 1", len(liveBefore))
	}

	if err := s.CompactVlog(ctx, ecID); err != nil {
		t.Fatalf("compact EC vlog: %v", err)
	}

	// The wasteful EC vlog is retired and a fresh EC vlog holds the survivor.
	if _, ok := s.vlogs[ecID]; ok {
		t.Fatalf("compacted EC vlog %d still mounted", ecID)
	}
	after := ecVlogs(t, s)
	if len(after) != 1 || after[0].ID == ecID {
		t.Fatalf("want one fresh EC vlog after compaction, got %+v", after)
	}
	if after[0].Length%storage.ECStripeWidth(3) != 0 {
		t.Fatalf("compacted EC vlog length %d is not whole stripe rows", after[0].Length)
	}
	live, err := s.db.LiveChunksInVlog(ctx, after[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 {
		t.Fatalf("fresh EC vlog holds %d live chunks, want 1", len(live))
	}
	if got := readServerFileInternal(t, s, "/ec/keep"); !bytes.Equal(got, keep) {
		t.Fatal("kept file corrupted by EC compaction")
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
