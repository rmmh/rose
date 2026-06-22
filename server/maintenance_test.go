package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
)

// writeVlog writes data through a mounted vlog and persists its cursor, the same
// sequence the Close path uses.
func writeVlog(t *testing.T, s *Server, vlogID uint32, data []byte) int64 {
	t.Helper()
	ctx := context.Background()
	v := s.vlogs[vlogID]
	offset, err := v.Write(ctx, 0, data)
	if err != nil {
		t.Fatalf("write vlog: %v", err)
	}
	if err := v.Commit(ctx, 0); err != nil {
		t.Fatalf("commit vlog: %v", err)
	}
	if err := s.db.SetVlogLength(ctx, vlogID, v.Length()); err != nil {
		t.Fatalf("persist vlog length: %v", err)
	}
	return offset
}

func diskOf(t *testing.T, s *Server, vlogID uint32, shard int) uint32 {
	t.Helper()
	shards, err := s.db.VlogShardDisks(context.Background(), vlogID)
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range shards {
		if sh.ShardIndex == shard {
			return sh.DiskID
		}
	}
	t.Fatalf("vlog %d has no shard %d", vlogID, shard)
	return 0
}

func TestDrainDiskRelocatesShardAndDetaches(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 4) // one spare beyond the 3 EC shards
	vlogID := provision(t, s, "EC", 2, 1)

	payload := bytes.Repeat([]byte("rose-drain-payload!"), 1000)
	offset := writeVlog(t, s, vlogID, payload)
	want, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatal(err)
	}

	victim := diskOf(t, s, vlogID, 0) // a disk that actually holds a shard
	if err := s.DrainDisk(ctx, victim); err != nil {
		t.Fatalf("drain disk %d: %v", victim, err)
	}

	// The disk is empty and detached, satisfying NoDetachedData.
	if got := s.DiskStates()[victim]; got != meta.DiskDetached {
		t.Fatalf("drained disk %d state = %q, want detached", victim, got)
	}
	left, err := s.db.PlogsOnDisk(ctx, victim)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("drained disk %d still holds %d plogs", victim, len(left))
	}

	// The vlog still has all three shards, none on the drained disk.
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 3 {
		t.Fatalf("vlog has %d shards after drain, want 3", len(shards))
	}
	for _, sh := range shards {
		if sh.DiskID == victim {
			t.Fatalf("shard %d still on drained disk %d", sh.ShardIndex, victim)
		}
	}

	// Data survives the relocation unchanged, read from the remounted vlog.
	got, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after drain: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("payload changed across drain")
	}

	// The durable job is finished, leaving nothing to resume.
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("drain left %d running jobs", len(jobs))
	}
}

func TestDrainWithoutPlacementRoomFails(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3) // EC 2+1 occupies all three disks
	vlogID := provision(t, s, "EC", 2, 1)
	writeVlog(t, s, vlogID, []byte("no room to move"))

	victim := diskOf(t, s, vlogID, 0)
	err := s.DrainDisk(ctx, victim)
	if err == nil {
		t.Fatal("drain succeeded with no placement-allowed destination; want failure")
	}
	// The disk is left draining (out of placement) with its job still running,
	// modeling a drain that is stuck until more capacity is added.
	if got := s.DiskStates()[victim]; got != meta.DiskDraining {
		t.Fatalf("stuck-drain disk state = %q, want draining", got)
	}
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Kind != meta.JobDrain || jobs[0].TargetDisk != victim {
		t.Fatalf("expected one running drain job for disk %d, got %+v", victim, jobs)
	}
}

func TestReprotectECReconstructsLostShard(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 4) // one spare beyond the 3 EC shards
	vlogID := provision(t, s, "EC", 2, 1)

	payload := bytes.Repeat([]byte("reprotect-ec-payload!"), 1000)
	offset := writeVlog(t, s, vlogID, payload)
	want, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatal(err)
	}

	victim := diskOf(t, s, vlogID, 0)
	if err := s.SetDiskState(ctx, victim, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}

	if err := s.ReprotectDisk(ctx, victim); err != nil {
		t.Fatalf("reprotect disk %d: %v", victim, err)
	}

	// The vlog has three shards again, none of them on the failed disk.
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 3 {
		t.Fatalf("vlog has %d shards after reprotect, want 3", len(shards))
	}
	for _, sh := range shards {
		if sh.DiskID == victim {
			t.Fatalf("shard %d still on failed disk %d after reprotect", sh.ShardIndex, victim)
		}
	}

	// The reprotected vlog is durably committable again (all shards live).
	ready, err := s.CommitReady(ctx, vlogID)
	if err != nil || !ready {
		t.Fatalf("CommitReady after reprotect = %v, %v; want true", ready, err)
	}

	// Data reads back unchanged from the regenerated shard set.
	got, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after reprotect: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("payload changed across reprotect")
	}

	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("reprotect left %d running jobs", len(jobs))
	}
}

func TestReprotectDuplicateRegeneratesCopy(t *testing.T) {
	ctx := context.Background()
	// Three disks, a DUPLICATE vlog with one copy per disk, plus a spare.
	s := newControlPlaneServer(t, 4)
	if err := s.SetDiskState(ctx, 4, meta.DiskDraining); err != nil {
		t.Fatal(err) // keep disk 4 out of the initial 3-copy placement
	}
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	if err := s.SetDiskState(ctx, 4, meta.DiskActive); err != nil {
		t.Fatal(err) // re-admit it as the reprotect destination
	}

	payload := bytes.Repeat([]byte("dup-copy"), 700)
	offset := writeVlog(t, s, vlogID, payload)

	victim := diskOf(t, s, vlogID, 0)
	if err := s.SetDiskState(ctx, victim, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.ReprotectDisk(ctx, victim); err != nil {
		t.Fatalf("reprotect disk %d: %v", victim, err)
	}

	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 3 {
		t.Fatalf("vlog has %d copies after reprotect, want 3", len(shards))
	}
	for _, sh := range shards {
		if sh.DiskID == victim {
			t.Fatalf("copy still on failed disk %d after reprotect", victim)
		}
	}
	got, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after reprotect: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload changed across reprotect")
	}
}

func TestReprotectResumesAfterRestart(t *testing.T) {
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
	vlogID := provision(t, s1, "EC", 2, 1)
	payload := bytes.Repeat([]byte("resume-reprotect"), 400)
	offset := writeVlog(t, s1, vlogID, payload)
	victim := diskOf(t, s1, vlogID, 0)

	// Simulate a crash right after StartReprotect: the disk is failed and a
	// reprotect job exists, but no shard has been regenerated yet.
	if _, err := db.GetOrCreateReprotectJob(ctx, victim); err != nil {
		t.Fatal(err)
	}
	if err := db.SetDiskState(ctx, victim, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	// This is a real cold-start loss, not merely a failed-state marker over an
	// still-present local file. Recover must mount an offline shard and let the
	// resumed reprotect reconstruct it from the surviving EC shards.
	for _, p := range mustPlogsOnDisk(t, db, victim) {
		if err := os.Remove(s1.plogPath(victim, p.PlogID)); err != nil {
			t.Fatal(err)
		}
	}

	// A fresh server resumes the reprotect from the running job during Recover.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover should complete the reprotect: %v", err)
	}
	shards, err := s2.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range shards {
		if sh.DiskID == victim {
			t.Fatalf("shard %d still on failed disk %d after resumed reprotect", sh.ShardIndex, victim)
		}
	}
	got, err := s2.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after resumed reprotect: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload changed across resumed reprotect")
	}
	jobs, err := db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("resumed reprotect left %d running jobs", len(jobs))
	}
}

func mustPlogsOnDisk(t *testing.T, db *meta.DB, diskID uint32) []meta.PlogOnDisk {
	t.Helper()
	plogs, err := db.PlogsOnDisk(context.Background(), diskID)
	if err != nil {
		t.Fatal(err)
	}
	return plogs
}

func TestReplaceDiskMovesShardsOntoNewDisk(t *testing.T) {
	ctx := context.Background()
	// Three disks fully occupied by an EC 2+1 vlog; the fourth is the replacement.
	s := newControlPlaneServer(t, 4)
	if err := s.SetDiskState(ctx, 4, meta.DiskDraining); err != nil {
		t.Fatal(err) // hold disk 4 out of the initial placement
	}
	vlogID := provision(t, s, "EC", 2, 1)
	if err := s.SetDiskState(ctx, 4, meta.DiskActive); err != nil {
		t.Fatal(err) // bring it back as the replacement target
	}

	payload := bytes.Repeat([]byte("replace-onto-new!"), 1000)
	offset := writeVlog(t, s, vlogID, payload)
	want, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatal(err)
	}

	old := diskOf(t, s, vlogID, 0)
	shardIdx := -1
	for _, sh := range mustShards(t, s, vlogID) {
		if sh.DiskID == old {
			shardIdx = sh.ShardIndex
		}
	}

	if err := s.ReplaceDiskWith(ctx, old, 4); err != nil {
		t.Fatalf("replace disk %d with 4: %v", old, err)
	}

	// The old disk is detached and empty; its shard now lives on disk 4.
	if got := s.DiskStates()[old]; got != meta.DiskDetached {
		t.Fatalf("replaced disk %d state = %q, want detached", old, got)
	}
	if dest := diskOf(t, s, vlogID, shardIdx); dest != 4 {
		t.Fatalf("shard %d landed on disk %d, want the replacement disk 4", shardIdx, dest)
	}
	left, err := s.db.PlogsOnDisk(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("replaced disk %d still holds %d plogs", old, len(left))
	}

	got, err := s.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after replace: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("payload changed across replace")
	}

	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("replace left %d running jobs", len(jobs))
	}
}

func TestReplaceResumesAfterRestart(t *testing.T) {
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
	// Disk 4 is the replacement: keep it out of the initial EC placement.
	if err := s1.SetDiskState(ctx, 4, meta.DiskDraining); err != nil {
		t.Fatal(err)
	}
	vlogID := provision(t, s1, "EC", 2, 1)
	if err := s1.SetDiskState(ctx, 4, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("resume-replace"), 400)
	offset := writeVlog(t, s1, vlogID, payload)
	old := diskOf(t, s1, vlogID, 0)

	// Simulate a crash right after the replace started: a pinned-destination job
	// exists and the old disk is draining, but no shard has moved yet.
	if _, err := db.GetOrCreateReplaceJob(ctx, old, 4); err != nil {
		t.Fatal(err)
	}
	if err := db.SetDiskState(ctx, old, meta.DiskDraining); err != nil {
		t.Fatal(err)
	}

	// A fresh server resumes the replace onto the same pinned destination.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover should complete the replace: %v", err)
	}
	if got := s2.DiskStates()[old]; got != meta.DiskDetached {
		t.Fatalf("resumed disk %d state = %q, want detached", old, got)
	}
	for _, sh := range mustShards(t, s2, vlogID) {
		if sh.DiskID == old {
			t.Fatalf("shard %d still on replaced disk %d after resume", sh.ShardIndex, old)
		}
	}
	got, err := s2.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after resumed replace: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload changed across resumed replace")
	}
}

func TestAttachDiskBringsCapacityOnline(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 2)

	root := t.TempDir()
	if err := s.AttachDisk(ctx, 3, root); err != nil {
		t.Fatalf("attach disk: %v", err)
	}
	if got := s.DiskStates()[3]; got != meta.DiskActive {
		t.Fatalf("attached disk 3 state = %q, want active", got)
	}
	// Re-attaching a configured disk is rejected.
	if err := s.AttachDisk(ctx, 3, root); err == nil {
		t.Fatal("re-attaching disk 3 succeeded, want error")
	}
	// New capacity is eligible for placement: a 3-copy DUPLICATE vlog uses it.
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	onNew := false
	for _, sh := range shards {
		if sh.DiskID == 3 {
			onNew = true
		}
	}
	if !onNew {
		t.Fatalf("attached disk 3 not used for placement: %+v", shards)
	}
}

// diskUsage reports the physical bytes stored on each given disk, the unit
// rebalance equalizes.
func diskUsage(t *testing.T, s *Server, disks ...uint32) map[uint32]int64 {
	t.Helper()
	out := make(map[uint32]int64, len(disks))
	for _, d := range disks {
		ps, err := s.db.PlogsOnDisk(context.Background(), d)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range ps {
			fi, err := os.Stat(s.plogPath(d, p.PlogID))
			if err != nil {
				t.Fatal(err)
			}
			out[d] += fi.Size()
		}
	}
	return out
}

func usageSpread(usage map[uint32]int64) int64 {
	min, max := int64(1)<<62, int64(-1)
	for _, u := range usage {
		if u < min {
			min = u
		}
		if u > max {
			max = u
		}
	}
	return max - min
}

// provisionSizedNone provisions a NONE vlog and writes size bytes to it. A NONE
// vlog is a single shard that placement always lands on the lowest active disk,
// so repeated calls pile differently-sized shards onto disk 1.
func provisionSizedNone(t *testing.T, s *Server, size int) uint32 {
	t.Helper()
	id := provision(t, s, "NONE", 1, 0)
	writeVlog(t, s, id, bytes.Repeat([]byte("x"), size))
	return id
}

func TestRebalanceEvensDiskBytes(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 2)
	band := int64(2 << 20)
	s.SetRebalancePolicy(RebalancePolicy{MinSkewBytes: band, MaxMovesPerPass: 100})

	// One dominant shard plus four small ones, all on disk 1. Balancing by bytes
	// must relocate the big shard; balancing by count would instead shuffle the
	// small ones and leave the disks lopsided.
	provisionSizedNone(t, s, 8<<20)
	for i := 0; i < 4; i++ {
		provisionSizedNone(t, s, 2<<20)
	}
	before := diskUsage(t, s, 1, 2)
	if before[2] != 0 {
		t.Fatalf("setup put %d bytes on disk 2, want 0", before[2])
	}

	moved, err := s.Rebalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved == 0 {
		t.Fatal("rebalance moved nothing on a fully lopsided cluster")
	}
	after := diskUsage(t, s, 1, 2)
	if got := usageSpread(after); got > band {
		t.Fatalf("after rebalance byte spread = %d (%v), want <= band %d", got, after, band)
	}
	// The dominant ~8 MiB shard is what moved, evening the bytes in few moves.
	if after[2] < 7<<20 {
		t.Fatalf("disk 2 received only %d bytes; byte rebalance should have moved the dominant shard", after[2])
	}

	// A second pass on the now-even cluster is a no-op (within the band).
	moved, err = s.Rebalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("second rebalance moved %d shards on a balanced cluster, want 0", moved)
	}
}

func TestRebalanceHysteresisToleratesMinorImbalance(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 2)
	// A band far larger than the data: the whole imbalance is within it.
	s.SetRebalancePolicy(RebalancePolicy{MinSkewBytes: 1 << 30, MaxMovesPerPass: 100})

	for i := 0; i < 3; i++ {
		provisionSizedNone(t, s, 2<<20)
	}
	moved, err := s.Rebalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("rebalance moved %d shards within the hysteresis band, want 0", moved)
	}
}

func TestRebalanceCooldownBacksOff(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 2)
	// Move one shard per pass, then refuse to start another pass for an hour.
	s.SetRebalancePolicy(RebalancePolicy{MinSkewBytes: 1 << 20, MaxMovesPerPass: 1, Cooldown: time.Hour})

	// Four equal shards: a single capped move (12->9 / 0->3 MiB) leaves the disks
	// still imbalanced beyond the band.
	for i := 0; i < 4; i++ {
		provisionSizedNone(t, s, 3<<20)
	}
	moved, err := s.Rebalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Fatalf("first pass moved %d shards, want 1 (per-pass cap)", moved)
	}

	if got := usageSpread(diskUsage(t, s, 1, 2)); got <= 1<<20 {
		t.Fatalf("cluster already balanced (spread %d) after one capped move; cannot test cooldown", got)
	}
	moved, err = s.Rebalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("rebalance moved %d shards inside the cooldown window, want 0", moved)
	}
}

func TestSweepStrayPlogFilesRemovesUnreferencedFiles(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	vlogID := provision(t, s, "DUPLICATE", 1, 0)
	writeVlog(t, s, vlogID, []byte("keep these bytes referenced"))

	// A real, catalog-referenced plog file the sweep must never touch.
	live := mustPlogsOnDisk(t, s.db, diskOf(t, s, vlogID, 0))
	if len(live) == 0 {
		t.Fatal("provisioned vlog left no plog on its first shard's disk")
	}
	livePath := s.plogPath(diskOf(t, s, vlogID, 0), live[0].PlogID)

	// A leaked source file: a crash after a relocation's catalog flip but before
	// the os.Remove leaves a plog file no catalog row references on that disk.
	strayPath := s.plogPath(2, 9999)
	if err := os.MkdirAll(filepath.Dir(strayPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strayPath, []byte("orphaned copy"), 0644); err != nil {
		t.Fatal(err)
	}
	// A non-plog file in a disk root must be ignored entirely.
	otherPath := filepath.Join(filepath.Dir(strayPath), "notaplog")
	if err := os.WriteFile(otherPath, []byte("leave me"), 0644); err != nil {
		t.Fatal(err)
	}

	removed, err := s.SweepStrayPlogFiles(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("sweep removed %d files, want 1", removed)
	}
	if _, err := os.Stat(strayPath); !os.IsNotExist(err) {
		t.Fatalf("stray plog file survived the sweep (err=%v)", err)
	}
	if _, err := os.Stat(livePath); err != nil {
		t.Fatalf("sweep deleted a catalog-referenced plog file: %v", err)
	}
	if _, err := os.Stat(otherPath); err != nil {
		t.Fatalf("sweep deleted a non-plog file: %v", err)
	}

	// Idempotent: a second pass finds nothing.
	if removed, err := s.SweepStrayPlogFiles(ctx); err != nil || removed != 0 {
		t.Fatalf("second sweep removed %d (err=%v), want 0", removed, err)
	}
}

func TestSweepStrayPlogFilesSkipsFailedDisk(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)

	// A stray file on a disk whose media is failed must be left untouched: the
	// sweep must not reach for unreachable media.
	strayPath := s.plogPath(2, 8888)
	if err := os.MkdirAll(filepath.Dir(strayPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strayPath, []byte("orphan on failed disk"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 2, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}

	removed, err := s.SweepStrayPlogFiles(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != 0 {
		t.Fatalf("sweep removed %d files on a failed disk, want 0", removed)
	}
	if _, err := os.Stat(strayPath); err != nil {
		t.Fatalf("sweep touched a failed disk's file: %v", err)
	}
}

func mustShards(t *testing.T, s *Server, vlogID uint32) []meta.VlogShardDisk {
	t.Helper()
	shards, err := s.db.VlogShardDisks(context.Background(), vlogID)
	if err != nil {
		t.Fatal(err)
	}
	return shards
}

func TestDrainResumesAfterRestart(t *testing.T) {
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
	vlogID := provision(t, s1, "EC", 2, 1)
	payload := bytes.Repeat([]byte("resume-me"), 500)
	offset := writeVlog(t, s1, vlogID, payload)
	victim := diskOf(t, s1, vlogID, 0)

	// Simulate a crash right after StartRemove: the disk is draining and a drain
	// job exists, but no shard has been migrated yet.
	if _, err := db.GetOrCreateDrainJob(ctx, victim); err != nil {
		t.Fatal(err)
	}
	if err := db.SetDiskState(ctx, victim, meta.DiskDraining); err != nil {
		t.Fatal(err)
	}

	// A fresh server resumes the drain from the running job during Recover.
	s2 := NewServerWithDiskRoots(db, roots)
	if err := s2.Recover(ctx); err != nil {
		t.Fatalf("recover should complete the drain: %v", err)
	}
	if got := s2.DiskStates()[victim]; got != meta.DiskDetached {
		t.Fatalf("resumed disk %d state = %q, want detached", victim, got)
	}
	got, err := s2.vlogs[vlogID].Read(ctx, offset, len(payload))
	if err != nil {
		t.Fatalf("read after resumed drain: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload changed across resumed drain")
	}
}
