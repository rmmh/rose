package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

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
