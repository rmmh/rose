package server

import (
	"bytes"
	"context"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

func TestAuditDedupMustNotPublishUnreadableData(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	data := []byte("only surviving copy will be taken offline")
	auditWrite(t, s, "bucket/a", data)
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: data}); err != nil {
		return
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err == nil {
		t.Fatal("Close acknowledged a new file whose sole chunk is on a failed disk")
	}
}

func TestAuditFreshWriteMustReplaceDeadChunkPlacement(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "bucket", ProtectionScheme: "NONE", DataShards: 1}); err != nil {
		t.Fatal(err)
	}
	data := []byte("rewrite these bytes after their old placement is lost")
	auditWrite(t, s, "bucket/a", data)
	old := auditPlacement(t, s, "bucket/a")
	shards, err := s.db.VlogShardDisks(ctx, old.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: "bucket/a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	// The zero-reference row still exists, but planChunk correctly declines
	// dedup and writes a fresh record to the healthy spare.
	auditWrite(t, s, "bucket/b", data)
	got := auditPlacement(t, s, "bucket/b")
	if got.VlogID == old.VlogID {
		t.Fatal("publication discarded the fresh placement and resurrected the dead chunk on a failed disk")
	}
}

func TestFreshPlacementKeepsPinnedUnlinkedReaderReadable(t *testing.T) {
	s := newControlPlaneServer(t, 2)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "bucket", ProtectionScheme: "NONE", DataShards: 1}); err != nil {
		t.Fatal(err)
	}
	data := []byte("old reader must follow verified replacement content")
	auditWrite(t, s, "bucket/a", data)
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/a"})
	if err != nil {
		t.Fatal(err)
	}
	old := auditPlacement(t, s, "bucket/a")
	shards, err := s.db.VlogShardDisks(ctx, old.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Unlink(ctx, &pb.UnlinkRequest{Path: "bucket/a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	auditWrite(t, s, "bucket/b", data)
	r, err := s.Read(ctx, &pb.ReadRequest{Handle: h.Handle, Length: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Buffer, data) {
		t.Fatal("old reader did not follow the replacement placement")
	}
}

type commitBarrierClient struct {
	storage.PlogClient
	id     uint32
	synced chan<- uint32
	resume <-chan struct{}
}

func (c commitBarrierClient) Commit(ctx context.Context, txn int64) error {
	if err := c.PlogClient.(interface {
		Commit(context.Context, int64) error
	}).Commit(ctx, txn); err != nil {
		return err
	}
	c.synced <- c.id
	<-c.resume
	return nil
}

func TestCommitVlogPublishesOnlyCapturedDurablePrefixes(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	synced := make(chan uint32, 2)
	resume := make(chan struct{}, 2)
	vlogs := make(map[uint32]*storage.Vlog)
	for range 2 {
		id := provision(t, s, "NONE", 1, 0)
		mappings, err := s.db.ListVlogPlogs(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		client := commitBarrierClient{PlogClient: &localPlogClient{plog: s.plogs[mappings[0].PlogID]}, id: id, synced: synced, resume: resume}
		v, err := storage.NewVlog(id, "NONE", 1, 0, []storage.PlogClient{client}, 0)
		if err != nil {
			t.Fatal(err)
		}
		s.vlogs[id], vlogs[id] = v, v
		if _, err := v.Write(ctx, 1, []byte("durable")); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { _, err := s.CommitVlog(ctx, &pb.CommitVlogRequest{}); done <- err }()
	first := <-synced
	resume <- struct{}{}
	<-synced // First sync returned; second vlog's sync has not returned.
	// This is the storage half of a raw append admitted before CommitVlog
	// acquired vlogMu. It can run after the first vlog releases writeMu.
	if _, err := vlogs[first].Write(ctx, 2, []byte("volatile")); err != nil {
		t.Fatal(err)
	}
	resume <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	info, err := s.db.GetVlog(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if info.Length != int64(len("durable")) {
		t.Fatalf("published prefix %d includes an unsynced append", info.Length)
	}
	if vlogs[first].Length() != int64(len("durablevolatile")) {
		t.Fatal("test did not append after sync")
	}
}

func TestDedupPublicationRequiresEveryProvisionedMirror(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	data := []byte("all three mirrors are promised")
	auditWrite(t, s, "bucket/base", data)
	p := auditPlacement(t, s, "bucket/base")
	shards, err := s.db.VlogShardDisks(ctx, p.VlogID)
	if err != nil || len(shards) != 3 {
		t.Fatalf("shards=%v err=%v", shards, err)
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/copy"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err == nil {
		t.Fatal("published with only two of three promised mirrors")
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
}

func TestKnownDegradationBlocksUnrelatedPublicationUntilRepair(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	auditWrite(t, s, "old/base", []byte("referenced promise"))
	p := auditPlacement(t, s, "old/base")
	info, err := s.db.GetVlog(ctx, p.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if info.RequiredShards != 3 {
		t.Fatalf("persisted shard requirement=%d", info.RequiredShards)
	}
	shards, err := s.db.VlogShardDisks(ctx, p.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "other", ProtectionScheme: "NONE", DataShards: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "other/new", OperationKey: "global-gate"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: []byte("independent healthy disk")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err == nil {
		t.Fatal("unrelated publication ignored known global degradation")
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	// A metadata mapping loss must not redefine the desired protection count.
	if _, err := s.db.GetDB().ExecContext(ctx, "DELETE FROM vlog_plog WHERE vlog_id=? AND plog_id=?", p.VlogID, shards[0].PlogID); err != nil {
		t.Fatal(err)
	}
	info, err = s.db.GetVlog(ctx, p.VlogID)
	if err != nil || info.RequiredShards != 3 {
		t.Fatalf("requirement after mapping loss=%d err=%v", info.RequiredShards, err)
	}
	h, err = s.Open(ctx, &pb.OpenRequest{Path: "other/empty", OperationKey: "missing-mapping"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err == nil {
		t.Fatal("missing mapping lowered the protection promise")
	}
}

func TestCompactionCannotLowerPersistedMirrorRequirement(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	auditWrite(t, s, "bucket/file", []byte("preserve three copies"))
	p := auditPlacement(t, s, "bucket/file")
	shards, err := s.db.VlogShardDisks(ctx, p.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactVlog(ctx, p.VlogID); err == nil {
		t.Fatal("compaction lowered three-copy requirement to two active disks")
	}
	after := auditPlacement(t, s, "bucket/file")
	if after.VlogID != p.VlogID {
		t.Fatal("failed compaction changed canonical placement")
	}
	if err := s.SetDiskState(ctx, shards[0].DiskID, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactVlog(ctx, p.VlogID); err != nil {
		t.Fatal(err)
	}
	after = auditPlacement(t, s, "bucket/file")
	info, err := s.db.GetVlog(ctx, after.VlogID)
	if err != nil || info.RequiredShards != 3 {
		t.Fatalf("destination requirement=%d err=%v", info.RequiredShards, err)
	}
}

func TestCompactionPreservesStagingPromotionTarget(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 2, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}
	auditWrite(t, s, "ec/file", []byte("still awaiting promotion"))
	p := auditPlacement(t, s, "ec/file")
	if err := s.CompactVlog(ctx, p.VlogID); err != nil {
		t.Fatal(err)
	}
	p = auditPlacement(t, s, "ec/file")
	info, err := s.db.GetVlog(ctx, p.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if info.TargetDataShards != 2 || info.TargetParityShards != 1 || info.RequiredShards != 2 {
		t.Fatalf("compaction lost staging geometry: target=%d+%d required=%d", info.TargetDataShards, info.TargetParityShards, info.RequiredShards)
	}
}
