package server

// Regression for the bucket-protection violation recorded in docs/correctness-plan.md.

import (
	"bytes"
	"context"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestAuditDedupMustRespectBucketProtection(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	for _, p := range []meta.BucketPolicy{
		{Name: "weak", ProtectionScheme: "NONE", DataShards: 1},
		{Name: "strong", ProtectionScheme: "EC", DataShards: 2, ParityShards: 1},
	} {
		if err := s.SetBucketPolicy(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	data := []byte("identical content in differently protected buckets")
	auditWrite(t, s, "weak/a", data)
	auditWrite(t, s, "strong/b", data)
	p := auditPlacement(t, s, "strong/b")
	info, err := s.db.GetVlog(ctx, p.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if info.ProtectionScheme == "NONE" {
		t.Fatal("EC bucket published a reference to an unprotected NONE vlog")
	}
	weak := auditPlacement(t, s, "weak/a")
	if bytes.Equal(weak.Hash, p.Hash) {
		t.Fatal("different buckets share a content address")
	}
	auditWrite(t, s, "strong/c", data)
	if again := auditPlacement(t, s, "strong/c"); !bytes.Equal(p.Hash, again.Hash) || p.VlogID != again.VlogID || p.VaddrOffset != again.VaddrOffset {
		t.Fatal("same bucket and policy no longer deduplicate")
	}
}

func TestPolicyChangeRehomesUnchangedChunksAndPreservesSnapshot(t *testing.T) {
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	weak := meta.BucketPolicy{Name: "bucket", ProtectionScheme: "NONE", DataShards: 1}
	strong := meta.BucketPolicy{Name: "bucket", ProtectionScheme: "EC", DataShards: 2, ParityShards: 1}
	if err := s.SetBucketPolicy(ctx, weak); err != nil {
		t.Fatal(err)
	}
	data := []byte("unchanged data must inherit the newly published version's policy")
	auditWrite(t, s, "bucket/a", data)
	old := auditPlacement(t, s, "bucket/a")
	snapshot, err := s.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "before-upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetBucketPolicy(ctx, strong); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/a", OperationKey: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	current := auditPlacement(t, s, "bucket/a")
	if bytes.Equal(current.Hash, old.Hash) {
		t.Fatal("new publication reused the obsolete protection domain")
	}
	info, err := s.db.GetVlog(ctx, current.VlogID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(info.DedupDomain, strong.DedupDomain()) || !info.IsStaging() {
		t.Fatal("unchanged data was not reprotected")
	}
	sh, err := s.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshot.SnapshotId, Path: "bucket/a"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, &pb.ReadRequest{Handle: sh.Handle, Length: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Buffer, data) {
		t.Fatal("policy change altered the snapshot")
	}
}
