package server

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestAbortedFileTailDoesNotBlockReprotection(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	if err := s.SetDiskState(ctx, 3, meta.DiskDraining); err != nil {
		t.Fatal(err)
	}
	want := []byte("previously published bytes")
	auditWrite(t, s, "bucket/old", want)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/abandoned"})
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 5<<20)
	_, _ = rand.New(rand.NewSource(19)).Read(data)
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.Handle, Buffer: data}); err != nil {
		t.Fatal(err)
	}
	leases, err := s.db.WriteOpLeases(ctx, s.handles[open.Handle].writeOpID)
	if err != nil || len(leases) == 0 {
		t.Fatalf("leases=%v err=%v", leases, err)
	}
	target := leases[0]
	info, err := s.db.GetVlog(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.DedupDomain) == 0 || s.vlogs[target].Length() <= info.Length {
		t.Fatal("fixture did not create an unpublished scoped tail")
	}
	shards, err := s.db.VlogShardDisks(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	victim := shards[0].DiskID
	if err := s.SetDiskState(ctx, 3, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, victim, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.ReprotectDisk(ctx, victim); err != nil {
		t.Fatal(err)
	}
	// An active writer still owns its tail, so the target must remain deferred.
	before, err := s.db.VlogShardDisks(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, sh := range before {
		found = found || sh.DiskID == victim
	}
	if !found {
		t.Fatal("relocated an actively leased file vlog")
	}
	if err := s.AbortHandle(ctx, open.Handle); err != nil {
		t.Fatal(err)
	}
	if err := s.ReprotectDisk(ctx, victim); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.db.PlogsOnDisk(ctx, victim)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("aborted tail prevented reprotection: remaining=%v err=%v", remaining, err)
	}
	reader, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/old"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, &pb.ReadRequest{Handle: reader.Handle, Length: int64(len(want))})
	if err != nil || !bytes.Equal(got.GetBuffer(), want) {
		t.Fatalf("published data changed: %v", err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: reader.Handle}); err != nil {
		t.Fatal(err)
	}
	auditWrite(t, s, "bucket/after-repair", []byte("publication resumes"))
}

func TestUncommittedRawTailStillDefersReprotection(t *testing.T) {
	ctx := context.Background()
	s := newControlPlaneServer(t, 3)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	if err := s.SetDiskState(ctx, 3, meta.DiskDraining); err != nil {
		t.Fatal(err)
	}
	id := provision(t, s, "DUPLICATE", 1, 0)
	if _, err := s.vlogs[id].Write(ctx, 0, []byte("uncommitted raw bytes")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 3, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.ReprotectDisk(ctx, 1); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.db.PlogsOnDisk(ctx, 1)
	if err != nil || len(remaining) != 1 {
		t.Fatalf("raw owner lost its tail: remaining=%v err=%v", remaining, err)
	}
}
