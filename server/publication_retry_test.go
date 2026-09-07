package server

import (
	"context"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestCommittedPublicationRetryReleasesPreparationPins(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	auditWrite(t, s, "bucket/source", []byte("reused bytes"))
	p := auditPlacement(t, s, "bucket/source")
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/copy", OperationKey: "lost-publication-reply"})
	if err != nil {
		t.Fatal(err)
	}
	op, err := s.db.WriteOpByKey(ctx, "lost-publication-reply")
	if err != nil {
		t.Fatal(err)
	}
	placement, found, err := s.pinAndResolveChunk(ctx, op.ID, p.Hash)
	if err != nil || !found {
		t.Fatalf("dedup preparation: found=%v err=%v", found, err)
	}
	// Leave the server exactly at the lost-reply boundary: the catalog has
	// committed but the handle and dedup preparation pins are still active.
	if _, err := s.db.CommitWriteOpVersion(ctx, op.ID, "bucket/copy", 2, []meta.ChunkPlacement{placement}); err != nil {
		t.Fatal(err)
	}
	if len(s.pinnedChunks[op.ID]) == 0 {
		t.Fatal("fixture lacks preparation pins")
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
	if len(s.pinnedChunks[op.ID]) != 0 {
		t.Fatal("committed retry leaked preparation pins")
	}
	if len(s.pinnedChunks[handlePinOwner(h.Handle)]) != 0 {
		t.Fatal("closed retry leaked handle pins")
	}
}
