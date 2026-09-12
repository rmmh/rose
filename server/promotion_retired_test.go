package server

import (
	"context"
	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
	"testing"
)

func TestPromotionIgnoresAlreadyRetiredCandidate(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)()
	s := newControlPlaneServer(t, 4)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "bucket", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}
	auditWrite(t, s, "bucket/file", promotionCrashPayload(11))
	candidate := stagingVlogID(t, s)
	if did, err := s.PromoteStagingVlog(ctx, candidate); err != nil || !did {
		t.Fatalf("first promotion=%v %v", did, err)
	}
	if _, err := s.db.GetVlog(ctx, candidate); err == nil {
		t.Fatal("fixture did not retire source")
	}
	if did, err := s.PromoteStagingVlog(ctx, candidate); err != nil || did {
		t.Fatalf("stale candidate=%v %v", did, err)
	}
}
