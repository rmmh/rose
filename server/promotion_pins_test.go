package server

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

func TestPromotionRetirementPreservesUnlinkedReader(t *testing.T) {
	defer storage.SetECColumnBytesForTest(64)()
	for _, moveLive := range []bool{false, true} {
		t.Run(fmt.Sprint(moveLive), func(t *testing.T) {
			s := newControlPlaneServer(t, 4)
			s.StopMaintenanceDriver()
			defer s.CloseStorage()
			ctx := context.Background()
			if err := s.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
				t.Fatal(err)
			}
			want := []byte("reader owns these unlinked bytes")
			auditWrite(t, s, "ec/reader", want)
			staging := stagingVlogID(t, s)
			h, err := s.Open(ctx, &pb.OpenRequest{Path: "ec/reader"})
			if err != nil {
				t.Fatal(err)
			}
			unlinkServerFile(t, s, "ec/reader")
			if moveLive {
				auditWrite(t, s, "ec/live", publicationCrashBytes(33))
				if p := auditPlacement(t, s, "ec/live"); p.VlogID != staging {
					t.Fatal("fixture did not share staging source")
				}
			}
			promoted, err := s.PromoteStagingVlog(ctx, staging)
			if err != nil || promoted != moveLive {
				t.Fatalf("promotion=%v err=%v", promoted, err)
			}
			if s.vlogs[staging] == nil {
				t.Fatal("promotion retired a reader-pinned source")
			}
			read, err := s.Read(ctx, &pb.ReadRequest{Handle: h.Handle, Length: int64(len(want))})
			if err != nil || !bytes.Equal(read.GetBuffer(), want) {
				t.Fatalf("promotion lost unlinked reader data: %v", err)
			}
			if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PromoteStagingVlog(ctx, staging); err != nil {
				t.Fatal(err)
			}
			if s.vlogs[staging] != nil {
				t.Fatal("source remained after final reader released pins")
			}
		})
	}
}
