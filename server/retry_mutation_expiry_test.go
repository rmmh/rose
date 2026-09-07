package server

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestExpiredRetryRejectsMutationsWithoutReaper(t *testing.T) {
	for _, kind := range []string{"write", "truncate", "mtime"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s := newControlPlaneServer(t, 1)
			s.StopMaintenanceDriver()
			defer s.CloseStorage()
			now := time.Now()
			s.now = func() time.Time { return now }
			if err := s.SetRetryRetention(time.Minute); err != nil {
				t.Fatal(err)
			}
			body := []byte("retained bytes")
			initial, err := s.Open(ctx, &pb.OpenRequest{Path: "file", OperationKey: "key"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(ctx, &pb.WriteRequest{Handle: initial.Handle, Buffer: body}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Close(ctx, &pb.CloseRequest{Handle: initial.Handle}); err != nil {
				t.Fatal(err)
			}
			retry, err := s.Open(ctx, &pb.OpenRequest{Path: "file", OperationKey: "key"})
			if err != nil {
				t.Fatal(err)
			}
			h := s.handles[retry.Handle]
			beforeLength := h.cache.Length()
			now = now.Add(time.Minute) // exact exclusive deadline, no expiry pass
			switch kind {
			case "write":
				_, err = s.Write(ctx, &pb.WriteRequest{Handle: retry.Handle, Buffer: body})
			case "truncate":
				_, err = s.Truncate(ctx, &pb.TruncateRequest{Handle: retry.Handle, Size: 0})
			case "mtime":
				err = s.SetHandleMtime(ctx, retry.Handle, 99)
			}
			if err == nil || !strings.Contains(err.Error(), "expired") {
				t.Fatalf("expired %s result=%v", kind, err)
			}
			if h.cache.Length() != beforeLength || h.writeTouched || h.mtimeSet.Load() {
				t.Fatal("expired request mutated handle state")
			}
			op, err := s.db.WriteOpByKey(ctx, "key")
			if err != nil || op.State != meta.WriteOpCommitted || op.RetryExpiresAt != now.UnixNano() {
				t.Fatalf("fixture required an expiry sweep: op=%v err=%v", op, err)
			}
			got, err := s.Read(ctx, &pb.ReadRequest{Handle: retry.Handle, Length: int64(len(body))})
			if err != nil || !bytes.Equal(got.GetBuffer(), body) {
				t.Fatalf("mutation fence lost pinned read: %v", err)
			}
		})
	}
}
