package server_test

import (
	"context"
	"math"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestReadRejectsInvalidRangesWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	writeAt(t, s, "/ranges-read", -1, [][2]any{{0, []byte("contents")}})
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/ranges-read"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []pb.ReadRequest{
		{Handle: open.GetHandle(), Offset: -1, Length: 1},
		{Handle: open.GetHandle(), Offset: 0, Length: -1},
		{Handle: open.GetHandle(), Offset: math.MaxInt64, Length: 2},
	}
	for _, req := range tests {
		if _, err := s.Read(ctx, &req); err == nil {
			t.Fatalf("read offset=%d length=%d succeeded", req.GetOffset(), req.GetLength())
		}
	}
}
