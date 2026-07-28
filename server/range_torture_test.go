package server_test

import (
	"context"
	"math"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestWriteRejectsInvalidRangesWithoutPanicking(t *testing.T) {
	ctx := context.Background()
	s := newServer(t)
	open, err := s.Open(ctx, &pb.OpenRequest{Path: "/ranges"})
	if err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{-1, math.MaxInt64} {
		if _, err := s.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: off, Buffer: []byte("xx")}); err == nil {
			t.Fatalf("write offset %d succeeded", off)
		}
	}
}
