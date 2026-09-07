package server_test

import (
	"context"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestRetryDeadlineRoundTripsThroughRPC(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	anonymous, err := client.Open(ctx, &pb.OpenRequest{Path: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	closed, err := client.Close(ctx, &pb.CloseRequest{Handle: anonymous.Handle})
	if err != nil || closed.GetRetryExpiresAtNs() != 0 {
		t.Fatalf("anonymous deadline=%v err=%v", closed, err)
	}
	prepared, err := client.Open(ctx, &pb.OpenRequest{Path: "keyed", OperationKey: "keyed"})
	if err != nil || prepared.GetRetryExpiresAtNs() != 0 {
		t.Fatalf("prepared deadline=%v err=%v", prepared, err)
	}
	closed, err = client.Close(ctx, &pb.CloseRequest{Handle: prepared.Handle})
	if err != nil || closed.GetRetryExpiresAtNs() <= 0 {
		t.Fatalf("committed deadline=%v err=%v", closed, err)
	}
	deadline := closed.RetryExpiresAtNs
	retry, err := client.Open(ctx, &pb.OpenRequest{Path: "keyed", OperationKey: "keyed"})
	if err != nil || retry.GetRetryExpiresAtNs() != deadline {
		t.Fatalf("retry Open deadline=%v err=%v", retry, err)
	}
	for _, handle := range []int64{retry.Handle, -1} {
		result, err := client.Close(ctx, &pb.CloseRequest{Handle: handle, IdempotencyKey: "keyed"})
		if err != nil || result.GetRetryExpiresAtNs() != deadline {
			t.Fatalf("retry Close deadline=%v err=%v", result, err)
		}
	}
}
