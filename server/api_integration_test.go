package server_test

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newClient(t *testing.T) pb.RoseClient {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterRoseServer(grpcServer, server.NewServerWithDataDir(db, filepath.Join(dir, "plogs")))
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///rose", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewRoseClient(conn)
}

func writeFile(t *testing.T, client pb.RoseClient, path string, data []byte) {
	t.Helper()
	ctx := context.Background()
	open, err := client.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()}); err != nil {
		t.Fatal(err)
	}
}

func readHandle(t *testing.T, client pb.RoseClient, handle int64) []byte {
	t.Helper()
	res, err := client.Read(context.Background(), &pb.ReadRequest{Handle: handle, Offset: 0, Length: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetBuffer()
}

func TestFileSnapshotNamespaceLifecycleOverGRPC(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	writeFile(t, client, "/alpha", []byte("first version"))
	snapshot, err := client.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: "before-rewrite"})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, client, "/alpha", []byte("second version"))

	snapOpen, err := client.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshot.GetSnapshotId(), Path: "/alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if got := readHandle(t, client, snapOpen.GetHandle()); !bytes.Equal(got, []byte("first version")) {
		t.Fatalf("snapshot read = %q, want first version", got)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: snapOpen.GetHandle()}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Rename(ctx, &pb.RenameRequest{OldPath: "/alpha", NewPath: "/beta"}); err != nil {
		t.Fatal(err)
	}
	live, err := client.Open(ctx, &pb.OpenRequest{Path: "/beta"})
	if err != nil {
		t.Fatal(err)
	}
	if got := readHandle(t, client, live.GetHandle()); !bytes.Equal(got, []byte("second version")) {
		t.Fatalf("live read = %q, want second version", got)
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: live.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Unlink(ctx, &pb.UnlinkRequest{Path: "/beta"}); err != nil {
		t.Fatal(err)
	}
	retained, err := client.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshot.GetSnapshotId(), Path: "/alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if got := readHandle(t, client, retained.GetHandle()); !bytes.Equal(got, []byte("first version")) {
		t.Fatalf("snapshot after unlink = %q, want first version", got)
	}
	if _, err := client.Write(ctx, &pb.WriteRequest{Handle: retained.GetHandle(), Buffer: []byte("forbidden")}); err == nil {
		t.Fatal("snapshot write unexpectedly succeeded")
	}
	if _, err := client.Close(ctx, &pb.CloseRequest{Handle: retained.GetHandle()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{SnapshotId: snapshot.GetSnapshotId()}); err != nil {
		t.Fatal(err)
	}
}
