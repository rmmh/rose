package server_test

import (
	"context"
	"testing"

	pb "github.com/rmmh/rose/proto"
)

func TestListDirAndMkdirRmdir(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	writeFile(t, client, "bucket/a.txt", []byte("hello"))
	writeFile(t, client, "bucket/sub/b.txt", []byte("world!!"))

	// ListDir returns only immediate children: file a.txt and directory sub.
	resp, err := client.ListDir(ctx, &pb.ListDirRequest{Path: "bucket"})
	if err != nil {
		t.Fatal(err)
	}
	var sawFile, sawDir bool
	for _, e := range resp.GetEntries() {
		switch {
		case e.GetName() == "a.txt" && !e.GetIsDir():
			sawFile = true
			if e.GetSize() != 5 {
				t.Errorf("a.txt size = %d, want 5", e.GetSize())
			}
		case e.GetName() == "sub" && e.GetIsDir():
			sawDir = true
		default:
			t.Errorf("unexpected entry %q (dir=%v)", e.GetName(), e.GetIsDir())
		}
	}
	if !sawFile || !sawDir {
		t.Fatalf("ListDir(bucket) entries = %+v", resp.GetEntries())
	}

	// Getattr distinguishes directories from files.
	da, err := client.Getattr(ctx, &pb.GetattrRequest{Path: "bucket/sub"})
	if err != nil {
		t.Fatal(err)
	}
	if !da.GetIsDir() {
		t.Error("bucket/sub should be a directory")
	}
	fa, err := client.Getattr(ctx, &pb.GetattrRequest{Path: "bucket/a.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if fa.GetIsDir() || fa.GetSize() != 5 {
		t.Errorf("a.txt getattr = %+v", fa)
	}

	// Mkdir creates an empty directory that then lists.
	if _, err := client.Mkdir(ctx, &pb.MkdirRequest{Path: "bucket/empty"}); err != nil {
		t.Fatal(err)
	}
	resp, err = client.ListDir(ctx, &pb.ListDirRequest{Path: "bucket"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range resp.GetEntries() {
		if e.GetName() == "empty" && e.GetIsDir() {
			found = true
		}
	}
	if !found {
		t.Fatal("mkdir'd dir not listed")
	}

	// Rmdir on a non-empty dir fails; on the empty dir succeeds.
	if _, err := client.Rmdir(ctx, &pb.RmdirRequest{Path: "bucket/sub"}); err == nil {
		t.Fatal("rmdir of non-empty dir should fail")
	}
	if _, err := client.Rmdir(ctx, &pb.RmdirRequest{Path: "bucket/empty"}); err != nil {
		t.Fatalf("rmdir empty: %v", err)
	}
}
