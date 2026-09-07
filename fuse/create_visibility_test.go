package fuse

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func TestCreatePublishesEmptyFileBeforeClose(t *testing.T) {
	root, s := adapterTree(t)
	ctx := context.Background()
	inode, handle, _, errno := root.Create(ctx, "new", syscall.O_RDWR, 0644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	h := handle.(*roseHandle)
	defer h.Release(ctx)
	entry, err := s.Getattr(ctx, &pb.GetattrRequest{Path: "new"})
	if err != nil || entry.GetSize() != 0 {
		t.Fatalf("created name not visible before close: %v", err)
	}
	want := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MTIME
	in.Mtime = uint64(want.Unix())
	// Linux may dispatch futimes as a node-only SETATTR, without FATTR_FH.
	if errno := inode.Operations().(*RoseFile).Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatal(errno)
	}
	if errno := h.Flush(ctx); errno != 0 {
		t.Fatal(errno)
	}
	entry, err = s.Getattr(ctx, &pb.GetattrRequest{Path: "new"})
	if err != nil || entry.GetMtime() != want.UnixNano() {
		t.Fatalf("timestamp lost on close: entry=%v err=%v", entry, err)
	}
}

func TestCreatePublicationFailureDoesNotLeavePreparedOperation(t *testing.T) {
	root, s := adapterTree(t)
	ctx := context.Background()
	putAdapterFile(t, s, "existing", "published data")
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	_, handle, _, errno := root.Create(ctx, "rejected", syscall.O_RDWR, 0644, &fuse.EntryOut{})
	if errno == 0 || handle != nil {
		t.Fatal("Create acknowledged publication during known degradation")
	}
	if _, err := s.Getattr(ctx, &pb.GetattrRequest{Path: "rejected"}); err == nil {
		t.Fatal("failed Create published its name")
	}
	ops, err := s.GetDB().ListPreparedWriteOps(ctx)
	if err != nil || len(ops) != 0 {
		t.Fatalf("failed Create retained preparation: ops=%v err=%v", ops, err)
	}
}
