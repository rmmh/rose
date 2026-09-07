package fuse_test

import (
	"bytes"
	"context"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	rosefuse "github.com/rmmh/rose/fuse"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

// Exercise the actual adapter without requiring a kernel mount. A new Server
// after fsync has no access to the writable handle or its cache.
func TestFsyncPublishesWithoutClosingAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dataDir := filepath.Join(dir, "data")
	s := server.NewServerWithDataDir(db, dataDir)
	s.SetMaintenanceInterval(0)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.CloseStorage()
	root := rosefuse.NewRoseRoot(s)
	fs.NewNodeFS(root, &fs.Options{})
	_, handle, _, errno := root.Create(ctx, "file", syscall.O_RDWR, 0644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	writer := handle.(fs.FileWriter)
	syncer, ok := handle.(fs.FileFsyncer)
	if !ok {
		t.Fatal("FUSE handle has no fsync implementation")
	}
	if _, errno := writer.Write(ctx, []byte("first"), 0); errno != 0 {
		t.Fatal(errno)
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	if errno := syncer.Fsync(ctx, 0); errno == 0 {
		t.Fatal("fsync hid a storage failure")
	}
	if err := s.SetDiskState(ctx, 1, meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	if errno := syncer.Fsync(ctx, 0); errno != 0 {
		t.Fatal(errno)
	}
	if _, errno := writer.Write(ctx, []byte("second"), 5); errno != 0 {
		t.Fatal(errno)
	}
	if errno := syncer.Fsync(ctx, 1); errno != 0 {
		t.Fatal(errno)
	}
	// Drop the server and the still-open adapter handle without Release/Close.
	s.CloseStorage()
	after := server.NewServerWithDataDir(db, dataDir)
	after.SetMaintenanceInterval(0)
	if err := after.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	defer after.CloseStorage()
	h, err := after.Open(ctx, &pb.OpenRequest{Path: "file"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := after.Read(ctx, &pb.ReadRequest{Handle: h.Handle, Length: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Buffer, []byte("firstsecond")) {
		t.Fatalf("fsynced data = %q", r.Buffer)
	}
}
