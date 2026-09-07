package fuse

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

func adapterTree(t *testing.T) (*RoseDir, *server.Server) {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := server.NewServerWithDataDir(db, filepath.Join(dir, "data"))
	s.SetMaintenanceInterval(0)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.CloseStorage)
	root := NewRoseRoot(s)
	fs.NewNodeFS(root, &fs.Options{})
	return root, s
}

func putAdapterFile(t *testing.T, s *server.Server, path, contents string) {
	t.Helper()
	ctx := context.Background()
	h, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: []byte(contents)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
}

func cachedChild(t *testing.T, parent *RoseDir, name string) fs.InodeEmbedder {
	t.Helper()
	child, errno := parent.Lookup(context.Background(), name, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	return child.Operations()
}

func openCached(t *testing.T, file *RoseFile) *roseHandle {
	t.Helper()
	h, _, errno := file.Open(context.Background(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	rh := h.(*roseHandle)
	t.Cleanup(func() { _ = rh.Release(context.Background()) })
	return rh
}

func checkAdapterRead(t *testing.T, h *roseHandle, want string) {
	t.Helper()
	r, errno := h.Read(context.Background(), make([]byte, 1024), 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	defer r.Done()
	data, status := r.Bytes(nil)
	if status != fuse.OK || string(data) != want {
		t.Fatalf("read = %q, %v; want %q", data, status, want)
	}
}

func TestCachedRenameDescendantsReplacementAndRecreatedNames(t *testing.T) {
	root, s := adapterTree(t)
	ctx := context.Background()
	putAdapterFile(t, s, "old/sub/file", "original")
	oldDir := cachedChild(t, root, "old").(*RoseDir)
	sub := cachedChild(t, oldDir, "sub").(*RoseDir)
	file := cachedChild(t, sub, "file").(*RoseFile)
	duplicate := cachedChild(t, sub, "file").(*RoseFile)
	openedBeforeRename := openCached(t, file)
	if errno := root.Rename(ctx, "old", root, "new", 0); errno != 0 {
		t.Fatal(errno)
	}
	// Reusing the old name must not redirect either cached Lookup result.
	putAdapterFile(t, s, "old/sub/file", "replacement")
	checkAdapterRead(t, openCached(t, file), "original")
	checkAdapterRead(t, openCached(t, duplicate), "original")
	stream, errno := sub.Readdir(ctx)
	if errno != 0 {
		t.Fatal(errno)
	}
	if !stream.HasNext() {
		t.Fatal("cached directory lost its renamed children")
	}
	stream.Close()

	var truncate fuse.SetAttrIn
	truncate.Valid, truncate.Size = fuse.FATTR_SIZE, 2
	if errno := file.Setattr(ctx, nil, &truncate, &fuse.AttrOut{}); errno != 0 {
		t.Fatal(errno)
	}
	checkAdapterRead(t, openCached(t, file), "or")
	checkAdapterRead(t, openedBeforeRename, "original")

	putAdapterFile(t, s, "target", "old destination")
	target := cachedChild(t, root, "target").(*RoseFile)
	oldTargetHandle := openCached(t, target)
	if errno := sub.Rename(ctx, "file", root, "target", 0); errno != 0 {
		t.Fatal(errno)
	}
	if _, _, errno := target.Open(ctx, syscall.O_RDONLY); errno != syscall.ENOENT {
		t.Fatalf("replaced inode open = %v", errno)
	}
	checkAdapterRead(t, oldTargetHandle, "old destination")
	checkAdapterRead(t, openCached(t, duplicate), "or")
	if errno := root.Unlink(ctx, "target"); errno != 0 {
		t.Fatal(errno)
	}
	if _, _, errno := file.Open(ctx, syscall.O_RDONLY); errno != syscall.ENOENT {
		t.Fatalf("unlinked inode open = %v", errno)
	}
}

func TestForgottenNodesReleasePathBookkeeping(t *testing.T) {
	root, s := adapterTree(t)
	putAdapterFile(t, s, "file", "content")
	file := cachedChild(t, root, "file").(*RoseFile)
	if len(root.paths.nodes) != 2 {
		t.Fatal("file was not registered")
	}
	file.OnForget()
	if len(root.paths.nodes) != 1 {
		t.Fatal("forgotten file retained by mount")
	}
}
