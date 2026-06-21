package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

type RoseRoot struct {
	fs.Inode
	srv *server.Server
}

func NewRoseRoot(srv *server.Server) *RoseRoot {
	return &RoseRoot{
		srv: srv,
	}
}

func (r *RoseRoot) OnAdd(ctx context.Context) {
	// For a first pass, we simply expose an empty directory.
	// You can create files in it and write to them.
}

func (r *RoseRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Let's create an inode for any file lookup, meaning files just magically "exist"
	// when requested, to simplify the first pass. We will rely on Open() to create it.
	child := r.GetChild(name)
	if child != nil {
		return child, 0
	}

	return nil, syscall.ENOENT
}

func (r *RoseRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Call to Rose RPC `Open`
	resp, err := r.srv.Open(ctx, &pb.OpenRequest{Path: name})
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	file := &RoseFile{
		srv:    r.srv,
		handle: resp.Handle,
		path:   name,
	}

	child := r.NewInode(ctx, file, fs.StableAttr{Mode: fuse.S_IFREG | mode})
	return child, file, 0, 0
}

func (r *RoseRoot) Open(ctx context.Context, name string, flags uint32, out *fuse.OpenOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Call to Rose RPC `Open`
	resp, err := r.srv.Open(ctx, &pb.OpenRequest{Path: name})
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	file := &RoseFile{
		srv:    r.srv,
		handle: resp.Handle,
		path:   name,
	}

	child := r.GetChild(name)
	if child == nil {
		child = r.NewInode(ctx, file, fs.StableAttr{Mode: fuse.S_IFREG | 0644})
		r.AddChild(name, child, true)
	}

	return child, file, 0, 0
}

type RoseFile struct {
	fs.Inode
	srv    *server.Server
	handle int64
	path   string
}

var _ = (fs.NodeOpener)((*RoseFile)(nil))
var _ = (fs.NodeWriter)((*RoseFile)(nil))
var _ = (fs.NodeReader)((*RoseFile)(nil))
var _ = (fs.NodeReleaser)((*RoseFile)(nil))
var _ = (fs.NodeFlusher)((*RoseFile)(nil))

func (f *RoseFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// If the FUSE kernel calls Open specifically on this inode (rather than the directory),
	// we just return ourselves as the handle if we already have an RPC handle open.
	if f.handle != 0 {
		return f, 0, 0
	}
	resp, err := f.srv.Open(ctx, &pb.OpenRequest{Path: f.path})
	if err != nil {
		return nil, 0, syscall.EIO
	}
	f.handle = resp.Handle
	return f, 0, 0
}

func (f *RoseFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	// We pass the write to the server. (First pass doesn't support random access write, we assume append)
	// RPC requires a handle.
	_, err := f.srv.Write(ctx, &pb.WriteRequest{
		Handle: f.handle,
		Buffer: data,
	})

	if err != nil {
		return 0, syscall.EIO
	}

	return uint32(len(data)), 0
}

func (f *RoseFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// If the FUSE kernel bypassed Open or we lost the handle, just reopen it inline for read
	handle := f.handle
	if handle == 0 {
		resp, err := f.srv.Open(ctx, &pb.OpenRequest{Path: f.path})
		if err != nil {
			return fuse.ReadResultData(nil), syscall.EIO
		}
		handle = resp.Handle
	}

	resp, err := f.srv.Read(ctx, &pb.ReadRequest{
		Handle: handle,
		Offset: off,
		Length: int64(len(dest)),
	})
	if err != nil {
		return fuse.ReadResultData(nil), syscall.EIO
	}

	return fuse.ReadResultData(resp.Buffer), 0
}

var _ = (fs.NodeGetattrer)((*RoseFile)(nil))

func (f *RoseFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	resp, err := f.srv.Getattr(ctx, &pb.GetattrRequest{Path: f.path})
	if err != nil {
		return syscall.EIO
	}
	out.Size = uint64(resp.Size)
	return 0
}

func (f *RoseFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	_, err := f.srv.Close(ctx, &pb.CloseRequest{Handle: f.handle})
	if err != nil {
		return 0 // Ignore errors if handle is already closed
	}
	return 0
}

func (f *RoseFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.srv.Close(ctx, &pb.CloseRequest{Handle: f.handle})
	return 0
}
