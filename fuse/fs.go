package fuse

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

// opSeq makes each Create's write-operation key unique within a process so a
// fresh file always gets its own idempotent write operation.
var opSeq atomic.Uint64

// mountOwner attributes every node to the process that mounted the filesystem,
// so the mounting user can read and write it (FUSE nodes default to uid/gid 0).
var mountOwner = fuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}

// opErrno maps a failed server call to an errno. A request whose context was
// canceled (the kernel interrupted the op) is reported as EINTR so it can be
// retried, rather than EIO which would surface as a hard I/O error.
func opErrno(ctx context.Context, err error) syscall.Errno {
	if ctx.Err() != nil {
		return syscall.EINTR
	}
	slog.Error("fuse op failed", "err", err)
	return syscall.EIO
}

// setTimes fills the kernel attr times from a stored mtime (ns since epoch).
// Rose tracks only modification time, so atime and ctime are reported as mtime.
func setTimes(attr *fuse.Attr, mtimeNs int64) {
	t := time.Unix(0, mtimeNs)
	attr.SetTimes(&t, &t, &t)
}

// join builds the namespace path of a child of dir. The root directory has the
// empty path, so a child of root is just its name.
func join(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// RoseDir is a directory node backed by the Rose namespace at a given path.
// The root is the RoseDir with path "".
type RoseDir struct {
	fs.Inode
	srv  *server.Server
	path string
}

func NewRoseRoot(srv *server.Server) *RoseDir {
	return &RoseDir{srv: srv}
}

var (
	_ = (fs.NodeReaddirer)((*RoseDir)(nil))
	_ = (fs.NodeLookuper)((*RoseDir)(nil))
	_ = (fs.NodeGetattrer)((*RoseDir)(nil))
	_ = (fs.NodeMkdirer)((*RoseDir)(nil))
	_ = (fs.NodeRmdirer)((*RoseDir)(nil))
	_ = (fs.NodeUnlinker)((*RoseDir)(nil))
	_ = (fs.NodeRenamer)((*RoseDir)(nil))
	_ = (fs.NodeCreater)((*RoseDir)(nil))
)

func (d *RoseDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.Owner = mountOwner
	// The root has no stored row and thus no mtime; every other directory carries
	// one in the namespace.
	if d.path != "" {
		if resp, err := d.srv.Getattr(ctx, &pb.GetattrRequest{Path: d.path}); err == nil {
			setTimes(&out.Attr, resp.GetMtime())
		}
	}
	return 0
}

func (d *RoseDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	resp, err := d.srv.ListDir(ctx, &pb.ListDirRequest{Path: d.path})
	if err != nil {
		return nil, opErrno(ctx, err)
	}
	entries := make([]fuse.DirEntry, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		mode := uint32(fuse.S_IFREG)
		if e.GetIsDir() {
			mode = fuse.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Name: e.GetName(), Mode: mode})
	}
	return fs.NewListDirStream(entries), 0
}

func (d *RoseDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := join(d.path, name)
	attr, err := d.srv.Getattr(ctx, &pb.GetattrRequest{Path: childPath})
	if err != nil {
		if ctx.Err() != nil {
			return nil, syscall.EINTR
		}
		return nil, syscall.ENOENT
	}
	out.Owner = mountOwner
	setTimes(&out.Attr, attr.GetMtime())
	if attr.GetIsDir() {
		out.Mode = fuse.S_IFDIR | 0755
		child := d.NewInode(ctx, &RoseDir{srv: d.srv, path: childPath}, fs.StableAttr{Mode: fuse.S_IFDIR})
		return child, 0
	}
	out.Mode = fuse.S_IFREG | 0644
	out.Size = uint64(attr.GetSize())
	child := d.NewInode(ctx, &RoseFile{srv: d.srv, path: childPath}, fs.StableAttr{Mode: fuse.S_IFREG})
	return child, 0
}

func (d *RoseDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := join(d.path, name)
	if _, err := d.srv.Mkdir(ctx, &pb.MkdirRequest{Path: childPath}); err != nil {
		return nil, opErrno(ctx, err)
	}
	out.Mode = fuse.S_IFDIR | 0755
	out.Owner = mountOwner
	return d.NewInode(ctx, &RoseDir{srv: d.srv, path: childPath}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

func (d *RoseDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	if _, err := d.srv.Rmdir(ctx, &pb.RmdirRequest{Path: join(d.path, name)}); err != nil {
		return syscall.ENOTEMPTY
	}
	return 0
}

func (d *RoseDir) Unlink(ctx context.Context, name string) syscall.Errno {
	if _, err := d.srv.Unlink(ctx, &pb.UnlinkRequest{Path: join(d.path, name)}); err != nil {
		return opErrno(ctx, err)
	}
	return 0
}

func (d *RoseDir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	dst, ok := newParent.(*RoseDir)
	if !ok {
		return syscall.EXDEV
	}
	_, err := d.srv.Rename(ctx, &pb.RenameRequest{
		OldPath: join(d.path, name),
		NewPath: join(dst.path, newName),
	})
	if err != nil {
		return opErrno(ctx, err)
	}
	return 0
}

func (d *RoseDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	childPath := join(d.path, name)
	// Bind a write operation up front so the file is published on Close even if
	// nothing is written (e.g. `touch`); otherwise a zero-write handle closes
	// without ever creating a file head.
	key := fmt.Sprintf("fuse-create-%s-%d-%d", childPath, time.Now().UnixNano(), opSeq.Add(1))
	resp, err := d.srv.Open(ctx, &pb.OpenRequest{Path: childPath, OperationKey: key})
	if err != nil {
		return nil, nil, 0, opErrno(ctx, err)
	}
	out.Mode = fuse.S_IFREG | mode
	out.Owner = mountOwner
	child := d.NewInode(ctx, &RoseFile{srv: d.srv, path: childPath}, fs.StableAttr{Mode: fuse.S_IFREG})
	return child, &roseHandle{srv: d.srv, handle: resp.Handle, path: childPath}, 0, 0
}

// RoseFile is a regular-file node.  It owns only node-level metadata (Getattr,
// Open); the per-open read/write/flush/release state lives in roseHandle so the
// file-handle and node dispatch paths never overlap.
type RoseFile struct {
	fs.Inode
	srv  *server.Server
	path string
}

var (
	_ = (fs.NodeOpener)((*RoseFile)(nil))
	_ = (fs.NodeGetattrer)((*RoseFile)(nil))
	_ = (fs.NodeSetattrer)((*RoseFile)(nil))
)

// Setattr applies a size change (ftruncate, or the O_TRUNC the kernel issues at
// open) via the server truncate path, and a modification-time change (utimes)
// via Setattr. chmod/chown remain no-ops since mode/owner are not persisted.
func (f *RoseFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0644
	out.Owner = mountOwner
	if m, ok := in.GetMTime(); ok {
		mtime := m.UnixNano()
		if _, err := f.srv.Setattr(ctx, &pb.SetattrRequest{Path: f.path, Mtime: &mtime}); err != nil {
			return opErrno(ctx, err)
		}
		setTimes(&out.Attr, mtime)
	}
	size, hasSize := in.GetSize()
	if hasSize {
		req := &pb.TruncateRequest{Path: f.path, Size: int64(size)}
		if h, isRose := fh.(*roseHandle); isRose {
			req.Handle = h.handle
		} else {
			req.OperationKey = fmt.Sprintf("fuse-truncate-%s-%d-%d", f.path, time.Now().UnixNano(), opSeq.Add(1))
		}
		if _, err := f.srv.Truncate(ctx, req); err != nil {
			return opErrno(ctx, err)
		}
		// Reflect the just-set length; the committed head may not yet show it for an
		// open write handle, so do not overwrite it with a by-path stat below.
		out.Size = size
		return 0
	}
	if resp, err := f.srv.Getattr(ctx, &pb.GetattrRequest{Path: f.path}); err == nil {
		out.Size = uint64(resp.Size)
		setTimes(&out.Attr, resp.GetMtime())
	}
	return 0
}

func (f *RoseFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	resp, err := f.srv.Open(ctx, &pb.OpenRequest{Path: f.path})
	if err != nil {
		return nil, 0, opErrno(ctx, err)
	}
	return &roseHandle{srv: f.srv, handle: resp.Handle, path: f.path}, 0, 0
}

func (f *RoseFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0644
	out.Owner = mountOwner
	// An fstat on an open write handle (e.g. rsync stat'ing a file it just wrote
	// but has not closed) must see the uncommitted length; route the stat through
	// the handle so the server can answer from its write cache.
	req := &pb.GetattrRequest{Path: f.path}
	if h, isRose := fh.(*roseHandle); isRose {
		req.Handle = h.handle
	}
	resp, err := f.srv.Getattr(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return syscall.EINTR
		}
		// The node exists as an open inode but its file head is not yet published
		// (a freshly created file before Close commits it). Report it as empty
		// rather than failing the stat the kernel issues right after Create.
		return 0
	}
	out.Size = uint64(resp.Size)
	setTimes(&out.Attr, resp.GetMtime())
	return 0
}

// roseHandle is one open file: it carries the server-side handle and serves the
// file-level read/write/flush/release operations.
type roseHandle struct {
	srv    *server.Server
	handle int64
	path   string
}

var (
	_ = (fs.FileReader)((*roseHandle)(nil))
	_ = (fs.FileWriter)((*roseHandle)(nil))
	_ = (fs.FileFlusher)((*roseHandle)(nil))
	_ = (fs.FileReleaser)((*roseHandle)(nil))
)

func (h *roseHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	resp, err := h.srv.Read(ctx, &pb.ReadRequest{Handle: h.handle, Offset: off, Length: int64(len(dest))})
	if err != nil {
		return fuse.ReadResultData(nil), opErrno(ctx, err)
	}
	return fuse.ReadResultData(resp.Buffer), 0
}

func (h *roseHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if _, err := h.srv.Write(ctx, &pb.WriteRequest{Handle: h.handle, Buffer: data, Offset: off}); err != nil {
		return 0, opErrno(ctx, err)
	}
	return uint32(len(data)), 0
}

func (h *roseHandle) Flush(ctx context.Context) syscall.Errno {
	// Flush is called on close(2) of every file descriptor, but multiple descriptors (e.g. from dup)
	// can share the same open handle. We must not close/destroy the server-side handle until
	// Release is called when the last descriptor is closed, so Flush is a no-op.
	return 0
}

func (h *roseHandle) Release(ctx context.Context) syscall.Errno {
	h.srv.Close(ctx, &pb.CloseRequest{Handle: h.handle})
	return 0
}
