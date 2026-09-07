package fuse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	"github.com/rmmh/rose/uid"
)

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
	if errors.Is(err, syscall.ENOSPC) {
		return syscall.ENOSPC
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

// mountPaths tracks every cached node, including duplicate Lookup results and
// descendants not currently attached to go-fuse's child tree. A rename changes
// these references atomically with the server call. Open file handles continue
// using their server identity. OnForget releases the mount's bookkeeping.
type mountPaths struct {
	mu    sync.Mutex
	nodes map[*cachedPath]struct{}
}

type cachedPath struct {
	path    string
	removed bool
}

func (m *mountPaths) add(path string) *cachedPath {
	p := &cachedPath{path: path}
	m.nodes[p] = struct{}{}
	return p
}

func atOrBelow(path, parent string) bool {
	return path == parent || strings.HasPrefix(path, parent+"/")
}

func (m *mountPaths) rename(oldPath, newPath string) {
	if oldPath == newPath {
		return
	}
	for p := range m.nodes {
		if p.removed {
			continue
		}
		if atOrBelow(p.path, oldPath) {
			p.path = newPath + strings.TrimPrefix(p.path, oldPath)
		} else if atOrBelow(p.path, newPath) {
			p.removed = true
		}
	}
}

func (m *mountPaths) remove(path string) {
	for p := range m.nodes {
		if atOrBelow(p.path, path) {
			p.removed = true
		}
	}
}

func (m *mountPaths) forget(p *cachedPath) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, p)
}

// RoseDir is a directory node backed by the Rose namespace at a given path.
// The root is the RoseDir with path "".
type RoseDir struct {
	fs.Inode
	srv   *server.Server
	paths *mountPaths
	ref   *cachedPath
}

func NewRoseRoot(srv *server.Server) *RoseDir {
	paths := &mountPaths{nodes: make(map[*cachedPath]struct{})}
	return &RoseDir{srv: srv, paths: paths, ref: paths.add("")}
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
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return syscall.ENOENT
	}
	out.Mode = fuse.S_IFDIR | 0755
	out.Owner = mountOwner
	// The root has no stored row and thus no mtime; every other directory carries
	// one in the namespace.
	if d.ref.path != "" {
		if resp, err := d.srv.Getattr(ctx, &pb.GetattrRequest{Path: d.ref.path}); err == nil {
			setTimes(&out.Attr, resp.GetMtime())
		}
	}
	return 0
}

func (d *RoseDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return nil, syscall.ENOENT
	}
	resp, err := d.srv.ListDir(ctx, &pb.ListDirRequest{Path: d.ref.path})
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
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return nil, syscall.ENOENT
	}
	childPath := join(d.ref.path, name)
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
		child := d.NewInode(ctx, &RoseDir{srv: d.srv, paths: d.paths, ref: d.paths.add(childPath)}, fs.StableAttr{Mode: fuse.S_IFDIR})
		return child, 0
	}
	out.Mode = fuse.S_IFREG | 0644
	out.Size = uint64(attr.GetSize())
	child := d.NewInode(ctx, &RoseFile{srv: d.srv, paths: d.paths, ref: d.paths.add(childPath)}, fs.StableAttr{Mode: fuse.S_IFREG})
	return child, 0
}

func (d *RoseDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return nil, syscall.ENOENT
	}
	childPath := join(d.ref.path, name)
	if _, err := d.srv.Mkdir(ctx, &pb.MkdirRequest{Path: childPath}); err != nil {
		return nil, opErrno(ctx, err)
	}
	out.Mode = fuse.S_IFDIR | 0755
	out.Owner = mountOwner
	return d.NewInode(ctx, &RoseDir{srv: d.srv, paths: d.paths, ref: d.paths.add(childPath)}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

func (d *RoseDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return syscall.ENOENT
	}
	if _, err := d.srv.Rmdir(ctx, &pb.RmdirRequest{Path: join(d.ref.path, name)}); err != nil {
		return syscall.ENOTEMPTY
	}
	d.paths.remove(join(d.ref.path, name))
	return 0
}

func (d *RoseDir) Unlink(ctx context.Context, name string) syscall.Errno {
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return syscall.ENOENT
	}
	if _, err := d.srv.Unlink(ctx, &pb.UnlinkRequest{Path: join(d.ref.path, name)}); err != nil {
		return opErrno(ctx, err)
	}
	d.paths.remove(join(d.ref.path, name))
	return 0
}

func (d *RoseDir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags != 0 {
		// The Rose RPC implements ordinary replacement rename only. Silently
		// accepting RENAME_NOREPLACE or RENAME_EXCHANGE would perform a different,
		// potentially destructive operation than the client requested.
		return syscall.EINVAL
	}
	dst, ok := newParent.(*RoseDir)
	if !ok || dst.paths != d.paths || dst.srv != d.srv {
		return syscall.EXDEV
	}
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed || dst.ref.removed {
		return syscall.ENOENT
	}
	oldPath, newPath := join(d.ref.path, name), join(dst.ref.path, newName)
	_, err := d.srv.Rename(ctx, &pb.RenameRequest{
		OldPath: oldPath,
		NewPath: newPath,
	})
	if err != nil {
		return opErrno(ctx, err)
	}
	d.paths.rename(oldPath, newPath)
	return 0
}

func (d *RoseDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	d.paths.mu.Lock()
	defer d.paths.mu.Unlock()
	if d.ref.removed {
		return nil, nil, 0, syscall.ENOENT
	}
	childPath := join(d.ref.path, name)
	// Bind a write operation up front so the file is published on Close even if
	// nothing is written (e.g. `touch`); otherwise a zero-write handle closes
	// without ever creating a file head.
	key := fmt.Sprintf("fuse-create-%s-%s", childPath, uid.New())
	resp, err := d.srv.Open(ctx, &pb.OpenRequest{Path: childPath, OperationKey: key})
	if err != nil {
		return nil, nil, 0, opErrno(ctx, err)
	}
	out.Mode = fuse.S_IFREG | mode
	out.Owner = mountOwner
	child := d.NewInode(ctx, &RoseFile{srv: d.srv, paths: d.paths, ref: d.paths.add(childPath)}, fs.StableAttr{Mode: fuse.S_IFREG})
	h, err := retainedHandle(d.srv, resp.Handle)
	if err != nil {
		return nil, nil, 0, opErrno(ctx, err)
	}
	return child, h, 0, 0
}

// RoseFile is a regular-file node.  It owns only node-level metadata (Getattr,
// Open); the per-open read/write/flush/release state lives in roseHandle so the
// file-handle and node dispatch paths never overlap.
type RoseFile struct {
	fs.Inode
	srv   *server.Server
	paths *mountPaths
	ref   *cachedPath
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
	f.paths.mu.Lock()
	defer f.paths.mu.Unlock()
	if _, hasHandle := fh.(*roseHandle); f.ref.removed && !hasHandle {
		return syscall.ENOENT
	}
	out.Mode = fuse.S_IFREG | 0644
	out.Owner = mountOwner
	if m, ok := in.GetMTime(); ok {
		mtime := m.UnixNano()
		if h, isRose := fh.(*roseHandle); isRose {
			if err := f.srv.SetHandleMtime(ctx, h.handle, mtime); err != nil {
				return opErrno(ctx, err)
			}
		} else {
			if _, err := f.srv.Setattr(ctx, &pb.SetattrRequest{Path: f.ref.path, Mtime: &mtime}); err != nil {
				return opErrno(ctx, err)
			}
		}
		setTimes(&out.Attr, mtime)
	}
	size, hasSize := in.GetSize()
	if hasSize {
		req := &pb.TruncateRequest{Path: f.ref.path, Size: int64(size)}
		if h, isRose := fh.(*roseHandle); isRose {
			req.Handle = h.handle
		} else {
			req.OperationKey = fmt.Sprintf("fuse-truncate-%s-%s", f.ref.path, uid.New())
		}
		if _, err := f.srv.Truncate(ctx, req); err != nil {
			return opErrno(ctx, err)
		}
		// Reflect the just-set length; the committed head may not yet show it for an
		// open write handle, so do not overwrite it with a by-path stat below.
		out.Size = size
		return 0
	}
	if resp, err := f.srv.Getattr(ctx, &pb.GetattrRequest{Path: f.ref.path}); err == nil {
		out.Size = uint64(resp.Size)
		setTimes(&out.Attr, resp.GetMtime())
	}
	return 0
}

func (f *RoseFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	f.paths.mu.Lock()
	defer f.paths.mu.Unlock()
	if f.ref.removed {
		return nil, 0, syscall.ENOENT
	}
	resp, err := f.srv.Open(ctx, &pb.OpenRequest{Path: f.ref.path})
	if err != nil {
		return nil, 0, opErrno(ctx, err)
	}
	h, err := retainedHandle(f.srv, resp.Handle)
	if err != nil {
		return nil, 0, opErrno(ctx, err)
	}
	return h, 0, 0
}

func (f *RoseFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.paths.mu.Lock()
	defer f.paths.mu.Unlock()
	if _, hasHandle := fh.(*roseHandle); f.ref.removed && !hasHandle {
		return syscall.ENOENT
	}
	out.Mode = fuse.S_IFREG | 0644
	out.Owner = mountOwner
	// An fstat on an open write handle (e.g. rsync stat'ing a file it just wrote
	// but has not closed) must see the uncommitted length; route the stat through
	// the handle so the server can answer from its write cache.
	req := &pb.GetattrRequest{Path: f.ref.path}
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
	srv          *server.Server
	handle       int64
	releaseLocal func()
	closed       atomic.Bool
}

func retainedHandle(srv *server.Server, id int64) (*roseHandle, error) {
	release, err := srv.RetainHandle(id)
	if err != nil {
		_ = srv.AbortHandle(context.Background(), id)
		return nil, err
	}
	return &roseHandle{srv: srv, handle: id, releaseLocal: release}, nil
}

var (
	_ = (fs.FileReader)((*roseHandle)(nil))
	_ = (fs.FileWriter)((*roseHandle)(nil))
	_ = (fs.FileFlusher)((*roseHandle)(nil))
	_ = (fs.FileFsyncer)((*roseHandle)(nil))
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
	if err := h.srv.FlushHandle(ctx, h.handle); err != nil {
		return opErrno(ctx, err)
	}
	return 0
}

// Fsync publishes the complete current version durably, including the metadata
// required to locate its bytes after restart. The handle remains writable; a
// later write starts a new publication just as it does after Flush.
func (h *roseHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return h.Flush(ctx)
}

func (h *roseHandle) Release(ctx context.Context) syscall.Errno {
	return h.close(ctx)
}

func (h *roseHandle) close(ctx context.Context) syscall.Errno {
	if h.releaseLocal != nil {
		defer h.releaseLocal()
	}
	if !h.closed.CompareAndSwap(false, true) {
		return 0
	}
	if _, err := h.srv.Close(ctx, &pb.CloseRequest{Handle: h.handle}); err != nil {
		h.closed.Store(false)
		return opErrno(ctx, err)
	}
	return 0
}

func (d *RoseDir) OnForget()  { d.paths.forget(d.ref) }
func (f *RoseFile) OnForget() { f.paths.forget(f.ref) }

var _ fs.NodeOnForgetter = (*RoseDir)(nil)
var _ fs.NodeOnForgetter = (*RoseFile)(nil)
