// Package webdav adapts a Rose server to golang.org/x/net/webdav.FileSystem so a
// Rose volume can be mounted over WebDAV. WebDAV is a userspace protocol with a
// client built into macOS (mount_webdav) and every major OS, so it is the
// dependency-free path to filesystem IOPS alongside the FUSE mount.
//
// WebDAV PUT is whole-file, sequential, which matches Rose's append-oriented
// write path: each open file streams its body into a single write operation that
// is committed on Close.
package webdav

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	"golang.org/x/net/webdav"
)

// FS implements webdav.FileSystem over a *server.Server.
type FS struct {
	srv *server.Server
}

// New returns a webdav.FileSystem backed by srv.
func New(srv *server.Server) *FS { return &FS{srv: srv} }

var _ webdav.FileSystem = (*FS)(nil)

// clean strips the leading slash WebDAV prepends so paths match the canonical
// form stored in the namespace.
func clean(name string) string { return strings.TrimLeft(name, "/") }

// base returns the final path element, used as the display name in FileInfo.
func base(path string) string {
	if path == "" {
		return "/"
	}
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}
	return path
}

func (f *FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	_, err := f.srv.Mkdir(ctx, &pb.MkdirRequest{Path: clean(name)})
	return err
}

func (f *FS) Rename(ctx context.Context, oldName, newName string) error {
	_, err := f.srv.Rename(ctx, &pb.RenameRequest{OldPath: clean(oldName), NewPath: clean(newName)})
	return err
}

// RemoveAll deletes a file, or a directory and everything beneath it. The
// namespace only removes empty directories, so subtrees are cleared depth-first.
func (f *FS) RemoveAll(ctx context.Context, name string) error {
	path := clean(name)
	attr, err := f.srv.Getattr(ctx, &pb.GetattrRequest{Path: path})
	if err != nil {
		// Removing something that is already gone is not an error (os.RemoveAll).
		return nil
	}
	if !attr.GetIsDir() {
		_, err := f.srv.Unlink(ctx, &pb.UnlinkRequest{Path: path})
		return err
	}
	resp, err := f.srv.ListDir(ctx, &pb.ListDirRequest{Path: path})
	if err != nil {
		return err
	}
	for _, e := range resp.GetEntries() {
		child := e.GetName()
		if path != "" {
			child = path + "/" + e.GetName()
		}
		if err := f.RemoveAll(ctx, child); err != nil {
			return err
		}
	}
	_, err = f.srv.Rmdir(ctx, &pb.RmdirRequest{Path: path})
	return err
}

func (f *FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	path := clean(name)
	attr, err := f.srv.Getattr(ctx, &pb.GetattrRequest{Path: path})
	if err != nil {
		return nil, os.ErrNotExist
	}
	return fileInfo{name: base(path), size: attr.GetSize(), mtime: attr.GetMtime(), isDir: attr.GetIsDir()}, nil
}

func (f *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	path := clean(name)
	writing := flag&(os.O_WRONLY|os.O_RDWR) != 0
	if writing {
		// Bind a fresh write operation so even a zero-byte PUT publishes a file
		// head on Close.
		key := fmt.Sprintf("webdav-%s-%d", path, time.Now().UnixNano())
		resp, err := f.srv.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: key})
		if err != nil {
			return nil, err
		}
		// A PUT replaces the whole resource: truncate any prior content to zero so
		// a shorter body does not leave a stale tail behind.
		if _, err := f.srv.Truncate(ctx, &pb.TruncateRequest{Handle: resp.GetHandle(), Size: 0}); err != nil {
			return nil, err
		}
		return &roseFile{ctx: ctx, srv: f.srv, path: path, handle: resp.GetHandle(), writing: true}, nil
	}

	attr, err := f.srv.Getattr(ctx, &pb.GetattrRequest{Path: path})
	if err != nil {
		return nil, os.ErrNotExist
	}
	rf := &roseFile{
		ctx:   ctx,
		srv:   f.srv,
		path:  path,
		size:  attr.GetSize(),
		mtime: attr.GetMtime(),
		isDir: attr.GetIsDir(),
	}
	if !attr.GetIsDir() {
		resp, err := f.srv.Open(ctx, &pb.OpenRequest{Path: path})
		if err != nil {
			return nil, err
		}
		rf.handle = resp.GetHandle()
	}
	return rf, nil
}

// roseFile is a single open WebDAV file or directory. A file is opened in exactly
// one mode: reading (random-access via Read/Seek) or writing (sequential append
// committed on Close).
type roseFile struct {
	ctx     context.Context
	srv     *server.Server
	path    string
	handle  int64
	writing bool

	size  int64
	mtime int64
	isDir bool

	offset   int64         // read cursor
	writeOff int64         // append cursor
	dirInfos []os.FileInfo // cached directory listing
	dirPos   int           // Readdir cursor
}

var _ webdav.File = (*roseFile)(nil)

func (f *roseFile) Read(p []byte) (int, error) {
	if f.isDir {
		return 0, fmt.Errorf("is a directory")
	}
	resp, err := f.srv.Read(f.ctx, &pb.ReadRequest{Handle: f.handle, Offset: f.offset, Length: int64(len(p))})
	if err != nil {
		return 0, err
	}
	if len(resp.GetBuffer()) == 0 {
		return 0, io.EOF
	}
	n := copy(p, resp.GetBuffer())
	f.offset += int64(n)
	return n, nil
}

func (f *roseFile) Write(p []byte) (int, error) {
	if _, err := f.srv.Write(f.ctx, &pb.WriteRequest{Handle: f.handle, Buffer: p, Offset: f.writeOff}); err != nil {
		return 0, err
	}
	f.writeOff += int64(len(p))
	return len(p), nil
}

func (f *roseFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.offset + offset
	case io.SeekEnd:
		abs = f.size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative seek position")
	}
	f.offset = abs
	return abs, nil
}

func (f *roseFile) Readdir(count int) ([]os.FileInfo, error) {
	if f.dirInfos == nil {
		resp, err := f.srv.ListDir(f.ctx, &pb.ListDirRequest{Path: f.path})
		if err != nil {
			return nil, err
		}
		f.dirInfos = make([]os.FileInfo, 0, len(resp.GetEntries()))
		for _, e := range resp.GetEntries() {
			f.dirInfos = append(f.dirInfos, fileInfo{
				name:  e.GetName(),
				size:  e.GetSize(),
				mtime: e.GetMtime(),
				isDir: e.GetIsDir(),
			})
		}
	}
	if count <= 0 {
		rest := f.dirInfos[f.dirPos:]
		f.dirPos = len(f.dirInfos)
		return rest, nil
	}
	if f.dirPos >= len(f.dirInfos) {
		return nil, io.EOF
	}
	end := f.dirPos + count
	if end > len(f.dirInfos) {
		end = len(f.dirInfos)
	}
	out := f.dirInfos[f.dirPos:end]
	f.dirPos = end
	return out, nil
}

func (f *roseFile) Stat() (os.FileInfo, error) {
	if f.writing {
		return fileInfo{name: base(f.path), size: f.writeOff, mtime: time.Now().UnixNano()}, nil
	}
	return fileInfo{name: base(f.path), size: f.size, mtime: f.mtime, isDir: f.isDir}, nil
}

func (f *roseFile) Close() error {
	if f.handle != 0 {
		f.srv.Close(f.ctx, &pb.CloseRequest{Handle: f.handle})
		f.handle = 0
	}
	return nil
}

// fileInfo is an os.FileInfo synthesized from a namespace DirEntry.
type fileInfo struct {
	name  string
	size  int64
	mtime int64
	isDir bool
}

func (fi fileInfo) Name() string { return fi.name }
func (fi fileInfo) Size() int64  { return fi.size }
func (fi fileInfo) Mode() os.FileMode {
	if fi.isDir {
		return os.ModeDir | 0755
	}
	return 0644
}
func (fi fileInfo) ModTime() time.Time { return time.Unix(0, fi.mtime) }
func (fi fileInfo) IsDir() bool        { return fi.isDir }
func (fi fileInfo) Sys() any           { return nil }
