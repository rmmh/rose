package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

type FileHandle struct {
	stateMu     sync.Mutex
	lastUsed    time.Time // renewed under stateMu; expiration fences under the same lock
	localOwners int       // in-process adapters explicitly retain handles until Release/Close
	id          int64
	// pathPtr is the namespace path the handle commits to. It is read on the
	// write path without the handle's write-op lock and mutated by Rename (to
	// retarget an open handle to its new name), so access goes through atomic
	// load/store to stay race-free.
	pathPtr      atomic.Pointer[string]
	snapshotID   uint64
	writeOpID    int64
	writeKey     string
	retainResult bool // client supplied a stable operation key
	// mtimeNs is an optional mtime override for an open write handle. FUSE can
	// receive futimens before a newly-created file has a published namespace row.
	mtimeNs  atomic.Int64
	mtimeSet atomic.Bool
	// cache holds pending modifications for a writable handle: it coalesces
	// out-of-order/overlapping writes, serves read-your-writes, and produces the
	// spliced placement list at Close. Nil for read-only and snapshot handles.
	cache        *writeCache
	chunker      *chunkers.Chunker
	chunkerInput bytes.Reader
	chunks       []meta.ChunkPlacement
	fileID64     uint64
	writeSeq     int64
	openedMtime  int64
	pinOwner     int64
	unlinked     bool
	writeTouched bool
}

// Every Read variant is a unary RPC, so its complete result must fit in memory.
// This is deliberately a result bound rather than a request bound: callers may
// ask for an enormous range on a small object and still receive the short bytes
// before EOF, but must chunk a genuinely large response.
const maxUnaryReadBytes = int64(64 << 20)

func (h *FileHandle) path() string {
	if p := h.pathPtr.Load(); p != nil {
		return *p
	}
	return ""
}

func (h *FileHandle) setPath(path string) { h.pathPtr.Store(&path) }

func (h *FileHandle) mtimeOrNow() int64 {
	if h.mtimeSet.Load() {
		return h.mtimeNs.Load()
	}
	return time.Now().UnixNano()
}

// handleStillRegistered closes the lookup-to-lock race for mutating handle
// operations. Close removes a handle while holding its stateMu; a Write or
// Truncate that looked it up earlier but acquired stateMu afterward must not
// mutate the now-orphaned object and report success for unpublished bytes.
func (s *Server) handleStillRegistered(handle int64, h *FileHandle) bool {
	s.handlesMu.Lock()
	current, ok := s.handles[handle]
	s.handlesMu.Unlock()
	return ok && current == h
}

func (s *Server) useHandle(handle int64, h *FileHandle) bool {
	if !s.handleStillRegistered(handle, h) {
		return false
	}
	h.lastUsed = s.now()
	return true
}

// RetainHandle gives an in-process adapter explicit ownership of a handle.
// Unlike an abandoned network connection, a mounted open fd can legitimately
// remain idle indefinitely. The owner must call the returned release function
// when its fd/request ends. Network clients renew through handle RPCs instead.
func (s *Server) RetainHandle(handle int64) (func(), error) {
	s.handlesMu.Lock()
	h, ok := s.handles[handle]
	s.handlesMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("invalid handle")
	}
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	if !s.useHandle(handle, h) {
		return nil, fmt.Errorf("invalid handle")
	}
	h.localOwners++
	var once sync.Once
	return func() {
		once.Do(func() {
			h.stateMu.Lock()
			defer h.stateMu.Unlock()
			h.localOwners--
		})
	}, nil
}

func (s *Server) discardHandle(handle int64) {
	s.handlesMu.Lock()
	h, ok := s.handles[handle]
	if ok {
		delete(s.handles, handle)
	}
	s.handlesMu.Unlock()
	if ok {
		s.releasePins(h.pinOwner)
	}
}

// AbortHandle discards an unpublished client write and durably releases every
// lease and pin it holds. It is used by protocol adapters when the client has
// abandoned the request, where committing a partial body would be incorrect but
// leaving the unreachable handle registered would prevent expiry and
// maintenance forever. Repeating an abort after the handle is gone is a no-op.
func (s *Server) AbortHandle(ctx context.Context, handle int64) error {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()

	s.handlesMu.Lock()
	h, ok := s.handles[handle]
	s.handlesMu.Unlock()
	if !ok {
		return nil
	}

	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	if !s.handleStillRegistered(handle, h) {
		return nil
	}
	var cancelErr error
	if h.writeOpID != 0 {
		cancelErr = s.db.CancelWriteOp(ctx, h.writeOpID)
		if cancelErr == nil {
			s.releasePins(h.writeOpID)
		}
	}
	// Even if the durable cancellation failed, this client is gone. Unregister
	// its unreachable handle so the write-op reaper can retry terminal cleanup;
	// retain the operation pins until that durable transition succeeds.
	s.handlesMu.Lock()
	delete(s.handles, handle)
	s.handlesMu.Unlock()
	s.releasePins(h.pinOwner)
	return cancelErr
}

func (s *Server) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if _, err := s.expireWriteResultsLocked(ctx); err != nil {
		return nil, err
	}
	// Simple implementation
	path := cleanPath(req.GetPath())
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	var openedMtime int64
	if entry, exists, err := s.db.StatPath(ctx, path); err != nil {
		return nil, err
	} else if exists && entry.IsDir {
		return nil, fmt.Errorf("path is a directory: %q", path)
	} else if exists {
		openedMtime = entry.Mtime
	}
	if err := s.validateFileAncestors(ctx, path); err != nil {
		return nil, err
	}
	// Resolve the opened version and publish its handle pin while holding the
	// same lock reclamation uses. GC cannot delete its chunk rows, and compaction
	// cannot pass its final pin check, in the gap between lookup and registration.
	s.pinMu.Lock()
	defer s.pinMu.Unlock()

	id, err := s.db.OpenFile(ctx, path)
	if err != nil {
		return nil, err
	}

	var chunks []meta.ChunkPlacement
	if id != 0 {
		chunks, err = s.db.FileVersionChunks(ctx, id)
		if err != nil {
			return nil, err
		}
	}

	s.handlesMu.Lock()
	hid := s.handleCounter
	s.handleCounter++
	s.handlesMu.Unlock()

	h := &FileHandle{lastUsed: s.now(), id: id, chunks: chunks, openedMtime: openedMtime, pinOwner: handlePinOwner(hid)}
	h.setPath(path)
	if req.GetOperationKey() != "" {
		op, err := s.db.CreateWriteOp(ctx, req.GetOperationKey(), path)
		if err != nil {
			return nil, err
		}
		if op.Path != path && !(op.State == meta.WriteOpCommitted && op.FileID == id) {
			return nil, fmt.Errorf("write operation key is already bound to %q", op.Path)
		}
		if op.State == meta.WriteOpCancelled || op.State == meta.WriteOpAbandoned || op.State == meta.WriteOpExpired {
			return nil, fmt.Errorf("write operation key is %s", op.State)
		}
		h.writeOpID, h.writeKey = op.ID, op.IdempotencyKey
		h.retainResult = true
		if op.State == meta.WriteOpCommitted {
			// A retry reads and validates the winning result, even after the
			// namespace head has been overwritten or removed.
			chunks, err = s.db.FileVersionChunks(ctx, op.FileID)
			if err != nil {
				return nil, err
			}
			h.id, h.chunks = op.FileID, chunks
			h.openedMtime, err = s.db.FileVersionMtime(ctx, op.FileID)
			if err != nil {
				return nil, err
			}
		}
		if err := s.ensureRecoveryFileID(ctx, h, op); err != nil {
			return nil, err
		}
		if err := s.buildCache(ctx, h); err != nil {
			return nil, err
		}
	}
	ack := int64(0)
	deadline := int64(0)
	if h.writeOpID != 0 {
		op, err := s.db.WriteOpByKey(ctx, h.writeKey)
		if err != nil {
			return nil, err
		}
		ack = op.AcknowledgedOffset
		deadline, err = s.db.WriteResultDeadline(ctx, op.ID)
		if err != nil {
			return nil, err
		}
	}
	s.replacePinsLocked(h.pinOwner, chunks)
	s.handlesMu.Lock()
	s.handles[hid] = h
	s.handlesMu.Unlock()

	slog.Info("Open", "handle", hid, "id", id, "path", path)
	return &pb.OpenResponse{Handle: hid, AcknowledgedOffset: ack, RetryExpiresAtNs: deadline}, nil
}

func (s *Server) OpenSnapshot(ctx context.Context, req *pb.OpenSnapshotRequest) (*pb.OpenResponse, error) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if req.GetPath() == "" || req.GetSnapshotId() == 0 {
		return nil, fmt.Errorf("snapshot_id and path are required")
	}
	path := cleanPath(req.GetPath())
	id, err := s.db.OpenSnapshotFile(ctx, req.GetSnapshotId(), path)
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("path not found in snapshot")
	}
	chunks, err := s.db.FileVersionChunks(ctx, id)
	if err != nil {
		return nil, err
	}
	s.handlesMu.Lock()
	hid := s.handleCounter
	s.handleCounter++
	s.handlesMu.Unlock()
	mtime, err := s.db.SnapshotFileMtime(ctx, req.GetSnapshotId(), path)
	if err != nil {
		return nil, err
	}
	hs := &FileHandle{lastUsed: s.now(),
		id: id, snapshotID: req.GetSnapshotId(), chunks: chunks,
		openedMtime: mtime, pinOwner: handlePinOwner(hid),
	}
	hs.setPath(path)
	s.replacePinsLocked(hs.pinOwner, chunks)
	s.handlesMu.Lock()
	s.handles[hid] = hs
	s.handlesMu.Unlock()
	return &pb.OpenResponse{Handle: hid}, nil
}

func (s *Server) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	path := cleanPath(req.GetPath())
	if entry, exists, err := s.db.StatPath(ctx, path); err != nil {
		return nil, err
	} else if exists && entry.IsDir {
		return nil, fmt.Errorf("path is a directory: %q", path)
	}
	if err := s.db.UnlinkFile(ctx, path); err != nil {
		return nil, err
	}
	if err := s.markOpenHandlesUnlinked(ctx, path, false); err != nil {
		return nil, err
	}
	return &pb.UnlinkResponse{}, nil
}

func (s *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if req.GetOldPath() == "" || req.GetNewPath() == "" {
		return nil, fmt.Errorf("old_path and new_path are required")
	}
	oldPath, newPath := cleanPath(req.GetOldPath()), cleanPath(req.GetNewPath())
	if err := s.validateFileAncestors(ctx, newPath); err != nil {
		return nil, err
	}
	err := s.db.RenameFile(ctx, oldPath, newPath)
	if errors.Is(err, sql.ErrNoRows) {
		// The source has no committed file head. It may still exist as an open
		// write handle whose data has not been published yet -- rsync renames its
		// temp file into place before closing it. Retarget the live handle so its
		// pending bytes commit at the new path on Close, and report success.
		if !s.hasOpenHandleAtOrBelow(oldPath) {
			return nil, err
		}
		if entry, exists, statErr := s.db.StatPath(ctx, newPath); statErr != nil {
			return nil, statErr
		} else if exists && entry.IsDir {
			return nil, fmt.Errorf("cannot replace directory %q with a file", newPath)
		}
		if oldPath != newPath {
			if err := s.markOpenHandlesUnlinked(ctx, newPath, false); err != nil {
				return nil, err
			}
		}
		if found, retargetErr := s.retargetOpenHandles(ctx, oldPath, newPath); retargetErr != nil {
			return nil, retargetErr
		} else if found {
			return &pb.RenameResponse{}, nil
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if oldPath != newPath {
		// Rename has just replaced the destination name. Any writer that opened
		// the previous destination now refers to an unlinked object and must not
		// overwrite the renamed source when it eventually closes.
		if err := s.markOpenHandlesUnlinked(ctx, newPath, false); err != nil {
			return nil, err
		}
	}
	// The committed head moved; redirect any open write handle on the old path so
	// a later Close republishes at the new path instead of resurrecting the old.
	if _, err := s.retargetOpenHandles(ctx, oldPath, newPath); err != nil {
		return nil, err
	}
	return &pb.RenameResponse{}, nil
}

func (s *Server) hasOpenHandleAtOrBelow(path string) bool {
	s.handlesMu.Lock()
	handles := make([]*FileHandle, 0, len(s.handles))
	for _, h := range s.handles {
		handles = append(handles, h)
	}
	s.handlesMu.Unlock()
	for _, h := range handles {
		h.stateMu.Lock()
		handlePath := h.path()
		found := !h.unlinked && h.snapshotID == 0 &&
			(handlePath == path || strings.HasPrefix(handlePath, path+"/"))
		h.stateMu.Unlock()
		if found {
			return true
		}
	}
	return false
}

func (s *Server) hasOpenHandleAtOrAbove(path string) bool {
	s.handlesMu.Lock()
	handles := make([]*FileHandle, 0, len(s.handles))
	for _, h := range s.handles {
		handles = append(handles, h)
	}
	s.handlesMu.Unlock()
	for _, h := range handles {
		h.stateMu.Lock()
		handlePath := h.path()
		found := !h.unlinked && h.snapshotID == 0 &&
			(handlePath == path || strings.HasPrefix(path, handlePath+"/"))
		h.stateMu.Unlock()
		if found {
			return true
		}
	}
	return false
}

func (s *Server) validateFileAncestors(ctx context.Context, path string) error {
	slash := strings.LastIndexByte(path, '/')
	if slash < 0 {
		return nil
	}
	parent := path[:slash]
	if s.hasOpenHandleAtOrAbove(parent) {
		return fmt.Errorf("regular file cannot be used as parent of %q", path)
	}
	for ancestor := parent; ancestor != ""; {
		entry, exists, err := s.db.StatPath(ctx, ancestor)
		if err != nil {
			return err
		}
		if exists && !entry.IsDir {
			return fmt.Errorf("regular file %q cannot be used as a parent directory", ancestor)
		}
		slash = strings.LastIndexByte(ancestor, '/')
		if slash < 0 {
			break
		}
		ancestor = ancestor[:slash]
	}
	return nil
}

// markOpenHandlesUnlinked prevents a later Close from publishing a pending
// version back at a name (or removed directory subtree) that has already been
// removed. namespaceMu keeps new opens and closes on the other side of the
// removal's linearization point.
func (s *Server) markOpenHandlesUnlinked(ctx context.Context, path string, descendants bool) error {
	s.handlesMu.Lock()
	handles := make([]*FileHandle, 0, len(s.handles))
	for _, h := range s.handles {
		handles = append(handles, h)
	}
	s.handlesMu.Unlock()
	opIDs := make(map[int64]struct{})
	for _, h := range handles {
		h.stateMu.Lock()
		handlePath := h.path()
		if h.snapshotID == 0 &&
			(handlePath == path || descendants && strings.HasPrefix(handlePath, path+"/")) {
			h.unlinked = true
			if h.writeOpID != 0 {
				opIDs[h.writeOpID] = struct{}{}
			}
		}
		h.stateMu.Unlock()
	}
	for opID := range opIDs {
		if err := s.db.CancelWriteOp(ctx, opID); err != nil {
			return err
		}
	}
	return nil
}

// retargetOpenHandles repoints every open handle for oldPath, or below oldPath
// when it is a directory, so pending writes follow the namespace rename rather
// than recreating an entry below the old name on Close. It reports whether any
// handle matched. The caller must hold no handle lock.
func (s *Server) retargetOpenHandles(ctx context.Context, oldPath, newPath string) (bool, error) {
	s.handlesMu.Lock()
	handles := make([]*FileHandle, 0, len(s.handles))
	for _, h := range s.handles {
		handles = append(handles, h)
	}
	s.handlesMu.Unlock()
	found := false
	for _, h := range handles {
		h.stateMu.Lock()
		if h.unlinked {
			h.stateMu.Unlock()
			continue
		}
		path := h.path()
		target := ""
		if path == oldPath {
			target = newPath
		} else if strings.HasPrefix(path, oldPath+"/") {
			target = newPath + strings.TrimPrefix(path, oldPath)
		}
		if target != "" {
			if h.writeOpID != 0 {
				if err := s.db.RetargetPreparedWriteOp(ctx, h.writeOpID, target); err != nil {
					h.stateMu.Unlock()
					return found, err
				}
			}
			h.setPath(target)
			found = true
		}
		h.stateMu.Unlock()
	}
	return found, nil
}

func (s *Server) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	if req.GetName() == "" {
		return nil, fmt.Errorf("snapshot name cannot be empty")
	}
	id, err := s.db.CreateSnapshot(ctx, req.GetName(), s.now().UnixNano())
	if err != nil {
		return nil, err
	}
	return &pb.CreateSnapshotResponse{SnapshotId: id}, nil
}

func (s *Server) DeleteSnapshot(ctx context.Context, req *pb.DeleteSnapshotRequest) (*pb.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == 0 {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	if err := s.db.DeleteSnapshot(ctx, req.GetSnapshotId()); err != nil {
		return nil, err
	}
	return &pb.DeleteSnapshotResponse{}, nil
}

func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if req.GetOffset() < 0 {
		return nil, fmt.Errorf("negative write offset")
	}
	if int64(len(req.GetBuffer())) > math.MaxInt64-req.GetOffset() {
		return nil, fmt.Errorf("write range overflows int64")
	}
	s.handlesMu.Lock()
	h, ok := s.handles[req.GetHandle()]
	s.handlesMu.Unlock()
	if !ok {
		slog.Error("Write failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
	}
	return s.writeHandle(ctx, req, h)
}

// writeHandle is the post-lookup half of Write, split out so deterministic
// concurrency tests can pause at the lookup-to-state-lock cut point.
func (s *Server) writeHandle(ctx context.Context, req *pb.WriteRequest, h *FileHandle) (*pb.WriteResponse, error) {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	if !s.useHandle(req.GetHandle(), h) {
		return nil, fmt.Errorf("invalid handle")
	}
	if h.snapshotID != 0 {
		return nil, fmt.Errorf("snapshot handles are read-only")
	}
	if err := s.ensureWriteOperation(ctx, h, req.GetHandle()); err != nil {
		return nil, err
	}
	unlock := s.writeOperationLock(h.writeOpID)
	defer unlock()
	op, err := s.writeOpForMutation(ctx, h.writeKey)
	if err != nil {
		return nil, err
	}
	if op.State == meta.WriteOpCommitted {
		if err := s.validateCommittedWriteRetry(ctx, op, req); err != nil {
			return nil, err
		}
		return &pb.WriteResponse{AcknowledgedOffset: op.AcknowledgedOffset}, nil
	}
	if op.State != meta.WriteOpPrepared {
		return nil, fmt.Errorf("write operation is %s", op.State)
	}
	if h.cache == nil {
		if err := s.buildCache(ctx, h); err != nil {
			return nil, err
		}
	}
	// Any offset, any order, overlapping, extending: the cache coalesces them.
	// A re-issued identical interval rewrites the same bytes, so it is idempotent.
	h.cache.WriteAt(req.GetOffset(), req.GetBuffer())
	if err := s.spillCache(ctx, h); err != nil {
		return nil, err
	}
	h.writeTouched = true
	// AcknowledgedOffset is the handle-local logical size: monotonic for the
	// sequential writer the retry contract is built around. In-flight bytes are
	// made durable at Close, not here, so a resume re-sends them (idempotently).
	return &pb.WriteResponse{AcknowledgedOffset: h.cache.Length()}, nil
}

func (s *Server) validateCommittedWriteRetry(ctx context.Context, op meta.WriteOp, req *pb.WriteRequest) error {
	end := req.GetOffset() + int64(len(req.GetBuffer()))
	if end > op.AcknowledgedOffset {
		return fmt.Errorf("conflicting retry for committed write operation %q", op.IdempotencyKey)
	}
	if len(req.GetBuffer()) == 0 {
		return nil
	}
	placements, err := s.db.FileVersionChunks(ctx, op.FileID)
	if err != nil {
		return err
	}
	committed, err := s.readChunksAt(ctx, placements, req.GetOffset(), int64(len(req.GetBuffer())))
	if err != nil {
		return err
	}
	if !bytes.Equal(committed, req.GetBuffer()) {
		return fmt.Errorf("conflicting retry for committed write operation %q", op.IdempotencyKey)
	}
	return nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.GetOffset() < 0 {
		return nil, fmt.Errorf("negative read offset")
	}
	if req.GetLength() < 0 {
		return nil, fmt.Errorf("negative read length")
	}
	if req.GetLength() > math.MaxInt64-req.GetOffset() {
		return nil, fmt.Errorf("read range overflows int64")
	}
	s.handlesMu.Lock()
	h, ok := s.handles[req.GetHandle()]
	s.handlesMu.Unlock()
	if !ok {
		slog.Error("Read failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
	}
	return s.readHandle(ctx, req, h)
}

// readHandle is the post-lookup half of Read, split out so deterministic
// concurrency tests can pause at the lookup-to-state-lock cut point.
func (s *Server) readHandle(ctx context.Context, req *pb.ReadRequest, h *FileHandle) (*pb.ReadResponse, error) {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	if !s.useHandle(req.GetHandle(), h) {
		return nil, fmt.Errorf("invalid handle")
	}
	if err := s.refreshCommittedHandle(ctx, h); err != nil {
		return nil, err
	}

	// A writable handle reads through its cache, so it sees its own uncommitted
	// writes (read-your-writes) overlaid on the opened version.
	if h.cache != nil {
		out, err := h.cache.ReadAt(ctx, req.GetOffset(), req.GetLength())
		if err != nil {
			return nil, err
		}
		return &pb.ReadResponse{Buffer: out}, nil
	}

	if h.id == 0 {
		return &pb.ReadResponse{Buffer: nil}, nil
	}
	out, err := s.readChunksAt(ctx, h.chunks, req.GetOffset(), req.GetLength())
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Buffer: out}, nil
}

// readChunksAt assembles the logical byte range [off, off+length) from an ordered
// chunk placement list, reading each overlapped chunk's bytes from its vlog. It
// is shared by Read (committed versions) and the write cache (base/settled
// fall-through).
func (s *Server) readChunksAt(ctx context.Context, chunks []meta.ChunkPlacement, off, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}
	var total int64
	for _, chunk := range chunks {
		total += int64(chunk.LogicalLen)
	}
	if off >= total {
		return nil, nil
	}
	resultLen := min(length, total-off)
	if resultLen > maxUnaryReadBytes {
		return nil, fmt.Errorf("read result length %d exceeds unary limit %d", resultLen, maxUnaryReadBytes)
	}
	// The requested length may be much larger than the bytes available before
	// EOF. Never use the untrusted request directly as an allocation capacity.
	const readPreallocLimit = int64(1 << 20)
	capacity := min(resultLen, readPreallocLimit)
	out := make([]byte, 0, int(capacity))
	var cur int64
	for _, chunk := range chunks {
		end := cur + int64(chunk.LogicalLen)
		if off < end && off+length > cur {
			vlog, placement, err := s.resolveVlog(ctx, chunk)
			if err != nil {
				return nil, err
			}
			readStart := cur
			if off > readStart {
				readStart = off
			}
			readEnd := end
			if off+length < readEnd {
				readEnd = off + length
			}
			data, err := s.readChunkPayload(ctx, vlog, placement, readStart-cur, int(readEnd-readStart))
			s.endVlogOp(placement.VlogID)
			if err != nil {
				return nil, err
			}
			out = append(out, data...)
		}
		cur = end
		if cur >= off+length {
			break
		}
	}
	return out, nil
}

// resolveVlog resolves content identity to the current placement and holds the
// mounted storage against retirement until endVlogOp.
func (s *Server) resolveVlog(ctx context.Context, chunk meta.ChunkPlacement) (*storage.Vlog, meta.ChunkPlacement, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	// Follow the canonical live placement even while the old vlog is mounted.
	// A rewrite can replace a dead location without retiring its whole vlog.
	// Unpublished cache chunks have no live row and retain their own placement.
	fresh, found, err := s.db.LiveChunkByHash(ctx, chunk.Hash)
	if err != nil {
		return nil, chunk, err
	}
	if found {
		chunk = fresh
	}
	vlog, ok := s.vlogs[chunk.VlogID]
	if !ok {
		return nil, chunk, fmt.Errorf("vlog %d not mounted", chunk.VlogID)
	}
	s.beginVlogOpLocked(chunk.VlogID)
	return vlog, chunk, nil
}

func (s *Server) Getattr(ctx context.Context, req *pb.GetattrRequest) (*pb.GetattrResponse, error) {
	if req.GetSnapshotId() != 0 {
		if req.GetHandle() != 0 {
			return nil, fmt.Errorf("snapshot stat cannot also specify a handle")
		}
		entry, exists, err := s.db.StatSnapshotPath(ctx, req.GetSnapshotId(), req.GetPath())
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("snapshot path not found")
		}
		return &pb.GetattrResponse{Size: entry.Size, IsDir: entry.IsDir, Mtime: entry.Mtime}, nil
	}

	// A stat against an open write handle must reflect read-your-writes within the
	// handle: the committed file head is not updated until Close, so serve the
	// live size from the handle's write cache.
	if req.GetHandle() != 0 {
		s.handlesMu.Lock()
		h, ok := s.handles[req.GetHandle()]
		s.handlesMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("invalid handle")
		}
		if ok {
			h.stateMu.Lock()
			if !s.useHandle(req.GetHandle(), h) {
				h.stateMu.Unlock()
				return nil, fmt.Errorf("invalid handle")
			}
			if err := s.refreshCommittedHandle(ctx, h); err != nil {
				h.stateMu.Unlock()
				return nil, err
			}
			if h.cache != nil {
				response := &pb.GetattrResponse{Size: h.cache.Length(), Mtime: h.mtimeOrNow()}
				h.stateMu.Unlock()
				return response, nil
			}
			if h.id != 0 {
				var size int64
				for _, chunk := range h.chunks {
					size += int64(chunk.LogicalLen)
				}
				response := &pb.GetattrResponse{Size: size, Mtime: h.openedMtime}
				h.stateMu.Unlock()
				return response, nil
			}
			h.stateMu.Unlock()
			return &pb.GetattrResponse{}, nil
		}
	}
	entry, ok, err := s.db.StatPath(ctx, req.GetPath())
	if err != nil {
		slog.Error("Getattr failed", "path", req.GetPath(), "error", err)
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("path not found: %q", req.GetPath())
	}
	return &pb.GetattrResponse{Size: entry.Size, IsDir: entry.IsDir, Mtime: entry.Mtime}, nil
}

// refreshCommittedHandle makes a duplicate retry handle converge after another
// handle for the same operation publishes. Until commit, each retry may carry a
// private replay cache; afterward the immutable committed version is canonical.
func (s *Server) refreshCommittedHandle(ctx context.Context, h *FileHandle) error {
	if h.writeOpID == 0 || h.cache == nil {
		return nil
	}
	// Preserve locally acknowledged writes until Close can compare them with a
	// peer's committed result for the same idempotency key.
	if h.writeTouched {
		return nil
	}
	unlock := s.writeOperationLock(h.writeOpID)
	defer unlock()
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	op, err := s.db.WriteOpByKey(ctx, h.writeKey)
	if err != nil {
		return err
	}
	if op.State != meta.WriteOpCommitted {
		return nil
	}
	chunks, err := s.db.FileVersionChunks(ctx, op.FileID)
	if err != nil {
		return err
	}
	h.id = op.FileID
	h.chunks = chunks
	if h.openedMtime, err = s.db.FileVersionMtime(ctx, op.FileID); err != nil {
		return err
	}
	h.cache = nil
	s.replacePinsLocked(h.pinOwner, chunks)
	return nil
}

// Setattr applies metadata-only changes to an existing path. Today the only
// mutable attribute is the modification time (utimes); size changes go through
// Truncate, and mode/owner are not persisted.
func (s *Server) Setattr(ctx context.Context, req *pb.SetattrRequest) (*pb.SetattrResponse, error) {
	if req.Mtime != nil {
		ok, err := s.db.SetMtime(ctx, req.GetPath(), req.GetMtime())
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("path not found: %q", req.GetPath())
		}
	}
	return &pb.SetattrResponse{}, nil
}

// SetHandleMtime applies an mtime update to an open handle. For unpublished
// write handles, the value is carried until Close publishes the file head.
func (s *Server) SetHandleMtime(ctx context.Context, handle int64, mtime int64) error {
	s.handlesMu.Lock()
	h, ok := s.handles[handle]
	s.handlesMu.Unlock()
	if !ok {
		return fmt.Errorf("invalid handle")
	}
	return s.setHandleMtime(ctx, handle, mtime, h)
}

// setHandleMtime is the post-lookup half of SetHandleMtime, split out so
// deterministic concurrency tests can pause at the lookup-to-state-lock cut point.
func (s *Server) setHandleMtime(ctx context.Context, handle int64, mtime int64, h *FileHandle) error {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	if !s.useHandle(handle, h) {
		return fmt.Errorf("invalid handle")
	}
	if h.snapshotID != 0 {
		return fmt.Errorf("snapshot handles are read-only")
	}
	if h.unlinked {
		return fmt.Errorf("handle refers to an unlinked file")
	}
	if h.writeOpID != 0 {
		if _, err := s.writeOpForMutation(ctx, h.writeKey); err != nil {
			return err
		}
		h.mtimeNs.Store(mtime)
		h.mtimeSet.Store(true)
		return nil
	}
	ok, err := s.db.SetMtime(ctx, h.path(), mtime)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("path not found: %q", h.path())
	}
	h.openedMtime = mtime
	h.mtimeNs.Store(mtime)
	h.mtimeSet.Store(true)
	return nil
}

func (s *Server) ListDir(ctx context.Context, req *pb.ListDirRequest) (*pb.ListDirResponse, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	var entries []meta.DirEntry
	var err error
	if req.GetSnapshotId() != 0 {
		entries, err = s.db.ListSnapshotDir(ctx, req.GetSnapshotId(), req.GetPath())
	} else {
		entries, err = s.db.ListDir(ctx, req.GetPath())
	}
	if err != nil {
		return nil, err
	}
	out := make([]*pb.DirEntry, len(entries))
	for i, e := range entries {
		out[i] = &pb.DirEntry{Name: e.Name, IsDir: e.IsDir, Size: e.Size, Mtime: e.Mtime}
	}
	return &pb.ListDirResponse{Entries: out}, nil
}

func (s *Server) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	path := cleanPath(req.GetPath())
	if s.hasOpenHandleAtOrAbove(path) {
		return nil, fmt.Errorf("mkdir %q: an open file occupies that path or an ancestor", path)
	}
	if err := s.db.Mkdir(ctx, path, time.Now().UnixNano()); err != nil {
		return nil, err
	}
	return &pb.MkdirResponse{}, nil
}

func (s *Server) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	path := cleanPath(req.GetPath())
	if entry, exists, err := s.db.StatPath(ctx, path); err != nil {
		return nil, err
	} else if exists && !entry.IsDir {
		return nil, fmt.Errorf("path is not a directory: %q", path)
	}
	if err := s.db.Rmdir(ctx, path); err != nil {
		return nil, err
	}
	if err := s.markOpenHandlesUnlinked(ctx, path, true); err != nil {
		return nil, err
	}
	return &pb.RmdirResponse{}, nil
}

// provisionBucketVlogLocked provisions a fresh vlog under a bucket's protection
// policy and records it as the bucket's active vlog. The caller must hold vlogMu.
func (s *Server) provisionBucketVlogLocked(ctx context.Context, bucket string) (uint32, *storage.Vlog, error) {
	pol := s.bucketPolicyLocked(bucket)
	id, v, err := s.provisionVlogLocked(ctx, pol.ProtectionScheme, pol.DataShards, pol.ParityShards)
	if err != nil {
		return 0, nil, err
	}
	s.activeVlogByBucket[bucket] = id
	return id, v, nil
}

// activeVlogForAppend rolls a bucket's active vlog before an append would cross
// the 32-bit virtual-offset boundary, provisioning a fresh one under the
// bucket's protection policy when needed. The caller does not hold vlogMu.
func (s *Server) activeVlogForAppend(ctx context.Context, bucket string, n int) (uint32, *storage.Vlog, error) {
	if int64(n) > MaxVlogBytes {
		return 0, nil, fmt.Errorf("append of %d bytes exceeds max vlog size %d", n, MaxVlogBytes)
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if id := s.activeVlogByBucket[bucket]; id != 0 {
		if v, ok := s.vlogs[id]; ok && v.Length()+int64(n) <= MaxVlogBytes {
			return id, v, nil
		}
		delete(s.activeVlogByBucket, bucket)
	}
	return s.provisionBucketVlogLocked(ctx, bucket)
}

type operationLock struct {
	mu    sync.Mutex
	users int // holders and waiters; guarded by writeOpsMu
}

// A registry entry exists only while a holder or waiter owns it. Counting before
// blocking is essential: removing the old lock while a waiter retains its pointer
// would let a new caller use a different lock for the same operation.
func (s *Server) writeOperationLock(id int64) func() {
	s.writeOpsMu.Lock()
	lock := s.writeOps[id]
	if lock == nil {
		lock = &operationLock{}
		s.writeOps[id] = lock
	}
	lock.users++
	s.writeOpsMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.writeOpsMu.Lock()
		lock.users--
		if lock.users == 0 {
			delete(s.writeOps, id)
		}
		s.writeOpsMu.Unlock()
	}
}

// ensureWriteOperation lazily supplies an operation for legacy callers that
// opened a read handle and then wrote it. New clients must supply operation_key
// to Open so an unknown Open outcome itself is retryable.
func (s *Server) ensureWriteOperation(ctx context.Context, h *FileHandle, handle int64) error {
	if h.writeOpID != 0 {
		return nil
	}
	key := fmt.Sprintf("legacy-handle-%d-%d", handle, h.writeSeq)
	h.writeSeq++
	op, err := s.db.CreateWriteOp(ctx, key, h.path())
	if err != nil {
		return err
	}
	h.writeOpID, h.writeKey = op.ID, key
	h.retainResult = false
	if err := s.ensureRecoveryFileID(ctx, h, op); err != nil {
		return err
	}
	return nil
}

func (s *Server) ensureRecoveryFileID(ctx context.Context, h *FileHandle, op meta.WriteOp) error {
	if h.fileID64 != 0 {
		return nil
	}
	if len(op.Tail) >= 8 {
		h.fileID64 = binary.LittleEndian.Uint64(op.Tail[:8])
		if h.fileID64 != 0 {
			return nil
		}
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	h.fileID64 = binary.LittleEndian.Uint64(buf[:])
	if h.fileID64 == 0 {
		h.fileID64 = uint64(time.Now().UnixNano())
	}
	binary.LittleEndian.PutUint64(buf[:], h.fileID64)
	return s.db.SetWriteOpTail(ctx, op.ID, buf[:])
}

func (s *Server) leasedVlogForWriteReserved(ctx context.Context, opID int64, pol meta.BucketPolicy, n int, reserved map[uint32]int64) (uint32, *storage.Vlog, error) {
	if int64(n) > MaxVlogBytes {
		return 0, nil, fmt.Errorf("chunk exceeds max vlog size")
	}
	leases, err := s.db.WriteOpLeases(ctx, opID)
	if err != nil {
		return 0, nil, err
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	lengthOf := func(id uint32, v *storage.Vlog) int64 {
		if reserved == nil {
			return v.Length()
		}
		if n, ok := reserved[id]; ok {
			return n
		}
		return v.Length()
	}
	for i := len(leases) - 1; i >= 0; i-- {
		info, err := s.db.GetVlog(ctx, leases[i])
		if err != nil {
			return 0, nil, err
		}
		if !bytes.Equal(info.DedupDomain, pol.DedupDomain()) {
			continue
		}
		if v := s.vlogs[leases[i]]; v != nil && lengthOf(leases[i], v)+int64(n) <= MaxVlogBytes {
			if reserved != nil {
				reserved[leases[i]] = lengthOf(leases[i], v) + int64(n)
			}
			return leases[i], v, nil
		}
	}
	// Prefer a compatible, currently unlocked vlog.  The unique vlog_lease row
	// arbitrates concurrent claimers; a uniqueness error simply means another
	// operation won that candidate.
	for id, v := range s.vlogs {
		if lengthOf(id, v)+int64(n) > MaxVlogBytes {
			continue
		}
		active, err := s.vlogPlacementActiveLocked(ctx, id)
		if err != nil || !active {
			continue
		}
		info, err := s.db.GetVlog(ctx, id)
		if err != nil {
			continue
		}
		held, err := s.db.VlogHasRunningJob(ctx, id)
		if err != nil {
			return 0, nil, err
		}
		if held || info.MaintenanceOwned {
			continue
		}
		if !vlogMatchesPolicy(info, pol) || !bytes.Equal(info.DedupDomain, pol.DedupDomain()) || s.activeVlogOps[id] != 0 || v.Length() != info.Length {
			continue
		}
		if err := s.db.ClaimVlogLease(ctx, id, opID, len(leases)); err == nil {
			if reserved != nil {
				reserved[id] = lengthOf(id, v) + int64(n)
			}
			return id, v, nil
		}
	}
	id, v, err := s.provisionForPolicyLocked(ctx, pol)
	if err != nil {
		return 0, nil, err
	}
	if err := s.db.ClaimVlogLease(ctx, id, opID, len(leases)); err != nil {
		return 0, nil, err
	}
	if reserved != nil {
		reserved[id] = lengthOf(id, v) + int64(n)
	}
	return id, v, nil
}

// vlogPlacementActiveLocked reports whether every shard of an existing vlog is
// still eligible for new writes. A draining disk remains reachable for an
// operation that already holds the vlog lease, but a different operation must
// not claim that vlog and extend the evacuation indefinitely.
func (s *Server) vlogPlacementActiveLocked(ctx context.Context, vlogID uint32) (bool, error) {
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		return false, err
	}
	if len(shards) == 0 {
		return false, nil
	}
	for _, sh := range shards {
		if s.offlinePlogs[sh.PlogID] || !s.diskLiveLocked(sh.DiskID) {
			return false, nil
		}
	}
	return true, nil
}

// vlogMatchesPolicy reports whether an existing vlog can hold chunks written
// under pol. An EC policy is served by a replicated staging vlog tagged with the
// EC scheme as its promotion target, never by writing chunks straight into an EC
// vlog (which only accepts whole stripe rows).
func vlogMatchesPolicy(info meta.VlogInfo, pol meta.BucketPolicy) bool {
	if pol.ProtectionScheme == "EC" {
		return info.IsStaging() &&
			int(info.TargetDataShards) == pol.DataShards &&
			int(info.TargetParityShards) == pol.ParityShards
	}
	return !info.IsStaging() &&
		info.ProtectionScheme == pol.ProtectionScheme &&
		int(info.DataShards) == pol.DataShards &&
		int(info.ParityShards) == pol.ParityShards
}

// provisionForPolicyLocked creates a vlog to receive chunks under pol: a
// replicated staging vlog for EC, or a plain vlog otherwise. The caller must
// hold vlogMu.
func (s *Server) provisionForPolicyLocked(ctx context.Context, pol meta.BucketPolicy) (uint32, *storage.Vlog, error) {
	if pol.ProtectionScheme == "EC" {
		return s.provisionStagingVlogInDomainLocked(ctx, pol.DataShards, pol.ParityShards, pol.DedupDomain())
	}
	return s.provisionVlogInDomainLocked(ctx, pol.ProtectionScheme, pol.DataShards, pol.ParityShards, pol.DedupDomain())
}

func (s *Server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	deadline, err := s.finishHandle(ctx, req.GetHandle(), true, req.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}
	return &pb.CloseResponse{RetryExpiresAtNs: deadline}, nil
}

// FlushHandle publishes the current write cache for an open handle without
// destroying the handle. FUSE Flush is issued for each close(2) of a duplicated
// file descriptor, so later writes may still arrive on the same open file
// description.
func (s *Server) FlushHandle(ctx context.Context, handle int64) error {
	_, err := s.finishHandle(ctx, handle, false, "")
	return err
}

func (s *Server) finishHandle(ctx context.Context, handle int64, remove bool, idempotencyKey string) (int64, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	if _, err := s.expireWriteResultsLocked(ctx); err != nil {
		return 0, err
	}
	s.handlesMu.Lock()
	h, ok := s.handles[handle]
	s.handlesMu.Unlock()
	if !ok {
		if idempotencyKey == "" {
			return 0, fmt.Errorf("invalid handle")
		}
		op, err := s.db.WriteOpByKey(ctx, idempotencyKey)
		if err != nil {
			return 0, err
		}
		if op.State == meta.WriteOpCommitted {
			return s.db.WriteResultDeadline(ctx, op.ID)
		}
		if op.State == meta.WriteOpCancelled {
			return 0, nil
		}
		return 0, fmt.Errorf("write operation %q has no active handle", idempotencyKey)
	}
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	h.lastUsed = s.now()
	if h.writeOpID != 0 && idempotencyKey != "" && idempotencyKey != h.writeKey {
		return 0, fmt.Errorf("close idempotency key does not match the write operation")
	}
	if h.unlinked && h.writeOpID != 0 {
		// Flush must not make an unlinked name visible, but the handle remains
		// usable until Release/Close. The final close abandons its unpublished
		// intent and releases every reclamation/lease hold.
		if !remove {
			return 0, nil
		}
		if err := s.db.CancelWriteOp(ctx, h.writeOpID); err != nil {
			return 0, err
		}
		s.releasePins(h.writeOpID)
		s.handlesMu.Lock()
		delete(s.handles, handle)
		s.handlesMu.Unlock()
		s.releasePins(h.pinOwner)
		return 0, nil
	}
	if h.writeOpID == 0 {
		if remove {
			s.handlesMu.Lock()
			delete(s.handles, handle)
			s.handlesMu.Unlock()
			s.releasePins(h.pinOwner)
		}
		return 0, nil
	}
	unlock := s.writeOperationLock(h.writeOpID)
	defer unlock()
	op, err := s.db.WriteOpByKey(ctx, h.writeKey)
	if err != nil {
		return 0, err
	}
	if op.State == meta.WriteOpExpired {
		return 0, fmt.Errorf("write operation key is expired")
	}
	var placements []meta.ChunkPlacement
	var fileID int64
	committedMtime := h.openedMtime
	if op.State != meta.WriteOpCommitted {
		if h.cache == nil {
			if err := s.buildCache(ctx, h); err != nil {
				return 0, err
			}
		}
		placements, err = s.finalizeCache(ctx, h)
		if err != nil {
			return 0, err
		}
		placements, err = s.rehomePublicationChunks(ctx, h, placements)
		if err != nil {
			return 0, err
		}
		committedMtime = h.mtimeOrNow()
		fileID, placements, err = s.publishPreparedVersion(ctx, op.ID, h.path(), committedMtime, placements, h.retainResult)
		if err != nil {
			return 0, fmt.Errorf("publish write operation: %w", err)
		}
	} else {
		fileID = op.FileID
		placements, err = s.db.FileVersionChunks(ctx, fileID)
		if err != nil {
			return 0, err
		}
		if h.writeTouched {
			if err := s.validateCommittedRetry(ctx, h, op, placements); err != nil {
				return 0, err
			}
		}
		committedMtime, err = s.db.FileVersionMtime(ctx, fileID)
		if err != nil {
			return 0, err
		}
		if h.mtimeSet.Load() && h.mtimeNs.Load() != committedMtime {
			return 0, fmt.Errorf("conflicting mtime retry for committed write operation %q", h.writeKey)
		}
	}
	deadline, err := s.db.WriteResultDeadline(ctx, op.ID)
	if err != nil {
		return 0, err
	}
	// Release preparation pins on both the first success and a successful retry
	// after the database committed but its reply was lost. The committed version
	// owns references now; handle pins are managed separately below.
	s.releasePins(op.ID)
	if remove {
		s.handlesMu.Lock()
		delete(s.handles, handle)
		s.handlesMu.Unlock()
		s.releasePins(h.pinOwner)
	} else {
		h.id = fileID
		h.chunks = placements
		h.openedMtime = committedMtime
		h.cache = nil
		h.writeTouched = false
		h.writeOpID = 0
		h.writeKey = ""
		h.retainResult = false
		h.fileID64 = 0
		h.mtimeNs.Store(0)
		h.mtimeSet.Store(false)
		s.replacePins(h.pinOwner, placements)
	}
	return deadline, nil
}

func (s *Server) validateCommittedRetry(ctx context.Context, h *FileHandle, op meta.WriteOp, placements []meta.ChunkPlacement) error {
	if h.cache == nil || h.cache.Length() != op.AcknowledgedOffset {
		return fmt.Errorf("conflicting retry for committed write operation %q", h.writeKey)
	}
	const compareBatch = int64(1 << 20)
	for offset := int64(0); offset < op.AcknowledgedOffset; offset += compareBatch {
		length := min(compareBatch, op.AcknowledgedOffset-offset)
		retried, err := h.cache.ReadAt(ctx, offset, length)
		if err != nil {
			return err
		}
		committed, err := s.readChunksAt(ctx, placements, offset, length)
		if err != nil {
			return err
		}
		if !bytes.Equal(retried, committed) {
			return fmt.Errorf("conflicting retry for committed write operation %q", h.writeKey)
		}
	}
	return nil
}

// buildCache loads the base version open at this handle and constructs its write
// cache.
func (s *Server) buildCache(ctx context.Context, h *FileHandle) error {
	h.cache = newWriteCache(h.chunks, s.readChunksAt)
	return nil
}

// spillCache drains the cache's contiguous dirty prefix to durable chunks while
// it exceeds the spill threshold, bounding per-handle memory on a large append.
// The caller holds the operation lock.
func (s *Server) spillCache(ctx context.Context, h *FileHandle) error {
	for {
		h.cache.mu.Lock()
		startOffset := h.cache.settledLen
		prev := placementHash64(h.cache.settled)
		ordinal := len(h.cache.settled)
		h.cache.mu.Unlock()
		data := h.cache.spillPrefix()
		if data == nil {
			return nil
		}
		placements, err := s.storeChunks(ctx, h, data, startOffset, prev, ordinal, false)
		if err != nil {
			return err
		}
		h.cache.commitSpill(placements, int64(len(data)))
	}
}

func placementHash64(placements []meta.ChunkPlacement) uint64 {
	if len(placements) == 0 {
		return 0
	}
	p := placements[len(placements)-1]
	if len(p.Hash) >= 8 {
		return binary.LittleEndian.Uint64(p.Hash[:8])
	}
	return 0
}

// pendingChunk is a new chunk's reserved vlog placement together with the bytes
// to seal there. The bytes are written to the append-only vlog and fsynced before
// the operation publishes its file version; they never touch the metadata DB.
type pendingChunk struct {
	vlogID  uint32
	vaddr   int64
	header  [storage.ChunkHeaderSize]byte
	payload []byte
}

func (c pendingChunk) len() int {
	return storage.ChunkHeaderSize + len(c.payload)
}

// FastCDC chunk sizing. The target (normal) size is ~1 MB per the design in
// README.md/plan.txt; the library requires NormalSize to be a power of two and
// MinSize < NormalSize < MaxSize. Coarser chunks keep metadata small at the cost
// of finer-grained deduplication.
const (
	chunkMinSize    = 256 * 1024
	chunkNormalSize = 1024 * 1024
	chunkMaxSize    = 4 * 1024 * 1024
)

var chunkBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, chunkMaxSize)
		return &buf
	},
}

func (h *FileHandle) chunkerForData(data []byte) (*chunkers.Chunker, error) {
	h.chunkerInput.Reset(data)
	if h.chunker != nil {
		h.chunker.Reset(&h.chunkerInput)
		return h.chunker, nil
	}
	chunker, err := chunkers.NewChunker("fastcdc", &h.chunkerInput, &chunkers.ChunkerOpts{
		MinSize:    chunkMinSize,
		NormalSize: chunkNormalSize,
		MaxSize:    chunkMaxSize,
	})
	if err != nil {
		return nil, err
	}
	h.chunker = chunker
	return chunker, nil
}

// storeChunks FastCDC-chunks a materialized byte window and stores each chunk
// durably (or reuses an existing one by content hash), returning the ordered
// placements. The chunks tile the input exactly, so their logical lengths sum to
// len(data).
func (s *Server) storeChunks(ctx context.Context, h *FileHandle, data []byte, startOffset int64, prevHash64 uint64, ordinalBase int, final bool) ([]meta.ChunkPlacement, error) {
	chunker, err := h.chunkerForData(data)
	if err != nil {
		return nil, err
	}
	var placements []meta.ChunkPlacement
	var pending []pendingChunk
	var pooledBufs []*[]byte

	defer func() {
		for _, bufPtr := range pooledBufs {
			chunkBufferPool.Put(bufPtr)
		}
	}()

	reserved := map[uint32]int64{}
	localOffset := int64(0)
	ordinal := ordinalBase
	for {
		chunk, nextErr := chunker.Next()
		if nextErr != nil && nextErr != io.EOF {
			return nil, nextErr
		}
		if len(chunk) > 0 {
			var chunkCopy []byte
			if len(chunk) <= chunkMaxSize {
				bufPtr := chunkBufferPool.Get().(*[]byte)
				pooledBufs = append(pooledBufs, bufPtr)
				chunkCopy = (*bufPtr)[:len(chunk)]
			} else {
				chunkCopy = make([]byte, len(chunk))
			}
			copy(chunkCopy, chunk)

			fileOffset := startOffset + localOffset
			isLast := final && nextErr == io.EOF

			p, plan, err := s.planChunk(ctx, h, chunkCopy, reserved, fileOffset, prevHash64, ordinal, isLast)
			if err != nil {
				return nil, err
			}
			placements = append(placements, p)
			if plan != nil {
				pending = append(pending, *plan)
			}
			prevHash64 = binary.LittleEndian.Uint64(p.Hash[:8])
			localOffset += int64(len(chunkCopy))
			ordinal++
		}
		if nextErr == io.EOF {
			break
		}
	}
	if err := s.sealChunks(ctx, pending); err != nil {
		return nil, err
	}
	return placements, nil
}

// planChunk plans one content-defined chunk and returns its placement. A chunk
// whose hash already exists is reused outright (dedup); otherwise its exact
// reserved placement is returned alongside the bytes to seal, so a later grouped
// vlog commit can make a whole batch durable together.
func (s *Server) planChunk(ctx context.Context, h *FileHandle, data []byte, reserved map[uint32]int64, fileOffset int64, prevHash64 uint64, ordinal int, last bool) (meta.ChunkPlacement, *pendingChunk, error) {
	s.vlogMu.Lock()
	pol := s.bucketPolicyLocked(bucketOf(h.path()))
	s.vlogMu.Unlock()
	sum := storage.ContentHash(pol.DedupDomain(), data)
	hash := sum[:15]
	// Pin-and-resolve rather than a bare lookup: a dedup hit reuses an existing
	// chunk's bytes without rewriting them, so it must hold those bytes live
	// against GC and compaction until this operation commits. The pin is released
	// when the op commits (its file version takes the real refcount) or is
	// abandoned.
	if p, ok, err := s.pinAndResolveChunk(ctx, h.writeOpID, hash); err != nil {
		return meta.ChunkPlacement{}, nil, err
	} else if ok {
		return p, nil, nil
	}
	recordLen := storage.ChunkHeaderSize + len(data)
	vlogID, _, err := s.leasedVlogForWriteReserved(ctx, h.writeOpID, pol, recordLen, reserved)
	if err != nil {
		return meta.ChunkPlacement{}, nil, err
	}
	vaddr := reserved[vlogID] - int64(recordLen)
	flags := byte(0)
	if fileOffset == 0 {
		flags |= storage.ChunkFlagInitial
	}
	if last {
		flags |= storage.ChunkFlagLast
	}
	hdr := storage.ChunkHeader{
		Flags:      flags,
		FileID:     h.fileID64,
		ChunkHash:  binary.LittleEndian.Uint64(sum[:8]),
		PayloadLen: uint32(len(data)),
		FileOffset: uint64(fileOffset),
		PrevHash:   prevHash64,
		PathHint:   pathHint(h.path(), flags&storage.ChunkFlagInitial != 0, ordinal),
	}
	header, err := s.encryptChunkInPlace(ctx, vlogID, hash, hdr, data)
	if err != nil {
		return meta.ChunkPlacement{}, nil, err
	}
	pending := &pendingChunk{vlogID: vlogID, vaddr: vaddr, header: header, payload: data}
	return meta.ChunkPlacement{Hash: append([]byte(nil), hash...), VlogID: vlogID, VaddrOffset: vaddr, LogicalLen: len(data), CompressedLen: len(data)}, pending, nil
}

func pathHint(path string, initial bool, ordinal int) [32]byte {
	var out [32]byte
	if path == "" {
		return out
	}
	clean := filepath.Clean(path)
	if clean == "." {
		clean = ""
	}
	if initial {
		if len(clean) < len(out) {
			copy(out[:], clean)
			return out
		}
		out[0] = byte(len(clean))
		if len(clean) > 255 {
			out[0] = 255
		}
		dir := filepath.Dir(clean)
		h := fnv.New32a()
		_, _ = h.Write([]byte(dir))
		binary.LittleEndian.PutUint32(out[1:5], h.Sum32())
		copy(out[5:], clean[len(clean)-(len(out)-5):])
		return out
	}
	if len(clean) == 0 {
		return out
	}
	start := (ordinal * 16) % len(clean)
	for i := 0; i < 16; i++ {
		out[i] = clean[(start+i)%len(clean)]
	}
	return out
}

// sealChunks writes each new chunk's reserved bytes into its leased vlog,
// grouped by vlog and coalesced by contiguous reserved offset. Each contiguous
// run is passed to the vlog as a vectored write (`[][]byte`), avoiding the
// merge-concatenation copy while still issuing one logical EnsureWrite per run.
// It does not fsync or record vlog lengths: a single Commit per leased vlog,
// followed by SetVlogLength, runs once at Close before the file version is
// published, so the whole operation pays one durability barrier per vlog rather
// than one per spill. No chunk bytes are written to the metadata DB.
func (s *Server) sealChunks(ctx context.Context, chunks []pendingChunk) error {
	if len(chunks) == 0 {
		return nil
	}
	byVlog := make(map[uint32][]pendingChunk)
	order := make([]uint32, 0, len(chunks))
	for _, chunk := range chunks {
		if _, ok := byVlog[chunk.vlogID]; !ok {
			order = append(order, chunk.vlogID)
		}
		byVlog[chunk.vlogID] = append(byVlog[chunk.vlogID], chunk)
	}
	for _, vlogID := range order {
		group := byVlog[vlogID]
		sort.Slice(group, func(i, j int) bool { return group[i].vaddr < group[j].vaddr })
		s.vlogMu.Lock()
		v := s.vlogs[vlogID]
		s.vlogMu.Unlock()
		if v == nil {
			return fmt.Errorf("leased vlog %d is not mounted", vlogID)
		}
		ready, err := s.CommitReady(ctx, vlogID)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("vlog %d is not commit-ready", vlogID)
		}
		runStart := group[0].vaddr
		runEnd := runStart
		runParts := make([][]byte, 0, len(group))
		for i := range group {
			chunk := &group[i]
			if i > 0 && chunk.vaddr != runEnd {
				if err := v.EnsureWrite(ctx, runStart, runParts); err != nil {
					return err
				}
				runStart = chunk.vaddr
				runParts = runParts[:0]
			}
			runParts = append(runParts, chunk.header[:], chunk.payload)
			runEnd = chunk.vaddr + int64(chunk.len())
		}
		if err := v.EnsureWrite(ctx, runStart, runParts); err != nil {
			return err
		}
	}
	return nil
}

// finalizeCache produces the new version's ordered placement list by the
// dirty-window splice: the settled prefix verbatim, then untouched base chunks
// reused by hash, with only the modified windows materialized and re-chunked.
// Window boundaries are real base-chunk boundaries, so content-defined boundary
// shifts stay contained to the window and untouched neighbors round-trip
// byte-identical (and dedup-identical). The caller holds the operation lock.
func (s *Server) finalizeCache(ctx context.Context, h *FileHandle) ([]meta.ChunkPlacement, error) {
	c := h.cache
	c.mu.Lock()
	base := c.base
	baseLen := c.baseLen
	settled := append([]meta.ChunkPlacement(nil), c.settled...)
	settledLen := c.settledLen
	length := c.length
	spans := append([]span(nil), c.spans...)
	c.mu.Unlock()

	result := append([]meta.ChunkPlacement(nil), settled...)

	type bchunk struct {
		start, end int64
		p          meta.ChunkPlacement
	}
	var bchunks []bchunk
	var cur int64
	for _, p := range base {
		end := cur + int64(p.LogicalLen)
		if end > settledLen && cur < baseLen {
			bchunks = append(bchunks, bchunk{start: cur, end: end, p: p})
		}
		cur = end
	}
	spanOverlaps := func(a, b int64) bool {
		for _, sp := range spans {
			if sp.start < b && sp.end() > a {
				return true
			}
		}
		return false
	}
	isClean := func(bc bchunk) bool {
		return bc.start >= settledLen && bc.end <= baseLen && !spanOverlaps(bc.start, bc.end)
	}

	pos := settledLen
	for pos < length {
		reused := false
		for _, bc := range bchunks {
			if bc.start == pos && isClean(bc) {
				result = append(result, bc.p)
				pos = bc.end
				reused = true
				break
			}
		}
		if reused {
			continue
		}
		// Dirty window: from pos to the start of the next reusable clean base
		// chunk (a real boundary), or to length.
		windowEnd := length
		for _, bc := range bchunks {
			if bc.start > pos && isClean(bc) {
				windowEnd = bc.start
				break
			}
		}
		// Process the dirty window in bounded batches to avoid materializing a
		// huge allocation when the file was truncated to a very large size.
		for pos < windowEnd {
			batchEnd := windowEnd
			if batchEnd-pos > spillThreshold {
				batchEnd = pos + spillThreshold
			}
			data, err := c.ReadAt(ctx, pos, batchEnd-pos)
			if err != nil {
				return nil, err
			}
			placements, err := s.storeChunks(ctx, h, data, pos, placementHash64(result), len(result), batchEnd == length)
			if err != nil {
				return nil, err
			}
			result = append(result, placements...)
			pos = batchEnd
		}
	}
	return result, nil
}

func (s *Server) Truncate(ctx context.Context, req *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	if req.GetSize() < 0 {
		return nil, fmt.Errorf("negative truncate size")
	}
	// An open write handle truncates its cache in place; the new size takes effect
	// at the handle's Close.
	if req.GetHandle() != 0 {
		s.handlesMu.Lock()
		h, ok := s.handles[req.GetHandle()]
		s.handlesMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("invalid handle")
		}
		h.stateMu.Lock()
		defer h.stateMu.Unlock()
		if !s.useHandle(req.GetHandle(), h) {
			return nil, fmt.Errorf("invalid handle")
		}
		if h.snapshotID != 0 {
			return nil, fmt.Errorf("snapshot handles are read-only")
		}
		if err := s.ensureWriteOperation(ctx, h, req.GetHandle()); err != nil {
			return nil, err
		}
		unlock := s.writeOperationLock(h.writeOpID)
		defer unlock()
		if _, err := s.writeOpForMutation(ctx, h.writeKey); err != nil {
			return nil, err
		}
		if h.cache == nil {
			if err := s.buildCache(ctx, h); err != nil {
				return nil, err
			}
		}
		if err := h.cache.Truncate(ctx, req.GetSize()); err != nil {
			return nil, err
		}
		h.writeTouched = true
		return &pb.TruncateResponse{}, nil
	}

	// No handle: a truncate(2) by path. Open a transient write operation, apply
	// the size, and publish it immediately.
	if req.GetPath() == "" {
		return nil, fmt.Errorf("truncate requires a handle or path")
	}
	open, err := s.Open(ctx, &pb.OpenRequest{Path: req.GetPath(), OperationKey: req.GetOperationKey()})
	if err != nil {
		return nil, err
	}
	defer s.discardHandle(open.GetHandle())
	if _, err := s.Truncate(ctx, &pb.TruncateRequest{Handle: open.GetHandle(), Size: req.GetSize()}); err != nil {
		return nil, err
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: req.GetOperationKey()}); err != nil {
		return nil, err
	}
	return &pb.TruncateResponse{}, nil
}

// Vlog Operations
func (s *Server) MakeVlog(ctx context.Context, req *pb.MakeVlogRequest) (*pb.MakeVlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	id, _, err := s.provisionVlogLocked(ctx, req.GetProtectionScheme(), int(req.GetDataShards()), int(req.GetParityShards()))
	if err != nil {
		return nil, err
	}
	return &pb.MakeVlogResponse{VlogId: id}, nil
}

// Plog Operations
func (s *Server) MakePlog(ctx context.Context, req *pb.MakePlogRequest) (*pb.MakePlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if _, ok := s.diskRoots[req.GetDiskId()]; !ok {
		return nil, fmt.Errorf("disk %d is not configured", req.GetDiskId())
	}
	if !s.diskLiveLocked(req.GetDiskId()) {
		return nil, fmt.Errorf("disk %d is not active", req.GetDiskId())
	}
	plogUID := uid.New()
	id, err := s.db.MakePlog(ctx, plogUID, req.GetDiskId())
	if err != nil {
		return nil, err
	}
	discard := func(cause error) error {
		if cleanupErr := s.db.DiscardUnassignedPlog(ctx, id); cleanupErr != nil {
			return errors.Join(cause, cleanupErr)
		}
		return cause
	}
	// A bare plog created via the RPC has no vlog membership yet, so its
	// superblock carries only the cluster/plog/disk identity.
	header, err := s.basePlogHeader(ctx, id, req.GetDiskId(), plogUID)
	if err != nil {
		return nil, discard(err)
	}
	plog, err := storage.OpenPlog(s.plogPath(req.GetDiskId(), id), id, storage.WithHeader(header))
	if err != nil {
		return nil, discard(err)
	}
	s.plogs[id] = plog
	return &pb.MakePlogResponse{PlogId: id}, nil
}

func (s *Server) WritePlog(ctx context.Context, req *pb.WritePlogRequest) (*pb.WritePlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	plog, ok := s.plogs[req.GetPlogId()]
	if !ok {
		return nil, fmt.Errorf("plog not found")
	}
	owners, err := s.db.VlogsForPlog(ctx, req.GetPlogId())
	if err != nil {
		return nil, err
	}
	if len(owners) != 0 {
		return nil, fmt.Errorf("plog %d is owned by vlog %d", req.GetPlogId(), owners[0])
	}
	length := plog.LogicalLength()
	if length > math.MaxUint32 || int64(len(req.GetBuffer())) > MaxVlogBytes-length {
		return nil, fmt.Errorf("plog %d would exceed its 32-bit address space", req.GetPlogId())
	}
	offset, err := plog.Write(req.GetTxnId(), req.GetBuffer())
	if err != nil {
		return nil, err
	}
	return &pb.WritePlogResponse{Offset: uint32(offset)}, nil
}

func (s *Server) ReadPlog(ctx context.Context, req *pb.ReadPlogRequest) (*pb.ReadPlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	plog, ok := s.plogs[req.GetPlogId()]
	if !ok {
		return nil, fmt.Errorf("plog not found")
	}
	available := plog.LogicalLength() - int64(req.GetOffset())
	if available > 0 && min(int64(req.GetLength()), available) > maxUnaryReadBytes {
		return nil, fmt.Errorf("plog read result exceeds unary limit %d", maxUnaryReadBytes)
	}
	data, err := plog.Read(int64(req.GetOffset()), int(req.GetLength()))
	if err != nil {
		return nil, err
	}
	return &pb.ReadPlogResponse{Buffer: data}, nil
}

func (s *Server) CommitPlog(ctx context.Context, req *pb.CommitPlogRequest) (*pb.CommitPlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	for id, plog := range s.plogs {
		owners, err := s.db.VlogsForPlog(ctx, id)
		if err != nil {
			return nil, err
		}
		if len(owners) != 0 {
			continue
		}
		if err := plog.Commit(); err != nil {
			return nil, err
		}
	}
	return &pb.CommitPlogResponse{}, nil
}

func (s *Server) ReadVlog(ctx context.Context, req *pb.ReadVlogRequest) (*pb.ReadVlogResponse, error) {
	s.vlogMu.Lock()
	v, ok := s.vlogs[req.GetVlogId()]
	if ok {
		s.beginVlogOpLocked(req.GetVlogId())
	}
	s.vlogMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("vlog not found")
	}
	defer s.endVlogOp(req.GetVlogId())

	available := v.Length() - int64(req.GetOffset())
	if available > 0 && min(int64(req.GetLength()), available) > maxUnaryReadBytes {
		return nil, fmt.Errorf("vlog read result exceeds unary limit %d", maxUnaryReadBytes)
	}
	data, err := v.Read(ctx, int64(req.GetOffset()), int(req.GetLength()))
	if err != nil {
		return nil, err
	}
	return &pb.ReadVlogResponse{Buffer: data}, nil
}

func (s *Server) WriteVlog(ctx context.Context, req *pb.WriteVlogRequest) (*pb.WriteVlogResponse, error) {
	s.vlogMu.Lock()
	v, ok := s.vlogs[req.GetVlogId()]
	if ok {
		allowed, err := s.rawVlogWritableLocked(ctx, req.GetVlogId())
		if err != nil {
			s.vlogMu.Unlock()
			return nil, err
		}
		if !allowed {
			s.vlogMu.Unlock()
			return nil, fmt.Errorf("vlog %d is owned by file publication", req.GetVlogId())
		}
		s.beginVlogOpLocked(req.GetVlogId())
	}
	s.vlogMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("vlog not found")
	}
	defer s.endVlogOp(req.GetVlogId())
	if v.Length() > math.MaxUint32 {
		return nil, fmt.Errorf("vlog %d has no representable write offset", req.GetVlogId())
	}
	offset, err := v.WriteWithin(ctx, req.GetTxnId(), req.GetBuffer(), MaxVlogBytes)
	if err != nil {
		return nil, err
	}
	return &pb.WriteVlogResponse{Offset: uint32(offset)}, nil
}

// rawVlogWritableLocked keeps raw storage transactions out of file-owned logs.
// Scoped logs remain file-owned after their lease ends. Admission and lease
// acquisition share vlogMu, and file allocation excludes active raw requests.
func (s *Server) rawVlogWritableLocked(ctx context.Context, id uint32) (bool, error) {
	info, err := s.db.GetVlog(ctx, id)
	if err != nil {
		return false, err
	}
	if len(info.DedupDomain) != 0 || info.MaintenanceOwned {
		return false, nil
	}
	held, err := s.db.VlogHasRunningJob(ctx, id)
	if err != nil || held {
		return false, err
	}
	leased, err := s.db.VlogLeased(ctx, id)
	return !leased, err
}

func (s *Server) beginVlogOpLocked(vlogID uint32) {
	if s.activeVlogOps == nil {
		s.activeVlogOps = make(map[uint32]int)
	}
	s.activeVlogOps[vlogID]++
}

func (s *Server) endVlogOp(vlogID uint32) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.activeVlogOps[vlogID]--
	if s.activeVlogOps[vlogID] == 0 {
		delete(s.activeVlogOps, vlogID)
	}
}

func (s *Server) CommitVlog(ctx context.Context, req *pb.CommitVlogRequest) (*pb.CommitVlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	prefixes := make(map[uint32]int64, len(s.vlogs))
	for id, vlog := range s.vlogs {
		allowed, err := s.rawVlogWritableLocked(ctx, id)
		if err != nil {
			return nil, err
		}
		if !allowed {
			continue
		}
		prefix, err := vlog.CommitPrefix(ctx, req.GetTxnId())
		if err != nil {
			return nil, err
		}
		prefixes[id] = prefix
	}
	// Publish lengths only after every vlog's bytes are durable. If a commit
	// fails, recovery may safely trim physical tails back to the older catalog
	// lengths; the catalog must never get ahead of disk.
	for id, prefix := range prefixes {
		if err := s.db.SetVlogLength(ctx, id, prefix); err != nil {
			return nil, err
		}
	}
	return &pb.CommitVlogResponse{}, nil
}

// Ensure the server implements pb.RoseServer
var _ pb.RoseServer = &Server{}
