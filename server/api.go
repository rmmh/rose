package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

type FileHandle struct {
	id         int64
	path       string
	snapshotID uint64
	writeOpID  int64
	writeKey   string
	// cache holds pending modifications for a writable handle: it coalesces
	// out-of-order/overlapping writes, serves read-your-writes, and produces the
	// spliced placement list at Close. Nil for read-only and snapshot handles.
	cache *writeCache
}

func (s *Server) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	// Simple implementation
	path := cleanPath(req.GetPath())
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	id, err := s.db.OpenFile(ctx, path)
	if err != nil {
		return nil, err
	}

	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()
	hid := s.handleCounter
	s.handleCounter++

	h := &FileHandle{
		id:   id,
		path: path,
	}
	if req.GetOperationKey() != "" {
		op, err := s.db.CreateWriteOp(ctx, req.GetOperationKey(), path, nil)
		if err != nil {
			return nil, err
		}
		if op.Path != path {
			return nil, fmt.Errorf("write operation key is already bound to %q", op.Path)
		}
		h.writeOpID, h.writeKey = op.ID, op.IdempotencyKey
		if err := s.buildCache(ctx, h); err != nil {
			return nil, err
		}
	}
	s.handles[hid] = h

	slog.Info("Open", "handle", hid, "id", id, "path", path)

	ack := int64(0)
	if h.writeOpID != 0 {
		op, err := s.db.WriteOpByKey(ctx, h.writeKey)
		if err != nil {
			return nil, err
		}
		ack = op.AcknowledgedOffset
	}
	return &pb.OpenResponse{Handle: hid, AcknowledgedOffset: ack}, nil
}

func (s *Server) OpenSnapshot(ctx context.Context, req *pb.OpenSnapshotRequest) (*pb.OpenResponse, error) {
	if req.GetPath() == "" || req.GetSnapshotId() == 0 {
		return nil, fmt.Errorf("snapshot_id and path are required")
	}
	id, err := s.db.OpenSnapshotFile(ctx, req.GetSnapshotId(), cleanPath(req.GetPath()))
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("path not found in snapshot")
	}
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()
	hid := s.handleCounter
	s.handleCounter++
	s.handles[hid] = &FileHandle{id: id, path: req.GetPath(), snapshotID: req.GetSnapshotId()}
	return &pb.OpenResponse{Handle: hid}, nil
}

func (s *Server) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if err := s.db.UnlinkFile(ctx, cleanPath(req.GetPath())); err != nil {
		return nil, err
	}
	return &pb.UnlinkResponse{}, nil
}

func (s *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	if req.GetOldPath() == "" || req.GetNewPath() == "" {
		return nil, fmt.Errorf("old_path and new_path are required")
	}
	if err := s.db.RenameFile(ctx, cleanPath(req.GetOldPath()), cleanPath(req.GetNewPath())); err != nil {
		return nil, err
	}
	return &pb.RenameResponse{}, nil
}

func (s *Server) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	if req.GetName() == "" {
		return nil, fmt.Errorf("snapshot name cannot be empty")
	}
	id, err := s.db.CreateSnapshot(ctx, req.GetName(), time.Now().UnixNano())
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
	s.handlesMu.Lock()
	h, ok := s.handles[req.GetHandle()]
	s.handlesMu.Unlock()
	if !ok {
		slog.Error("Write failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
	}
	if h.snapshotID != 0 {
		return nil, fmt.Errorf("snapshot handles are read-only")
	}
	if err := s.ensureWriteOperation(ctx, h, req.GetHandle()); err != nil {
		return nil, err
	}
	mu := s.writeOperationLock(h.writeOpID)
	mu.Lock()
	defer mu.Unlock()
	op, err := s.db.WriteOpByKey(ctx, h.writeKey)
	if err != nil {
		return nil, err
	}
	if op.State == meta.WriteOpCommitted {
		return &pb.WriteResponse{AcknowledgedOffset: op.AcknowledgedOffset}, nil
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
	// AcknowledgedOffset is the handle-local logical size: monotonic for the
	// sequential writer the retry contract is built around. In-flight bytes are
	// made durable at Close, not here, so a resume re-sends them (idempotently).
	return &pb.WriteResponse{AcknowledgedOffset: h.cache.Length()}, nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.handlesMu.Lock()
	h, ok := s.handles[req.GetHandle()]
	s.handlesMu.Unlock()
	if !ok {
		slog.Error("Read failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
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
	chunks, err := s.db.FileVersionChunks(ctx, h.id)
	if err != nil {
		slog.Error("Read failed to get chunks", "fileID", h.id, "error", err)
		return nil, err
	}
	out, err := s.readChunksAt(ctx, chunks, req.GetOffset(), req.GetLength())
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
	var out []byte
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
			data, err := vlog.Read(ctx, placement.VaddrOffset+(readStart-cur), int(readEnd-readStart))
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

// maxRepointRetries bounds how many times resolveVlog will follow a compaction
// repoint before giving up. Each retry observes a distinct relocation, so a
// handful covers any realistic burst of back-to-back compactions; exceeding it
// means the chunk is genuinely unresolvable.
const maxRepointRetries = 16

// resolveVlog returns the mounted vlog and the placement to read a chunk from,
// following a compaction repoint when the caller's snapshotted placement names
// a vlog that has since been retired. Compaction relocates a live chunk's bytes
// into a fresh vlog and repoints the chunk row (RelocateChunk) before unmounting
// the old vlog (retireVlogLocked), both under vlogMu -- so a read holding a
// placement captured before the move finds its vlog gone. Because relocation is
// content-preserving, re-resolving the chunk by its content hash yields the same
// bytes at their new home, which is what this does. It only surfaces the
// not-mounted error when re-resolution makes no progress (the chunk row is gone
// or still points at the unmounted vlog), i.e. a genuine inconsistency.
func (s *Server) resolveVlog(ctx context.Context, chunk meta.ChunkPlacement) (*storage.Vlog, meta.ChunkPlacement, error) {
	for attempt := 0; ; attempt++ {
		s.vlogMu.Lock()
		vlog, ok := s.vlogs[chunk.VlogID]
		s.vlogMu.Unlock()
		if ok {
			return vlog, chunk, nil
		}
		if attempt >= maxRepointRetries {
			return nil, chunk, fmt.Errorf("vlog %d not mounted", chunk.VlogID)
		}
		fresh, found, err := s.db.ChunkByHash(ctx, chunk.Hash)
		if err != nil {
			return nil, chunk, err
		}
		if !found || fresh.VlogID == chunk.VlogID {
			// No live chunk row, or it still resolves to the unmounted vlog:
			// there is no repoint to follow, so fail with the original error.
			return nil, chunk, fmt.Errorf("vlog %d not mounted", chunk.VlogID)
		}
		chunk.VlogID = fresh.VlogID
		chunk.VaddrOffset = fresh.VaddrOffset
	}
}
func (s *Server) Getattr(ctx context.Context, req *pb.GetattrRequest) (*pb.GetattrResponse, error) {
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

func (s *Server) ListDir(ctx context.Context, req *pb.ListDirRequest) (*pb.ListDirResponse, error) {
	entries, err := s.db.ListDir(ctx, req.GetPath())
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
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if err := s.db.Mkdir(ctx, req.GetPath(), time.Now().UnixNano()); err != nil {
		return nil, err
	}
	return &pb.MkdirResponse{}, nil
}

func (s *Server) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if err := s.db.Rmdir(ctx, req.GetPath()); err != nil {
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

func (s *Server) writeOperationLock(id int64) *sync.Mutex {
	s.writeOpsMu.Lock()
	defer s.writeOpsMu.Unlock()
	mu := s.writeOps[id]
	if mu == nil {
		mu = &sync.Mutex{}
		s.writeOps[id] = mu
	}
	return mu
}

// ensureWriteOperation lazily supplies an operation for legacy callers that
// opened a read handle and then wrote it. New clients must supply operation_key
// to Open so an unknown Open outcome itself is retryable.
func (s *Server) ensureWriteOperation(ctx context.Context, h *FileHandle, handle int64) error {
	if h.writeOpID != 0 {
		return nil
	}
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()
	if h.writeOpID != 0 {
		return nil
	}
	key := fmt.Sprintf("legacy-handle-%d", handle)
	op, err := s.db.CreateWriteOp(ctx, key, h.path, nil)
	if err != nil {
		return err
	}
	h.writeOpID, h.writeKey = op.ID, key
	return nil
}

func (s *Server) leasedVlogForWrite(ctx context.Context, opID int64, path string, n int) (uint32, *storage.Vlog, error) {
	if int64(n) > MaxVlogBytes {
		return 0, nil, fmt.Errorf("chunk exceeds max vlog size")
	}
	leases, err := s.db.WriteOpLeases(ctx, opID)
	if err != nil {
		return 0, nil, err
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	for i := len(leases) - 1; i >= 0; i-- {
		if v := s.vlogs[leases[i]]; v != nil && v.Length()+int64(n) <= MaxVlogBytes {
			return leases[i], v, nil
		}
	}
	pol := s.bucketPolicyLocked(bucketOf(path))
	// Prefer a compatible, currently unlocked vlog.  The unique vlog_lease row
	// arbitrates concurrent claimers; a uniqueness error simply means another
	// operation won that candidate.
	for id, v := range s.vlogs {
		if v.Length()+int64(n) > MaxVlogBytes {
			continue
		}
		info, err := s.db.GetVlog(ctx, id)
		if err != nil {
			continue
		}
		if !vlogMatchesPolicy(info, pol) {
			continue
		}
		if err := s.db.ClaimVlogLease(ctx, id, opID, len(leases)); err == nil {
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
	return id, v, nil
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
		return s.provisionStagingVlogLocked(ctx, pol.DataShards, pol.ParityShards)
	}
	return s.provisionVlogLocked(ctx, pol.ProtectionScheme, pol.DataShards, pol.ParityShards)
}

func (s *Server) finishPlannedWriteChunks(ctx context.Context, op meta.WriteOp) error {
	chunks, err := s.db.WriteOpChunks(ctx, op.ID)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		if chunk.State == meta.WriteChunkDurable {
			continue
		}
		s.vlogMu.Lock()
		v := s.vlogs[chunk.VlogID]
		s.vlogMu.Unlock()
		if v == nil {
			return fmt.Errorf("leased vlog %d is not mounted", chunk.VlogID)
		}
		ready, err := s.CommitReady(ctx, chunk.VlogID)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("vlog %d is not commit-ready", chunk.VlogID)
		}
		if err := v.EnsureWrite(ctx, chunk.VaddrOffset, chunk.Data); err != nil {
			return err
		}
		if err := v.Commit(ctx, op.ID); err != nil {
			return err
		}
		if err := s.db.SetVlogLength(ctx, chunk.VlogID, v.Length()); err != nil {
			return err
		}
		if err := s.db.MarkWriteOpChunkDurable(ctx, op.ID, chunk.Index); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	s.handlesMu.Lock()
	h, ok := s.handles[req.GetHandle()]
	s.handlesMu.Unlock()
	if !ok {
		if req.GetIdempotencyKey() == "" {
			return nil, fmt.Errorf("invalid handle")
		}
		op, err := s.db.WriteOpByKey(ctx, req.GetIdempotencyKey())
		if err != nil {
			return nil, err
		}
		if op.State == meta.WriteOpCommitted {
			return &pb.CloseResponse{}, nil
		}
		return nil, fmt.Errorf("write operation %q has no active handle", req.GetIdempotencyKey())
	}
	if h.writeOpID == 0 {
		delete(s.handles, req.GetHandle())
		return &pb.CloseResponse{}, nil
	}
	mu := s.writeOperationLock(h.writeOpID)
	mu.Lock()
	defer mu.Unlock()
	op, err := s.db.WriteOpByKey(ctx, h.writeKey)
	if err != nil {
		return nil, err
	}
	if op.State != meta.WriteOpCommitted {
		if h.cache == nil {
			if err := s.buildCache(ctx, h); err != nil {
				return nil, err
			}
		}
		placements, err := s.finalizeCache(ctx, h)
		if err != nil {
			return nil, err
		}
		if _, err := s.db.CommitWriteOpVersion(ctx, op.ID, h.path, time.Now().UnixNano(), placements); err != nil {
			return nil, fmt.Errorf("publish write operation: %w", err)
		}
	}
	s.handlesMu.Lock()
	delete(s.handles, req.GetHandle())
	s.handlesMu.Unlock()
	return &pb.CloseResponse{}, nil
}

// buildCache loads the base version open at this handle and constructs its write
// cache. The chunk-index cursor resumes after any chunks a prior attempt already
// reserved for the operation, so a recovered handle keeps planned indexes unique.
func (s *Server) buildCache(ctx context.Context, h *FileHandle) error {
	base, err := s.db.FileVersionChunks(ctx, h.id)
	if err != nil {
		return err
	}
	existing, err := s.db.WriteOpChunks(ctx, h.writeOpID)
	if err != nil {
		return err
	}
	h.cache = newWriteCache(base, s.readChunksAt, len(existing))
	return nil
}

// spillCache drains the cache's contiguous dirty prefix to durable chunks while
// it exceeds the spill threshold, bounding per-handle memory on a large append.
// The caller holds the operation lock.
func (s *Server) spillCache(ctx context.Context, h *FileHandle) error {
	for {
		data := h.cache.spillPrefix()
		if data == nil {
			return nil
		}
		placements, err := s.storeChunks(ctx, h, data)
		if err != nil {
			return err
		}
		h.cache.commitSpill(placements, int64(len(data)))
	}
}

// storeChunks FastCDC-chunks a materialized byte window and stores each chunk
// durably (or reuses an existing one by content hash), returning the ordered
// placements. The chunks tile the input exactly, so their logical lengths sum to
// len(data).
func (s *Server) storeChunks(ctx context.Context, h *FileHandle, data []byte) ([]meta.ChunkPlacement, error) {
	chunker, err := chunkers.NewChunker("fastcdc", bytes.NewReader(data), nil)
	if err != nil {
		return nil, err
	}
	var placements []meta.ChunkPlacement
	for {
		chunk, nextErr := chunker.Next()
		if nextErr != nil && nextErr != io.EOF {
			return nil, nextErr
		}
		if len(chunk) > 0 {
			p, err := s.storeChunk(ctx, h, append([]byte(nil), chunk...))
			if err != nil {
				return nil, err
			}
			placements = append(placements, p)
		}
		if nextErr == io.EOF {
			break
		}
	}
	return placements, nil
}

// storeChunk makes one content-defined chunk durable and returns its placement.
// A chunk whose hash already exists is reused outright (dedup); otherwise its
// bytes are appended to a leased vlog under the operation and committed, reusing
// the existing write_op_chunk + vlog-lease durability machinery.
func (s *Server) storeChunk(ctx context.Context, h *FileHandle, data []byte) (meta.ChunkPlacement, error) {
	sum := sha256.Sum256(data)
	hash := sum[:15]
	if p, ok, err := s.db.ChunkByHash(ctx, hash); err != nil {
		return meta.ChunkPlacement{}, err
	} else if ok {
		return p, nil
	}
	vlogID, v, err := s.leasedVlogForWrite(ctx, h.writeOpID, h.path, len(data))
	if err != nil {
		return meta.ChunkPlacement{}, err
	}
	vaddr := v.Length()
	idx := h.cache.nextChunkIdx
	h.cache.nextChunkIdx++
	if err := s.db.AppendWriteOpChunk(ctx, h.writeOpID, meta.WriteOpChunk{
		Index: idx, Data: data, Hash: hash, VlogID: vlogID, VaddrOffset: vaddr, LogicalLen: len(data),
	}); err != nil {
		return meta.ChunkPlacement{}, err
	}
	if err := s.durableAppendChunk(ctx, h.writeOpID, idx, vlogID, vaddr, data); err != nil {
		return meta.ChunkPlacement{}, err
	}
	return meta.ChunkPlacement{Hash: append([]byte(nil), hash...), VlogID: vlogID, VaddrOffset: vaddr, LogicalLen: len(data), CompressedLen: len(data)}, nil
}

// durableAppendChunk seals one reserved chunk's bytes into its leased vlog and
// marks it durable -- the single-chunk form of finishPlannedWriteChunks.
func (s *Server) durableAppendChunk(ctx context.Context, opID int64, idx int, vlogID uint32, vaddr int64, data []byte) error {
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
	if err := v.EnsureWrite(ctx, vaddr, data); err != nil {
		return err
	}
	if err := v.Commit(ctx, opID); err != nil {
		return err
	}
	if err := s.db.SetVlogLength(ctx, vlogID, v.Length()); err != nil {
		return err
	}
	return s.db.MarkWriteOpChunkDurable(ctx, opID, idx)
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
		data, err := c.ReadAt(ctx, pos, windowEnd-pos)
		if err != nil {
			return nil, err
		}
		placements, err := s.storeChunks(ctx, h, data)
		if err != nil {
			return nil, err
		}
		result = append(result, placements...)
		pos = windowEnd
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
		if h.snapshotID != 0 {
			return nil, fmt.Errorf("snapshot handles are read-only")
		}
		if err := s.ensureWriteOperation(ctx, h, req.GetHandle()); err != nil {
			return nil, err
		}
		mu := s.writeOperationLock(h.writeOpID)
		mu.Lock()
		defer mu.Unlock()
		if h.cache == nil {
			if err := s.buildCache(ctx, h); err != nil {
				return nil, err
			}
		}
		h.cache.Truncate(req.GetSize())
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
	id, err := s.db.MakePlog(ctx, req.GetDiskId())
	if err != nil {
		return nil, err
	}
	plog, err := storage.OpenPlog(s.plogPath(req.GetDiskId(), id), id)
	if err != nil {
		return nil, err
	}
	s.plogs[id] = plog
	return &pb.MakePlogResponse{PlogId: id}, nil
}

func (s *Server) WritePlog(ctx context.Context, req *pb.WritePlogRequest) (*pb.WritePlogResponse, error) {
	plog, ok := s.plogs[req.GetPlogId()]
	if !ok {
		return nil, fmt.Errorf("plog not found")
	}
	offset, err := plog.Write(req.GetTxnId(), req.GetBuffer())
	if err != nil {
		return nil, err
	}
	return &pb.WritePlogResponse{Offset: uint32(offset)}, nil
}

func (s *Server) ReadPlog(ctx context.Context, req *pb.ReadPlogRequest) (*pb.ReadPlogResponse, error) {
	plog, ok := s.plogs[req.GetPlogId()]
	if !ok {
		return nil, fmt.Errorf("plog not found")
	}
	data, err := plog.Read(int64(req.GetOffset()), int(req.GetLength()))
	if err != nil {
		return nil, err
	}
	return &pb.ReadPlogResponse{Buffer: data}, nil
}

func (s *Server) CommitPlog(ctx context.Context, req *pb.CommitPlogRequest) (*pb.CommitPlogResponse, error) {
	for _, plog := range s.plogs {
		if err := plog.Commit(); err != nil {
			return nil, err
		}
	}
	return &pb.CommitPlogResponse{}, nil
}

func (s *Server) ReadVlog(ctx context.Context, req *pb.ReadVlogRequest) (*pb.ReadVlogResponse, error) {
	v, ok := s.vlogs[req.GetVlogId()]
	if !ok {
		return nil, fmt.Errorf("vlog not found")
	}

	data, err := v.Read(ctx, int64(req.GetOffset()), int(req.GetLength()))
	if err != nil {
		return nil, err
	}
	return &pb.ReadVlogResponse{Buffer: data}, nil
}

func (s *Server) WriteVlog(ctx context.Context, req *pb.WriteVlogRequest) (*pb.WriteVlogResponse, error) {
	v, ok := s.vlogs[req.GetVlogId()]
	if !ok {
		return nil, fmt.Errorf("vlog not found")
	}
	if v.Length()+int64(len(req.GetBuffer())) > MaxVlogBytes {
		return nil, fmt.Errorf("vlog %d would exceed max size %d", req.GetVlogId(), MaxVlogBytes)
	}

	offset, err := v.Write(ctx, req.GetTxnId(), req.GetBuffer())
	if err != nil {
		return nil, err
	}
	if err := s.db.SetVlogLength(ctx, req.GetVlogId(), v.Length()); err != nil {
		return nil, err
	}
	return &pb.WriteVlogResponse{Offset: uint32(offset)}, nil
}

func (s *Server) CommitVlog(ctx context.Context, req *pb.CommitVlogRequest) (*pb.CommitVlogResponse, error) {
	for _, vlog := range s.vlogs {
		if err := vlog.Commit(ctx, req.GetTxnId()); err != nil {
			return nil, err
		}
	}
	return &pb.CommitVlogResponse{}, nil
}

// Ensure the server implements pb.RoseServer
var _ pb.RoseServer = &Server{}
