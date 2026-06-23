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
	if req.GetOffset() > op.AcknowledgedOffset {
		return nil, fmt.Errorf("write offset %d skips acknowledged offset %d", req.GetOffset(), op.AcknowledgedOffset)
	}
	if req.GetOffset() < op.AcknowledgedOffset {
		if req.GetOffset()+int64(len(req.GetBuffer())) > op.AcknowledgedOffset {
			return nil, fmt.Errorf("write overlaps acknowledged range")
		}
		return &pb.WriteResponse{AcknowledgedOffset: op.AcknowledgedOffset}, nil
	}
	// A previous attempt may have durably recorded this exact input as the
	// operation's tail before its response was lost.  Do not append it twice.
	if len(op.Tail) > 0 {
		if !bytes.Equal(op.Tail, req.GetBuffer()) {
			return nil, fmt.Errorf("retry data differs from pending write")
		}
	} else if err := s.db.UpdateWriteOpStream(ctx, op.ID, op.AcknowledgedOffset, append([]byte(nil), req.GetBuffer()...)); err != nil {
		return nil, err
	}
	if err := s.flushWriteOp(ctx, op.ID, op.AcknowledgedOffset+int64(len(req.GetBuffer()))); err != nil {
		return nil, err
	}
	return &pb.WriteResponse{AcknowledgedOffset: op.AcknowledgedOffset + int64(len(req.GetBuffer()))}, nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()

	slog.Info("Read", "handle", req.GetHandle(), "offset", req.GetOffset(), "length", req.GetLength())

	h, ok := s.handles[req.GetHandle()]
	if !ok {
		slog.Error("Read failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
	}

	if h.id == 0 {
		return &pb.ReadResponse{Buffer: nil}, nil
	}

	// Fetch chunks for this file
	chunks, err := s.db.GetFileChunks(ctx, h.id)
	if err != nil {
		slog.Error("Read failed to get chunks", "fileID", h.id, "error", err)
		return nil, err
	}

	slog.Info("Read chunks", "fileID", h.id, "chunks", len(chunks))

	var out []byte
	var currentLogicalOffset int64 = 0

	for _, chunk := range chunks {
		chunkEnd := currentLogicalOffset + int64(chunk.LogicalLen)

		// Check if this chunk intersects with the read request window
		if req.GetOffset() < chunkEnd && req.GetOffset()+req.GetLength() > currentLogicalOffset {
			vlog, ok := s.vlogs[chunk.VlogID]
			if !ok {
				slog.Error("Read map vlog missing", "vlogID", chunk.VlogID)
				return nil, fmt.Errorf("vlog %d not mounted", chunk.VlogID)
			}

			// Calculate how much to read from this chunk
			readStart := currentLogicalOffset
			if req.GetOffset() > readStart {
				readStart = req.GetOffset()
			}

			readEnd := chunkEnd
			if req.GetOffset()+req.GetLength() < readEnd {
				readEnd = req.GetOffset() + req.GetLength()
			}

			chunkOffset := readStart - currentLogicalOffset
			chunkReadLen := readEnd - readStart

			data, err := vlog.Read(ctx, chunk.VaddrOffset+chunkOffset, int(chunkReadLen))
			if err != nil {
				return nil, err
			}
			out = append(out, data...)
		}

		currentLogicalOffset += int64(chunk.LogicalLen)

		// Optimization: Break early if we've fulfilled the read length
		if currentLogicalOffset >= req.GetOffset()+req.GetLength() {
			break
		}
	}

	return &pb.ReadResponse{Buffer: out}, nil
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

func (s *Server) flushWriteOp(ctx context.Context, opID, acknowledged int64) error {
	// Load by id through the prepared scan: this keeps the metadata API small
	// while recovery uses the same durable operation listing.
	ops, err := s.db.PreparedWriteOps(ctx)
	if err != nil {
		return err
	}
	var found meta.WriteOp
	for _, candidate := range ops {
		if candidate.ID == opID {
			found = candidate
			break
		}
	}
	if found.ID == 0 {
		return fmt.Errorf("write operation %d is not prepared", opID)
	}
	if err := s.finishPlannedWriteChunks(ctx, found); err != nil {
		return err
	}
	if len(found.Tail) == 0 {
		if acknowledged != found.AcknowledgedOffset {
			return fmt.Errorf("write operation %d has no pending data", opID)
		}
		return nil
	}
	rd := bytes.NewReader(found.Tail)
	chunker, err := chunkers.NewChunker("fastcdc", rd, nil)
	if err != nil {
		return err
	}
	chunks, err := s.db.WriteOpChunks(ctx, opID)
	if err != nil {
		return err
	}
	index := len(chunks)
	for {
		data, nextErr := chunker.Next()
		if nextErr != nil && nextErr != io.EOF {
			return nextErr
		}
		if len(data) > 0 {
			owned := append([]byte(nil), data...)
			vlogID, v, err := s.leasedVlogForWrite(ctx, opID, found.Path, len(owned))
			if err != nil {
				return err
			}
			hash := sha256.Sum256(owned)
			if err := s.db.AppendWriteOpChunk(ctx, opID, meta.WriteOpChunk{Index: index, Data: owned, Hash: hash[:15], VlogID: vlogID, VaddrOffset: v.Length(), LogicalLen: len(owned)}); err != nil {
				return err
			}
			if err := s.finishPlannedWriteChunks(ctx, found); err != nil {
				return err
			}
			index++
		}
		if nextErr == io.EOF {
			break
		}
	}
	return s.db.UpdateWriteOpStream(ctx, opID, acknowledged, nil)
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
		if err := s.flushWriteOp(ctx, op.ID, op.AcknowledgedOffset); err != nil {
			return nil, err
		}
		if _, err := s.db.CommitWriteOp(ctx, op.ID, time.Now().UnixNano()); err != nil {
			return nil, fmt.Errorf("publish write operation: %w", err)
		}
	}
	s.handlesMu.Lock()
	delete(s.handles, req.GetHandle())
	s.handlesMu.Unlock()
	return &pb.CloseResponse{}, nil
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
